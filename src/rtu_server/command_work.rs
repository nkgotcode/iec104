//! Bounded command futures owned and joined by the RTU actor.
use std::{
	collections::{HashMap, HashSet},
	net::SocketAddr,
	sync::Arc,
	time::Duration,
};

use tokio::task::{Id, JoinSet};

use super::{
	CommandCompletion, CommandContext, CommandDispatch, CommandHandling, CommandSession,
	PointAddress, PointValue, RtuCommandHandler, commands, selection::selection_key,
};
use crate::{
	asdu::Asdu,
	cot::Cot,
	server::{ConnectionId, Server},
};

struct Request {
	asdu: Asdu,
	id: ConnectionId,
	peer: SocketAddr,
	session: CommandSession,
	snapshot: HashMap<PointAddress, PointValue>,
}
enum ResultKind {
	Prepared(CommandDispatch),
	Completed(CommandCompletion),
	Cancelled,
}
pub(super) struct Outcome {
	request: Request,
	result: ResultKind,
}
pub(super) struct CommandWork {
	tasks: JoinSet<Outcome>,
	ids: HashMap<Id, (ConnectionId, CommandSession)>,
	busy: HashSet<ConnectionId>,
	capacity: usize,
	timeout: Duration,
	owner: Id,
}
impl CommandWork {
	pub(super) fn new(capacity: usize, timeout: Duration) -> Self {
		Self {
			tasks: JoinSet::new(),
			ids: HashMap::new(),
			busy: HashSet::new(),
			capacity,
			timeout,
			owner: tokio::task::id(),
		}
	}
	pub(super) fn is_empty(&self) -> bool {
		self.tasks.is_empty()
	}
	pub(super) async fn next(&mut self) -> Option<Result<(Id, Outcome), tokio::task::JoinError>> {
		self.tasks.join_next_with_id().await
	}
	pub(super) async fn shutdown(&mut self) {
		self.tasks.shutdown().await;
	}
	#[allow(clippy::too_many_arguments)]
	pub(super) async fn admit(
		&mut self,
		server: &Server,
		handler: Arc<dyn RtuCommandHandler>,
		model: &HashMap<PointAddress, PointValue>,
		asdu: Asdu,
		id: ConnectionId,
		peer: SocketAddr,
		session: CommandSession,
	) {
		if !session.is_active() {
			return;
		}
		if self.tasks.len() >= self.capacity
			|| self.busy.contains(&id)
			|| asdu.test
			|| asdu.negative
			|| asdu.information_objects.len() != 1
			|| asdu.cot == Cot::Request
		{
			let _ = commands::confirm_guarded(
				server,
				id,
				&asdu,
				&session,
				&CommandHandling::echo(&asdu, true),
			)
			.await;
			return;
		}
		self.busy.insert(id);
		let cancellation = session.clone();
		let request = Request { asdu, id, peer, session, snapshot: model.clone() };
		let deadline = self.timeout;
		let task=self.tasks.spawn(super::COMMAND_OWNER.scope(self.owner,async move {
            let ctx=CommandContext{connection_id:request.id,peer:request.peer,asdu:&request.asdu,model:&request.snapshot,session:&request.session};
            let prepare=async {
                if selection_key(&request.asdu).is_some_and(|(_,select)|select) && request.asdu.cot==Cot::Activation {
                    let accepted=handler.select(ctx).await;
                    CommandDispatch::Immediate(CommandHandling::echo(&request.asdu,!accepted))
                }else{handler.prepare_command(ctx).await}
            };
            let result=tokio::select!{biased;
                _=request.session.cancelled()=>ResultKind::Cancelled,
                result=tokio::time::timeout(deadline,prepare)=>ResultKind::Prepared(result.unwrap_or_else(|_|CommandDispatch::Immediate(CommandHandling::echo(&request.asdu,true)))),
            };
            Outcome{request,result}
        }));
		self.ids.insert(task.id(), (id, cancellation));
	}
	pub(super) async fn finish(
		&mut self,
		result: Result<(Id, Outcome), tokio::task::JoinError>,
		server: &Server,
		model: &mut HashMap<PointAddress, PointValue>,
	) {
		let (task_id, Outcome { request, result }) = match result {
			Ok(result) => result,
			Err(error) => {
				if let Some((id, session)) = self.ids.remove(&error.id()) {
					session.cancel();
					self.busy.remove(&id);
					let _ = server.disconnect(id).await;
				}
				return;
			}
		};
		self.ids.remove(&task_id);
		if !request.session.is_active() {
			self.busy.remove(&request.id);
			return;
		}
		match result {
			ResultKind::Cancelled => {}
			ResultKind::Prepared(CommandDispatch::Immediate(mut handling)) => {
				if !handling.negative
					&& handling.apply_updates.iter().any(|(address, value)| {
						!model.get(address).is_some_and(|current| {
							current.type_id() == value.type_id()
								&& request
									.snapshot
									.get(address)
									.is_some_and(|snapshot| snapshot.same_snapshot(current))
						})
					}) {
					handling = CommandHandling::echo(&request.asdu, true);
				}
				if let Err(error) = commands::apply_handling(
					model,
					server,
					&request.asdu,
					request.id,
					&request.session,
					handling,
				)
				.await
				{
					tracing::debug!(%error,"Immediate command reply failed");
				}
			}
			ResultKind::Prepared(CommandDispatch::Deferred(command)) => {
				if request.asdu.cot != Cot::Activation
					|| selection_key(&request.asdu).is_some_and(|(_, select)| select)
				{
					let _ = commands::confirm_guarded(
						server,
						request.id,
						&request.asdu,
						&request.session,
						&CommandHandling::echo(&request.asdu, true),
					)
					.await;
				} else if commands::confirm_guarded(
					server,
					request.id,
					&request.asdu,
					&request.session,
					&CommandHandling::echo(&request.asdu, false),
				)
				.await
				.is_ok()
				{
					let timeout = self.timeout;
					let id = request.id;
					let cancellation = request.session.clone();
					let task=self.tasks.spawn(super::COMMAND_OWNER.scope(self.owner,async move {
                        let result=tokio::select!{biased;
                            _=request.session.cancelled()=>ResultKind::Cancelled,
                            result=tokio::time::timeout(timeout,command.action)=>ResultKind::Completed(result.unwrap_or(CommandCompletion::Failed)),
                        };
                        Outcome{request,result}
                    }));
					self.ids.insert(task.id(), (id, cancellation));
					return;
				}
			}
			ResultKind::Completed(completion) => {
				Self::complete(server, model, &request, completion).await;
			}
		}
		self.busy.remove(&request.id);
	}
	async fn complete(
		server: &Server,
		model: &mut HashMap<PointAddress, PointValue>,
		request: &Request,
		completion: CommandCompletion,
	) {
		match completion {
			CommandCompletion::Succeeded { apply_updates }
				if commands::updates_valid(model, &apply_updates) =>
			{
				// Apply all values before broadcasting, so output
				// failure cannot partially mutate the model.
				if let Err(error) =
					commands::apply_model_updates(model, server, &apply_updates).await
				{
					tracing::debug!(%error,"Completion monitoring delivery failed");
				}
				if request.session.is_active() {
					let _ = commands::send_command_termination(
						server,
						request.id,
						&request.asdu,
						&request.session,
					)
					.await;
				}
			}
			_ => {
				request.session.cancel();
				let _ = server.disconnect(request.id).await;
			}
		}
	}
}
