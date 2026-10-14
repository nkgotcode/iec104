//! User-defined interpretation of incoming command ASDUs.
//!
//! The protocol defines command telegrams (e.g. single command, set-point
//! command) with a type identifier and information object address, but **not**
//! which process variables or outputs they must drive. That mapping is
//! **application / engineering** responsibility; compare IEC 60870-5-104
//! and typical RTU/SCADA integration where the same IOA may mean different
//! things per project.

use std::{collections::HashMap, future::Future, net::SocketAddr, pin::Pin, sync::Arc};

use super::model::{PointAddress, PointValue};
use crate::{asdu::Asdu, server::ConnectionId, types::InformationObjects, types_id::TypeId};

/// Read-only view passed to [`RtuCommandHandler::handle_command`].
#[derive(Debug)]
pub struct CommandContext<'a> {
	/// Current data-transfer session, cancelled on STOPDT, disconnect or
	/// shutdown.
	pub session: &'a CommandSession,
	/// Connection the ASDU arrived on (e.g. for per-master logic).
	pub connection_id: ConnectionId,
	/// Peer address (if needed for logging or policy).
	pub peer: SocketAddr,
	/// Full incoming ASDU (`type_id`, `address_field` = common address,
	/// `information_objects`, …).
	pub asdu: &'a Asdu,
	/// Immutable snapshot of monitoring points at command admission.
	pub model: &'a HashMap<PointAddress, PointValue>,
}

/// Result of handling one command ASDU: confirmation payload and optional model
/// updates. The wire [`crate::cot::Cot`] for the reply is
/// [`crate::cot::Cot::ActivationConfirmation`] or
/// [`crate::cot::Cot::DeactivationConfirmation`] depending on the incoming
/// command [`crate::cot::Cot`] (see
/// `send_command_confirmation`).
#[derive(Debug, Clone)]
pub struct CommandHandling {
	/// `true` = negative confirm (e.g. unknown command or interlock).
	pub negative: bool,
	/// Usually the same as the incoming command `type_id`.
	pub reply_type_id: TypeId,
	/// Objects mirrored in the confirmation (typically echo of the command).
	pub reply_information_objects: InformationObjects,
	/// When [`Self::negative`] is false, updates are validated as a batch before
	/// mutation. Unknown, duplicate, type-mismatched or stale targets reject
	/// the immediate command without applying any updates.
	pub apply_updates: Vec<(PointAddress, PointValue)>,
}

#[async_trait::async_trait]
#[expect(
	clippy::double_must_use,
	reason = "async-trait 0.1.89 generates redundant must_use on boxed futures"
)]
pub trait RtuCommandHandler: Send + Sync {
	/// Decide confirmation and any process-image updates for this command ASDU.
	async fn handle_command(&self, ctx: CommandContext<'_>) -> CommandHandling {
		CommandHandling::echo(ctx.asdu, true)
	}
	/// Admission only: do not actuate here. Return a lazy deferred action to
	/// start after positive ACT_CON. Legacy handlers default to immediate mode.
	async fn prepare_command(&self, ctx: CommandContext<'_>) -> CommandDispatch {
		CommandDispatch::Immediate(self.handle_command(ctx).await)
	}
	/// Authorize selection without actuation. Unsupported by default.
	async fn select(&self, _ctx: CommandContext<'_>) -> bool {
		false
	}
}

/// Always answers with **negative** activation confirmation and echoes the
/// command objects. Use when commands are not supported or until you plug in
/// real logic.
#[derive(Debug, Default, Clone, Copy)]
pub struct RejectAllCommands;

#[async_trait::async_trait]
impl RtuCommandHandler for RejectAllCommands {
	async fn handle_command(&self, ctx: CommandContext<'_>) -> CommandHandling {
		CommandHandling {
			negative: true,
			reply_type_id: ctx.asdu.type_id,
			reply_information_objects: ctx.asdu.information_objects.clone(),
			apply_updates: Vec::new(),
		}
	}
}

/// Wraps a closure as a handler (e.g. for tests or small binaries).
pub struct FnCommandHandler<F> {
	f: F,
}

impl<F> FnCommandHandler<F> {
	pub const fn new(f: F) -> Self {
		Self { f }
	}
}

#[async_trait::async_trait]
impl<F> RtuCommandHandler for FnCommandHandler<F>
where
	F: Fn(CommandContext<'_>) -> CommandHandling + Send + Sync,
{
	async fn handle_command(&self, ctx: CommandContext<'_>) -> CommandHandling {
		(self.f)(ctx)
	}
}

/// Build an [`Arc<dyn RtuCommandHandler>`] from a sync closure (no `.await`
/// inside the closure).
pub fn command_handler_from_fn(
	f: impl Fn(CommandContext<'_>) -> CommandHandling + Send + Sync + 'static,
) -> Arc<dyn RtuCommandHandler> {
	Arc::new(FnCommandHandler::new(f))
}

/// A non-forgeable data-transfer session. Cancellation is permanent: restarting
/// the same TCP connection creates a different token.
#[derive(Debug, Clone)]
pub struct CommandSession {
	active: Arc<tokio::sync::watch::Sender<bool>>,
}
impl CommandSession {
	pub(super) fn new() -> Self {
		let (tx, _) = tokio::sync::watch::channel(true);
		Self { active: Arc::new(tx) }
	}
	/// Whether this session may still accept command results.
	#[must_use]
	pub fn is_active(&self) -> bool {
		*self.active.borrow()
	}
	/// Wait until STOPDT, disconnect or shutdown cancels the session.
	pub async fn cancelled(&self) {
		let mut rx = self.active.subscribe();
		while *rx.borrow_and_update() {
			if rx.changed().await.is_err() {
				break;
			}
		}
	}
	pub(super) fn delivery_guard(&self) -> tokio::sync::watch::Receiver<bool> {
		self.active.subscribe()
	}
	pub(super) fn cancel(&self) {
		self.active.send_replace(false);
	}
	pub(super) fn same_session(&self, other: &Self) -> bool {
		Arc::ptr_eq(&self.active, &other.active)
	}
}
impl CommandHandling {
	/// Echo the request as a positive or negative confirmation, without
	/// updates.
	#[must_use]
	pub fn echo(asdu: &Asdu, negative: bool) -> Self {
		Self {
			negative,
			reply_type_id: asdu.type_id,
			reply_information_objects: asdu.information_objects.clone(),
			apply_updates: Vec::new(),
		}
	}
}
/// Completion of an accepted deferred command. Failure/timeout closes the link;
/// it never emits a positive termination or claims that an external action was
/// undone.
#[derive(Debug)]
pub enum CommandCompletion {
	/// Confirm completion with ACT_TERM and optionally update monitoring
	/// points.
	Succeeded { apply_updates: Vec<(PointAddress, PointValue)> },
	/// Execution failed or its outcome is unknown.
	Failed,
}
/// A lazy action polled only after ACT_CON has entered the connection's
/// protocol queue. Cancellation drops this future; external work must honour
/// session cancellation itself. Do not detach untracked tasks from this future.
pub struct DeferredCommand {
	pub(super) action: Pin<Box<dyn Future<Output = CommandCompletion> + Send + 'static>>,
}
impl std::fmt::Debug for DeferredCommand {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("DeferredCommand").finish_non_exhaustive()
	}
}
impl DeferredCommand {
	/// Wrap a lazy, cancellation-safe action.
	pub fn new(action: impl Future<Output = CommandCompletion> + Send + 'static) -> Self {
		Self { action: Box::pin(action) }
	}
}
/// An immediate legacy result or an accepted action with deferred completion.
#[derive(Debug)]
pub enum CommandDispatch {
	/// Existing confirmation/update behavior. Negative results never apply
	/// updates.
	Immediate(CommandHandling),
	/// Positive ACT_CON, then action, then positive ACT_TERM only on success.
	Deferred(DeferredCommand),
}
