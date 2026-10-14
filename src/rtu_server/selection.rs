//! Optional, bounded select-before-execute policy.
use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::time::Instant;

use super::{
	CommandContext, CommandDispatch, CommandHandling, CommandSession, PointAddress,
	RtuCommandHandler,
};
use crate::{asdu::Asdu, cot::Cot, error::Error, types::InformationObjects};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct CommandKey {
	address: PointAddress,
	payload: Vec<u8>,
}
/// Decode one supported selection-capable command, retaining the complete
/// qualifier, timestamp and originator identity. Only the S/E bit is
/// normalized.
pub(super) fn selection_key(asdu: &Asdu) -> Option<(CommandKey, bool)> {
	use InformationObjects as IO;
	if asdu.test || asdu.negative || asdu.information_objects.len() != 1 {
		return None;
	}
	let qualifier = match &asdu.information_objects {
		IO::CScNa1(_)
		| IO::CScTa1(_)
		| IO::CdcNa1(_)
		| IO::CdcTa1(_)
		| IO::CrcNa1(_)
		| IO::CrcTa1(_) => 3,
		IO::CSeNa1(_) | IO::CSeTa1(_) | IO::CSeNb1(_) | IO::CSeTb1(_) => 5,
		IO::CSeNc1(_) | IO::CSeTc1(_) => 7,
		_ => return None,
	};
	let mut payload = Vec::new();
	asdu.information_objects.to_bytes(&mut payload).ok()?;
	let byte = payload.get_mut(qualifier)?;
	let selected = *byte & 0x80 != 0;
	*byte &= 0x7f;
	let address = PointAddress::new(
		asdu.address_field,
		u32::from_le_bytes([payload[0], payload[1], payload[2], 0]),
	);
	payload.push(asdu.type_id as u8);
	payload.push(asdu.originator_address);
	Some((CommandKey { address, payload }, selected))
}
#[derive(Debug)]
struct Reservation {
	key: CommandKey,
	session: CommandSession,
	expires: Instant,
}

/// Bounded, one-use reservations. Values, qualifiers, timestamps, type, common
/// address, IOA and originator must match. STOPDT or disconnect invalidates
/// them.
#[derive(Debug)]
pub struct SelectionState {
	entries: HashMap<PointAddress, Reservation>,
	timeout: Duration,
	capacity: usize,
}
impl SelectionState {
	/// Reject zero timeout/capacity. Expired or cancelled entries are pruned on
	/// use.
	pub fn new(timeout: Duration, capacity: usize) -> Result<Self, Error> {
		if timeout.is_zero() || capacity == 0 {
			snafu::whatever!("Selection timeout and capacity must be nonzero");
		}
		Ok(Self { entries: HashMap::new(), timeout, capacity })
	}
	fn prune(&mut self) {
		let now = Instant::now();
		self.entries.retain(|_, entry| entry.expires > now && entry.session.is_active());
	}
	/// Reserve one select command. An existing reservation cannot be
	/// overwritten.
	pub fn select(&mut self, session: &CommandSession, asdu: &Asdu) -> bool {
		self.prune();
		let Some((key, true)) = selection_key(asdu) else {
			return false;
		};
		if asdu.cot != Cot::Activation
			|| !session.is_active()
			|| self.entries.len() >= self.capacity
			|| self.entries.contains_key(&key.address)
		{
			return false;
		}
		self.entries.insert(
			key.address,
			Reservation { key, session: session.clone(), expires: Instant::now() + self.timeout },
		);
		true
	}
	/// Consume a matching execute exactly once. Mismatches leave the
	/// reservation.
	pub fn execute(&mut self, session: &CommandSession, asdu: &Asdu) -> bool {
		if asdu.cot != Cot::Activation {
			return false;
		}
		self.consume(session, asdu, false)
	}
	/// Cancel a matching selection; no command handler or action is invoked.
	pub fn cancel(&mut self, session: &CommandSession, asdu: &Asdu) -> bool {
		if asdu.cot != Cot::Deactivation {
			return false;
		}
		self.consume(session, asdu, true)
	}
	fn consume(&mut self, session: &CommandSession, asdu: &Asdu, allow_select: bool) -> bool {
		self.prune();
		let Some((key, select)) = selection_key(asdu) else {
			return false;
		};
		if select && !allow_select {
			return false;
		}
		if self
			.entries
			.get(&key.address)
			.is_some_and(|entry| entry.key == key && entry.session.same_session(session))
		{
			self.entries.remove(&key.address);
			true
		} else {
			false
		}
	}
}

/// Require a valid selection before invoking an inner execution handler.
/// Bitstring commands have no S/E field and are rejected by this policy.
/// The inner handler must explicitly implement `select` to authorize selection.
pub struct SelectBeforeExecute {
	inner: Arc<dyn RtuCommandHandler>,
	state: tokio::sync::Mutex<SelectionState>,
}
impl std::fmt::Debug for SelectBeforeExecute {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("SelectBeforeExecute").finish_non_exhaustive()
	}
}
impl SelectBeforeExecute {
	/// Construct a selection policy with explicit lifetime and capacity limits.
	pub fn new(
		inner: Arc<dyn RtuCommandHandler>,
		timeout: Duration,
		capacity: usize,
	) -> Result<Self, Error> {
		Ok(Self { inner, state: tokio::sync::Mutex::new(SelectionState::new(timeout, capacity)?) })
	}
}
#[async_trait::async_trait]
impl RtuCommandHandler for SelectBeforeExecute {
	async fn select(&self, ctx: CommandContext<'_>) -> bool {
		let session = ctx.session.clone();
		let asdu = ctx.asdu.clone();
		if !self.inner.select(ctx).await {
			return false;
		}
		self.state.lock().await.select(&session, &asdu)
	}
	async fn prepare_command(&self, ctx: CommandContext<'_>) -> CommandDispatch {
		if ctx.asdu.cot == Cot::Deactivation {
			let accepted = self.state.lock().await.cancel(ctx.session, ctx.asdu);
			return CommandDispatch::Immediate(CommandHandling::echo(ctx.asdu, !accepted));
		}
		if !self.state.lock().await.execute(ctx.session, ctx.asdu) {
			return CommandDispatch::Immediate(CommandHandling::echo(ctx.asdu, true));
		}
		self.inner.prepare_command(ctx).await
	}
}
