//! Dispatch incoming command ASDUs to
//! [`super::command_handler::RtuCommandHandler`], apply returned model updates,
//! send activation or deactivation confirmation (COT matches IEC 60870-5-104).

use std::collections::HashMap;

use super::{
	command_handler::{CommandHandling, CommandSession},
	model::{PointAddress, PointValue},
	output::spontaneous_asdu,
};
use crate::{
	asdu::Asdu,
	cot::Cot,
	server::{ConnectionId, Server, error::ServerError},
	types_id::TypeId,
};

#[must_use]
const fn is_command_cot(cot: Cot) -> bool {
	matches!(cot, Cot::Request | Cot::Activation | Cot::Deactivation)
}

/// Process commands (`C_SC_*`, `C_DC_*`, `C_RC_*`, `C_SE_*`, `C_BO_*`) with
/// [`Cot::Request`], [`Cot::Activation`], or [`Cot::Deactivation`].
#[must_use]
pub(super) const fn is_process_command(asdu: &Asdu) -> bool {
	is_command_cot(asdu.cot) && is_supported_command_type(asdu.type_id)
}

#[must_use]
pub(super) const fn is_supported_command_type(type_id: TypeId) -> bool {
	matches!(
		type_id,
		TypeId::C_SC_NA_1
			| TypeId::C_DC_NA_1
			| TypeId::C_RC_NA_1
			| TypeId::C_SE_NA_1
			| TypeId::C_SE_NB_1
			| TypeId::C_SE_NC_1
			| TypeId::C_BO_NA_1
			| TypeId::C_SC_TA_1
			| TypeId::C_DC_TA_1
			| TypeId::C_RC_TA_1
			| TypeId::C_SE_TA_1
			| TypeId::C_SE_TB_1
			| TypeId::C_SE_TC_1
			| TypeId::C_BO_TA_1
	)
}

const fn command_confirmation_asdu(
	incoming: &Asdu,
	reply_cot: Cot,
	type_id: TypeId,
	information_objects: crate::types::InformationObjects,
	negative: bool,
) -> Asdu {
	Asdu {
		type_id,
		cot: reply_cot,
		originator_address: incoming.originator_address,
		address_field: incoming.address_field,
		sequence: incoming.sequence,
		test: incoming.test,
		negative,
		information_objects,
	}
}

#[must_use]
const fn command_reply_cot(incoming_cot: Cot) -> Cot {
	match incoming_cot {
		Cot::Deactivation => Cot::DeactivationConfirmation,
		_ => Cot::ActivationConfirmation,
	}
}

pub(super) async fn send_command_confirmation(
	server: &Server,
	connection_id: ConnectionId,
	incoming: &Asdu,
	type_id: TypeId,
	objs: crate::types::InformationObjects,
	negative: bool,
) -> Result<(), ServerError> {
	let reply_cot = command_reply_cot(incoming.cot);
	let reply = command_confirmation_asdu(incoming, reply_cot, type_id, objs, negative);
	server.send_asdu(connection_id, reply).await
}

pub(super) async fn confirm_guarded(
	server: &Server,
	id: ConnectionId,
	asdu: &Asdu,
	session: &CommandSession,
	handling: &CommandHandling,
) -> Result<(), ServerError> {
	server
		.send_asdu_guarded(
			id,
			command_confirmation_asdu(
				asdu,
				command_reply_cot(asdu.cot),
				handling.reply_type_id,
				handling.reply_information_objects.clone(),
				handling.negative,
			),
			session.delivery_guard(),
		)
		.await
}

fn can_apply_update(
	model: &HashMap<PointAddress, PointValue>,
	addr: PointAddress,
	val: &PointValue,
) -> bool {
	model.get(&addr).is_some_and(|ex| ex.type_id() == val.type_id())
}

pub(super) async fn apply_model_updates(
	model: &mut HashMap<PointAddress, PointValue>,
	server: &Server,
	updates: &[(PointAddress, PointValue)],
) -> Result<(), ServerError> {
	for (addr, val) in updates {
		model.insert(*addr, val.clone());
	}
	let mut result = Ok(());
	for (addr, val) in updates {
		if let Err(error) = server.broadcast_asdu(spontaneous_asdu(*addr, val)).await
			&& result.is_ok()
		{
			result = Err(error);
		}
	}
	result
}

pub(super) fn updates_valid(
	model: &HashMap<PointAddress, PointValue>,
	updates: &[(PointAddress, PointValue)],
) -> bool {
	let mut seen = std::collections::HashSet::new();
	updates
		.iter()
		.all(|(address, value)| seen.insert(*address) && can_apply_update(model, *address, value))
}

pub(super) async fn apply_handling(
	model: &mut HashMap<PointAddress, PointValue>,
	server: &Server,
	asdu: &Asdu,
	id: ConnectionId,
	session: &CommandSession,
	mut handling: CommandHandling,
) -> Result<(), ServerError> {
	if !handling.negative && !updates_valid(model, &handling.apply_updates) {
		handling = CommandHandling::echo(asdu, true);
	}
	if !handling.negative {
		// Partial broadcast failure does not suppress the originating command
		// reply.
		if let Err(error) = apply_model_updates(model, server, &handling.apply_updates).await {
			tracing::debug!(%error,"Command monitoring delivery failed");
		}
	}
	confirm_guarded(server, id, asdu, session, &handling).await
}

pub(super) async fn send_command_termination(
	server: &Server,
	id: ConnectionId,
	asdu: &Asdu,
	session: &CommandSession,
) -> Result<(), ServerError> {
	server
		.send_asdu_guarded(
			id,
			command_confirmation_asdu(
				asdu,
				Cot::ActivationTermination,
				asdu.type_id,
				asdu.information_objects.clone(),
				false,
			),
			session.delivery_guard(),
		)
		.await
}

#[cfg(test)]
mod tests {
	use super::command_reply_cot;
	use crate::cot::Cot;

	#[test]
	fn command_reply_cot_matches_incoming() {
		assert_eq!(command_reply_cot(Cot::Activation), Cot::ActivationConfirmation);
		assert_eq!(command_reply_cot(Cot::Request), Cot::ActivationConfirmation);
		assert_eq!(command_reply_cot(Cot::Deactivation), Cot::DeactivationConfirmation);
	}
}
