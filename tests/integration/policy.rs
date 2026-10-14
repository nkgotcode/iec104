//! Explicit system-command policy over the public RTU API.
use std::sync::Arc;

use async_trait::async_trait;
use iec104::{
	asdu::Asdu,
	cot::Cot,
	rtu_server::{
		PointAddress, PointValue, RejectAllCommands, RejectClockSync, RtuClockSyncSystemHandler,
		RtuInitialPoint, RtuSystemHandlers, SystemCommandContext,
	},
	server::{Server, error::ServerError},
	types::{
		InformationObjects, MItNa1,
		commands::{Frz, Qrp, Rqt},
		quality_descriptors::SeqQd,
		time::Cp56Time2a,
	},
	types_id::TypeId,
};
use tokio::{sync::mpsc, time::timeout};

use crate::{
	WAIT, c_ci_activation, c_cs_activation, c_rp_activation, drain_until_m_ei, spawn_rtu_and_client,
};
fn timestamp() -> Cp56Time2a {
	Cp56Time2a {
		ms: 0,
		iv: false,
		min: 30,
		summer_time: false,
		hour: 12,
		weekday: 3,
		day: 10,
		month: 4,
		year: 26,
	}
}
async fn next(rx: &mut mpsc::UnboundedReceiver<Asdu>) -> Asdu {
	timeout(WAIT, rx.recv()).await.expect("reply deadline").expect("reply")
}
fn rejection(mut request: Asdu) -> Asdu {
	request.cot = Cot::ActivationConfirmation;
	request.negative = true;
	request
}
#[tokio::test]
async fn monitoring_policy_rejects_mutations_and_preserves_counter_values() {
	let initial = vec![RtuInitialPoint::new(
		PointAddress::new(9, 50),
		PointValue::MItNa1(MItNa1 { bcr: 99, sqd: SeqQd::default() }),
	)];
	let (rtu, mut client, mut rx) = spawn_rtu_and_client(
		initial,
		Arc::new(RejectAllCommands),
		RtuSystemHandlers::monitoring_only(),
	)
	.await;
	drain_until_m_ei(&mut rx).await;
	for mut request in [
		c_cs_activation(9, 0, timestamp()),
		c_rp_activation(9, 0, Qrp::General),
		c_ci_activation(9, 0, Rqt::ReqCoGen, Frz::Freeze),
		c_ci_activation(9, 0, Rqt::ReqCoGen, Frz::FreezeAndReset),
		c_ci_activation(9, 0, Rqt::ReqCoGen, Frz::Reset),
	] {
		request.originator_address = 42;
		request.test = true;
		client.send_asdu(request.clone()).await.expect("send request");
		assert_eq!(
			next(&mut rx).await,
			rejection(request),
			"negative confirmation echoes identity and payload"
		);
	}
	client.send_asdu(c_ci_activation(9, 0, Rqt::ReqCoGen, Frz::Read)).await.expect("counter read");
	let confirm = next(&mut rx).await;
	assert_eq!(confirm.cot, Cot::ActivationConfirmation);
	assert!(!confirm.negative);
	let data = next(&mut rx).await;
	let InformationObjects::MItNa1(objects) = data.information_objects else {
		panic!("expected counter data");
	};
	assert_eq!(objects[0].object.bcr, 99, "rejected resets cannot change counters");
	assert_eq!(next(&mut rx).await.cot, Cot::ActivationTermination);
	client.shutdown().await.expect("client shutdown");
	rtu.shutdown().await.expect("rtu shutdown");
}
struct ClockProbe(mpsc::UnboundedSender<bool>);
#[async_trait]
impl RtuClockSyncSystemHandler for ClockProbe {
	async fn handle_clock_sync(
		&self,
		ctx: &mut SystemCommandContext<'_>,
		server: &Server,
	) -> Result<(), ServerError> {
		let mut previous = timestamp();
		previous.hour = 1;
		*ctx.last_master_clock = Some(previous.clone());
		RejectClockSync.handle_clock_sync(ctx, server).await?;
		let _ = self.0.send(*ctx.last_master_clock == Some(previous));
		Ok(())
	}
}
#[tokio::test]
async fn reject_clock_does_not_replace_previously_stored_time() {
	let (tx, mut clock_rx) = mpsc::unbounded_channel();
	let handlers = RtuSystemHandlers::monitoring_only().with_clock_sync(Arc::new(ClockProbe(tx)));
	let (rtu, mut client, mut rx) =
		spawn_rtu_and_client(Vec::new(), Arc::new(RejectAllCommands), handlers).await;
	drain_until_m_ei(&mut rx).await;
	client.send_asdu(c_cs_activation(1, 0, timestamp())).await.expect("clock request");
	assert!(next(&mut rx).await.negative);
	assert_eq!(timeout(WAIT, clock_rx.recv()).await.expect("probe deadline"), Some(true));
	client.shutdown().await.expect("client shutdown");
	rtu.shutdown().await.expect("rtu shutdown");
}
#[tokio::test]
async fn empty_requests_do_not_acknowledge_or_prevent_next_valid_request() {
	let (rtu, mut client, mut rx) = spawn_rtu_and_client(
		Vec::new(),
		Arc::new(RejectAllCommands),
		RtuSystemHandlers::monitoring_only(),
	)
	.await;
	drain_until_m_ei(&mut rx).await;
	let mut clock = c_cs_activation(1, 0, timestamp());
	clock.information_objects = InformationObjects::CCsNa1(Vec::new());
	let mut reset = c_rp_activation(1, 0, Qrp::General);
	reset.information_objects = InformationObjects::CRpNa1(Vec::new());
	let mut counter = c_ci_activation(1, 0, Rqt::ReqCoGen, Frz::Reset);
	counter.information_objects = InformationObjects::CCiNa1(Vec::new());
	for request in [clock, reset, counter] {
		client.send_asdu(request).await.expect("empty request");
	}
	let mut valid = c_rp_activation(1, 0, Qrp::General);
	valid.originator_address = 63;
	client.send_asdu(valid.clone()).await.expect("valid request");
	let reply = next(&mut rx).await;
	assert_eq!(reply.type_id, TypeId::C_RP_NA_1);
	assert_eq!(reply, rejection(valid));
	client.shutdown().await.expect("client shutdown");
	rtu.shutdown().await.expect("rtu shutdown");
}
