//! Real-wire command admission, completion and selection policies.
#![allow(missing_docs, clippy::expect_used)]
use std::{
	net::SocketAddr,
	sync::{
		Arc,
		atomic::{AtomicUsize, Ordering},
	},
	time::Duration,
};

use async_trait::async_trait;
use iec104::{
	config::ServerConfig,
	rtu_server::{
		CommandCompletion, CommandContext, CommandDispatch, CommandHandling, CommandSession,
		DeferredCommand, PointAddress, PointValue, RtuCommandHandler, RtuInitialPoint,
		RtuServerHandle, RtuSystemHandlers, SelectBeforeExecute, start_rtu_server_ephemeral,
	},
	types::{MMeNc1, quality_descriptors::Qds},
};
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::TcpStream,
	sync::{Semaphore, mpsc},
	time::timeout,
};
const WAIT: Duration = Duration::from_secs(3);
struct Peer {
	socket: TcpStream,
	sent: u16,
	received: u16,
}
impl Peer {
	async fn connect(addr: SocketAddr) -> Self {
		let mut peer =
			Self { socket: TcpStream::connect(addr).await.expect("connect"), sent: 0, received: 0 };
		peer.u(7).await;
		assert_eq!(peer.frame().await, [0x68, 4, 11, 0, 0, 0]);
		assert_eq!(peer.asdu().await[0], 70);
		peer
	}
	async fn u(&mut self, control: u8) {
		self.socket.write_all(&[0x68, 4, control, 0, 0, 0]).await.expect("U frame");
	}
	async fn frame(&mut self) -> Vec<u8> {
		timeout(WAIT, async {
			let mut head = [0; 2];
			self.socket.read_exact(&mut head).await.expect("header");
			let mut frame = vec![0; usize::from(head[1]) + 2];
			frame[..2].copy_from_slice(&head);
			self.socket.read_exact(&mut frame[2..]).await.expect("body");
			frame
		})
		.await
		.expect("frame deadline")
	}
	async fn asdu(&mut self) -> Vec<u8> {
		loop {
			let frame = self.frame().await;
			if frame[2] & 1 != 0 {
				continue;
			}
			assert_eq!(u16::from_le_bytes([frame[2], frame[3]]) >> 1, self.received);
			self.received = (self.received + 1) % 32768;
			let nr = (self.received << 1).to_le_bytes();
			self.socket.write_all(&[0x68, 4, 1, 0, nr[0], nr[1]]).await.expect("ack");
			return frame[6..].to_vec();
		}
	}
	async fn send(&mut self, tid: u8, cot: u8, ioa: u8, value: u8) {
		let ns = (self.sent << 1).to_le_bytes();
		let nr = (self.received << 1).to_le_bytes();
		self.sent = (self.sent + 1) % 32768;
		self.socket
			.write_all(&[
				0x68, 14, ns[0], ns[1], nr[0], nr[1], tid, 1, cot, 0, 1, 0, ioa, 0, 0, value,
			])
			.await
			.expect("ASDU");
	}
	async fn command(&mut self, ioa: u8, value: u8, negative: bool) {
		self.send(45, 6, ioa, value).await;
		let reply = self.asdu().await;
		assert_eq!(
			(reply[0], reply[2], reply[6], reply[9]),
			(45, if negative { 71 } else { 7 }, ioa, value)
		);
	}
	async fn gi(&mut self) {
		self.send(100, 6, 0, 20).await;
		loop {
			let asdu = self.asdu().await;
			assert_ne!(asdu[0], 45, "stale command reply in new session");
			if asdu[0] == 100 && asdu[2] == 10 {
				break;
			}
		}
	}
	async fn stop_restart(&mut self) {
		self.u(0x13).await;
		loop {
			let frame = self.frame().await;
			if frame[2] == 0x23 {
				break;
			}
			assert_eq!(frame[2], 1, "only an S ack can precede STOPDT-CON");
		}
		self.u(7).await;
		assert_eq!(self.frame().await, [0x68, 4, 11, 0, 0, 0]);
	}
}
struct DropCount(Arc<AtomicUsize>);
impl Drop for DropCount {
	fn drop(&mut self) {
		self.0.fetch_add(1, Ordering::SeqCst);
	}
}
struct Actuator {
	calls: Arc<AtomicUsize>,
	finished: Arc<AtomicUsize>,
	dropped: Arc<AtomicUsize>,
	release: Arc<Semaphore>,
	started: mpsc::UnboundedSender<CommandSession>,
	reject: bool,
}
#[async_trait]
impl RtuCommandHandler for Actuator {
	async fn select(&self, _: CommandContext<'_>) -> bool {
		!self.reject
	}
	async fn prepare_command(&self, ctx: CommandContext<'_>) -> CommandDispatch {
		if self.reject {
			return CommandDispatch::Immediate(CommandHandling::echo(ctx.asdu, true));
		}
		let calls = self.calls.clone();
		let finished = self.finished.clone();
		let dropped = self.dropped.clone();
		let release = self.release.clone();
		let started = self.started.clone();
		let session = ctx.session.clone();
		CommandDispatch::Deferred(DeferredCommand::new(async move {
			let _drop = DropCount(dropped);
			calls.fetch_add(1, Ordering::SeqCst);
			started.send(session).expect("start receiver");
			let _permit = release.acquire_owned().await.expect("release");
			finished.fetch_add(1, Ordering::SeqCst);
			CommandCompletion::Succeeded { apply_updates: Vec::new() }
		}))
	}
}
struct Fixture {
	rtu: RtuServerHandle,
	addr: SocketAddr,
	actuator: Arc<Actuator>,
	started: mpsc::UnboundedReceiver<CommandSession>,
}
async fn fixture(select: bool, reject: bool, deadline: Duration) -> Fixture {
	let (tx, rx) = mpsc::unbounded_channel();
	let actuator = Arc::new(Actuator {
		calls: Arc::new(AtomicUsize::new(0)),
		finished: Arc::new(AtomicUsize::new(0)),
		dropped: Arc::new(AtomicUsize::new(0)),
		release: Arc::new(Semaphore::new(0)),
		started: tx,
		reject,
	});
	let handler: Arc<dyn RtuCommandHandler> = if select {
		Arc::new(
			SelectBeforeExecute::new(actuator.clone(), Duration::from_secs(1), 8)
				.expect("selection"),
		)
	} else {
		actuator.clone()
	};
	let (rtu, addr) = start_rtu_server_ephemeral(
		ServerConfig { command_timeout: deadline, ..ServerConfig::default() },
		Vec::<RtuInitialPoint>::new(),
		handler,
		RtuSystemHandlers::monitoring_only(),
	)
	.await
	.expect("rtu");
	Fixture { rtu, addr, actuator, started: rx }
}
async fn count_reaches(count: &AtomicUsize, expected: usize) {
	timeout(WAIT, async {
		while count.load(Ordering::SeqCst) != expected {
			tokio::task::yield_now().await;
		}
	})
	.await
	.expect("count deadline");
}
#[tokio::test]
async fn deferred_command_orders_replies_and_keeps_model_and_gi_responsive() {
	let mut f = fixture(false, false, WAIT).await;
	let mut peer = Peer::connect(f.addr).await;
	peer.command(1, 1, false).await;
	timeout(WAIT, f.started.recv()).await.expect("action starts").expect("session");
	timeout(
		WAIT,
		f.rtu.register_point(
			PointAddress::new(1, 10),
			PointValue::MMeNc1(MMeNc1 { value: 4.0, qds: Qds::default() }),
		),
	)
	.await
	.expect("actor responsive")
	.expect("register");
	peer.gi().await;
	assert_eq!(f.actuator.finished.load(Ordering::SeqCst), 0);
	f.actuator.release.add_permits(1);
	let term = peer.asdu().await;
	assert_eq!((term[0], term[2]), (45, 10));
	assert_eq!(f.actuator.calls.load(Ordering::SeqCst), 1);
	f.rtu.shutdown().await.expect("shutdown");
}
#[tokio::test]
async fn rejected_and_test_commands_never_actuate() {
	for reject in [false, true] {
		let f = fixture(false, reject, WAIT).await;
		let mut peer = Peer::connect(f.addr).await;
		peer.send(45, if reject { 6 } else { 134 }, 1, 1).await;
		let reply = peer.asdu().await;
		assert_eq!(reply[2] & 0x7f, 71);
		assert_eq!(f.actuator.calls.load(Ordering::SeqCst), 0);
		f.rtu.shutdown().await.expect("shutdown");
	}
}
#[tokio::test]
async fn selection_matches_owner_value_and_ioa_and_execute_is_one_use() {
	let mut f = fixture(true, false, WAIT).await;
	let mut a = Peer::connect(f.addr).await;
	let mut b = Peer::connect(f.addr).await;
	a.command(1, 0x81, false).await;
	assert_eq!(f.actuator.calls.load(Ordering::SeqCst), 0);
	b.command(1, 1, true).await;
	a.command(1, 0, true).await;
	a.command(2, 1, true).await;
	a.command(1, 1, false).await;
	f.started.recv().await.expect("started");
	f.actuator.release.add_permits(1);
	assert_eq!(a.asdu().await[2], 10);
	a.command(1, 1, true).await;
	assert_eq!(f.actuator.calls.load(Ordering::SeqCst), 1);
	f.rtu.shutdown().await.expect("shutdown");
}
#[tokio::test]
async fn selection_expires_and_is_cancelled_by_stop_disconnect_and_deactivation() {
	let f = fixture(true, false, WAIT).await;
	let mut a = Peer::connect(f.addr).await;
	a.command(1, 0x81, false).await;
	tokio::time::sleep(Duration::from_millis(1100)).await;
	a.command(1, 1, true).await;
	a.command(1, 0x81, false).await;
	a.stop_restart().await;
	a.command(1, 1, true).await;
	a.command(1, 0x81, false).await;
	a.send(45, 8, 1, 0x81).await;
	assert_eq!(a.asdu().await[2], 9);
	a.command(1, 1, true).await;
	a.command(1, 0x81, false).await;
	drop(a);
	let mut b = Peer::connect(f.addr).await;
	b.command(1, 1, true).await;
	assert_eq!(f.actuator.calls.load(Ordering::SeqCst), 0);
	f.rtu.shutdown().await.expect("shutdown");
}
#[tokio::test]
async fn stop_cancels_pending_action_and_prevents_completion_in_restarted_session() {
	let mut f = fixture(false, false, WAIT).await;
	let mut peer = Peer::connect(f.addr).await;
	peer.command(1, 1, false).await;
	let session = f.started.recv().await.expect("started");
	peer.stop_restart().await;
	assert!(!session.is_active());
	count_reaches(&f.actuator.dropped, 1).await;
	f.actuator.release.add_permits(1);
	peer.gi().await;
	assert_eq!(f.actuator.finished.load(Ordering::SeqCst), 0);
	f.rtu.shutdown().await.expect("shutdown");
}
#[tokio::test]
async fn disconnect_and_shutdown_cancel_and_join_pending_actions() {
	for shutdown in [false, true] {
		let mut f = fixture(false, false, WAIT).await;
		let mut peer = Peer::connect(f.addr).await;
		peer.command(1, 1, false).await;
		let session = f.started.recv().await.expect("started");
		if shutdown {
			f.rtu.shutdown().await.expect("shutdown");
		} else {
			drop(peer);
		}
		timeout(WAIT, session.cancelled()).await.expect("session cancelled");
		count_reaches(&f.actuator.dropped, 1).await;
		f.actuator.release.add_permits(1);
		assert_eq!(f.actuator.finished.load(Ordering::SeqCst), 0);
		f.rtu.shutdown().await.expect("shutdown");
	}
}
#[tokio::test]
async fn completion_timeout_closes_without_false_positive_termination() {
	let mut f = fixture(false, false, Duration::from_millis(150)).await;
	let mut peer = Peer::connect(f.addr).await;
	peer.command(1, 1, false).await;
	let session = f.started.recv().await.expect("started");
	let mut data = [0; 16];
	assert_eq!(
		timeout(WAIT, peer.socket.read(&mut data)).await.expect("deadline").expect("closed"),
		0
	);
	assert!(!session.is_active());
	assert_eq!(f.actuator.finished.load(Ordering::SeqCst), 0);
	count_reaches(&f.actuator.dropped, 1).await;
	f.rtu.shutdown().await.expect("shutdown");
}
#[tokio::test]
async fn duplicate_while_running_is_rejected_without_starting_another_action() {
	let mut f = fixture(false, false, WAIT).await;
	let mut peer = Peer::connect(f.addr).await;
	peer.command(1, 1, false).await;
	f.started.recv().await.expect("started");
	peer.command(1, 1, true).await;
	assert_eq!(f.actuator.calls.load(Ordering::SeqCst), 1);
	f.rtu.shutdown().await.expect("shutdown");
}

struct Fault(u8);
#[async_trait]
impl RtuCommandHandler for Fault {
	async fn prepare_command(&self, _: CommandContext<'_>) -> CommandDispatch {
		let mode = self.0;
		CommandDispatch::Deferred(DeferredCommand::new(async move {
			match mode {
				0 => CommandCompletion::Failed,
				1 => panic!("test actuator panic"),
				_ => CommandCompletion::Succeeded {
					apply_updates: vec![(
						PointAddress::new(1, 99),
						PointValue::MMeNc1(MMeNc1 { value: 1.0, qds: Qds::default() }),
					)],
				},
			}
		}))
	}
}
#[tokio::test]
async fn failed_panicking_or_invalid_completion_never_emits_success_termination() {
	for mode in 0..3 {
		let (rtu, addr) = start_rtu_server_ephemeral(
			ServerConfig::default(),
			Vec::<RtuInitialPoint>::new(),
			Arc::new(Fault(mode)),
			RtuSystemHandlers::monitoring_only(),
		)
		.await
		.expect("rtu");
		let mut peer = Peer::connect(addr).await;
		peer.command(1, 1, false).await;
		let mut data = [0; 16];
		let result = timeout(WAIT, peer.socket.read(&mut data)).await.expect("close deadline");
		assert!(matches!(result, Ok(0) | Err(_)), "no positive ACT_TERM after failed completion");
		rtu.shutdown().await.expect("shutdown");
	}
}
