//! Bounded admission and isolation over real TCP sockets.
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
	asdu::Asdu,
	config::ServerConfig,
	rtu_server::{
		RejectAllCommands, RtuHandleError, RtuInitialPoint, RtuResetProcessSystemHandler,
		RtuSystemHandlers, SystemCommandContext, start_rtu_server_ephemeral,
	},
	server::{ConnectionId, Server, ServerCallback, error::ServerError},
};
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::TcpStream,
	sync::{Notify, mpsc},
	time::timeout,
};
const WAIT: Duration = Duration::from_secs(3);
async fn started(addr: SocketAddr) -> TcpStream {
	let mut peer = TcpStream::connect(addr).await.expect("connect");
	peer.write_all(&[0x68, 4, 7, 0, 0, 0]).await.expect("start");
	assert_eq!(frame(&mut peer).await, [0x68, 4, 11, 0, 0, 0]);
	peer
}
async fn frame(peer: &mut TcpStream) -> Vec<u8> {
	timeout(WAIT, async {
		let mut header = [0; 2];
		peer.read_exact(&mut header).await.expect("header");
		let mut result = vec![0; usize::from(header[1]) + 2];
		result[..2].copy_from_slice(&header);
		peer.read_exact(&mut result[2..]).await.expect("body");
		result
	})
	.await
	.expect("frame deadline")
}
fn sample() -> Asdu {
	Asdu::parse(&[1, 1, 3, 0, 1, 0, 1, 0, 0, 1]).expect("sample")
}
struct BlockFirst {
	count: AtomicUsize,
	entered: mpsc::UnboundedSender<ConnectionId>,
}
#[async_trait]
impl ServerCallback for BlockFirst {
	async fn on_new_objects(&self, _: Asdu, _: ConnectionId, _: SocketAddr) {}
	async fn on_connection_started(&self, id: ConnectionId, _: SocketAddr) {
		let first = self.count.fetch_add(1, Ordering::Relaxed) == 0;
		self.entered.send(id).expect("test receiver");
		if first {
			std::future::pending::<()>().await;
		}
	}
}
#[tokio::test]
async fn blocked_peer_does_not_delay_healthy_broadcast_or_accept() {
	let (tx, mut rx) = mpsc::unbounded_channel();
	let (server, addr) = Server::start_with_listen_addr(
		ServerConfig {
			port: 0,
			delivery_timeout: Duration::from_millis(700),
			max_connections: 2,
			..ServerConfig::default()
		},
		BlockFirst { count: AtomicUsize::new(0), entered: tx },
	)
	.await
	.expect("server");
	let mut blocked = started(addr).await;
	let _ = rx.recv().await.expect("first callback");
	let mut healthy = started(addr).await;
	let _ = rx.recv().await.expect("second callback");
	let sender = server.clone();
	let delivery = tokio::spawn(async move { sender.broadcast_asdu(sample()).await });
	let received = timeout(Duration::from_millis(400), frame(&mut healthy))
		.await
		.expect("healthy peer delivered before blocked deadline");
	assert_eq!(&received[6..], &[1, 1, 3, 0, 1, 0, 1, 0, 0, 1]);
	assert!(matches!(delivery.await.expect("task"), Err(ServerError::DeliveryTimedOut)));
	let mut byte = [0];
	assert_eq!(
		timeout(WAIT, blocked.read(&mut byte)).await.expect("closed deadline").expect("closed"),
		0
	);
	// The aborted peer must release its admission slot.
	timeout(WAIT, async {
		loop {
			let mut peer = TcpStream::connect(addr).await.expect("connect");
			if peer.write_all(&[0x68, 4, 7, 0, 0, 0]).await.is_ok() {
				let mut reply = [0; 6];
				if peer.read_exact(&mut reply).await.is_ok() {
					assert_eq!(reply, [0x68, 4, 11, 0, 0, 0]);
					break;
				}
			}
			tokio::task::yield_now().await;
		}
	})
	.await
	.expect("capacity reclaimed");
	server.shutdown().await.expect("shutdown");
}
struct BlockCommand {
	entered: Notify,
}
#[async_trait]
impl RtuResetProcessSystemHandler for BlockCommand {
	async fn handle_reset_process(
		&self,
		_: &mut SystemCommandContext<'_>,
		_: &Server,
	) -> Result<(), ServerError> {
		self.entered.notify_one();
		std::future::pending().await
	}
}
#[tokio::test]
async fn full_rtu_mailbox_rejects_local_work_and_closes_ingress_peer() {
	let handler = Arc::new(BlockCommand { entered: Notify::new() });
	let (rtu, addr) = start_rtu_server_ephemeral(
		ServerConfig { rtu_queue_capacity: 2, ..ServerConfig::default() },
		Vec::<RtuInitialPoint>::new(),
		Arc::new(RejectAllCommands),
		RtuSystemHandlers { reset_process: handler.clone(), ..RtuSystemHandlers::default() },
	)
	.await
	.expect("rtu");
	let mut peer = started(addr).await;
	let init = frame(&mut peer).await;
	assert_eq!(init[6], 70);
	peer.write_all(&[0x68, 14, 0, 0, 2, 0, 105, 1, 6, 0, 1, 0, 1, 0, 0, 1])
		.await
		.expect("first command");
	timeout(WAIT, handler.entered.notified()).await.expect("handler entered");
	for sequence in 1..=3 {
		peer.write_all(&[0x68, 14, sequence * 2, 0, 2, 0, 105, 1, 6, 0, 1, 0, 1, 0, 0, 1])
			.await
			.expect("queued command");
	}
	timeout(WAIT, async {
		let mut buf = [0; 128];
		loop {
			match peer.read(&mut buf).await {
				Ok(0) | Err(_) => break,
				Ok(_) => {}
			}
		}
	})
	.await
	.expect("overloaded peer closes");
	assert!(matches!(rtu.unregister_all().await, Err(RtuHandleError::Overloaded)));
	timeout(WAIT, rtu.shutdown()).await.expect("shutdown deadline").expect("shutdown");
}
#[tokio::test]
async fn invalid_queue_configuration_is_rejected() {
	for config in [
		ServerConfig { command_capacity: 0, ..ServerConfig::default() },
		ServerConfig { rtu_queue_capacity: 0, ..ServerConfig::default() },
		ServerConfig { delivery_timeout: Duration::ZERO, ..ServerConfig::default() },
	] {
		let (tx, _rx) = mpsc::unbounded_channel();
		assert!(
			Server::start(config, BlockFirst { count: AtomicUsize::new(0), entered: tx })
				.await
				.is_err()
		);
	}
}

struct SlowSystem;
#[async_trait]
impl RtuResetProcessSystemHandler for SlowSystem {
	async fn handle_reset_process(
		&self,
		ctx: &mut SystemCommandContext<'_>,
		server: &Server,
	) -> Result<(), ServerError> {
		tokio::time::sleep(Duration::from_millis(60)).await;
		let mut reply = ctx.asdu.clone();
		reply.cot = iec104::cot::Cot::ActivationConfirmation;
		server.send_asdu(ctx.connection_id, reply).await
	}
}
#[tokio::test]
async fn process_command_timeout_does_not_cancel_system_traffic() {
	let (rtu, addr) = start_rtu_server_ephemeral(
		ServerConfig { command_timeout: Duration::from_millis(10), ..Default::default() },
		Vec::<RtuInitialPoint>::new(),
		Arc::new(RejectAllCommands),
		RtuSystemHandlers { reset_process: Arc::new(SlowSystem), ..Default::default() },
	)
	.await
	.expect("rtu");
	let mut peer = started(addr).await;
	assert_eq!(frame(&mut peer).await[6], 70);
	peer.write_all(&[0x68, 14, 0, 0, 2, 0, 105, 1, 6, 0, 1, 0, 0, 0, 0, 1])
		.await
		.expect("system request");
	let reply = frame(&mut peer).await;
	assert_eq!((reply[6], reply[8]), (105, 7));
	// Interrogation remains available on the same connection after the slow
	// handler.
	peer.write_all(&[0x68, 14, 2, 0, 4, 0, 100, 1, 6, 0, 1, 0, 0, 0, 0, 20])
		.await
		.expect("interrogation");
	let confirm = frame(&mut peer).await;
	assert_eq!((confirm[6], confirm[8]), (100, 7));
	let termination = frame(&mut peer).await;
	assert_eq!((termination[6], termination[8]), (100, 10));
	rtu.shutdown().await.expect("shutdown");
}
