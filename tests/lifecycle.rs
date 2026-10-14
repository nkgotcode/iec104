//! Lifecycle and connection admission tests over real TCP sockets.
#![allow(missing_docs, clippy::expect_used)]
use std::{net::SocketAddr, sync::Arc, time::Duration};

use async_trait::async_trait;
use iec104::{
	asdu::Asdu,
	client::{Client, ClientCallback},
	config::{ClientConfig, ServerConfig},
	rtu_server::{
		RejectAllCommands, RtuInitialPoint, RtuServer, RtuSystemHandlers,
		start_rtu_server_ephemeral,
	},
	server::{ConnectionId, Server, ServerCallback},
};
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::{TcpListener, TcpStream},
	sync::mpsc,
	time::timeout,
};
const WAIT: Duration = Duration::from_secs(2);
const START: [u8; 6] = [0x68, 4, 7, 0, 0, 0];
const CONFIRM: [u8; 6] = [0x68, 4, 11, 0, 0, 0];
struct Callback;
#[async_trait]
impl ServerCallback for Callback {
	async fn on_new_objects(&self, _: Asdu, _: ConnectionId, _: SocketAddr) {}
}
#[async_trait]
impl ClientCallback for Callback {
	async fn on_new_objects(&self, _: Asdu) {}
}
async fn closed(peer: &mut TcpStream) {
	let mut buf = [0; 1024];
	timeout(WAIT, async {
		loop {
			match peer.read(&mut buf).await {
				Ok(0) | Err(_) => break,
				Ok(_) => {}
			}
		}
	})
	.await
	.expect("peer closes");
}
async fn started(addr: SocketAddr) -> TcpStream {
	let mut peer = TcpStream::connect(addr).await.expect("peer");
	peer.write_all(&START).await.expect("start");
	let mut reply = [0; 6];
	timeout(WAIT, peer.read_exact(&mut reply)).await.expect("STARTDT deadline").expect("reply");
	assert_eq!(reply, CONFIRM);
	peer
}
#[tokio::test]
async fn server_shutdown_joins_started_and_waiting_peers_and_all_clones() {
	let (server, addr) = Server::start_with_listen_addr(
		ServerConfig { port: 0, ..ServerConfig::default() },
		Callback,
	)
	.await
	.expect("server");
	let clone = server.clone();
	let mut active = started(addr).await;
	let mut waiting = TcpStream::connect(addr).await.expect("waiting peer");
	timeout(WAIT, async {
		let (a, b) = tokio::join!(server.shutdown(), clone.shutdown());
		a.expect("shutdown");
		b.expect("clone shutdown");
	})
	.await
	.expect("shutdown deadline");
	closed(&mut active).await;
	closed(&mut waiting).await;
	let _rebound = TcpListener::bind(addr).await.expect("listener released");
	server.shutdown().await.expect("idempotent");
}
struct Blocking(mpsc::UnboundedSender<()>);
#[async_trait]
impl ServerCallback for Blocking {
	async fn on_new_objects(&self, _: Asdu, _: ConnectionId, _: SocketAddr) {}
	async fn on_connection_started(&self, _: ConnectionId, _: SocketAddr) {
		let _ = self.0.send(());
		std::future::pending::<()>().await;
	}
}
#[tokio::test]
async fn shutdown_cancels_blocked_callback() {
	let (tx, mut rx) = mpsc::unbounded_channel();
	let (server, addr) = Server::start_with_listen_addr(
		ServerConfig { port: 0, ..ServerConfig::default() },
		Blocking(tx),
	)
	.await
	.expect("server");
	let mut peer = started(addr).await;
	timeout(WAIT, rx.recv()).await.expect("callback deadline").expect("callback entered");
	timeout(WAIT, server.shutdown()).await.expect("shutdown deadline").expect("shutdown");
	closed(&mut peer).await;
	assert_eq!(rx.recv().await, None, "callback was dropped before shutdown returned");
}
#[tokio::test]
async fn dropping_last_server_handle_closes_connections() {
	let (server, addr) = Server::start_with_listen_addr(
		ServerConfig { port: 0, ..ServerConfig::default() },
		Callback,
	)
	.await
	.expect("server");
	let mut peer = started(addr).await;
	let clone = server.clone();
	drop(server);
	peer.write_all(&[0x68, 4, 0x43, 0, 0, 0]).await.expect("clone keeps alive");
	let mut reply = [0; 6];
	timeout(WAIT, peer.read_exact(&mut reply)).await.expect("test deadline").expect("TESTFR");
	assert_eq!(reply, [0x68, 4, 0x83, 0, 0, 0]);
	drop(clone);
	closed(&mut peer).await;
}
#[tokio::test]
async fn connection_limit_rejects_excess_and_recovers_capacity() {
	let (server, addr) = Server::start_with_listen_addr(
		ServerConfig { port: 0, max_connections: 1, ..ServerConfig::default() },
		Callback,
	)
	.await
	.expect("server");
	let peer = started(addr).await;
	let mut excess = TcpStream::connect(addr).await.expect("excess");
	closed(&mut excess).await;
	drop(peer);
	// A new accept can precede the close notification; retry within a deadline.
	timeout(WAIT, async {
		loop {
			let mut next = TcpStream::connect(addr).await.expect("next");
			if next.write_all(&START).await.is_ok() {
				let mut reply = [0; 6];
				if next.read_exact(&mut reply).await.is_ok() {
					assert_eq!(reply, CONFIRM);
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
#[tokio::test]
async fn invalid_admission_config_fails_before_bind() {
	for cfg in [
		ServerConfig { max_connections: 0, ..ServerConfig::default() },
		ServerConfig { tls_handshake_timeout: Duration::ZERO, ..ServerConfig::default() },
	] {
		assert!(Server::start(cfg, Callback).await.is_err());
	}
}
#[tokio::test]
async fn client_shutdown_closes_socket_and_allows_reconnect() {
	let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
	let addr = listener.local_addr().expect("address");
	let mut client =
		Client::new(ClientConfig { port: addr.port(), ..ClientConfig::default() }, Callback);
	for _ in 0..2 {
		client.connect().await.expect("connect");
		let (mut peer, _) = listener.accept().await.expect("accept");
		timeout(WAIT, client.shutdown()).await.expect("shutdown deadline").expect("shutdown");
		closed(&mut peer).await;
		assert!(client.start_receiving().await.is_err());
		client.shutdown().await.expect("idempotent");
	}
}
#[tokio::test]
async fn rtu_shutdown_and_last_drop_break_actor_cycle() {
	for explicit in [true, false] {
		let (rtu, addr) = start_rtu_server_ephemeral(
			ServerConfig::default(),
			Vec::<RtuInitialPoint>::new(),
			Arc::new(RejectAllCommands),
			RtuSystemHandlers::default(),
		)
		.await
		.expect("rtu");
		let mut peer = started(addr).await;
		if explicit {
			timeout(WAIT, rtu.shutdown()).await.expect("shutdown deadline").expect("shutdown");
			assert!(rtu.unregister_all().await.is_err());
			rtu.shutdown().await.expect("idempotent");
		}
		drop(rtu);
		closed(&mut peer).await;
		let _rebound = TcpListener::bind(addr).await.expect("listener released");
	}
}
#[tokio::test]
async fn invalid_rtu_model_does_not_leave_listener_running() {
	use iec104::{
		rtu_server::{PointAddress, PointValue},
		types::{MMeNc1, quality_descriptors::Qds},
	};
	let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
	let addr = listener.local_addr().expect("address");
	drop(listener);
	let point = RtuInitialPoint::new(
		PointAddress::new(1, 1),
		PointValue::MMeNc1(MMeNc1 { value: 0.0, qds: Qds::default() }),
	);
	assert!(
		RtuServer::start(
			ServerConfig { port: addr.port(), ..ServerConfig::default() },
			vec![point.clone(), point],
			Arc::new(RejectAllCommands)
		)
		.await
		.is_err()
	);
	let _rebound = TcpListener::bind(addr).await.expect("invalid model never binds");
}

struct Reconnecting(mpsc::UnboundedSender<()>);
#[async_trait]
impl ClientCallback for Reconnecting {
	async fn on_new_objects(&self, _: Asdu) {}
	async fn on_reconnecting(&self) {
		let _ = self.0.send(());
	}
}
#[tokio::test]
async fn client_shutdown_interrupts_reconnect_wait() {
	let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
	let addr = listener.local_addr().expect("address");
	let (tx, mut rx) = mpsc::unbounded_channel();
	let mut config = ClientConfig { port: addr.port(), ..ClientConfig::default() };
	config.protocol.t0 = Duration::from_secs(60);
	let mut client = Client::new(config, Reconnecting(tx));
	client.connect().await.expect("connect");
	let (mut peer, _) = listener.accept().await.expect("accept");
	client.start_receiving().await.expect("start");
	let mut frame = [0; 6];
	peer.read_exact(&mut frame).await.expect("STARTDT");
	assert_eq!(frame, START);
	drop(listener);
	drop(peer);
	timeout(WAIT, rx.recv()).await.expect("reconnect deadline").expect("reconnecting");
	timeout(WAIT, client.shutdown())
		.await
		.expect("shutdown interrupts reconnect")
		.expect("shutdown");
	assert!(rx.try_recv().is_err(), "no new callbacks after shutdown");
}
