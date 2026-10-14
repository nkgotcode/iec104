//! Same-socket data-transfer transitions preserve link state.
#![allow(missing_docs, clippy::expect_used)]
use std::{net::SocketAddr, time::Duration};

use async_trait::async_trait;
use iec104::{
	asdu::Asdu,
	config::ServerConfig,
	server::{ConnectionId, Server, ServerCallback},
};
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::TcpStream,
	sync::mpsc,
	time::timeout,
};
const WAIT: Duration = Duration::from_secs(2);
struct Callback(mpsc::UnboundedSender<Asdu>);
#[async_trait]
impl ServerCallback for Callback {
	async fn on_new_objects(&self, asdu: Asdu, _: ConnectionId, _: SocketAddr) {
		self.0.send(asdu).expect("receiver");
	}
}
async fn frame(peer: &mut TcpStream) -> Vec<u8> {
	timeout(WAIT, async {
		let mut h = [0; 2];
		peer.read_exact(&mut h).await.expect("header");
		let mut b = vec![0; usize::from(h[1]) + 2];
		b[..2].copy_from_slice(&h);
		peer.read_exact(&mut b[2..]).await.expect("body");
		b
	})
	.await
	.expect("frame deadline")
}
#[tokio::test]
async fn server_preserves_sequence_and_coalesced_restart_on_same_socket() {
	let (tx, mut rx) = mpsc::unbounded_channel();
	let (server, addr) = Server::start_with_listen_addr(
		ServerConfig { port: 0, ..ServerConfig::default() },
		Callback(tx),
	)
	.await
	.expect("server");
	let mut peer = TcpStream::connect(addr).await.expect("peer");
	peer.write_all(&[0x68, 4, 7, 0, 0, 0]).await.expect("start");
	assert_eq!(frame(&mut peer).await, [0x68, 4, 11, 0, 0, 0]);
	let sample = Asdu::parse(&[1, 1, 3, 0, 1, 0, 1, 0, 0, 1]).expect("sample");
	peer.write_all(&[0x68, 14, 0, 0, 0, 0, 1, 1, 3, 0, 1, 0, 1, 0, 0, 1]).await.expect("first I");
	assert_eq!(timeout(WAIT, rx.recv()).await.expect("ingress"), Some(sample.clone()));
	server.broadcast_asdu(sample.clone()).await.expect("send");
	assert_eq!(&frame(&mut peer).await[2..6], &[0, 0, 2, 0]);
	// Acknowledge N(S)=0, stop, test, and restart in a single TCP write.
	peer.write_all(&[
		0x68, 4, 1, 0, 2, 0, 0x68, 4, 0x13, 0, 0, 0, 0x68, 4, 0x43, 0, 0, 0, 0x68, 4, 7, 0, 0, 0,
	])
	.await
	.expect("coalesced transitions");
	assert_eq!(frame(&mut peer).await, [0x68, 4, 0x23, 0, 0, 0]);
	assert_eq!(frame(&mut peer).await, [0x68, 4, 0x83, 0, 0, 0]);
	assert_eq!(frame(&mut peer).await, [0x68, 4, 11, 0, 0, 0]);
	peer.write_all(&[0x68, 14, 2, 0, 2, 0, 1, 1, 3, 0, 1, 0, 1, 0, 0, 1]).await.expect("second I");
	assert_eq!(timeout(WAIT, rx.recv()).await.expect("ingress resumed"), Some(sample.clone()));
	server.broadcast_asdu(sample).await.expect("resumed send");
	assert_eq!(&frame(&mut peer).await[2..6], &[2, 0, 4, 0]);
	server.shutdown().await.expect("shutdown");
}

struct ClientEvents {
	objects: mpsc::UnboundedSender<Asdu>,
	states: mpsc::UnboundedSender<bool>,
}
#[async_trait]
impl iec104::client::ClientCallback for ClientEvents {
	async fn on_new_objects(&self, asdu: Asdu) {
		self.objects.send(asdu).expect("objects");
	}
	async fn on_connection_started(&self) {
		self.states.send(true).expect("states");
	}
	async fn on_connection_stopped(&self) {
		self.states.send(false).expect("states");
	}
}
#[tokio::test]
async fn client_preserves_sequence_after_stop_and_restart() {
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("listener");
	let (otx, mut objects) = mpsc::unbounded_channel();
	let (stx, mut states) = mpsc::unbounded_channel();
	let mut client = iec104::client::Client::new(
		iec104::config::ClientConfig {
			port: listener.local_addr().expect("address").port(),
			..Default::default()
		},
		ClientEvents { objects: otx, states: stx },
	);
	client.connect().await.expect("connect");
	let (mut peer, _) = listener.accept().await.expect("accept");
	client.start_receiving().await.expect("start");
	assert_eq!(frame(&mut peer).await, [0x68, 4, 7, 0, 0, 0]);
	peer.write_all(&[0x68, 4, 11, 0, 0, 0]).await.expect("start confirm");
	assert_eq!(timeout(WAIT, states.recv()).await.expect("started"), Some(true));
	let sample = Asdu::parse(&[1, 1, 3, 0, 1, 0, 1, 0, 0, 1]).expect("sample");
	client.send_asdu(sample.clone()).await.expect("send");
	assert_eq!(&frame(&mut peer).await[2..6], &[0, 0, 0, 0]);
	peer.write_all(&[0x68, 14, 0, 0, 2, 0, 1, 1, 3, 0, 1, 0, 1, 0, 0, 1]).await.expect("first I");
	assert_eq!(timeout(WAIT, objects.recv()).await.expect("received"), Some(sample.clone()));
	client.stop_receiving().await.expect("stop");
	assert_eq!(frame(&mut peer).await, [0x68, 4, 1, 0, 2, 0]);
	assert_eq!(frame(&mut peer).await, [0x68, 4, 0x13, 0, 0, 0]);
	peer.write_all(&[0x68, 4, 0x23, 0, 0, 0]).await.expect("stop confirm");
	assert_eq!(timeout(WAIT, states.recv()).await.expect("stopped"), Some(false));
	assert!(client.send_asdu(sample.clone()).await.is_err());
	client.start_receiving().await.expect("restart");
	assert_eq!(frame(&mut peer).await, [0x68, 4, 7, 0, 0, 0]);
	peer.write_all(&[0x68, 4, 11, 0, 0, 0, 0x68, 14, 2, 0, 2, 0, 1, 1, 3, 0, 1, 0, 1, 0, 0, 1])
		.await
		.expect("restart and data");
	assert_eq!(timeout(WAIT, states.recv()).await.expect("restarted"), Some(true));
	assert_eq!(
		timeout(WAIT, objects.recv()).await.expect("resumed received"),
		Some(sample.clone())
	);
	client.send_asdu(sample).await.expect("resumed send");
	assert_eq!(&frame(&mut peer).await[2..6], &[2, 0, 4, 0]);
	client.shutdown().await.expect("shutdown");
}
#[tokio::test]
async fn rtu_initialization_is_once_per_tcp_session() {
	use iec104::rtu_server::{
		RejectAllCommands, RtuInitialPoint, RtuSystemHandlers, start_rtu_server_ephemeral,
	};
	let (rtu, addr) = start_rtu_server_ephemeral(
		ServerConfig::default(),
		Vec::<RtuInitialPoint>::new(),
		std::sync::Arc::new(RejectAllCommands),
		RtuSystemHandlers::default(),
	)
	.await
	.expect("rtu");
	let mut peer = TcpStream::connect(addr).await.expect("peer");
	peer.write_all(&[0x68, 4, 7, 0, 0, 0]).await.expect("start");
	assert_eq!(frame(&mut peer).await, [0x68, 4, 11, 0, 0, 0]);
	assert_eq!(frame(&mut peer).await[6], 70);
	peer.write_all(&[0x68, 4, 1, 0, 2, 0, 0x68, 4, 0x13, 0, 0, 0, 0x68, 4, 7, 0, 0, 0])
		.await
		.expect("stop restart");
	assert_eq!(frame(&mut peer).await, [0x68, 4, 0x23, 0, 0, 0]);
	assert_eq!(frame(&mut peer).await, [0x68, 4, 11, 0, 0, 0]);
	peer.write_all(&[0x68, 14, 0, 0, 2, 0, 100, 1, 6, 0, 0, 0, 0, 0, 0, 20])
		.await
		.expect("interrogation");
	let confirmation = frame(&mut peer).await;
	assert_eq!(&confirmation[2..9], &[2, 0, 2, 0, 100, 1, 7]);
	assert_eq!(frame(&mut peer).await[8], 10);
	rtu.shutdown().await.expect("shutdown");
}
#[tokio::test]
async fn stopped_peer_keeps_pending_queue_until_restart() {
	let (tx, _rx) = mpsc::unbounded_channel();
	let mut config = ServerConfig { port: 0, ..Default::default() };
	config.protocol.k = 3;
	config.protocol.w = 2;
	let (server, addr) =
		Server::start_with_listen_addr(config, Callback(tx)).await.expect("server");
	let mut peer = TcpStream::connect(addr).await.expect("peer");
	peer.write_all(&[0x68, 4, 7, 0, 0, 0]).await.expect("start");
	assert_eq!(frame(&mut peer).await, [0x68, 4, 11, 0, 0, 0]);
	for _ in 0..4 {
		server
			.broadcast_asdu(Asdu::parse(&[1, 1, 3, 0, 1, 0, 1, 0, 0, 1]).expect("sample"))
			.await
			.expect("send");
	}
	for seq in 0..3 {
		assert_eq!(frame(&mut peer).await[2], seq * 2);
	}
	// Stop before acknowledging; the fourth ASDU remains pending.
	peer.write_all(&[0x68, 4, 0x13, 0, 0, 0, 0x68, 4, 1, 0, 6, 0, 0x68, 4, 0x43, 0, 0, 0])
		.await
		.expect("stop ack test");
	assert_eq!(frame(&mut peer).await, [0x68, 4, 0x23, 0, 0, 0]);
	assert_eq!(frame(&mut peer).await, [0x68, 4, 0x83, 0, 0, 0]);
	peer.write_all(&[0x68, 4, 7, 0, 0, 0]).await.expect("restart");
	assert_eq!(frame(&mut peer).await, [0x68, 4, 11, 0, 0, 0]);
	assert_eq!(frame(&mut peer).await[2], 6);
	server.shutdown().await.expect("shutdown");
}

struct Lifecycle(mpsc::UnboundedSender<&'static str>);
#[async_trait]
impl ServerCallback for Lifecycle {
	async fn on_new_objects(&self, _: Asdu, _: ConnectionId, _: SocketAddr) {}
	async fn on_connection_started(&self, _: ConnectionId, _: SocketAddr) {
		self.0.send("started").expect("receiver");
	}
	async fn on_connection_stopped(&self, _: ConnectionId, _: SocketAddr) {
		self.0.send("stopped").expect("receiver");
	}
	async fn on_connection_disconnected(&self, _: ConnectionId, _: SocketAddr) {
		self.0.send("disconnected").expect("receiver");
	}
}
#[tokio::test]
async fn stop_and_disconnect_are_distinct_events() {
	let (tx, mut events) = mpsc::unbounded_channel();
	let (server, addr) = Server::start_with_listen_addr(
		ServerConfig { port: 0, ..Default::default() },
		Lifecycle(tx),
	)
	.await
	.expect("server");
	let mut peer = TcpStream::connect(addr).await.expect("peer");
	peer.write_all(&[0x68, 4, 7, 0, 0, 0]).await.expect("start");
	assert_eq!(frame(&mut peer).await, [0x68, 4, 11, 0, 0, 0]);
	assert_eq!(timeout(WAIT, events.recv()).await.expect("started event"), Some("started"));
	peer.write_all(&[0x68, 4, 0x13, 0, 0, 0]).await.expect("stop");
	assert_eq!(frame(&mut peer).await, [0x68, 4, 0x23, 0, 0, 0]);
	assert_eq!(timeout(WAIT, events.recv()).await.expect("stopped event"), Some("stopped"));
	drop(peer);
	assert_eq!(
		timeout(WAIT, events.recv()).await.expect("disconnected event"),
		Some("disconnected")
	);
	server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn test_confirmation_cannot_cancel_stop_confirmation_deadline() {
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("listener");
	let (otx, _objects) = mpsc::unbounded_channel();
	let (stx, mut states) = mpsc::unbounded_channel();
	let mut config = iec104::config::ClientConfig {
		port: listener.local_addr().expect("address").port(),
		..Default::default()
	};
	config.protocol.t1 = Duration::from_millis(250);
	let mut client =
		iec104::client::Client::new(config, ClientEvents { objects: otx, states: stx });
	client.connect().await.expect("connect");
	let (mut peer, _) = listener.accept().await.expect("accept");
	client.start_receiving().await.expect("start");
	assert_eq!(frame(&mut peer).await, [0x68, 4, 7, 0, 0, 0]);
	peer.write_all(&[0x68, 4, 11, 0, 0, 0]).await.expect("confirm");
	assert_eq!(timeout(WAIT, states.recv()).await.expect("started"), Some(true));
	client.stop_receiving().await.expect("stop");
	assert_eq!(frame(&mut peer).await, [0x68, 4, 0x13, 0, 0, 0]);
	client.send_test_frame().await.expect("test");
	assert_eq!(frame(&mut peer).await, [0x68, 4, 0x43, 0, 0, 0]);
	peer.write_all(&[0x68, 4, 0x83, 0, 0, 0]).await.expect("test confirm");
	// Withhold STOPDT-CON. TESTFR-CON must not disable its deadline.
	let (_next, _) = timeout(WAIT, listener.accept())
		.await
		.expect("reconnect after missing STOPDT-CON")
		.expect("accept replacement");
	client.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn rtu_retries_initialization_after_immediate_stop() {
	use iec104::rtu_server::{
		RejectAllCommands, RtuInitialPoint, RtuSystemHandlers, start_rtu_server_ephemeral,
	};
	let (rtu, addr) = start_rtu_server_ephemeral(
		ServerConfig::default(),
		Vec::<RtuInitialPoint>::new(),
		std::sync::Arc::new(RejectAllCommands),
		RtuSystemHandlers::default(),
	)
	.await
	.expect("rtu");
	let mut peer = TcpStream::connect(addr).await.expect("peer");
	peer.write_all(&[0x68, 4, 7, 0, 0, 0, 0x68, 4, 0x13, 0, 0, 0]).await.expect("start and stop");
	assert_eq!(frame(&mut peer).await, [0x68, 4, 11, 0, 0, 0]);
	assert_eq!(frame(&mut peer).await, [0x68, 4, 0x23, 0, 0, 0]);
	// This actor operation is a barrier after the initial STARTDT notification.
	rtu.unregister_all().await.expect("actor barrier");
	peer.write_all(&[0x68, 4, 7, 0, 0, 0]).await.expect("restart");
	assert_eq!(frame(&mut peer).await, [0x68, 4, 11, 0, 0, 0]);
	assert_eq!(frame(&mut peer).await[6], 70);
	rtu.shutdown().await.expect("shutdown");
}

struct ClientLoss(mpsc::UnboundedSender<()>);
#[async_trait]
impl iec104::client::ClientCallback for ClientLoss {
	async fn on_new_objects(&self, _: Asdu) {}
	async fn on_connection_disconnected(&self) {
		let _ = self.0.send(());
	}
}
#[tokio::test]
async fn client_reports_disconnects_during_initial_and_reconnect_startdt() {
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("listener");
	let (tx, mut losses) = mpsc::unbounded_channel();
	let mut client = iec104::client::Client::new(
		iec104::config::ClientConfig {
			port: listener.local_addr().expect("address").port(),
			..Default::default()
		},
		ClientLoss(tx),
	);
	client.connect().await.expect("connect");
	client.start_receiving().await.expect("start");
	for _ in 0..2 {
		let (mut peer, _) =
			timeout(WAIT, listener.accept()).await.expect("accept deadline").expect("accept");
		assert_eq!(frame(&mut peer).await, [0x68, 4, 7, 0, 0, 0]);
		drop(peer);
		timeout(WAIT, losses.recv())
			.await
			.expect("disconnect event deadline")
			.expect("disconnect event");
	}
	client.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn failed_handshake_releases_old_socket_before_retrying_connect() {
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("listener");
	let (tx, _losses) = mpsc::unbounded_channel();
	let mut client = iec104::client::Client::new(
		iec104::config::ClientConfig {
			port: listener.local_addr().expect("address").port(),
			..Default::default()
		},
		ClientLoss(tx),
	);
	client.connect().await.expect("connect");
	client.start_receiving().await.expect("start");
	let (mut peer, _) = listener.accept().await.expect("accept");
	drop(listener);
	assert_eq!(frame(&mut peer).await, [0x68, 4, 7, 0, 0, 0]);
	peer.write_all(&[0x68, 4, 1, 0, 0, 0]).await.expect("invalid start response");
	let mut byte = [0];
	assert_eq!(
		timeout(WAIT, peer.read(&mut byte)).await.expect("old socket closes").expect("read"),
		0
	);
	client.shutdown().await.expect("shutdown");
}
