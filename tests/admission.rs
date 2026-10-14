//! Connection admission precedes protocol traffic and is isolated per peer.
#![allow(missing_docs, clippy::expect_used)]
use std::{
	net::SocketAddr,
	sync::atomic::{AtomicUsize, Ordering},
	time::Duration,
};

use async_trait::async_trait;
use iec104::{
	asdu::Asdu,
	config::ServerConfig,
	server::{ConnectionId, PeerIdentity, Server, ServerCallback},
};
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::TcpStream,
	sync::mpsc,
	time::timeout,
};
struct Admission {
	count: AtomicUsize,
	entered: mpsc::UnboundedSender<usize>,
}
#[async_trait]
impl ServerCallback for Admission {
	async fn on_new_objects(&self, _: Asdu, _: ConnectionId, _: SocketAddr) {
		panic!("No ASDU admitted");
	}
	async fn authorize_peer(
		&self,
		_: ConnectionId,
		_: SocketAddr,
		identity: &PeerIdentity,
	) -> bool {
		assert!(!identity.encrypted);
		assert!(identity.verified_client_certificate_der.is_none());
		let n = self.count.fetch_add(1, Ordering::SeqCst);
		self.entered.send(n).expect("receiver");
		match n {
			0 => std::future::pending().await,
			1 => false,
			_ => true,
		}
	}
}
#[tokio::test]
async fn denied_and_stalled_admission_do_not_block_other_peers() {
	let (tx, mut rx) = mpsc::unbounded_channel();
	let (server, addr) = Server::start_with_listen_addr(
		ServerConfig {
			port: 0,
			tls_handshake_timeout: Duration::from_millis(100),
			..Default::default()
		},
		Admission { count: AtomicUsize::new(0), entered: tx },
	)
	.await
	.expect("server");
	let mut peers = Vec::new();
	for n in 0..3 {
		let mut peer = TcpStream::connect(addr).await.expect("connect");
		peer.write_all(&[0x68, 4, 7, 0, 0, 0]).await.expect("start");
		assert_eq!(
			timeout(Duration::from_secs(2), rx.recv()).await.expect("callback deadline"),
			Some(n)
		);
		peers.push(peer);
	}
	let mut response = [0; 6];
	timeout(Duration::from_secs(2), peers[2].read_exact(&mut response))
		.await
		.expect("healthy deadline")
		.expect("healthy reply");
	assert_eq!(response, [0x68, 4, 11, 0, 0, 0]);
	for peer in &mut peers[..2] {
		assert!(
			timeout(Duration::from_secs(2), peer.read_exact(&mut response))
				.await
				.expect("closure deadline")
				.is_err()
		);
	}
	server.shutdown().await.expect("shutdown");
}
