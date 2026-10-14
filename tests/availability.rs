//! Real socket and TLS transport regression tests.
#![allow(missing_docs, clippy::expect_used)]
#![cfg(any(feature = "native_tls", feature = "rustls"))]

use std::{net::SocketAddr, path::PathBuf, time::Duration};

use async_trait::async_trait;
use iec104::{
	asdu::Asdu,
	client::{Client, ClientCallback},
	config::{ClientConfig, ServerConfig, TlsClientConfig, TlsServerConfig},
	server::{ConnectionId, Server, ServerCallback},
};
use tokio::{
	net::TcpStream,
	sync::mpsc,
	time::{sleep, timeout},
};

const WAIT: Duration = Duration::from_secs(2);
struct Noop;
#[async_trait]
impl ServerCallback for Noop {
	async fn on_new_objects(&self, _: Asdu, _: ConnectionId, _: SocketAddr) {}
}
struct Started(mpsc::UnboundedSender<()>);
#[async_trait]
impl ClientCallback for Started {
	async fn on_new_objects(&self, _: Asdu) {}
	async fn on_connection_started(&self) {
		let _ = self.0.send(());
	}
}
fn fixture(name: &str) -> PathBuf {
	PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)
}
fn server_config() -> ServerConfig {
	ServerConfig {
		port: 0,
		tls: Some(TlsServerConfig {
			server_certificate: fixture("localhost-cert.pem"),
			server_key: fixture("localhost-key.pem"),
		}),
		..ServerConfig::default()
	}
}
async fn healthy_client(addr: SocketAddr) -> Client<Started> {
	let (tx, mut rx) = mpsc::unbounded_channel();
	let mut client = Client::new(
		ClientConfig {
			address: addr.ip().to_string(),
			port: addr.port(),
			tls: Some(TlsClientConfig {
				server_certificate: Some(fixture("localhost-cert.pem")),
				client_key: None,
				client_certificate: None,
				danger_disable_tls_verify: false,
			}),
			..ClientConfig::default()
		},
		Started(tx),
	);
	timeout(WAIT, client.connect()).await.expect("TLS connect deadline").expect("TLS connect");
	client.start_receiving().await.expect("STARTDT");
	timeout(WAIT, rx.recv()).await.expect("STARTDT deadline").expect("started");
	client
}
#[tokio::test]
async fn stalled_tls_peer_does_not_block_healthy_client() {
	let (_server, addr) =
		Server::start_with_listen_addr(server_config(), Noop).await.expect("server");
	let _silent = TcpStream::connect(addr).await.expect("silent peer");
	sleep(Duration::from_millis(50)).await;
	let _client = healthy_client(addr).await;
}
#[tokio::test]
async fn failed_tls_peer_does_not_stop_listener() {
	let (_server, addr) =
		Server::start_with_listen_addr(server_config(), Noop).await.expect("server");
	let peer = TcpStream::connect(addr).await.expect("abandoned peer");
	drop(peer);
	sleep(Duration::from_millis(100)).await;
	let _client = healthy_client(addr).await;
}

#[tokio::test]
async fn pending_tls_handshake_has_deadline_and_counts_toward_limit() {
	use tokio::io::AsyncReadExt;
	let mut config = server_config();
	config.max_connections = 1;
	config.tls_handshake_timeout = Duration::from_millis(300);
	let (server, addr) = Server::start_with_listen_addr(config, Noop).await.expect("server");
	let mut silent = TcpStream::connect(addr).await.expect("silent");
	// Establish ordering: accept the first socket before attempting the second.
	sleep(Duration::from_millis(50)).await;
	let mut excess = TcpStream::connect(addr).await.expect("excess");
	let mut byte = [0];
	let result = timeout(Duration::from_millis(150), excess.read(&mut byte))
		.await
		.expect("excess promptly rejected");
	assert!(matches!(result, Ok(0) | Err(_)));
	let result = timeout(WAIT, silent.read(&mut byte)).await.expect("handshake deadline");
	assert!(matches!(result, Ok(0) | Err(_)));
	server.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn shutdown_joins_pending_tls_handshake() {
	use tokio::io::AsyncReadExt;
	let (server, addr) =
		Server::start_with_listen_addr(server_config(), Noop).await.expect("server");
	let mut peer = TcpStream::connect(addr).await.expect("peer");
	sleep(Duration::from_millis(50)).await;
	timeout(WAIT, server.shutdown()).await.expect("shutdown deadline").expect("shutdown");
	let mut byte = [0];
	let result = timeout(WAIT, peer.read(&mut byte)).await.expect("closed socket");
	assert!(matches!(result, Ok(0) | Err(_)));
	let _rebound = tokio::net::TcpListener::bind(addr).await.expect("listener released");
}

#[tokio::test]
async fn client_tls_handshake_is_time_bounded() {
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("listener");
	let addr = listener.local_addr().expect("address");
	let (tx, _rx) = mpsc::unbounded_channel();
	let mut config = ClientConfig { port: addr.port(), ..ClientConfig::default() };
	config.protocol.t0 = Duration::from_millis(100);
	config.tls = Some(TlsClientConfig {
		server_certificate: Some(fixture("localhost-cert.pem")),
		client_key: None,
		client_certificate: None,
		danger_disable_tls_verify: false,
	});
	let mut client = Client::new(config, Started(tx));
	assert!(timeout(WAIT, client.connect()).await.expect("client handshake deadline").is_err());
}
