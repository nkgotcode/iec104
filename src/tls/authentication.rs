//! Actual TLS handshakes, trust failures and authenticated authorization.
#![allow(missing_docs, clippy::expect_used)]
#![cfg(any(feature = "rustls", feature = "native_tls"))]
use std::{net::SocketAddr, path::PathBuf, time::Duration};

use async_trait::async_trait;
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::TcpStream,
	sync::mpsc,
	time::timeout,
};

use crate::{
	asdu::Asdu,
	config::{ServerConfig, TlsClientConfig, TlsServerConfig},
	server::{ConnectionId, PeerIdentity, Server, ServerCallback},
	tls::{TlsClientConnector, TlsStream, build_client_connector},
};
const WAIT: Duration = Duration::from_secs(3);
fn file(name: &str) -> PathBuf {
	PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/certs/auth").join(name)
}
fn config(required: bool) -> ServerConfig {
	ServerConfig {
		port: 0,
		tls: Some(TlsServerConfig {
			server_key: file("server-key.pem"),
			server_certificate: file("server.pem"),
			client_ca_certificates: required.then(|| file("trusted-ca.pem")),
		}),
		..Default::default()
	}
}
struct Authorize {
	allow: bool,
	seen: mpsc::UnboundedSender<PeerIdentity>,
}
#[async_trait]
impl ServerCallback for Authorize {
	async fn on_new_objects(&self, _: Asdu, _: ConnectionId, _: SocketAddr) {}
	async fn authorize_peer(
		&self,
		_: ConnectionId,
		_: SocketAddr,
		identity: &PeerIdentity,
	) -> bool {
		let _ = self.seen.send(identity.clone());
		self.allow
	}
}
async fn connect(
	addr: SocketAddr,
	identity: Option<&str>,
	host: &str,
	root: &str,
) -> Result<TlsStream, crate::error::Error> {
	let connector = build_client_connector(&TlsClientConfig {
		client_key: identity.map(|name| file(&format!("{name}-key.pem"))),
		client_certificate: identity.map(|name| file(&format!("{name}.pem"))),
		server_certificate: Some(file(root)),
		danger_disable_tls_verify: false,
	})?;
	let tcp = TcpStream::connect(addr).await.expect("TCP connect");
	timeout(WAIT, connector.connect(host, tcp)).await.expect("TLS deadline")
}
async fn start(peer: &mut TlsStream) -> Result<[u8; 6], std::io::Error> {
	peer.write_all(&[0x68, 4, 7, 0, 0, 0]).await?;
	let mut reply = [0; 6];
	peer.read_exact(&mut reply).await?;
	Ok(reply)
}
#[tokio::test]
async fn server_only_tls_reports_encryption_without_authenticated_client() {
	let (tx, mut seen) = mpsc::unbounded_channel();
	let (server, addr) =
		Server::start_with_listen_addr(config(false), Authorize { allow: true, seen: tx })
			.await
			.expect("server");
	let mut peer = connect(addr, None, "localhost", "trusted-ca.pem").await.expect("TLS connect");
	assert_eq!(
		timeout(WAIT, start(&mut peer)).await.expect("start deadline").expect("start"),
		[0x68, 4, 11, 0, 0, 0]
	);
	let identity = timeout(WAIT, seen.recv()).await.expect("identity deadline").expect("identity");
	assert!(identity.encrypted);
	assert!(identity.verified_client_certificate_der.is_none());
	server.shutdown().await.expect("shutdown");
}
#[tokio::test]
async fn client_rejects_untrusted_server_and_wrong_hostname() {
	let (tx, _seen) = mpsc::unbounded_channel();
	let (server, addr) =
		Server::start_with_listen_addr(config(false), Authorize { allow: true, seen: tx })
			.await
			.expect("server");
	assert!(connect(addr, None, "wrong.invalid", "trusted-ca.pem").await.is_err());
	assert!(connect(addr, None, "localhost", "untrusted-ca.pem").await.is_err());
	server.shutdown().await.expect("shutdown");
}
#[cfg(feature = "native_tls")]
#[tokio::test]
async fn unsupported_native_client_authentication_is_rejected() {
	let (tx, _seen) = mpsc::unbounded_channel();
	let error = Server::start(config(true), Authorize { allow: true, seen: tx })
		.await
		.expect_err("native policy must not downgrade");
	assert!(error.to_string().contains("cannot enforce client authentication"));
}
#[cfg(feature = "rustls")]
#[tokio::test]
async fn trusted_client_identity_is_verified_and_authorization_is_enforced() {
	for allow in [true, false] {
		let (tx, mut seen) = mpsc::unbounded_channel();
		let (server, addr) =
			Server::start_with_listen_addr(config(true), Authorize { allow, seen: tx })
				.await
				.expect("server");
		let mut peer = connect(addr, Some("client"), "localhost", "trusted-ca.pem")
			.await
			.expect("TLS connect");
		let result = timeout(WAIT, start(&mut peer)).await.expect("protocol admission deadline");
		if allow {
			assert_eq!(result.expect("start"), [0x68, 4, 11, 0, 0, 0]);
		} else {
			assert!(result.is_err());
		}
		let identity =
			timeout(WAIT, seen.recv()).await.expect("identity deadline").expect("identity");
		assert!(identity.encrypted);
		assert_eq!(
			identity.verified_client_certificate_der,
			Some(std::fs::read(file("client.der")).expect("fixture"))
		);
		server.shutdown().await.expect("shutdown");
	}
}
#[cfg(feature = "rustls")]
#[tokio::test]
async fn required_auth_rejects_absent_untrusted_and_expired_clients() {
	let (tx, mut seen) = mpsc::unbounded_channel();
	let (server, addr) =
		Server::start_with_listen_addr(config(true), Authorize { allow: true, seen: tx })
			.await
			.expect("server");
	for credential in [None, Some("untrusted-client"), Some("expired")] {
		if let Ok(mut peer) = connect(addr, credential, "localhost", "trusted-ca.pem").await {
			assert!(timeout(WAIT, start(&mut peer)).await.expect("rejection deadline").is_err());
		}
		assert!(seen.try_recv().is_err(), "rejected TLS peers never reach authorization");
	}
	// Failed authentication must not stop the listener.
	let mut peer =
		connect(addr, Some("client"), "localhost", "trusted-ca.pem").await.expect("trusted client");
	assert_eq!(
		timeout(WAIT, start(&mut peer)).await.expect("start deadline").expect("start"),
		[0x68, 4, 11, 0, 0, 0]
	);
	server.shutdown().await.expect("shutdown");
}

#[cfg(feature = "rustls")]
#[tokio::test]
async fn invalid_required_trust_bundle_fails_before_listening() {
	for name in ["missing-ca.pem", "server-key.pem"] {
		let mut cfg = config(true);
		cfg.tls.as_mut().expect("TLS configuration").client_ca_certificates = Some(file(name));
		let (tx, _rx) = mpsc::unbounded_channel();
		assert!(Server::start(cfg, Authorize { allow: true, seen: tx }).await.is_err());
	}
}
