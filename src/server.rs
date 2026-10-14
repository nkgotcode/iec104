use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use crate::{
	Connection,
	asdu::Asdu,
	config::ServerConfig,
	error::Error,
	receive_handler::ReceiveHandlerCallback,
	server::connection_handler::{AtomicConnectionState, ConnectionHandler, ConnectionState},
};

mod connection_handler;
pub mod error;

use snafu::{ResultExt as _, whatever};
use tokio::{
	net::TcpListener,
	sync::{Mutex, mpsc, oneshot, watch},
	task::{JoinHandle, JoinSet},
};

#[cfg(any(feature = "native_tls", feature = "rustls"))]
use crate::tls::TlsServerAcceptor;

// Connection tasks carry the owning server ID through every callback await.
// A task-local ID does not keep a Server handle alive or create an ownership
// cycle.
tokio::task_local! { static CALLBACK_OWNER: tokio::task::Id; }

/// Identifies a connection so it can be removed from the list when it
/// terminates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ConnectionId(u64);

impl ConnectionId {
	const fn new() -> Self {
		Self(0)
	}
	const fn next(self) -> Self {
		Self(self.0.wrapping_add(1))
	}
}

/// Transport identity available before protocol data transfer. A certificate
/// is present only after successful required client-certificate verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PeerIdentity {
	/// True for TLS, including server-only TLS without client authentication.
	pub encrypted: bool,
	/// Authenticated leaf certificate in DER form. Applications can match a
	/// provisioned certificate or parse it for their authorization policy.
	pub verified_client_certificate_der: Option<Vec<u8>>,
}

#[async_trait::async_trait]
#[expect(
	clippy::double_must_use,
	reason = "async-trait 0.1.89 generates redundant must_use on boxed futures"
)]
pub trait ServerCallback {
	/// Authorize a peer before STARTDT or ingress callbacks. The default allows
	/// admission; certificate authentication alone is not application
	/// permission.
	async fn authorize_peer(
		&self,
		_id: ConnectionId,
		_address: SocketAddr,
		_identity: &PeerIdentity,
	) -> bool {
		true
	}
	async fn on_new_objects(&self, asdu: Asdu, id: ConnectionId, address: SocketAddr);
	/// Fallible ingress admission. Returning an error closes this peer. The
	/// default preserves the existing callback; adapters can reject overload.
	async fn on_new_objects_checked(
		&self,
		asdu: Asdu,
		id: ConnectionId,
		address: SocketAddr,
	) -> Result<(), Error> {
		self.on_new_objects(asdu, id, address).await;
		Ok(())
	}
	/// Fallible STARTDT notification; returning an error closes this peer.
	async fn on_connection_started_checked(
		&self,
		id: ConnectionId,
		address: SocketAddr,
	) -> Result<(), Error> {
		self.on_connection_started(id, address).await;
		Ok(())
	}
	async fn on_new_connection(&self, address: SocketAddr) {
		tracing::debug!("New connection from {address}");
	}
	async fn on_connection_started(&self, id: ConnectionId, address: SocketAddr) {
		tracing::debug!("Connection {id:?} from {address} started");
	}
	async fn on_connection_stopped(&self, id: ConnectionId, address: SocketAddr) {
		tracing::debug!("Connection {id:?} from {address} stopped");
	}
	/// Synchronous close notification, including task cancellation. Must not
	/// block or panic. Use it to invalidate session tokens before queued work
	/// runs; use `on_connection_disconnected` for asynchronous
	/// notifications.
	fn on_connection_closed(&self, _id: ConnectionId, _address: SocketAddr) {}

	/// TCP session ended, distinct from a STOPDT transition.
	async fn on_connection_disconnected(&self, id: ConnectionId, address: SocketAddr) {
		tracing::debug!("Connection {id:?} from {address} disconnected");
	}
	async fn on_error(&self, error: &Error) {
		tracing::debug!("Error: {error}");
	}
}

struct InnerServerCallback<C: ServerCallback + Send + Sync> {
	state: Arc<AtomicConnectionState>,
	id: ConnectionId,
	address: SocketAddr,
	callback: Arc<C>,
}

#[async_trait::async_trait]
impl<C: ServerCallback + Send + Sync> ReceiveHandlerCallback for InnerServerCallback<C> {
	async fn on_data_transfer(&self, active: bool) -> Result<(), Error> {
		self.state.store(
			if active { ConnectionState::Started } else { ConnectionState::WaitingStartDT },
			std::sync::atomic::Ordering::Relaxed,
		);
		if active {
			self.callback.on_connection_started_checked(self.id, self.address).await?;
		} else {
			self.callback.on_connection_stopped(self.id, self.address).await;
		}
		Ok(())
	}

	async fn on_new_objects(&self, asdu: Asdu) -> Result<(), Error> {
		self.callback.on_new_objects_checked(asdu, self.id, self.address).await
	}
}

impl<C: ServerCallback + Send + Sync> InnerServerCallback<C> {
	async fn on_connection_started(
		&self,
		id: ConnectionId,
		address: SocketAddr,
	) -> Result<(), Error> {
		self.callback.on_connection_started_checked(id, address).await
	}
	async fn on_error(&self, error: &Error) {
		self.callback.on_error(error).await;
	}
}

/// Commands for the server to send to the inner server
#[derive(Debug)]
pub enum ServerCommand {
	/// Close one TCP session without replaying its queued output.
	Disconnect {
		id: ConnectionId,
		tx: oneshot::Sender<Result<(), error::ServerError>>,
	},
	SingleConnection {
		id: ConnectionId,
		asdu: Asdu,
		guard: Option<watch::Receiver<bool>>,
		tx: oneshot::Sender<Result<(), error::ServerError>>,
	},
	Broadcast {
		asdu: Asdu,
		tx: oneshot::Sender<Result<(), error::ServerError>>,
	},
}

#[derive(Debug, Clone)]
pub struct Server {
	tx: mpsc::Sender<ServerCommand>,
	lifecycle: Arc<ServerLifecycle>,
}

#[derive(Debug)]
struct ServerLifecycle {
	id: tokio::task::Id,
	stop: watch::Sender<()>,
	task: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for ServerLifecycle {
	fn drop(&mut self) {
		self.stop.send_replace(());
	}
}

struct InnerServer<C: ServerCallback + Send + Sync + 'static> {
	callback: Arc<C>,
	config: ServerConfig,
	connections: HashMap<ConnectionId, ConnectionHandler>,
	rx: mpsc::Receiver<ServerCommand>,
	#[cfg(any(feature = "native_tls", feature = "rustls"))]
	acceptor: Option<crate::tls::TlsAcceptor>,
	listener: TcpListener,
	pending: JoinSet<(SocketAddr, Result<Connection, Error>)>,
	deliveries: JoinSet<()>,
}

impl Server {
	pub async fn start<C: ServerCallback + Send + Sync + 'static>(
		config: ServerConfig,
		callback: C,
	) -> Result<Self, Error> {
		Self::start_with_listen_addr(config, callback).await.map(|(server, _)| server)
	}

	/// Like [`Self::start`], but also returns the socket address the TCP
	/// listener bound to (for example after setting
	/// [`crate::config::ServerConfig::port`] to `0` for an ephemeral port).
	pub async fn start_with_listen_addr<C: ServerCallback + Send + Sync + 'static>(
		config: ServerConfig,
		callback: C,
	) -> Result<(Self, SocketAddr), Error> {
		if config.command_capacity == 0 || config.rtu_queue_capacity == 0 {
			whatever!("Queue capacities must be nonzero");
		}
		let (tx, rx) = mpsc::channel(config.command_capacity);
		let inner_server = InnerServer::connect(config, Arc::new(callback), rx).await?;
		let listen_addr = inner_server
			.listen_local_addr()
			.with_whatever_context(|e| format!("Unable to read listener local address: {e}"))?;
		let (stop, mut stopped) = watch::channel(());
		let task = tokio::spawn(async move {
			let mut inner_server = inner_server;
			tokio::select! {
				biased;
				_ = stopped.changed() => {},
				result = inner_server.start() => {
					if let Err(e) = result { tracing::error!("Error in running server: {e}"); }
				}
			}
			inner_server.pending.shutdown().await;
			inner_server.deliveries.shutdown().await;
			for connection in inner_server.connections.values() {
				connection.abort();
			}
			for (_, mut connection) in inner_server.connections.drain() {
				connection.join().await;
			}
		});
		let lifecycle =
			Arc::new(ServerLifecycle { id: task.id(), stop, task: Mutex::new(Some(task)) });
		Ok((Self { tx, lifecycle }, listen_addr))
	}

	/// Stop accepting, cancel pending handshakes, I/O and callbacks, then join
	/// all server tasks. This is idempotent and applies to every clone.
	/// Queued output is discarded, not flushed. No callback runs after success.
	/// Cancelling this future still requests shutdown; a later call can join
	/// it. Dropping the last handle requests the same cleanup without
	/// waiting. Calling from any server-owned callback requests shutdown
	/// and returns an error; use [`Self::request_shutdown`] inside callbacks.
	pub async fn shutdown(&self) -> Result<(), Error> {
		self.request_shutdown();
		if tokio::task::try_id() == Some(self.lifecycle.id)
			|| CALLBACK_OWNER.try_with(|owner| *owner == self.lifecycle.id).unwrap_or(false)
		{
			whatever!("Shutdown requested from a server callback; join from an external task");
		}
		let mut task = self.lifecycle.task.lock().await;
		if let Some(handle) = task.as_mut() {
			let result = handle.await;
			*task = None;
			result.whatever_context("Server task failed during shutdown")?;
		}
		Ok(())
	}

	/// Request cancellation without joining. Safe to call inside a callback.
	/// An external task should call [`Self::shutdown`] to await cleanup.
	pub fn request_shutdown(&self) {
		self.lifecycle.stop.send_replace(());
	}

	/// Request closure of one connection. Cancellation is requested before
	/// success; server shutdown can be used to join all connection tasks.
	pub async fn disconnect(&self, id: ConnectionId) -> Result<(), error::ServerError> {
		let (tx, rx) = oneshot::channel();
		self.tx.send(ServerCommand::Disconnect { id, tx }).await.context(error::SendCommand)?;
		rx.await.context(error::ReceiveResponse)?
	}

	pub async fn send_asdu(&self, id: ConnectionId, asdu: Asdu) -> Result<(), error::ServerError> {
		let (tx, rx) = oneshot::channel();
		self.tx
			.send(ServerCommand::SingleConnection { id, asdu, guard: None, tx })
			.await
			.context(error::SendCommand)?;
		rx.await.context(error::ReceiveResponse)?
	}
	pub(crate) async fn send_asdu_guarded(
		&self,
		id: ConnectionId,
		asdu: Asdu,
		guard: watch::Receiver<bool>,
	) -> Result<(), error::ServerError> {
		let (tx, rx) = oneshot::channel();
		self.tx
			.send(ServerCommand::SingleConnection { id, asdu, guard: Some(guard), tx })
			.await
			.context(error::SendCommand)?;
		rx.await.context(error::ReceiveResponse)?
	}

	pub async fn broadcast_asdu(&self, asdu: Asdu) -> Result<(), error::ServerError> {
		let (tx, rx) = oneshot::channel();
		self.tx.send(ServerCommand::Broadcast { asdu, tx }).await.context(error::SendCommand)?;
		rx.await.context(error::ReceiveResponse)?
	}
}

impl<C: ServerCallback + Send + Sync + 'static> InnerServer<C> {
	async fn connect(
		config: ServerConfig,
		callback: Arc<C>,
		rx: mpsc::Receiver<ServerCommand>,
	) -> Result<Self, Error> {
		// Reject spec-violating k/w combinations (IEC 60870-5-104 §5.2)
		// before binding the socket so misconfiguration fails fast.
		config.protocol.validate().whatever_context("Invalid protocol configuration")?;
		if config.max_connections == 0
			|| config.tls_handshake_timeout.is_zero()
			|| config.delivery_timeout.is_zero()
		{
			whatever!("Server connection limit and TLS handshake timeout must be nonzero");
		}

		let bind_addr = format!("{}:{}", config.address, config.port);
		let listener = TcpListener::bind(&bind_addr)
			.await
			.with_whatever_context(|_| format!("Unable to bind to address '{bind_addr}'"))?;

		#[cfg(not(any(feature = "native_tls", feature = "rustls")))]
		if config.tls.is_some() {
			whatever!("TLS support is disabled; enable the `native_tls` or `rustls` Cargo feature");
		}

		#[cfg(any(feature = "native_tls", feature = "rustls"))]
		let acceptor = match &config.tls {
			None => None,
			Some(tls) => Some(crate::tls::build_server_acceptor(tls)?),
		};
		Ok(Self {
			callback,
			config,
			connections: HashMap::new(),
			rx,
			#[cfg(any(feature = "native_tls", feature = "rustls"))]
			acceptor,
			listener,
			pending: JoinSet::new(),
			deliveries: JoinSet::new(),
		})
	}

	fn listen_local_addr(&self) -> std::io::Result<SocketAddr> {
		self.listener.local_addr()
	}

	fn dispatch(&mut self, command: ServerCommand) {
		if let ServerCommand::Disconnect { id, tx } = command {
			let result = match self.connections.get(&id) {
				Some(peer) => {
					peer.abort();
					Ok(())
				}
				None => error::ConnectionNotFound { id }.fail(),
			};
			let _ = tx.send(result);
			return;
		}
		if self.deliveries.len() >= self.config.command_capacity {
			let tx = match command {
				ServerCommand::SingleConnection { tx, .. }
				| ServerCommand::Broadcast { tx, .. }
				| ServerCommand::Disconnect { tx, .. } => tx,
			};
			let _ = tx.send(error::Overloaded.fail());
			return;
		}
		let deadline = tokio::time::Instant::now() + self.config.delivery_timeout;
		let mut replies = Vec::new();
		let mut result = Ok(());
		let tx = match command {
			ServerCommand::Disconnect { .. } => return,
			ServerCommand::SingleConnection { id, asdu, guard, tx } => {
				match self.connections.get(&id) {
					Some(peer) => match peer.enqueue_asdu(asdu, guard) {
						Ok(rx) => replies.push((rx, peer.task.abort_handle())),
						Err(error) => result = Err(error),
					},
					None => result = error::ConnectionNotFound { id }.fail(),
				}
				tx
			}
			ServerCommand::Broadcast { asdu, tx } => {
				for peer in self.connections.values().filter(|peer| peer.is_started()) {
					match peer.enqueue_asdu(asdu.clone(), None) {
						Ok(rx) => replies.push((rx, peer.task.abort_handle())),
						Err(error) => {
							if result.is_ok() {
								result = Err(error);
							}
						}
					}
				}
				tx
			}
		};
		self.deliveries.spawn(async move {
			for (rx, abort) in replies {
				if let Err(error) = ConnectionHandler::wait_reply(rx, abort, deadline).await
					&& result.is_ok()
				{
					result = Err(error);
				}
			}
			let _ = tx.send(result);
		});
	}

	async fn start(&mut self) -> Result<(), Error> {
		let (closed_tx, mut closed_rx) = mpsc::unbounded_channel::<ConnectionId>();
		let mut next_id = ConnectionId::new();

		loop {
			tokio::select! {
				accept_result = self.listener.accept() => {
					let (socket, address) =
						accept_result.whatever_context("Error accepting a new connection")?;
					if self.connections.len() + self.pending.len() >= self.config.max_connections {
						drop(socket);
						continue;
					}
					#[cfg(any(feature = "native_tls", feature = "rustls"))]
					let acceptor = self.acceptor.clone();
					#[cfg(any(feature = "native_tls", feature = "rustls"))]
					let deadline = self.config.tls_handshake_timeout;
					self.pending.spawn(async move {
						let result = async {
							#[cfg(any(feature = "native_tls", feature = "rustls"))]
							if let Some(acceptor) = acceptor {
								let stream = tokio::time::timeout(deadline, acceptor.accept(socket))
									.await.whatever_context("TLS handshake timed out")?
									.whatever_context("TLS handshake failed")?;
								return Ok(Connection::Tls(Box::new(stream)));
							}
							Ok(Connection::Tcp(socket))
						}.await;
						(address, result)
					});
				}
				result = self.pending.join_next(), if !self.pending.is_empty() => {
					let Some(result) = result else { continue; };
					let (address, connection) = match result {
						Ok((address, Ok(connection))) => (address, connection),
						Ok((address, Err(error))) => {
							tracing::debug!(?address, %error, "Connection setup failed");
							continue;
						}
						Err(error) => {
							tracing::error!(%error, "Connection setup task failed");
							continue;
						}
					};
					// Gets the next available connection id
					while self.connections.contains_key(&next_id) {
						next_id = next_id.next();
					}

					let id = next_id;
					next_id = next_id.next();
					let callback = InnerServerCallback { id, address, callback: self.callback.clone(), state: Arc::new(AtomicConnectionState::new(ConnectionState::WaitingStartDT)) };
					let connection_handler = ConnectionHandler::start(id, address, connection, self.config.clone(), callback, closed_tx.clone());
					self.connections.insert(id, connection_handler);
				}
				closed_id = closed_rx.recv() => {
					if let Some(id) = closed_id {
						let connection_handler = self.connections.remove(&id);
						match connection_handler {
							Some(mut connection_handler) => {
								connection_handler.join().await;
								self.callback.on_connection_disconnected(id, connection_handler.address).await;
							}
							None => {
								tracing::error!("Connection {id:?} not found");
							}
						}
					}
				}
				_ = self.deliveries.join_next(), if !self.deliveries.is_empty() => {},
				command = self.rx.recv() => {
					let Some(command) = command else {
						tracing::error!("Error receiving command. Aborting...");
						whatever!("");
					};
					self.dispatch(command);
				}
			}
		}
	}
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod guarded_output_tests {
	use std::time::Duration;

	use tokio::{
		io::{AsyncReadExt, AsyncWriteExt},
		net::TcpStream,
	};

	use super::*;
	struct Capture(mpsc::UnboundedSender<ConnectionId>);
	#[async_trait::async_trait]
	impl ServerCallback for Capture {
		async fn on_new_objects(&self, _: Asdu, _: ConnectionId, _: SocketAddr) {}
		async fn on_connection_started(&self, id: ConnectionId, _: SocketAddr) {
			let _ = self.0.send(id);
		}
	}
	async fn frame(peer: &mut TcpStream) -> Vec<u8> {
		let mut h = [0; 2];
		peer.read_exact(&mut h).await.expect("header");
		let mut b = vec![0; usize::from(h[1]) + 2];
		b[..2].copy_from_slice(&h);
		peer.read_exact(&mut b[2..]).await.expect("body");
		b
	}
	#[tokio::test]
	async fn cancelled_guarded_output_is_not_replayed_after_startdt() {
		tokio::time::timeout(Duration::from_secs(3), async {
			let (tx, mut rx) = mpsc::unbounded_channel();
			let mut config = ServerConfig { port: 0, ..Default::default() };
			config.protocol.k = 3;
			config.protocol.w = 2;
			let (server, addr) =
				Server::start_with_listen_addr(config, Capture(tx)).await.expect("server");
			let mut peer = TcpStream::connect(addr).await.expect("peer");
			peer.write_all(&[0x68, 4, 7, 0, 0, 0]).await.expect("start");
			assert_eq!(frame(&mut peer).await, [0x68, 4, 11, 0, 0, 0]);
			let id = rx.recv().await.expect("id");
			let sample = Asdu::parse(&[1, 1, 3, 0, 1, 0, 1, 0, 0, 1]).expect("sample");
			for _ in 0..3 {
				server.send_asdu(id, sample.clone()).await.expect("fill window");
			}
			for seq in 0..3 {
				assert_eq!(frame(&mut peer).await[2], seq * 2);
			}
			let (active, guard) = watch::channel(true);
			server.send_asdu_guarded(id, sample, guard).await.expect("queued guarded output");
			active.send_replace(false);
			peer.write_all(&[0x68, 4, 0x13, 0, 0, 0, 0x68, 4, 1, 0, 6, 0, 0x68, 4, 7, 0, 0, 0])
				.await
				.expect("stop ack restart");
			assert_eq!(frame(&mut peer).await, [0x68, 4, 0x23, 0, 0, 0]);
			assert_eq!(frame(&mut peer).await, [0x68, 4, 11, 0, 0, 0]);
			rx.recv().await.expect("restarted");
			server
				.send_asdu(id, Asdu::parse(&[1, 1, 3, 0, 1, 0, 1, 0, 0, 0]).expect("fresh sample"))
				.await
				.expect("fresh send");
			let received = frame(&mut peer).await;
			assert_eq!(received[2], 6);
			assert_eq!(received[15], 0, "old guarded output was discarded");
			server.shutdown().await.expect("shutdown");
		})
		.await
		.expect("test deadline");
	}
}
