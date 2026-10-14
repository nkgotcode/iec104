use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use crate::{
	Connection, asdu::Asdu, config::ServerConfig, error::Error,
	receive_handler::ReceiveHandlerCallback, server::connection_handler::ConnectionHandler,
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

#[async_trait::async_trait]
#[expect(
	clippy::double_must_use,
	reason = "async-trait 0.1.89 generates redundant must_use on boxed futures"
)]
pub trait ServerCallback {
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
	async fn on_error(&self, error: &Error) {
		tracing::debug!("Error: {error}");
	}
}

struct InnerServerCallback<C: ServerCallback + Send + Sync> {
	id: ConnectionId,
	address: SocketAddr,
	callback: Arc<C>,
}

#[async_trait::async_trait]
impl<C: ServerCallback + Send + Sync> ReceiveHandlerCallback for InnerServerCallback<C> {
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
	SingleConnection {
		id: ConnectionId,
		asdu: Asdu,
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
	/// waiting. Calling from a callback on the server task requests shutdown
	/// and returns an error; use [`Self::request_shutdown`] inside callbacks.
	pub async fn shutdown(&self) -> Result<(), Error> {
		self.request_shutdown();
		if tokio::task::try_id() == Some(self.lifecycle.id) {
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

	pub async fn send_asdu(&self, id: ConnectionId, asdu: Asdu) -> Result<(), error::ServerError> {
		let (tx, rx) = oneshot::channel();
		self.tx
			.send(ServerCommand::SingleConnection { id, asdu, tx })
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
		if self.deliveries.len() >= self.config.command_capacity {
			let tx = match command {
				ServerCommand::SingleConnection { tx, .. }
				| ServerCommand::Broadcast { tx, .. } => tx,
			};
			let _ = tx.send(error::Overloaded.fail());
			return;
		}
		let deadline = tokio::time::Instant::now() + self.config.delivery_timeout;
		let mut replies = Vec::new();
		let mut result = Ok(());
		let tx = match command {
			ServerCommand::SingleConnection { id, asdu, tx } => {
				match self.connections.get(&id) {
					Some(peer) => match peer.enqueue_asdu(asdu) {
						Ok(rx) => replies.push((rx, peer.task.abort_handle())),
						Err(error) => result = Err(error),
					},
					None => result = error::ConnectionNotFound { id }.fail(),
				}
				tx
			}
			ServerCommand::Broadcast { asdu, tx } => {
				for peer in self.connections.values().filter(|peer| peer.is_started()) {
					match peer.enqueue_asdu(asdu.clone()) {
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
					// TODO: Do we want to accept or decline the connection based on the callback response?
					self.callback.on_new_connection(address).await;


					// Gets the next available connection id
					while self.connections.contains_key(&next_id) {
						next_id = next_id.next();
					}

					let id = next_id;
					next_id = next_id.next();
					let callback = InnerServerCallback { id, address, callback: self.callback.clone() };
					let connection_handler = ConnectionHandler::start(id, address, connection, self.config.clone(), callback, closed_tx.clone());
					self.connections.insert(id, connection_handler);
				}
				closed_id = closed_rx.recv() => {
					if let Some(id) = closed_id {
						let connection_handler = self.connections.remove(&id);
						match connection_handler {
							Some(mut connection_handler) => {
								connection_handler.join().await;
								self.callback.on_connection_stopped(id, connection_handler.address).await;
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
