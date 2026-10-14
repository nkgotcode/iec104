use std::sync::{Arc, atomic::AtomicBool};

use atomic_enum::atomic_enum;
use snafu::{OptionExt as _, ResultExt as _, whatever};
use tokio::{
	io::{ReadHalf, WriteHalf},
	net::TcpStream,
	sync::mpsc,
};
use tracing::instrument;

#[cfg(any(feature = "native_tls", feature = "rustls"))]
use crate::tls::TlsClientConnector;
use crate::{
	START_DT_ACT_FRAME,
	apdu::{Frame, MAX_APDU_FRAME_SIZE},
	client::{ClientCallback, Connection, InnerClientCallback},
	config::ClientConfig,
	error::Error,
	receive_handler::{ReceiveHandler, ReceiveHandlerCommand, receive_apdu, send_frame},
};

#[atomic_enum]
#[derive(PartialEq)]
pub enum ConnectionHandlerState {
	WaitingForStart,
	Starting,
	Started,
	Reconnecting,
}

pub struct ConnectionHandler<C: ClientCallback + Send + Sync + 'static> {
	connection: Option<(ReadHalf<Connection>, WriteHalf<Connection>)>,
	callback: Arc<InnerClientCallback<C>>,
	config: ClientConfig,
	state: Arc<AtomicConnectionHandlerState>,
	rx: mpsc::Receiver<ReceiveHandlerCommand>,
	out_buffer_full: Arc<AtomicBool>,
}

impl<C: ClientCallback + Send + Sync + 'static> ConnectionHandler<C> {
	pub async fn new(
		callback: Arc<InnerClientCallback<C>>,
		config: ClientConfig,
		rx: mpsc::Receiver<ReceiveHandlerCommand>,
		out_buffer_full: Arc<AtomicBool>,
	) -> Result<Self, Error> {
		let connection =
			Self::make_connection(&config).await.whatever_context("Error making connection")?;
		let (read_connection, write_connection) = tokio::io::split(connection);
		callback
			.state
			.store(ConnectionHandlerState::WaitingForStart, std::sync::atomic::Ordering::Relaxed);
		out_buffer_full.store(false, std::sync::atomic::Ordering::Relaxed);
		Ok(Self {
			callback: callback.clone(),
			config,
			state: callback.state.clone(),
			connection: Some((read_connection, write_connection)),
			rx,
			out_buffer_full,
		})
	}

	pub fn get_state(&self) -> Arc<AtomicConnectionHandlerState> {
		self.state.clone()
	}

	#[instrument(level = "debug", skip_all)]
	pub async fn run(&mut self) -> Result<(), Error> {
		loop {
			match self.state.load(std::sync::atomic::Ordering::Relaxed) {
				ConnectionHandlerState::WaitingForStart => {
					if let Some(cmd) = self.rx.recv().await {
						match cmd {
							ReceiveHandlerCommand::Start { session }
								if session
									== self
										.callback
										.session
										.load(std::sync::atomic::Ordering::SeqCst) =>
							{
								self.state.store(
									ConnectionHandlerState::Starting,
									std::sync::atomic::Ordering::Relaxed,
								);
							}
							_ => {
								tracing::error!("Received unexpected command: {cmd:?}");
							}
						}
					} else {
						tracing::error!("Error receiving command. Aborting...");
						whatever!("Error receiving command.");
					}
				}
				ConnectionHandlerState::Starting => {
					tracing::debug!("Starting");
					if let Err(e) = self.send_start_dt().await {
						tracing::error!("Error sending startDT: {e}. Reconnecting");
						self.state.store(
							ConnectionHandlerState::Reconnecting,
							std::sync::atomic::Ordering::Relaxed,
						);
						self.connection = None;
						self.callback.callback.on_connection_disconnected().await;
						self.callback.on_error(e).await;
						continue;
					}
					tracing::debug!("StartDT activation confirmed");
					self.state.store(
						ConnectionHandlerState::Started,
						std::sync::atomic::Ordering::Relaxed,
					);
				}
				ConnectionHandlerState::Started => {
					self.callback.on_connection_started().await;
					let (read, write) =
						self.connection.as_mut().whatever_context("Connection missing")?;
					if let Err(e) = ReceiveHandler::new(
						read,
						write,
						self.callback.clone(),
						self.config.protocol.clone(),
						&mut self.rx,
						self.out_buffer_full.clone(),
						Some(self.callback.session.load(std::sync::atomic::Ordering::SeqCst)),
					)
					.receive_task()
					.await
					{
						tracing::error!("Error receiving task: {e}. Reconnecting");
						self.state.store(
							ConnectionHandlerState::Reconnecting,
							std::sync::atomic::Ordering::Relaxed,
						);
						self.connection = None;
						self.callback.callback.on_connection_disconnected().await;
						self.callback.on_error(e).await;
						continue;
					}
					tracing::debug!("Received a stop. Going back to waiting for start");
					self.state.store(
						ConnectionHandlerState::WaitingForStart,
						std::sync::atomic::Ordering::Relaxed,
					);
					self.callback.on_connection_stopped().await;
				}
				ConnectionHandlerState::Reconnecting => {
					tracing::debug!("Reconnecting");
					self.callback.on_reconnecting().await;
					let Ok(connection) = Self::make_connection(&self.config).await else {
						tracing::error!("Error making connection");
						tokio::time::sleep(self.config.protocol.t0).await;
						continue;
					};
					self.connection = Some(tokio::io::split(connection));
					self.callback.session.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
					self.out_buffer_full.store(false, std::sync::atomic::Ordering::Relaxed);
					self.state.store(
						ConnectionHandlerState::Starting,
						std::sync::atomic::Ordering::Relaxed,
					);
				}
			}
		}
	}

	#[instrument(level = "debug")]
	async fn make_connection(config: &ClientConfig) -> Result<Connection, Error> {
		let stream = tokio::time::timeout(
			config.protocol.t0,
			TcpStream::connect(format!("{}:{}", config.address, config.port)),
		)
		.await
		.whatever_context("Connection timeout")?
		.whatever_context("Error connecting")?;

		match &config.tls {
			None => Ok(Connection::Tcp(stream)),
			#[cfg(any(feature = "native_tls", feature = "rustls"))]
			Some(tls) => {
				let connector = crate::tls::build_client_connector(tls)?;
				Ok(Connection::Tls(Box::new(
					tokio::time::timeout(
						config.protocol.t0,
						connector.connect(&config.address, stream),
					)
					.await
					.whatever_context("TLS handshake timed out")?
					.whatever_context("Error connecting")?,
				)))
			}
			#[cfg(not(any(feature = "native_tls", feature = "rustls")))]
			Some(_) => {
				whatever!(
					"TLS support is disabled; enable the `native_tls` or `rustls` Cargo feature"
				);
			}
		}
	}

	#[instrument(level = "debug", skip_all)]
	pub async fn send_start_dt(&mut self) -> Result<(), Error> {
		let (read, write) = self.connection.as_mut().whatever_context("Connection missing")?;
		let mut buffer = [0; MAX_APDU_FRAME_SIZE];
		send_frame(write, &START_DT_ACT_FRAME)
			.await
			.whatever_context("Error sending startDT activation")?;

		let apdu = tokio::time::timeout(self.config.protocol.t1, receive_apdu(read, &mut buffer))
			.await
			.whatever_context("Timeout waiting for startDT activation")?;

		let apdu = apdu.whatever_context("Error receiving APDU")?;
		// Per IEC 60870-5-104 §5.3, only a U-frame with the
		// startDT-confirmation flag set is a valid response. I/S frames
		// or any other U-frame variant is a protocol violation.
		match apdu.frame {
			Frame::U(u) if u.start_dt_confirmation => Ok(()),
			frame => {
				whatever!("StartDT activation not confirmed; received {frame:?}");
			}
		}
	}
}
