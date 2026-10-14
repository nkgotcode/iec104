use std::{
	net::SocketAddr,
	sync::{Arc, atomic::AtomicBool},
};

use atomic_enum::atomic_enum;
use snafu::{ResultExt as _, whatever};
use tokio::{
	io::{ReadHalf, WriteHalf},
	sync::{mpsc, oneshot},
};

use super::ConnectionId;
use crate::{
	Connection, START_DT_CON_FRAME, TEST_FR_CON_FRAME,
	apdu::{Frame, MAX_APDU_FRAME_SIZE},
	asdu::Asdu,
	config::ServerConfig,
	error::Error,
	receive_handler::{
		ReceiveHandler, ReceiveHandlerCommand, SendAsduQueueError, receive_apdu, send_frame,
	},
	server::{InnerServerCallback, ServerCallback, error},
};

#[atomic_enum]
#[derive(PartialEq)]
pub enum ConnectionState {
	WaitingStartDT,
	Started,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct ConnectionHandler {
	pub id: ConnectionId,
	pub address: SocketAddr,
	pub task: tokio::task::JoinHandle<()>,
	pub tx: mpsc::Sender<ReceiveHandlerCommand>,
	pub state: Arc<AtomicConnectionState>,
}

impl Drop for ConnectionHandler {
	fn drop(&mut self) {
		self.task.abort();
	}
}

struct ClosedNotification {
	id: ConnectionId,
	tx: mpsc::UnboundedSender<ConnectionId>,
}
impl Drop for ClosedNotification {
	fn drop(&mut self) {
		let _ = self.tx.send(self.id);
	}
}

struct InnerConnectionHandler<C: ServerCallback + Send + Sync + 'static> {
	read_connection: ReadHalf<Connection>,
	write_connection: WriteHalf<Connection>,
	callback: Arc<InnerServerCallback<C>>,
	config: ServerConfig,
	rx: mpsc::Receiver<ReceiveHandlerCommand>,
	id: ConnectionId,
	address: SocketAddr,
	state: Arc<AtomicConnectionState>,
}

impl ConnectionHandler {
	pub fn start<C: ServerCallback + Send + Sync + 'static>(
		id: ConnectionId,
		address: SocketAddr,
		connection: Connection,
		config: ServerConfig,
		callback: InnerServerCallback<C>,
		tx: mpsc::UnboundedSender<ConnectionId>,
	) -> Self {
		let (read_connection, write_connection) = tokio::io::split(connection);
		let (cmd_tx, rx) = mpsc::channel(config.command_capacity);
		let state = callback.state.clone();
		let inner_connection_handler = InnerConnectionHandler {
			read_connection,
			write_connection,
			callback: Arc::new(callback),
			config,
			rx,
			id,
			address,
			state: state.clone(),
		};
		let closed = ClosedNotification { id, tx };
		let owner = tokio::task::id();
		let task = tokio::spawn(super::CALLBACK_OWNER.scope(owner, async move {
			let _closed = closed;
			let _ = inner_connection_handler
				.start()
				.await
				.inspect_err(|e| tracing::error!("Error in running connection handler: {e}"));
		}));
		Self { id, address, task, tx: cmd_tx, state }
	}
	pub fn abort(&self) {
		self.task.abort();
	}
	pub async fn join(&mut self) {
		if let Err(error) = (&mut self.task).await
			&& !error.is_cancelled()
		{
			tracing::error!(%error, "Connection task failed");
		}
	}

	pub fn enqueue_asdu(
		&self,
		asdu: Asdu,
	) -> Result<oneshot::Receiver<Result<(), SendAsduQueueError>>, error::ServerError> {
		if !self.is_started() {
			return error::NotStarted.fail();
		}
		let (reply_tx, reply_rx) = oneshot::channel();
		match self.tx.try_send(ReceiveHandlerCommand::Asdu { asdu, session: None, reply: reply_tx })
		{
			Ok(()) => Ok(reply_rx),
			Err(mpsc::error::TrySendError::Full(_)) => error::Overloaded.fail(),
			Err(mpsc::error::TrySendError::Closed(cmd)) => {
				Err(mpsc::error::SendError(cmd)).context(error::SendConnectionCommand)
			}
		}
	}

	pub async fn wait_reply(
		rx: oneshot::Receiver<Result<(), SendAsduQueueError>>,
		abort: tokio::task::AbortHandle,
		deadline: tokio::time::Instant,
	) -> Result<(), error::ServerError> {
		match tokio::time::timeout_at(deadline, rx).await {
			Ok(result) => match result.context(error::ReceiveHandlerAsduAck)? {
				Ok(()) => Ok(()),
				Err(SendAsduQueueError::NotStarted) => error::NotStarted.fail(),
				Err(SendAsduQueueError::PendingBufferFull) => {
					error::PendingOutgoingAsduBufferFull.fail()
				}
			},
			Err(_) => {
				abort.abort();
				error::DeliveryTimedOut.fail()
			}
		}
	}

	pub fn is_started(&self) -> bool {
		self.state.load(std::sync::atomic::Ordering::Relaxed) == ConnectionState::Started
	}
}

impl<C: ServerCallback + Send + Sync + 'static> InnerConnectionHandler<C> {
	pub async fn start(mut self) -> Result<(), Error> {
		let mut buffer = [0; MAX_APDU_FRAME_SIZE];
		let out_buffer_full = Arc::new(AtomicBool::new(false));

		loop {
			match self.state.load(std::sync::atomic::Ordering::Relaxed) {
				ConnectionState::WaitingStartDT => {
					let apdu = tokio::time::timeout(
						self.config.protocol.t1,
						receive_apdu(&mut self.read_connection, &mut buffer),
					)
					.await
					.whatever_context("Timeout waiting for startDT activation")?
					.whatever_context("Error receiving APDU")?;

					// Per IEC 60870-5-104 §5.3, before data transfer begins
					// only U-frames are valid. StartDT-ACT
					// advances to the started
					// state; TestFR-ACT must always be answered with TestFR-CON
					// regardless of state. Anything else is a protocol
					// violation and closes the connection.
					match apdu.frame {
						Frame::U(u) if u.start_dt_activation => {
							send_frame(&mut self.write_connection, &START_DT_CON_FRAME)
								.await
								.whatever_context("Error sending startDT confirmation")?;
							self.state.store(
								ConnectionState::Started,
								std::sync::atomic::Ordering::Relaxed,
							);
						}
						Frame::U(u) if u.test_fr_activation => {
							send_frame(&mut self.write_connection, &TEST_FR_CON_FRAME)
								.await
								.whatever_context("Error sending testFR confirmation")?;
						}
						_ => {
							whatever!(
								"Unexpected frame while waiting for startDT activation: {:?}",
								apdu.frame
							);
						}
					}
				}
				ConnectionState::Started => {
					self.callback.on_connection_started(self.id, self.address).await?;
					let rcv_handler = ReceiveHandler::new(
						&mut self.read_connection,
						&mut self.write_connection,
						self.callback.clone(),
						self.config.protocol.clone(),
						&mut self.rx,
						out_buffer_full.clone(),
						None,
					);
					if let Err(e) = rcv_handler.receive_task().await {
						self.callback.on_error(&e).await;
						return Err(e);
					}
					self.state.store(
						ConnectionState::WaitingStartDT,
						std::sync::atomic::Ordering::Relaxed,
					);
				}
			}
		}
	}
}
