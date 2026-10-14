use snafu::Snafu;
use tokio::sync::{mpsc, oneshot};

use crate::{
	error::SpanTraceWrapper,
	receive_handler::ReceiveHandlerCommand,
	server::{ConnectionId, ServerCommand},
};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub), context(suffix(false)))]
pub enum ServerError {
	/// Server or per-peer command admission is full; caller may back off.
	#[snafu(display("Command queue is full"))]
	Overloaded,
	/// Data transfer has not started on this connection.
	#[snafu(display("Connection has not started data transfer"))]
	NotStarted,
	/// Peer failed to accept output within the configured deadline and was
	/// closed.
	#[snafu(display("Peer output deadline exceeded"))]
	DeliveryTimedOut,

	///Failed to start server
	Start {
		source: std::io::Error,
		#[snafu(implicit)]
		context: Box<SpanTraceWrapper>,
	},
	/// Failed to send command
	SendCommand {
		source: mpsc::error::SendError<ServerCommand>,
		#[snafu(implicit)]
		context: Box<SpanTraceWrapper>,
	},
	/// Failed to receive response
	ReceiveResponse {
		source: oneshot::error::RecvError,
		#[snafu(implicit)]
		context: Box<SpanTraceWrapper>,
	},
	/// Connection not found
	ConnectionNotFound {
		id: ConnectionId,
		#[snafu(implicit)]
		context: Box<SpanTraceWrapper>,
	},
	/// Failed to send connection command
	SendConnectionCommand {
		source: mpsc::error::SendError<ReceiveHandlerCommand>,
		#[snafu(implicit)]
		context: Box<SpanTraceWrapper>,
	},
	/// Receive-handler task ended or dropped before acknowledging an ASDU
	/// enqueue.
	ReceiveHandlerAsduAck {
		source: oneshot::error::RecvError,
		#[snafu(implicit)]
		context: Box<SpanTraceWrapper>,
	},
	/// Per-connection pending outgoing queue is full
	/// ([`crate::config::ProtocolConfig::max_pending_outgoing_asdu`]).
	PendingOutgoingAsduBufferFull {
		#[snafu(implicit)]
		context: Box<SpanTraceWrapper>,
	},
}
