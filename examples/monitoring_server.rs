//! Explicit command policy for a station that publishes a read-only model.
use std::sync::Arc;

use iec104::{
	config::ServerConfig,
	rtu_server::{
		PointAddress, PointValue, RejectAllCommands, RtuInitialPoint, RtuServer, RtuSystemHandlers,
	},
	types::{MItNa1, quality_descriptors::SeqQd},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
	let points = [RtuInitialPoint::new(
		PointAddress::new(1, 1),
		PointValue::MItNa1(MItNa1 { bcr: 42, sqd: SeqQd::default() }),
	)];
	// Explicitly reject process commands, clock sync, reset and counter
	// freeze/reset. Read requests and interrogation use the current
	// in-memory values.
	let server = RtuServer::start_with_system_handlers(
		ServerConfig::default(),
		points,
		Arc::new(RejectAllCommands),
		RtuSystemHandlers::monitoring_only(),
	)
	.await?;
	tokio::signal::ctrl_c().await?;
	server.shutdown().await?;
	Ok(())
}
