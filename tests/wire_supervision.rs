//! Independent wire checks for timers, windows, wrapping and large
//! interrogation.
#![allow(missing_docs, clippy::expect_used)]
use std::{collections::HashSet, net::SocketAddr, sync::Arc, time::Duration};

use async_trait::async_trait;
use iec104::{
	asdu::Asdu,
	config::{ProtocolConfig, ServerConfig},
	rtu_server::{
		PointAddress, PointValue, RejectAllCommands, RtuInitialPoint, RtuSystemHandlers,
		start_rtu_server_ephemeral,
	},
	server::{ConnectionId, Server, ServerCallback},
	types::{MMeNc1, MSpNa1},
};
use tokio::{
	io::{AsyncReadExt, AsyncWriteExt},
	net::TcpStream,
	sync::mpsc,
	time::timeout,
};
const WAIT: Duration = Duration::from_secs(3);
struct Ingress(mpsc::UnboundedSender<()>);
#[async_trait]
impl ServerCallback for Ingress {
	async fn on_new_objects(&self, _: Asdu, _: ConnectionId, _: SocketAddr) {
		let _ = self.0.send(());
	}
}
async fn start(config: ProtocolConfig) -> (Server, TcpStream, mpsc::UnboundedReceiver<()>) {
	let (tx, rx) = mpsc::unbounded_channel();
	let (server, addr) = Server::start_with_listen_addr(
		ServerConfig { port: 0, protocol: config, ..Default::default() },
		Ingress(tx),
	)
	.await
	.expect("server");
	let mut peer = TcpStream::connect(addr).await.expect("connect");
	peer.write_all(&[0x68, 4, 7, 0, 0, 0]).await.expect("start");
	assert_eq!(frame(&mut peer).await, [0x68, 4, 11, 0, 0, 0]);
	(server, peer, rx)
}
async fn frame(peer: &mut TcpStream) -> Vec<u8> {
	timeout(WAIT, async {
		let mut head = [0; 2];
		peer.read_exact(&mut head).await.expect("header");
		assert_eq!(head[0], 0x68);
		assert!((4..=253).contains(&head[1]));
		let mut bytes = vec![0; usize::from(head[1]) + 2];
		bytes[..2].copy_from_slice(&head);
		peer.read_exact(&mut bytes[2..]).await.expect("payload");
		bytes
	})
	.await
	.expect("frame deadline")
}
fn sample() -> Asdu {
	Asdu::parse(&[1, 1, 3, 0, 1, 0, 1, 0, 0, 1]).expect("sample")
}
const fn incoming(ns: u16) -> [u8; 16] {
	let n = (ns << 1).to_le_bytes();
	[0x68, 14, n[0], n[1], 0, 0, 1, 1, 3, 0, 1, 0, 1, 0, 0, 1]
}
async fn ack(peer: &mut TcpStream, nr: u16) {
	let n = (nr << 1).to_le_bytes();
	peer.write_all(&[0x68, 4, 1, 0, n[0], n[1]]).await.expect("ack");
}
#[tokio::test]
async fn last_ack_disarms_t1_before_the_next_send() {
	let (server, mut peer, _rx) =
		start(ProtocolConfig { t1: Duration::from_millis(200), ..Default::default() }).await;
	server.broadcast_asdu(sample()).await.expect("first send");
	assert_eq!(frame(&mut peer).await[2], 0);
	ack(&mut peer, 1).await;
	peer.write_all(&[0x68, 4, 0x43, 0, 0, 0]).await.expect("barrier");
	assert_eq!(frame(&mut peer).await[2], 0x83);
	tokio::time::sleep(Duration::from_millis(350)).await;
	server.broadcast_asdu(sample()).await.expect("send after old t1");
	assert_eq!(frame(&mut peer).await[2], 2);
	ack(&mut peer, 2).await;
	server.shutdown().await.expect("shutdown");
}
#[tokio::test]
async fn additional_input_does_not_postpone_the_first_t2_deadline() {
	let (server, mut peer, mut rx) =
		start(ProtocolConfig { t2: Duration::from_millis(600), ..Default::default() }).await;
	peer.write_all(&incoming(0)).await.expect("first");
	timeout(WAIT, rx.recv()).await.expect("first admitted").expect("event");
	tokio::time::sleep(Duration::from_millis(400)).await;
	peer.write_all(&incoming(1)).await.expect("second");
	timeout(WAIT, rx.recv()).await.expect("second admitted").expect("event");
	let response = timeout(Duration::from_millis(350), frame(&mut peer))
		.await
		.expect("first receive deadline retained");
	assert_eq!(response[2], 1);
	let nr = u16::from_le_bytes([response[4], response[5]]) >> 1;
	assert!((1..=2).contains(&nr));
	server.shutdown().await.expect("shutdown");
}
#[tokio::test]
async fn testfr_without_confirmation_closes_the_peer() {
	let (server, mut peer, _rx) = start(ProtocolConfig {
		t3: Duration::from_millis(200),
		t1: Duration::from_millis(120),
		..Default::default()
	})
	.await;
	assert_eq!(frame(&mut peer).await, [0x68, 4, 0x43, 0, 0, 0]);
	let mut byte = [0];
	assert_eq!(
		timeout(WAIT, peer.read(&mut byte)).await.expect("closure deadline").expect("read"),
		0
	);
	server.shutdown().await.expect("shutdown");
}
#[tokio::test]
async fn saturated_output_recovers_without_replaying_rejected_work() {
	let (server, mut peer, _rx) =
		start(ProtocolConfig { k: 3, w: 2, max_pending_outgoing_asdu: 2, ..Default::default() })
			.await;
	for _ in 0..5 {
		server.broadcast_asdu(sample()).await.expect("admitted output");
	}
	assert!(server.broadcast_asdu(sample()).await.is_err());
	for n in 0..3 {
		assert_eq!(frame(&mut peer).await[2], n * 2);
	}
	ack(&mut peer, 3).await;
	for n in 3..5 {
		assert_eq!(frame(&mut peer).await[2], n * 2);
	}
	ack(&mut peer, 5).await;
	peer.write_all(&[0x68, 4, 0x43, 0, 0, 0]).await.expect("barrier");
	assert_eq!(frame(&mut peer).await[2], 0x83);
	server.broadcast_asdu(sample()).await.expect("recovered output");
	assert_eq!(frame(&mut peer).await[2], 10);
	server.shutdown().await.expect("shutdown");
}
#[tokio::test]
async fn receive_sequence_wraps_over_a_real_connection() {
	let (server, mut peer, mut rx) =
		start(ProtocolConfig { k: 24576, w: 16384, ..Default::default() }).await;
	let mut batch = Vec::with_capacity(32770 * 16);
	for n in 0..32770_u32 {
		batch.extend_from_slice(&incoming((n % 32768) as u16));
	}
	peer.write_all(&batch).await.expect("batch");
	for nr in [16384, 0] {
		let response = frame(&mut peer).await;
		assert_eq!(response[2], 1);
		assert_eq!(u16::from_le_bytes([response[4], response[5]]) >> 1, nr);
	}
	timeout(WAIT, async {
		for _ in 0..32770 {
			rx.recv().await.expect("ingress");
		}
	})
	.await
	.expect("all input admitted");
	server.broadcast_asdu(sample()).await.expect("piggyback ack");
	let response = frame(&mut peer).await;
	assert_eq!(u16::from_le_bytes([response[4], response[5]]) >> 1, 2);
	server.shutdown().await.expect("shutdown");
}
#[tokio::test]
async fn large_mixed_interrogation_obeys_frame_limits_and_contains_every_point() {
	let mut points = Vec::<RtuInitialPoint>::new();
	for n in 1..=100 {
		points.push(
			(
				PointAddress::new(1, n),
				PointValue::MMeNc1(MMeNc1 { value: n as f32, ..Default::default() }),
			)
				.into(),
		);
	}
	for n in 1001..=1200 {
		points.push((PointAddress::new(1, n), PointValue::MSpNa1(MSpNa1::default())).into());
	}
	let (rtu, addr) = start_rtu_server_ephemeral(
		ServerConfig::default(),
		points,
		Arc::new(RejectAllCommands),
		RtuSystemHandlers::monitoring_only(),
	)
	.await
	.expect("rtu");
	let mut peer = TcpStream::connect(addr).await.expect("connect");
	peer.write_all(&[0x68, 4, 7, 0, 0, 0]).await.expect("start");
	assert_eq!(frame(&mut peer).await[2], 11);
	assert_eq!(frame(&mut peer).await[6], 70);
	peer.write_all(&[0x68, 14, 0, 0, 2, 0, 100, 1, 6, 0, 1, 0, 0, 0, 0, 20]).await.expect("GI");
	let mut expected_ns = 1_u16;
	let mut addresses = HashSet::new();
	let mut confirmed = false;
	loop {
		let bytes = frame(&mut peer).await;
		if bytes[2] & 1 != 0 {
			continue;
		}
		assert_eq!(u16::from_le_bytes([bytes[2], bytes[3]]) >> 1, expected_ns);
		expected_ns += 1;
		ack(&mut peer, expected_ns).await;
		let a = &bytes[6..];
		let cot = a[2] & 0x3f;
		if a[0] == 100 {
			if cot == 7 {
				confirmed = true;
				continue;
			}
			assert_eq!(cot, 10);
			assert!(confirmed);
			break;
		}
		assert!(confirmed);
		assert_eq!(cot, 20);
		assert_eq!(a[1] & 0x80, 0);
		let size = match a[0] {
			1 => 1,
			13 => 5,
			other => panic!("unexpected type {other}"),
		};
		assert_eq!(a.len(), 6 + usize::from(a[1]) * (3 + size));
		for object in a[6..].chunks_exact(3 + size) {
			let ioa = u32::from_le_bytes([object[0], object[1], object[2], 0]);
			assert!(addresses.insert(ioa));
			if a[0] == 13 {
				assert_eq!(&object[3..7], &(ioa as f32).to_le_bytes());
				assert_eq!(object[7], 0);
			} else {
				assert_eq!(object[3], 0);
			}
		}
	}
	assert_eq!(addresses, (1..=100).chain(1001..=1200).collect());
	rtu.shutdown().await.expect("shutdown");
}
