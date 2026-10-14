//! Bounded malformed-frame corpus and serialization invariants.
#![allow(missing_docs, clippy::expect_used)]
use iec104::{
	apdu::{Apdu, Frame, SFrame, UFrame},
	asdu::Asdu,
};
#[test]
fn declared_length_must_match_the_complete_frame() {
	for length in 0..=255 {
		let bytes = [0x68, length, 7, 0, 0, 0];
		assert_eq!(Apdu::from_bytes(&bytes).is_ok(), length == 4, "length {length}");
	}
	assert!(Apdu::from_bytes(&[0x68, 4, 7, 0, 0, 0, 0]).is_err());
	let frame = Frame::U(UFrame { start_dt_activation: true, ..Default::default() });
	assert!(Apdu { length: 5, frame }.to_bytes().is_err());
}
#[test]
fn reserved_control_bits_and_ambiguous_u_functions_are_rejected() {
	assert!(Apdu::from_bytes(&[0x68, 4, 1, 0, 1, 0]).is_err());
	for control in (3..=255).step_by(4) {
		assert_eq!(
			Apdu::from_bytes(&[0x68, 4, control, 0, 0, 0]).is_ok(),
			[7, 11, 19, 35, 67, 131].contains(&control),
			"control {control}"
		);
	}
	assert!(Frame::U(UFrame::default()).to_apdu_bytes().is_err());
	assert!(
		Frame::U(UFrame {
			start_dt_activation: true,
			stop_dt_activation: true,
			..Default::default()
		})
		.to_apdu_bytes()
		.is_err()
	);
	assert!(Frame::S(SFrame { receive_sequence_number: 32768 }).to_apdu_bytes().is_err());
}
#[test]
fn sequence_objects_round_trip_and_reject_address_overflow() {
	let bytes = [1, 0x82, 3, 0, 1, 0, 0xfe, 0xff, 0xff, 0, 1];
	let parsed = Asdu::parse(&bytes).expect("sequence");
	let mut encoded = Vec::new();
	parsed.to_bytes(&mut encoded).expect("encode");
	assert_eq!(encoded, bytes);
	let mut overflow = bytes;
	overflow[6] = 0xff;
	assert!(Asdu::parse(&overflow).is_err());
	let mut nonconsecutive =
		Asdu::parse(&[1, 2, 3, 0, 1, 0, 1, 0, 0, 0, 3, 0, 0, 1]).expect("explicit addresses");
	nonconsecutive.sequence = true;
	assert!(nonconsecutive.to_bytes(&mut Vec::new()).is_err());
}
#[test]
fn bounded_parser_corpus_never_panics() {
	let seeds: [&[u8]; 4] = [
		&[0x68, 4, 7, 0, 0, 0],
		&[0x68, 4, 1, 0, 0, 0],
		&[0x68, 14, 0, 0, 0, 0, 1, 1, 3, 0, 1, 0, 1, 0, 0, 1],
		&[0x68, 15, 0, 0, 0, 0, 1, 0x82, 3, 0, 1, 0, 1, 0, 0, 0, 1],
	];
	for seed in seeds {
		for split in 0..=seed.len() {
			let _ = Apdu::from_bytes(&seed[..split]);
			let _ = Asdu::parse(&seed[..split]);
		}
		for at in 0..seed.len() {
			for value in 0..=255 {
				let mut bytes = seed.to_vec();
				bytes[at] = value;
				let _ = Apdu::from_bytes(&bytes);
				let _ = Asdu::parse(&bytes);
			}
		}
	}
	let mut state = 0x7104_1234_u32;
	for n in 0..8192 {
		let mut bytes = vec![0; n % 256];
		for byte in &mut bytes {
			state ^= state << 13;
			state ^= state >> 17;
			state ^= state << 5;
			*byte = state as u8;
		}
		let _ = Apdu::from_bytes(&bytes);
		let _ = Asdu::parse(&bytes);
	}
}

#[test]
fn every_type_handles_bounded_vsq_and_short_payloads_without_panicking() {
	for tid in 0..=255 {
		for vsq in [0, 1, 2, 127, 128, 129, 130, 255] {
			for size in [0, 1, 2, 3, 4, 8, 16, 64, 249] {
				let mut bytes = vec![tid, vsq, 3, 0, 1, 0];
				bytes.resize(6 + size, 0);
				let _ = Asdu::parse(&bytes);
			}
		}
	}
}

#[test]
fn outgoing_objects_reject_truncated_addresses_and_mismatched_types() {
	let mut asdu = Asdu::parse(&[1, 1, 3, 0, 1, 0, 1, 0, 0, 1]).expect("sample");
	asdu.type_id = iec104::types_id::TypeId::M_DP_NA_1;
	assert!(asdu.to_bytes(&mut Vec::new()).is_err());
	asdu.type_id = iec104::types_id::TypeId::M_SP_NA_1;
	if let iec104::types::InformationObjects::MSpNa1(objects) = &mut asdu.information_objects {
		objects[0].address = 0x0100_0000;
	}
	assert!(asdu.to_bytes(&mut Vec::new()).is_err());
}
