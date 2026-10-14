//! [`InformationObjects`]: `TypeId` ↔ wire-type mapping for parsed ASDU payloads.
//!
//! RTU-specific [`crate::rtu_server::PointValue`] lives in the RTU layer; keep
//! its `TypeId` / payload pairs aligned with the matching rows here when you
//! extend either side.

use tracing::instrument;

use super::{
	CBoNa1, CBoTa1, CCdNa1, CCiNa1, CCsNa1, CIcNa1, CRdNa1, CRpNa1, CScNa1, CScTa1, CSeNa1, CSeNb1,
	CSeNc1, CSeTa1, CSeTb1, CSeTc1, CTsNa1, CTsTa1, CdcNa1, CdcTa1, CrcNa1, CrcTa1, FromBytes,
	GenericObject, MBoNa1, MBoTb1, MDpNa1, MDpTa1, MDpTb1, MEiNa1, MEpTa1, MEpTb1, MEpTc1, MEpTd1,
	MEpTe1, MEpTf1, MItNa1, MItTb1, MMeNa1, MMeNb1, MMeNc1, MMeNd1, MMeTa1, MMeTb1, MMeTc1, MMeTd1,
	MMeTe1, MMeTf1, MPsNa1, MSpNa1, MSpTa1, MSpTb1, MStNa1, MStTa1, MStTb1, NotEnoughBytes,
	NotImplemented, PAcNa1, PMeNa1, PMeNb1, PMeNc1, ParseError, RawObject, ToBytes,
};
use crate::types_id::TypeId;

/// Information object address (IOA) size on the wire.
pub(crate) const ADDRESS_SIZE: usize = 3;

macro_rules! define_information_objects {
	(
		$(
			$type_id:ident => $variant:ident($payload:ty) ;
		)*
	) => {
		#[derive(Debug, Clone, PartialEq)]
		pub enum InformationObjects {
			$( $variant(Vec<GenericObject<$payload>>), )*
			Raw(Vec<GenericObject<RawObject>>),
		}

		impl InformationObjects {
			#[instrument]
			fn build_objects<T: FromBytes + ToBytes + Default>(
				type_id: TypeId,
				sequence: bool,
				num_objs: u8,
				bytes: &[u8],
			) -> Result<Vec<GenericObject<T>>, ParseError> {
				if !type_id.is_standard() {
					tracing::trace!("Building RAW information objects. Bytes: {:?}", bytes);
					if bytes.len() < ADDRESS_SIZE {
						return NotEnoughBytes.fail();
					}
					let address = u32::from_be_bytes([0, bytes[2], bytes[1], bytes[0]]);
					let object = T::from_bytes(&bytes[ADDRESS_SIZE..])?;
					return Ok(vec![GenericObject { address, object }]);
				}
				let object_size = type_id.size();
				tracing::trace!(
					"Building information objects. Object size: {object_size}. Bytes: {:?}",
					bytes
				);

				let expected = if sequence {
					ADDRESS_SIZE + usize::from(num_objs) * object_size
				} else {
					usize::from(num_objs) * (ADDRESS_SIZE + object_size)
				};
				if bytes.len() != expected || (sequence && num_objs == 0) {
					return NotEnoughBytes.fail();
				}
				if sequence {
					let first_addr = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], 0]);
					if first_addr + u32::from(num_objs) - 1 > 0x00ff_ffff {
						return super::InvalidAddress.fail();
					}
					// Index by count rather than chunks_exact: read-command objects
					// have zero payload bytes and must never cause a zero-chunk panic.
					(0..num_objs).map(|i| {
						let start = ADDRESS_SIZE + usize::from(i) * object_size;
						Ok(GenericObject {
							address: first_addr + u32::from(i),
							object: T::from_bytes(&bytes[start..start + object_size])?,
						})
					}).collect::<Result<Vec<_>, ParseError>>()
				} else {
					// `chunks_exact` rejects a short trailing chunk; pre-fix
					// `chunks` would yield it and panic on `chunk[2]`. The
					// remainder must be empty for a well-formed ASDU.
					let chunks = bytes.chunks_exact(object_size + ADDRESS_SIZE);
					if !chunks.remainder().is_empty() {
						return NotEnoughBytes.fail();
					}
					Ok(chunks
						.map(|chunk| {
							tracing::trace!("Building object: {:?}", chunk);
							let address = u32::from_be_bytes([0, chunk[2], chunk[1], chunk[0]]);
							let object = T::from_bytes(&chunk[ADDRESS_SIZE..])?;
							Ok(GenericObject { address, object })
						})
						.collect::<Result<Vec<_>, ParseError>>()?)
				}
			}

			#[instrument(skip_all)]
			fn serialize_objects<T: FromBytes + ToBytes + Default>(
				objects: &[GenericObject<T>],
				sequence: bool,
				buffer: &mut Vec<u8>,
			) -> Result<(), ParseError> {
				if sequence && objects.is_empty() { return NotEnoughBytes.fail(); }
				for (index, obj) in objects.iter().enumerate() {
					if obj.address > 0x00ff_ffff
						|| (sequence && objects[0].address.checked_add(index as u32) != Some(obj.address))
					{
						return super::InvalidAddress.fail();
					}
					if !sequence || index == 0 {
						buffer.extend_from_slice(&obj.address.to_le_bytes()[..ADDRESS_SIZE]);
					}
					obj.object.to_bytes(buffer)?;
				}
				Ok(())
			}

			#[allow(clippy::too_many_lines)]
			#[instrument(skip_all)]
			pub fn from_bytes(
				type_id: TypeId,
				sequence: bool,
				num_objs: u8,
				bytes: &[u8],
			) -> Result<Self, ParseError> {
				Ok(match type_id {
					$(
						TypeId::$type_id => Self::$variant(Self::build_objects::<$payload>(
							type_id, sequence, num_objs, bytes,
						)?),
					)*
					TypeId::F_FR_NA_1
					| TypeId::F_SR_NA_1
					| TypeId::F_SC_NA_1
					| TypeId::F_LS_NA_1
					| TypeId::F_FA_NA_1
					| TypeId::F_SG_NA_1
					| TypeId::F_DR_TA_1 => NotImplemented.fail()?,
					_ => Self::Raw(Self::build_objects::<RawObject>(
						type_id, sequence, num_objs, bytes,
					)?),
				})
			}

			#[must_use]
			pub const fn len(&self) -> usize {
				match self {
					$( Self::$variant(objs) => objs.len(), )*
					Self::Raw(objs) => objs.len(),
				}
			}

			#[must_use]
			pub const fn is_empty(&self) -> bool {
				match self {
					$( Self::$variant(objs) => objs.is_empty(), )*
					Self::Raw(objs) => objs.is_empty(),
				}
			}

			pub(crate) fn to_asdu_bytes(
				&self, type_id: TypeId, sequence: bool, buffer: &mut Vec<u8>,
			) -> Result<(), ParseError> {
				match self {
					$( Self::$variant(objs) if type_id == TypeId::$type_id =>
						Self::serialize_objects(objs, sequence, buffer), )*
					Self::Raw(objs) if !type_id.is_standard() && !sequence =>
						Self::serialize_objects(objs, false, buffer),
					_ => super::InvalidType.fail(),
				}
			}

			pub fn to_bytes(&self, buffer: &mut Vec<u8>) -> Result<(), ParseError> {
				match self {
					$( Self::$variant(objs) => Self::serialize_objects(objs, false, buffer), )*
					Self::Raw(objs) => Self::serialize_objects(objs, false, buffer),
				}
			}
		}

		$(
			impl From<Vec<GenericObject<$payload>>> for InformationObjects {
				fn from(objs: Vec<GenericObject<$payload>>) -> Self {
					Self::$variant(objs)
				}
			}
		)*

		impl From<Vec<GenericObject<RawObject>>> for InformationObjects {
			fn from(objs: Vec<GenericObject<RawObject>>) -> Self {
				Self::Raw(objs)
			}
		}
	};
}

define_information_objects! {
	M_SP_NA_1 => MSpNa1(MSpNa1) ;
	M_SP_TA_1 => MSpTa1(MSpTa1) ;
	M_DP_NA_1 => MDpNa1(MDpNa1) ;
	M_DP_TA_1 => MDpTa1(MDpTa1) ;
	M_ST_NA_1 => MStNa1(MStNa1) ;
	M_ST_TA_1 => MStTa1(MStTa1) ;
	M_BO_NA_1 => MBoNa1(MBoNa1) ;
	M_ME_NA_1 => MMeNa1(MMeNa1) ;
	M_ME_TA_1 => MMeTa1(MMeTa1) ;
	M_ME_NB_1 => MMeNb1(MMeNb1) ;
	M_ME_TB_1 => MMeTb1(MMeTb1) ;
	M_ME_NC_1 => MMeNc1(MMeNc1) ;
	M_ME_TC_1 => MMeTc1(MMeTc1) ;
	M_IT_NA_1 => MItNa1(MItNa1) ;
	M_EP_TA_1 => MEpTa1(MEpTa1) ;
	M_EP_TB_1 => MEpTb1(MEpTb1) ;
	M_EP_TC_1 => MEpTc1(MEpTc1) ;
	M_PS_NA_1 => MPsNa1(MPsNa1) ;
	M_ME_ND_1 => MMeNd1(MMeNd1) ;
	M_SP_TB_1 => MSpTb1(MSpTb1) ;
	M_DP_TB_1 => MDpTb1(MDpTb1) ;
	M_ST_TB_1 => MStTb1(MStTb1) ;
	M_BO_TB_1 => MBoTb1(MBoTb1) ;
	M_ME_TD_1 => MMeTd1(MMeTd1) ;
	M_ME_TE_1 => MMeTe1(MMeTe1) ;
	M_ME_TF_1 => MMeTf1(MMeTf1) ;
	M_IT_TB_1 => MItTb1(MItTb1) ;
	M_EP_TD_1 => MEpTd1(MEpTd1) ;
	M_EP_TE_1 => MEpTe1(MEpTe1) ;
	M_EP_TF_1 => MEpTf1(MEpTf1) ;
	M_EI_NA_1 => MEiNa1(MEiNa1) ;
	C_SC_NA_1 => CScNa1(CScNa1) ;
	C_DC_NA_1 => CdcNa1(CdcNa1) ;
	C_RC_NA_1 => CrcNa1(CrcNa1) ;
	C_SE_NA_1 => CSeNa1(CSeNa1) ;
	C_SE_NB_1 => CSeNb1(CSeNb1) ;
	C_SE_NC_1 => CSeNc1(CSeNc1) ;
	C_BO_NA_1 => CBoNa1(CBoNa1) ;
	C_SC_TA_1 => CScTa1(CScTa1) ;
	C_DC_TA_1 => CdcTa1(CdcTa1) ;
	C_RC_TA_1 => CrcTa1(CrcTa1) ;
	C_SE_TA_1 => CSeTa1(CSeTa1) ;
	C_SE_TB_1 => CSeTb1(CSeTb1) ;
	C_SE_TC_1 => CSeTc1(CSeTc1) ;
	C_BO_TA_1 => CBoTa1(CBoTa1) ;
	C_IC_NA_1 => CIcNa1(CIcNa1) ;
	C_CI_NA_1 => CCiNa1(CCiNa1) ;
	C_RD_NA_1 => CRdNa1(CRdNa1) ;
	C_CS_NA_1 => CCsNa1(CCsNa1) ;
	C_TS_NA_1 => CTsNa1(CTsNa1) ;
	C_RP_NA_1 => CRpNa1(CRpNa1) ;
	C_CD_NA_1 => CCdNa1(CCdNa1) ;
	C_TS_TA_1 => CTsTa1(CTsTa1) ;
	P_ME_NA_1 => PMeNa1(PMeNa1) ;
	P_ME_NB_1 => PMeNb1(PMeNb1) ;
	P_ME_NC_1 => PMeNc1(PMeNc1) ;
	P_AC_NA_1 => PAcNa1(PAcNa1) ;
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn from_bytes_non_sequence_rejects_short_trailing_chunk() {
		// `from_bytes` is `pub` and may be called outside `Asdu::parse`'s
		// pre-validation. Passing a M_SP_NA_1 (1-byte payload + ADDRESS_SIZE
		// IOA → chunk size ADDRESS_SIZE + 1) tail with 5 bytes used to
		// surface a panic via `chunks` indexing the short trailing chunk.
		let bytes = [0x10, 0x00, 0x00, 0x00, 0xFF];
		let err = InformationObjects::from_bytes(TypeId::M_SP_NA_1, false, 1, &bytes)
			.expect_err("must reject malformed tail");
		assert!(matches!(err, ParseError::NotEnoughBytes { .. }), "got: {err:?}");
	}

	#[test]
	fn from_bytes_non_sequence_with_zero_tail_yields_empty() {
		// Empty tail with num_objs=0 is well-formed (no objects).
		let parsed = InformationObjects::from_bytes(TypeId::M_SP_NA_1, false, 0, &[])
			.expect("empty tail should parse");
		assert_eq!(parsed.len(), 0);
	}
}
