//! RTU in-memory point value: monitoring payloads (Type IDs 1–40 per IEC
//! 60870-5-104) the simulator stores per IOA.
//!
//! Aligned with [`crate::types_id::TypeId`] variants in the 1–40 range (gaps 8,
//! 16). Keep the macro table in sync with [`crate::types::InformationObjects`]
//! and with the match in `output::interrogation_data_asdus`.

use crate::{
	types::{
		GenericObject, InformationObjects, MBoNa1, MBoTb1, MDpNa1, MDpTa1, MDpTb1, MEpTa1, MEpTb1,
		MEpTc1, MEpTd1, MEpTe1, MEpTf1, MItNa1, MItTb1, MMeNa1, MMeNb1, MMeNc1, MMeNd1, MMeTa1,
		MMeTb1, MMeTc1, MMeTd1, MMeTe1, MMeTf1, MPsNa1, MSpNa1, MSpTa1, MSpTb1, MStNa1, MStTa1,
		MStTb1,
	},
	types_id::TypeId,
};

macro_rules! define_rtu_point_value {
	(
		$(
			$type_id:ident => $variant:ident($payload:ty) ;
		)*
	) => {
		#[derive(Debug, Clone, PartialEq)]
		pub enum PointValue {
			$( $variant($payload), )*
		}

		impl PointValue {
			#[must_use]
			pub const fn type_id(&self) -> TypeId {
				match self {
					$( Self::$variant(_) => TypeId::$type_id, )*
				}
			}

			/// One information object at `ioa` for spontaneous / read responses.
			#[must_use]
			pub fn single_object_information_objects(
				&self,
				ioa: u32,
			) -> (TypeId, InformationObjects) {
				match self {
					$(
						Self::$variant(m) => (
							TypeId::$type_id,
							InformationObjects::from(vec![GenericObject {
								address: ioa,
								object: m.clone(),
							}]),
						),
					)*
				}
			}
		}

		#[cfg(test)]
		mod type_id_tests {
			use crate::types_id::TypeId;

			use super::PointValue;

			#[test]
			fn point_value_type_ids_match_mapping() {
				$(
					assert_eq!(
						PointValue::$variant(Default::default()).type_id(),
						TypeId::$type_id,
					);
				)*
			}
		}
	};
}

define_rtu_point_value! {
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
}

impl PointValue {
	// Snapshot identity is representation equality, not IEEE numeric equality:
	// unchanged NaNs are stable, while a changed payload or signed zero is
	// stale.
	pub(super) fn same_snapshot(&self, other: &Self) -> bool {
		match (self, other) {
			(Self::MMeNc1(a), Self::MMeNc1(b)) => {
				a.value.to_bits() == b.value.to_bits() && a.qds == b.qds
			}
			(Self::MMeTc1(a), Self::MMeTc1(b)) => {
				a.value.to_bits() == b.value.to_bits() && a.qds == b.qds && a.time == b.time
			}
			(Self::MMeTf1(a), Self::MMeTf1(b)) => {
				a.value.to_bits() == b.value.to_bits() && a.qds == b.qds && a.time == b.time
			}
			_ => self == other,
		}
	}

	/// Integrated totals (`M_IT_NA_1` / `M_IT_TB_1`) used with `C_CI_NA_1`
	/// counter interrogation.
	#[must_use]
	pub const fn is_counter_integration(&self) -> bool {
		matches!(self, Self::MItNa1(_) | Self::MItTb1(_))
	}
}

#[cfg(test)]
mod snapshot_tests {
	use super::*;

	#[test]
	fn float_snapshots_compare_bits_quality_and_time() {
		macro_rules! check {
			($variant:ident, $payload:ident) => {{
				let initial = $payload { value: f32::from_bits(0x7fc0_0001), ..Default::default() };
				let point = PointValue::$variant(initial.clone());
				assert!(point.same_snapshot(&point.clone()));
				let mut changed = initial.clone();
				changed.value = f32::from_bits(0x7fc0_0002);
				assert!(!point.same_snapshot(&PointValue::$variant(changed)));
				let mut changed = initial.clone();
				changed.qds.iv = true;
				assert!(!point.same_snapshot(&PointValue::$variant(changed)));
				let positive = PointValue::$variant($payload { value: 0.0, ..Default::default() });
				let negative = PointValue::$variant($payload { value: -0.0, ..Default::default() });
				assert!(!positive.same_snapshot(&negative));
			}};
		}
		check!(MMeNc1, MMeNc1);
		check!(MMeTc1, MMeTc1);
		check!(MMeTf1, MMeTf1);
		let a = MMeTc1 { value: f32::NAN, ..Default::default() };
		let mut b = a.clone();
		b.time.ms = 1;
		assert!(!PointValue::MMeTc1(a).same_snapshot(&PointValue::MMeTc1(b)));
		let a = MMeTf1 { value: f32::NAN, ..Default::default() };
		let mut b = a.clone();
		b.time.ms = 1;
		assert!(!PointValue::MMeTf1(a).same_snapshot(&PointValue::MMeTf1(b)));
	}
}
