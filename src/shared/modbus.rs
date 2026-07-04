//! Modbus register byte-order semantics — pure functions, no I/O.
//!
//! Lives in the `shared` module because:
//! 1. Mapping parameters have a `modbus.byte_order` field that must deserialize
//!    → the enum must be visible to the shared mapping loader.
//! 2. Edge-client `telemetry.rs` decodes register bytes with these methods to
//!    publish telemetry values.
//!
//! Deliberate design choices:
//!
//! - **No `round(value, 2)`**: rounding in the decoder would bake data loss into
//!   the lowest layer. The Rust decoder is a pure, full-precision conversion.
//!   Domain logic to reduce precision (if needed) is handled in the polling loop
//!   / publisher, not here.
//! - **`f32` not `f64`**: a Modbus 32-bit float = single-precision IEEE 754.
//!   Using f64 only spreads false bit-noise (12.5 becomes 12.500000476...).
//! - **No multi-address coil AND**: a variant that takes a list of addresses and
//!   ANDs them all. The use case is unclear. Skipped until a register map proves
//!   it's needed. A Modbus coil is already bool — use it directly.
//! - **No `holding_registers_to_timestamp`**: an HH:MM:SS timestamp parser from
//!   3 registers with awkward seconds-overflow handling. Skipped by an early
//!   decision; reactivate if RUNNING_HOURS_* is needed.

use serde::Deserialize;

/// Byte order for a 32-bit value that spans 2 Modbus registers.
///
/// PLC vendors have different conventions for packing a 32-bit value (float or
/// u32) into two 16-bit registers. 4 combinations:
///
/// - `BigBig` (BE byte, BE word) — "network order" standard. Schneider, ABB default.
/// - `LittleBig` (LE byte, BE word) — Siemens S7 PLC default (byte swap within a register).
/// - `BigLittle` (BE byte, LE word) — Schneider Quantum, some Modicon (word swap).
/// - `LittleLittle` (LE byte, LE word) — some Mitsubishi, some generic PLCs.
///
/// The `{byteorder}_{wordorder}` naming scheme deserializes automatically via
/// serde snake_case in JSON: `"big_big"`, `"little_big"`, etc., so the mapping
/// JSON just names the string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ByteOrder {
    BigBig,
    LittleBig,
    BigLittle,
    LittleLittle,
}

impl Default for ByteOrder {
    /// Default `BigBig` (BE byte, BE word) — the "network order" standard.
    /// Used by `#[serde(default)]` on `ModbusBinding::HoldingRegisters.byte_order`
    /// so a parameter without an explicit `byte_order` doesn't fail to parse.
    fn default() -> Self {
        Self::BigBig
    }
}

impl ByteOrder {
    /// Decode 2 registers as an IEEE 754 single-precision float (32-bit).
    /// `regs[0]` = the register read first from the Modbus response.
    pub fn decode_f32(self, regs: [u16; 2]) -> f32 {
        f32::from_be_bytes(self.normalize_to_be(regs))
    }

    /// Encode f32 into 2 registers. `result[0]` = the register written first to
    /// Modbus (lower address).
    pub fn encode_f32(self, value: f32) -> [u16; 2] {
        self.denormalize_from_be(value.to_be_bytes())
    }

    /// Decode 2 registers as an unsigned 32-bit integer. For `run_time_*`
    /// parameters or counters larger than 16-bit.
    pub fn decode_u32(self, regs: [u16; 2]) -> u32 {
        u32::from_be_bytes(self.normalize_to_be(regs))
    }

    /// Encode u32 ke 2 register.
    pub fn encode_u32(self, value: u32) -> [u16; 2] {
        self.denormalize_from_be(value.to_be_bytes())
    }

    // -----------------------------------------------------------------------
    // Core: reorder bytes & words per variant
    // -----------------------------------------------------------------------

    /// Arrange 2 registers into a 4-byte array in "logical big-endian" order
    /// (MSB first). After that any consumer can `from_be_bytes` to decode any
    /// type (f32, u32, i32).
    fn normalize_to_be(self, regs: [u16; 2]) -> [u8; 4] {
        // Step 1: choose the word order
        let (hi_reg, lo_reg) = match self {
            // BE word: first register = high word
            Self::BigBig | Self::LittleBig => (regs[0], regs[1]),
            // LE word: first register = low word, swap the order
            Self::BigLittle | Self::LittleLittle => (regs[1], regs[0]),
        };
        // Step 2: extract bytes from each register per the byte order
        let (hi_bytes, lo_bytes) = match self {
            // BE byte: use as-is (high byte first within the register)
            Self::BigBig | Self::BigLittle => (hi_reg.to_be_bytes(), lo_reg.to_be_bytes()),
            // LE byte: swap bytes within the register
            Self::LittleBig | Self::LittleLittle => (hi_reg.to_le_bytes(), lo_reg.to_le_bytes()),
        };
        [hi_bytes[0], hi_bytes[1], lo_bytes[0], lo_bytes[1]]
    }

    /// Inverse: from 4 logical-BE bytes back to 2 registers with the appropriate
    /// byte/word order. For encoding (write to PLC).
    fn denormalize_from_be(self, bytes: [u8; 4]) -> [u16; 2] {
        let hi_bytes = [bytes[0], bytes[1]];
        let lo_bytes = [bytes[2], bytes[3]];
        // Step 1: form registers from byte pairs per the byte order
        let (hi_reg, lo_reg) = match self {
            Self::BigBig | Self::BigLittle => {
                (u16::from_be_bytes(hi_bytes), u16::from_be_bytes(lo_bytes))
            }
            Self::LittleBig | Self::LittleLittle => {
                (u16::from_le_bytes(hi_bytes), u16::from_le_bytes(lo_bytes))
            }
        };
        // Step 2: arrange the register order per the word order
        match self {
            Self::BigBig | Self::LittleBig => [hi_reg, lo_reg],
            Self::BigLittle | Self::LittleLittle => [lo_reg, hi_reg],
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // f32 decode — all 4 byte orders use the same value (1.0f32)
    //
    // IEEE 754 single-precision 1.0 = 0x3F800000 = bytes [0x3F, 0x80, 0x00, 0x00]
    // -----------------------------------------------------------------------

    #[test]
    fn decode_f32_big_big_one() {
        // BE byte (as-is), BE word (hi first): regs = [0x3F80, 0x0000]
        assert_eq!(ByteOrder::BigBig.decode_f32([0x3F80, 0x0000]), 1.0);
    }

    #[test]
    fn decode_f32_little_big_one() {
        // LE byte (swap within register), BE word (hi first): regs = [0x803F, 0x0000]
        assert_eq!(ByteOrder::LittleBig.decode_f32([0x803F, 0x0000]), 1.0);
    }

    #[test]
    fn decode_f32_big_little_one() {
        // BE byte (as-is), LE word (lo first): regs = [0x0000, 0x3F80]
        assert_eq!(ByteOrder::BigLittle.decode_f32([0x0000, 0x3F80]), 1.0);
    }

    #[test]
    fn decode_f32_little_little_one() {
        // LE byte (swap), LE word (lo first): regs = [0x0000, 0x803F]
        assert_eq!(ByteOrder::LittleLittle.decode_f32([0x0000, 0x803F]), 1.0);
    }

    // -----------------------------------------------------------------------
    // f32 decode — a realistic value
    //
    // 75.5f32 = 0x42970000 = bytes [0x42, 0x97, 0x00, 0x00]
    // e.g. for a temp/tank parameter
    // -----------------------------------------------------------------------

    #[test]
    fn decode_f32_big_big_realistic_temp() {
        assert_eq!(ByteOrder::BigBig.decode_f32([0x4297, 0x0000]), 75.5);
    }

    #[test]
    fn decode_f32_little_big_realistic_temp() {
        // Siemens-style: byte swap per register
        assert_eq!(ByteOrder::LittleBig.decode_f32([0x9742, 0x0000]), 75.5);
    }

    // -----------------------------------------------------------------------
    // f32 decode — negative value
    //
    // -12.5f32 = 0xC1480000 = bytes [0xC1, 0x48, 0x00, 0x00]
    // -----------------------------------------------------------------------

    #[test]
    fn decode_f32_big_big_negative() {
        assert_eq!(ByteOrder::BigBig.decode_f32([0xC148, 0x0000]), -12.5);
    }

    #[test]
    fn decode_f32_little_little_negative() {
        // LE byte, LE word: word swap + byte swap
        // -12.5 bytes = [0xC1, 0x48, 0x00, 0x00]
        // LittleLittle: bytes[0..2] from hi_reg.to_le, bytes[2..4] from lo_reg.to_le
        //   where hi_reg = regs[1], lo_reg = regs[0]
        // Want bytes = [0xC1, 0x48, 0x00, 0x00]
        // hi_reg.to_le_bytes() = [0xC1, 0x48] → hi_reg = 0x48C1
        // lo_reg.to_le_bytes() = [0x00, 0x00] → lo_reg = 0x0000
        // So regs[1] = 0x48C1, regs[0] = 0x0000 → input [0x0000, 0x48C1]
        assert_eq!(ByteOrder::LittleLittle.decode_f32([0x0000, 0x48C1]), -12.5);
    }

    // -----------------------------------------------------------------------
    // f32 decode — edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn decode_f32_zero() {
        // 0.0 = all zero bytes, same across all byte orders
        for bo in [
            ByteOrder::BigBig,
            ByteOrder::LittleBig,
            ByteOrder::BigLittle,
            ByteOrder::LittleLittle,
        ] {
            assert_eq!(bo.decode_f32([0x0000, 0x0000]), 0.0);
        }
    }

    #[test]
    fn decode_f32_negative_zero_preserved() {
        // -0.0 = 0x80000000. The sign bit matters; don't treat it as +0.
        let v = ByteOrder::BigBig.decode_f32([0x8000, 0x0000]);
        assert_eq!(v, 0.0); // numerically equal
        assert!(v.is_sign_negative(), "must preserve sign of negative zero");
    }

    #[test]
    fn decode_f32_infinity() {
        // +infinity = 0x7F800000
        assert!(ByteOrder::BigBig.decode_f32([0x7F80, 0x0000]).is_infinite());
    }

    // -----------------------------------------------------------------------
    // f32 round-trip encode→decode
    // -----------------------------------------------------------------------

    #[test]
    fn round_trip_f32_all_byte_orders() {
        let values = [0.0f32, 1.0, -1.0, 12.5, -75.25, 1.234e6, 1.5e-10];
        for bo in [
            ByteOrder::BigBig,
            ByteOrder::LittleBig,
            ByteOrder::BigLittle,
            ByteOrder::LittleLittle,
        ] {
            for v in values {
                let regs = bo.encode_f32(v);
                let back = bo.decode_f32(regs);
                assert_eq!(back, v, "round trip failed for {bo:?} v={v}");
            }
        }
    }

    // -----------------------------------------------------------------------
    // f32 cross-byte-order: the output register bytes REALLY differ between
    // byte orders (catches the "all variants use the same logic" bug)
    // -----------------------------------------------------------------------

    #[test]
    fn encode_f32_byte_orders_differ() {
        let v = 1.0f32; // bytes 3F 80 00 00 — a pattern that distinguishes position
        let bb = ByteOrder::BigBig.encode_f32(v);
        let lb = ByteOrder::LittleBig.encode_f32(v);
        let bl = ByteOrder::BigLittle.encode_f32(v);
        let ll = ByteOrder::LittleLittle.encode_f32(v);
        // At least the adjacent pairs must differ
        assert_ne!(bb, lb, "BigBig vs LittleBig must differ");
        assert_ne!(bb, bl, "BigBig vs BigLittle must differ");
        assert_ne!(lb, ll, "LittleBig vs LittleLittle must differ");
        assert_ne!(bl, ll, "BigLittle vs LittleLittle must differ");
    }

    // -----------------------------------------------------------------------
    // u32 decode
    //
    // 0x12345678 = bytes [0x12, 0x34, 0x56, 0x78]
    // -----------------------------------------------------------------------

    #[test]
    fn decode_u32_big_big() {
        assert_eq!(ByteOrder::BigBig.decode_u32([0x1234, 0x5678]), 0x12345678);
    }

    #[test]
    fn decode_u32_little_big() {
        // Byte swap within each register
        assert_eq!(
            ByteOrder::LittleBig.decode_u32([0x3412, 0x7856]),
            0x12345678
        );
    }

    #[test]
    fn decode_u32_big_little() {
        // Word swap (register order reversed)
        assert_eq!(
            ByteOrder::BigLittle.decode_u32([0x5678, 0x1234]),
            0x12345678
        );
    }

    // -----------------------------------------------------------------------
    // u32 round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn round_trip_u32_all_byte_orders() {
        let values = [0u32, 1, 100, 86400, u32::MAX, 0xDEADBEEF];
        for bo in [
            ByteOrder::BigBig,
            ByteOrder::LittleBig,
            ByteOrder::BigLittle,
            ByteOrder::LittleLittle,
        ] {
            for v in values {
                let regs = bo.encode_u32(v);
                let back = bo.decode_u32(regs);
                assert_eq!(back, v, "u32 round trip failed for {bo:?} v={v:#X}");
            }
        }
    }

    // -----------------------------------------------------------------------
    // ByteOrder deserialization from JSON (for integration with the mapping config)
    // -----------------------------------------------------------------------

    #[test]
    fn deserialize_byte_order_from_json_snake_case() {
        assert_eq!(
            serde_json::from_str::<ByteOrder>("\"big_big\"").unwrap(),
            ByteOrder::BigBig
        );
        assert_eq!(
            serde_json::from_str::<ByteOrder>("\"little_big\"").unwrap(),
            ByteOrder::LittleBig
        );
        assert_eq!(
            serde_json::from_str::<ByteOrder>("\"big_little\"").unwrap(),
            ByteOrder::BigLittle
        );
        assert_eq!(
            serde_json::from_str::<ByteOrder>("\"little_little\"").unwrap(),
            ByteOrder::LittleLittle
        );
    }

    #[test]
    fn deserialize_byte_order_rejects_unknown() {
        // Configs with a typo like "BIG_BIG", "bigbig", "middle_endian" must error.
        assert!(serde_json::from_str::<ByteOrder>("\"BIG_BIG\"").is_err());
        assert!(serde_json::from_str::<ByteOrder>("\"bigbig\"").is_err());
        assert!(serde_json::from_str::<ByteOrder>("\"middle_endian\"").is_err());
    }

    #[test]
    fn default_byte_order_is_big_big() {
        // ModbusBinding uses #[serde(default)] so a JSON param can omit
        // byte_order → fallback to big_big.
        assert_eq!(ByteOrder::default(), ByteOrder::BigBig);
    }
}
