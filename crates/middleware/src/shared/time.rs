//! Time helpers for Scylla timestamp and date encoding.

use scylla::value::CqlDate;
use std::time::{SystemTime, UNIX_EPOCH};

/// Convert ms since the Unix epoch to a CqlDate.
/// CqlDate is a u32 counting days since -5877641-06-23, which is 2^31 days
/// before the Unix epoch, so the value is days_since_unix + 2^31.
pub fn cql_date_from_ms(ts_ms: i64) -> CqlDate {
    const MS_PER_DAY: i64 = 86_400_000;
    const EPOCH_OFFSET: i64 = 1 << 31;
    let days = ts_ms.div_euclid(MS_PER_DAY);
    CqlDate((days + EPOCH_OFFSET) as u32)
}

/// Current time in ms-since-epoch (UTC).
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cql_date_unix_epoch() {
        assert_eq!(cql_date_from_ms(0).0, 1u32 << 31);
    }

    #[test]
    fn cql_date_advances_by_day() {
        let d0 = cql_date_from_ms(0);
        let d1 = cql_date_from_ms(86_400_000);
        assert_eq!(d1.0, d0.0 + 1);
        let d7 = cql_date_from_ms(86_400_000 * 7);
        assert_eq!(d7.0, d0.0 + 7);
    }

    #[test]
    fn cql_date_floors_within_day() {
        let d = cql_date_from_ms(86_400_000 - 1);
        assert_eq!(d.0, 1u32 << 31);
    }

    #[test]
    fn now_ms_reasonable() {
        let t = now_ms();
        assert!(t > 1_700_000_000_000);
        assert!(t < 32_000_000_000_000);
    }
}
