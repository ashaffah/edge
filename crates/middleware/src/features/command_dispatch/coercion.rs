//! Pure value coercion & equality. No I/O.

use anyhow::{Result, anyhow, bail};

use edge_shared::DataType;

pub fn value_as_f64(v: &serde_json::Value, expected: DataType) -> Result<f64> {
    match expected {
        DataType::Float => v
            .as_f64()
            .ok_or_else(|| anyhow!("expected float, got: {v}")),
        DataType::Integer => {
            let f = v
                .as_f64()
                .ok_or_else(|| anyhow!("expected integer, got: {v}"))?;
            if f.fract() != 0.0 {
                bail!("expected integer, got non-integral: {f}");
            }
            Ok(f)
        }
        DataType::Boolean => bail!("data type is boolean, not numeric"),
        DataType::String => bail!("data type is string, which cannot be used for set/control"),
    }
}

pub fn value_as_bool(v: &serde_json::Value) -> Result<bool> {
    if let Some(b) = v.as_bool() {
        return Ok(b);
    }
    if let Some(n) = v.as_i64() {
        return match n {
            0 => Ok(false),
            1 => Ok(true),
            other => bail!("expected boolean (true/false or 0/1), got: {other}"),
        };
    }
    if let Some(s) = v.as_str() {
        return match s.to_ascii_lowercase().as_str() {
            "true" | "on" | "1" => Ok(true),
            "false" | "off" | "0" => Ok(false),
            other => bail!("expected boolean string, got: {other:?}"),
        };
    }
    bail!("expected boolean, got: {v}")
}

pub fn numbers_equal(a: f64, b: f64, dt: DataType) -> bool {
    match dt {
        DataType::Integer => (a as i64) == (b as i64),
        DataType::Float => (a - b).abs() < 1e-6,
        DataType::Boolean => unreachable!("boolean handled separately"),
        DataType::String => unreachable!("string cannot be used for set/control"),
    }
}

pub fn format_value_for_plc(v: f64, dt: DataType) -> String {
    match dt {
        DataType::Integer => format!("{}", v as i64),
        DataType::Float => format!("{v}"),
        DataType::Boolean => unreachable!("boolean handled separately"),
        DataType::String => unreachable!("string cannot be used for set/control"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn coerce_float_accepts_numbers() {
        assert_eq!(
            value_as_f64(&json!(std::f64::consts::PI), DataType::Float).unwrap(),
            std::f64::consts::PI
        );
        assert_eq!(value_as_f64(&json!(42), DataType::Float).unwrap(), 42.0);
    }

    #[test]
    fn coerce_float_rejects_non_number() {
        assert!(value_as_f64(&json!("3.14"), DataType::Float).is_err());
        assert!(value_as_f64(&json!(true), DataType::Float).is_err());
        assert!(value_as_f64(&json!(null), DataType::Float).is_err());
    }

    #[test]
    fn coerce_integer_accepts_integral_floats() {
        assert_eq!(value_as_f64(&json!(42), DataType::Integer).unwrap(), 42.0);
        assert_eq!(value_as_f64(&json!(42.0), DataType::Integer).unwrap(), 42.0);
    }

    #[test]
    fn coerce_integer_rejects_fractional() {
        assert!(value_as_f64(&json!(3.5), DataType::Integer).is_err());
    }

    #[test]
    fn coerce_integer_rejects_boolean_type_mismatch() {
        assert!(value_as_f64(&json!(1), DataType::Boolean).is_err());
    }

    #[test]
    fn coerce_bool_native() {
        assert!(value_as_bool(&json!(true)).unwrap());
        assert!(!value_as_bool(&json!(false)).unwrap());
    }

    #[test]
    fn coerce_bool_integer_01() {
        assert!(value_as_bool(&json!(1)).unwrap());
        assert!(!value_as_bool(&json!(0)).unwrap());
        assert!(value_as_bool(&json!(2)).is_err());
    }

    #[test]
    fn coerce_bool_string_variants() {
        assert!(value_as_bool(&json!("true")).unwrap());
        assert!(value_as_bool(&json!("ON")).unwrap());
        assert!(!value_as_bool(&json!("off")).unwrap());
        assert!(value_as_bool(&json!("yes")).is_err());
    }

    #[test]
    fn equality_integer_truncates() {
        assert!(numbers_equal(42.0, 42.0, DataType::Integer));
        assert!(numbers_equal(42.4, 42.0, DataType::Integer));
        assert!(!numbers_equal(42.0, 43.0, DataType::Integer));
    }

    #[test]
    fn equality_float_epsilon() {
        assert!(numbers_equal(1.0, 1.0 + 1e-9, DataType::Float));
        assert!(!numbers_equal(1.0, 1.01, DataType::Float));
    }

    #[test]
    fn format_integer_drops_decimal() {
        assert_eq!(format_value_for_plc(42.0, DataType::Integer), "42");
        assert_eq!(format_value_for_plc(-7.9, DataType::Integer), "-7");
    }

    #[test]
    fn format_float_preserves_decimal() {
        assert_eq!(
            format_value_for_plc(std::f64::consts::PI, DataType::Float),
            "3.141592653589793"
        );
    }
}
