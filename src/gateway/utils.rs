//! Gateway helper functions.
//!
//! Contains time utilities and other common functions.

use chrono::Utc;

/// Returns the current time as a Unix timestamp (seconds since the epoch).
pub fn now_epoch() -> i64 {
    Utc::now().timestamp()
}

/// Formats a byte size into a human-readable string.
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit_idx = 0;
    while value >= 1024.0 && unit_idx < UNITS.len() - 1 {
        value /= 1024.0;
        unit_idx += 1;
    }
    if unit_idx == 0 {
        format!("{} {}", bytes, UNITS[unit_idx])
    } else {
        format!("{:.2} {}", value, UNITS[unit_idx])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
        assert_eq!(format_bytes(512), "512 B");
    }

    #[test]
    fn test_now_epoch() {
        let ts = now_epoch();
        assert!(ts > 1_700_000_000);
    }
}
