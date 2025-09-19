use chrono::DateTime;
use chrono::Utc;

// Use mock_instant's global time when the `mock-time` feature is enabled.
// Otherwise, fall back to the real system clock.
#[cfg(feature = "mock-time")]
use mock_instant::global::{SystemTime, UNIX_EPOCH};

#[cfg(not(feature = "mock-time"))]
use std::time::{SystemTime, UNIX_EPOCH};

pub fn now() -> DateTime<Utc> {
    #[cfg(all(feature = "mock-time", not(test), not(debug_assertions)))]
    compile_error!("mock-time feature must not be enabled in production builds");

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before Unix epoch");
    DateTime::from_timestamp(now.as_secs() as i64, now.subsec_nanos()).unwrap()
}
