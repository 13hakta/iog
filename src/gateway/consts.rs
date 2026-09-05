//! Gateway constants and data structures.
//!
//! Contains timeouts, buffer sizes and gateway state types.
//! Traffic statistics live in `gateway::mod::Statistics` (atomic counters).

use std::fmt;

/// Buffer size for reading data from TCP sockets (16 KB).
pub const BUFFER_SIZE: usize = 16384;

/// Ping packet sending period for keep-alive checks (seconds).
pub const GW_PING_PERIOD: u64 = 30;

/// Client reconnection timeout after a link loss (milliseconds).
pub const GW_RECONNECT_TIMEOUT: u64 = 5000;

/// Authentication wait timeout on the server (milliseconds).
pub const GW_READINESS_TIMEOUT: u64 = 5000;

/// Gateway state.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum GatewayState {
    /// Waiting for connection or reconnection.
    Waiting,
    /// Processing incoming connections.
    Processing,
    /// Active connection established.
    Connected,
}

impl fmt::Display for GatewayState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GatewayState::Waiting => write!(f, "WAITING"),
            GatewayState::Processing => write!(f, "PROCESSING"),
            GatewayState::Connected => write!(f, "CONNECTED"),
        }
    }
}

