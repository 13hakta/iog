//! Gateway constants and data structures.
//!
//! Contains timeouts, buffer sizes and structures for statistics collection.

use std::fmt;

/// Buffer size for reading data from TCP sockets (16 KB).
pub const BUFFER_SIZE: usize = 16384;

/// Ping packet sending period for keep-alive checks (seconds).
pub const GW_PING_PERIOD: u64 = 30;

/// Client reconnection timeout after a link loss (milliseconds).
pub const GW_RECONNECT_TIMEOUT: u64 = 5000;

/// Authentication wait timeout on the server (milliseconds).
pub const GW_READINESS_TIMEOUT: u64 = 5000;

/// Maximum data size in a single packet (10 MB).
pub const MAX_DATA_SIZE: usize = 10_485_760;

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

/// Global gateway connection statistics.
///
/// Stores aggregated data across all active connections.
#[derive(Debug, Clone, Copy)]
pub struct ConnectionStatistics {
    /// Number of received packets.
    pub packets_in: u64,
    /// Number of sent packets.
    pub packets_out: u64,
    /// Total volume of received data (bytes).
    pub bytes_in: u64,
    /// Total volume of sent data (bytes).
    pub bytes_out: u64,
    /// Connection establishment time (Unix timestamp in seconds).
    pub connection_time: i64,
}

impl ConnectionStatistics {
    /// Creates new statistics with zero values.
    pub fn new() -> Self {
        ConnectionStatistics {
            packets_in: 0,
            packets_out: 0,
            bytes_in: 0,
            bytes_out: 0,
            connection_time: 0,
        }
    }

    /// Increments the incoming packets counter.
    pub fn inc_packets_in(&mut self) {
        self.packets_in += 1;
    }

    /// Increments the outgoing packets counter.
    pub fn inc_packets_out(&mut self) {
        self.packets_out += 1;
    }

    /// Adds to the received data volume.
    pub fn add_bytes_in(&mut self, bytes: u64) {
        self.bytes_in += bytes;
    }

    /// Adds to the sent data volume.
    pub fn add_bytes_out(&mut self, bytes: u64) {
        self.bytes_out += bytes;
    }
}

impl Default for ConnectionStatistics {
    fn default() -> Self {
        Self::new()
    }
}

