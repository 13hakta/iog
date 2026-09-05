//! Management of a single TCP connection to the application.
//!
//! A `Link` represents one logical connection to the local application,
//! splitting the TCP socket into read and write halves and tracking traffic statistics.

use std::fmt;
use std::sync::Arc;

use log::{error, info};
use tokio::io::AsyncWriteExt;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Notify};

use crate::gateway::consts::ConnectionStatistics;

/// State of the connection to the application.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionState {
    /// Connection is broken.
    Disconnected,
    /// Connection is active and ready for data transfer.
    Connected,
    /// Waiting for the remote side to confirm the connection opening.
    Waiting,
}

impl fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConnectionState::Disconnected => write!(f, "DISCONNECTED"),
            ConnectionState::Connected => write!(f, "CONNECTED"),
            ConnectionState::Waiting => write!(f, "WAITING"),
        }
    }
}

/// Representation of a single TCP connection to the application.
///
/// The socket is split into read (`OwnedReadHalf`) and write (`OwnedWriteHalf`) halves,
/// allowing incoming and outgoing traffic to be handled independently.
pub struct Link {
    /// Read half of the socket (accessed via a mutex for concurrent reads).
    /// Shared as an `Arc` so the forwarding loop can read without holding
    /// the whole link lock.
    pub reader: Arc<Mutex<OwnedReadHalf>>,
    /// Connection identifier (unique within the gateway).
    pub connection_id: u16,
    /// Write half of the socket.
    writer: OwnedWriteHalf,
    /// Current connection state.
    state: ConnectionState,
    /// Traffic statistics for this connection.
    statistics: ConnectionStatistics,
    /// Notifier for tasks waiting for the connection open confirmation.
    connection_awaiter: Option<Arc<Notify>>,
}

impl Link {
    /// Creates a new `Link` from a TCP connection.
    pub fn new(socket: TcpStream, connection_id: u16) -> Self {
        if let Ok(addr) = socket.peer_addr() {
            info!("App #{} {}", connection_id, addr);
        }

        let (read_half, write_half) = socket.into_split();

        Link {
            writer: write_half,
            reader: Arc::new(Mutex::new(read_half)),
            connection_id,
            state: ConnectionState::Disconnected,
            statistics: ConnectionStatistics::new(),
            connection_awaiter: None,
        }
    }

    /// Sends data into the connection.
    ///
    /// If the connection is not in the `CONNECTED` state, the data is not sent.
    pub async fn send_packet(&mut self, payload: Option<&[u8]>) {
        if self.state != ConnectionState::Connected {
            return;
        }

        let data_to_send = payload.unwrap_or(&[]);
        let data_size = data_to_send.len() as u64;

        if let Err(e) = self.writer.write_all(data_to_send).await {
            error!("Error sending data to connection #{}: {}", self.connection_id, e);
        } else {
            self.statistics.bytes_out += data_size;
        }
    }

    /// Registers incoming data volume.
    pub fn add_incoming(&mut self, bytes: u64) {
        self.statistics.bytes_in += bytes;
    }

    /// Returns connection statistics: (bytes_in, bytes_out, state).
    pub fn get_stats(&self) -> (u64, u64, ConnectionState) {
        (
            self.statistics.bytes_in,
            self.statistics.bytes_out,
            self.state.clone(),
        )
    }

    /// Sets the awaiter for the open confirmation.
    // Reserved for the connection open confirmation wait mechanism.
    #[allow(dead_code)]
    pub fn set_awaiter(&mut self, awaiter: Option<Arc<Notify>>) {
        self.connection_awaiter = awaiter;
    }

    /// Changes the connection state.
    ///
    /// When switching to `CONNECTED`, statistics are reset and awaiting tasks are notified.
    pub fn set_state(&mut self, new_state: ConnectionState) {
        self.state = new_state;
        self.statistics.bytes_in = 0;
        self.statistics.bytes_out = 0;

        if self.state == ConnectionState::Connected {
            if let Some(awaiter) = &self.connection_awaiter {
                awaiter.notify_one();
            }
        }
    }

    /// Returns the current connection state.
    // Reserved as part of the public Link API (statistics obtain the
    // state via `get_stats`).
    #[allow(dead_code)]
    pub fn state(&self) -> ConnectionState {
        self.state.clone()
    }

    /// Closes the connection (shuts down writing and switches to `DISCONNECTED`).
    pub async fn close_connection(&mut self) {
        let _ = self.writer.shutdown().await;
        self.state = ConnectionState::Disconnected;
    }
}

/// Shared reference to a `Link`.
pub type LinkRef = Arc<Mutex<Link>>;

