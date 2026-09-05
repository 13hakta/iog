//! Common operations for gateway packet handling.
//!
//! Contains the `GatewayOperations` trait with methods reused by
//! both the client and the server gateway implementations.

use std::io;
use std::sync::Arc;

use async_trait::async_trait;
use std::marker::Unpin;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Mutex;

use crate::gateway::packet::PacketGW;

/// Trait for common gateway packet operations.
///
/// Provides a unified interface for reading and sending packets
/// and generating unique identifiers.
#[async_trait]
pub trait GatewayOperations: Send + Sync {
    /// Reads a single packet from the gateway socket.
    ///
    /// # Arguments
    /// * `socket` - TCP connection to the remote gateway
    ///
    /// # Returns
    /// - `Some((packet, data))` - packet read successfully with optional data
    /// - `None` - read error or connection loss
    ///
    /// # Errors
    /// Returns `None` when:
    /// - Socket read error
    /// - Invalid packet header format
    /// - Data size in the packet is too large
    async fn read_packet<R>(
        &self,
        socket: &Arc<Mutex<R>>,
    ) -> Option<(PacketGW, Option<Vec<u8>>)> where R: AsyncRead + Unpin + Send;

    /// Sends a packet to the remote gateway.
    ///
    /// # Arguments
    /// * `socket` - TCP connection to the remote gateway
    /// * `packet` - packet to send
    /// * `data` - optional data following the packet header
    ///
    /// # Errors
    /// Returns `io::Error` on socket write failure.
    #[allow(dead_code)]
    async fn send_packet<W>(
        &self,
        socket: &Arc<Mutex<W>>,
        packet: &PacketGW,
        data: Option<&[u8]>,
    ) -> Result<(), io::Error> where W: AsyncWrite + Unpin + Send;

    /// Generates the next unique packet identifier.
    ///
    /// Packet identifiers are unique within a single connection session
    /// and are used to match requests with responses.
    // Duplicate the identically named inherent Gateway methods; called via trait objects.
    #[allow(dead_code)]
    async fn next_packet_id(&self) -> u64;

    /// Generates the next unique connection identifier.
    ///
    /// Connection identifiers grow monotonically and are used
    /// to uniquely identify each TCP connection to the application.
    #[allow(dead_code)]
    async fn next_link_id(&self) -> u16;
}
