//! Base gateway module.
//!
//! Contains common structures and traits, and re-exports the nested modules
//! for the client and server parts of the application.

pub mod auth;
pub mod buffer_pool;
pub mod client;
pub mod compression;
pub mod consts;
pub mod link;
pub mod metrics;
pub mod operations;
pub mod packet;
pub mod server;
pub mod session_store;
pub mod utils;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use std::marker::Unpin;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Mutex, RwLock};

use crate::configuration::Configuration;
use crate::gateway::buffer_pool::acquire_buffer;
use crate::gateway::compression::{compress_data, decompress_data, MIN_COMPRESS_SIZE};
use crate::gateway::consts::{ConnectionStatistics, GatewayState};
use crate::gateway::link::LinkRef;
use crate::gateway::packet::{PacketGW, PacketType, FLAG_COMPRESSED, PACKET_HEADER_SIZE};
use crate::gateway::operations::GatewayOperations;

/// Common trait for gateway components (client and server).
///
/// Defines a unified interface for gateway lifecycle management,
/// packet sending and statistics retrieval.
#[async_trait]
pub trait GatewayTrait: Send + Sync {
    /// Runs the main loop of the component.
    ///
    /// For the client this is the connect/reconnect loop, for the server —
    /// endless listening for incoming connections.
    async fn run(&self);

    /// Returns the current gateway state.
    fn state(&self) -> GatewayState;

    /// Prints the current connection and traffic statistics to stdout.
    async fn print_stats(&self);

    /// Handles the connection loss with the remote gateway.
    ///
    /// Closes all active Links and resets the state to `WAITING`.
    async fn on_disconnection(&self);

    /// Closes all active connections during graceful shutdown.
    ///
    /// Called when a termination signal is received (SIGTERM, Ctrl+C).
    async fn shutdown(&self);
}

/// Base gateway structure containing common logic and state.
///
/// Used as the inner part of `GatewayClient` and `GatewayServer`
/// via composition to reuse the common logic for managing
/// connections (`Link`), statistics and identifiers.
#[derive(Clone)]
pub struct Gateway {
    /// Application configuration.
    pub config: Configuration,
    /// Current gateway state.
    pub state: Arc<RwLock<GatewayState>>,
    /// Active connections (`Link`) to local applications.
    pub links: Arc<RwLock<HashMap<u16, LinkRef>>>,
    /// Global gateway statistics.
    pub statistics: Arc<RwLock<ConnectionStatistics>>,
    /// Identifier counter for new packets (unique within a session).
    pub packet_id_counter: Arc<RwLock<u64>>,
    /// Identifier counter for new connections (monotonically increasing).
    pub link_id_counter: Arc<RwLock<u16>>,
    /// Timestamp of the last activity for keep-alive.
    pub keep_alive_stamp: Arc<RwLock<i64>>,
}

impl Gateway {
    /// Creates a new base gateway instance with the given configuration.
    pub fn new(config: Configuration) -> Self {
        Self {
            config,
            state: Arc::new(RwLock::new(GatewayState::Waiting)),
            links: Arc::new(RwLock::new(HashMap::new())),
            statistics: Arc::new(RwLock::new(ConnectionStatistics::new())),
            packet_id_counter: Arc::new(RwLock::new(0)),
            link_id_counter: Arc::new(RwLock::new(1)),
            keep_alive_stamp: Arc::new(RwLock::new(crate::gateway::utils::now_epoch())),
        }
    }

    /// Generates the next unique packet identifier.
    pub async fn next_packet_id(&self) -> u64 {
        let mut counter = self.packet_id_counter.write().await;
        *counter = counter.wrapping_add(1);
        *counter
    }

    /// Generates the next connection identifier.
    pub async fn next_link_id(&self) -> u16 {
        let mut counter = self.link_id_counter.write().await;
        let id = *counter;
        *counter = counter.wrapping_add(1);
        id
    }

    /// Adds a new connection to the active registry.
    pub async fn add_link(&self, link: LinkRef) {
        let id = link.lock().await.connection_id;
        self.links.write().await.insert(id, link);
        self.update_connections_metric().await;
    }

    /// Removes a connection from the registry by its identifier.
    pub async fn remove_link(&self, id: u16) -> Option<LinkRef> {
        let link = self.links.write().await.remove(&id);
        self.update_connections_metric().await;
        link
    }

    /// Closes all active connections and clears the registry.
    ///
    /// Used on link loss with the remote gateway to release resources.
    pub async fn close_all_links(&self) {
        let mut links = self.links.write().await;
        for (_, link) in links.drain() {
            link.lock().await.close_connection().await;
        }
        drop(links);
        self.update_connections_metric().await;
    }

    /// Updates the active connections metric.
    pub async fn update_connections_metric(&self) {
        let count = self.links.read().await.len();
        metrics::set_active_connections(count);
    }

    /// Sends a packet to the remote gateway.
    ///
    /// Data larger than `MIN_COMPRESS_SIZE` is compressed (zstd),
    /// and the `FLAG_COMPRESSED` flag is set in the header.
    pub async fn send_gw_packet<W>(
        &self,
        socket: &Arc<Mutex<W>>,
        packet: &PacketGW,
        data: Option<&[u8]>,
    ) -> Result<(), std::io::Error>
    where
        W: AsyncWrite + Unpin + Send,
    {
        let start = std::time::Instant::now();

        // Compress the data when it is worthwhile.
        let mut send_packet = packet.clone();
        let (payload, compressed_flag) = match data {
            Some(d) if d.len() >= MIN_COMPRESS_SIZE => {
                let result = compress_data(d);
                if result.is_compressed {
                    send_packet.set_flag(FLAG_COMPRESSED);
                    send_packet.data_size = result.data.len() as u16;
                    (Some(result.data), true)
                } else {
                    (Some(d.to_vec()), false)
                }
            }
            Some(d) => (Some(d.to_vec()), false),
            None => (None, false),
        };

        let payload_len = payload.as_ref().map(|p| p.len()).unwrap_or(0);
        let total_bytes = PACKET_HEADER_SIZE + payload_len;

        let mut sock = socket.lock().await;
        sock.write_all(&send_packet.to_bytes()).await?;
        if let Some(p) = &payload {
            sock.write_all(p).await?;
        }
        drop(sock);

        // Update internal statistics
        {
            let mut stats = self.statistics.write().await;
            stats.inc_packets_out();
            stats.add_bytes_out(total_bytes as u64);
        }

        // Update Prometheus metrics
        let packet_type_str = format!("{:?}", send_packet.packet_type);
        metrics::inc_packets_total(&packet_type_str, "out");
        metrics::inc_bytes_total(total_bytes as u64, "out");
        if compressed_flag {
            metrics::inc_packets_total(&packet_type_str, "compressed");
        }
        metrics::observe_packet_processing(&packet_type_str, start.elapsed().as_secs_f64());

        Ok(())
    }

    /// Reads a single packet from the gateway socket.
    ///
    /// If the `FLAG_COMPRESSED` flag is set, the data is decompressed (zstd).
    /// A reusable buffer from the pool is used for reading data.
    pub async fn read_gw_packet<R>(
        &self,
        socket: &Arc<Mutex<R>>,
    ) -> Option<(PacketGW, Option<Vec<u8>>)>
    where
        R: AsyncRead + Unpin + Send,
    {
        let start = std::time::Instant::now();
        let mut sock = socket.lock().await;
        let mut header_buf = [0u8; PACKET_HEADER_SIZE];

        if sock.read_exact(&mut header_buf).await.is_err() {
            return None;
        }

        let packet = match PacketGW::from_bytes(&header_buf) {
            Some(p) => p,
            None => {
                log::error!("Invalid packet header");
                return None;
            }
        };

        let mut total_bytes = PACKET_HEADER_SIZE;
        {
            let mut stats = self.statistics.write().await;
            stats.inc_packets_in();
            stats.add_bytes_in(PACKET_HEADER_SIZE as u64);
        }

        let data = if packet.data_size > 0 {
            if packet.data_size as usize > consts::MAX_DATA_SIZE {
                log::error!("Packet is too large: {} bytes", packet.data_size);
                return None;
            }
            // Use the buffer pool for reading (zero-copy optimization).
            let mut pooled = acquire_buffer().await;
            pooled.resize(packet.data_size as usize, 0);
            if sock.read_exact(&mut pooled).await.is_err() {
                return None;
            }
            total_bytes += pooled.len();
            self.statistics
                .write()
                .await
                .add_bytes_in(pooled.len() as u64);

            // Decompress if necessary.
            let raw = pooled.to_vec();
            match decompress_data(&raw, packet.is_compressed()) {
                Some(decompressed) => Some(decompressed),
                None => {
                    log::error!("Packet decompression error (packet_id={})", packet.packet_id);
                    return None;
                }
            }
        } else {
            None
        };

        drop(sock);

        // Update Prometheus metrics
        let packet_type_str = format!("{:?}", packet.packet_type);
        metrics::inc_packets_total(&packet_type_str, "in");
        metrics::inc_bytes_total(total_bytes as u64, "in");
        metrics::observe_packet_processing(&packet_type_str, start.elapsed().as_secs_f64());

        Some((packet, data))
    }

    /// Handles an incoming Ping packet — sends a Pong in reply.
    pub async fn send_pong<W>(
        &self,
        socket: &Arc<Mutex<W>>,
        original_packet_id: u64,
    ) where
        W: AsyncWrite + Unpin + Send,
    {
        let pong_id = self.next_packet_id().await;
        let pong = PacketGW::new(PacketType::Pong, pong_id, 0, 0);
        let _ = self.send_gw_packet(socket, &pong, None).await;
        log::debug!("Pong sent in reply to packet_id={}", original_packet_id);
    }

    /// Updates the last activity timestamp.
    pub async fn update_keepalive(&self) {
        *self.keep_alive_stamp.write().await = crate::gateway::utils::now_epoch();
    }
}

/// Implementation of the `GatewayOperations` trait for the `Gateway` structure.
#[async_trait]
impl GatewayOperations for Gateway {
    async fn read_packet<R>(
        &self,
        socket: &Arc<Mutex<R>>,
    ) -> Option<(PacketGW, Option<Vec<u8>>)> where R: AsyncRead + Unpin + Send {
        self.read_gw_packet(socket).await
    }

    async fn send_packet<W>(
        &self,
        socket: &Arc<Mutex<W>>,
        packet: &PacketGW,
        data: Option<&[u8]>,
    ) -> Result<(), std::io::Error> where W: AsyncWrite + Unpin + Send {
        self.send_gw_packet(socket, packet, data).await
    }

    async fn next_packet_id(&self) -> u64 {
        Gateway::next_packet_id(self).await
    }

    async fn next_link_id(&self) -> u16 {
        Gateway::next_link_id(self).await
    }
}

impl Gateway {
    /// Closes all active connections during graceful shutdown.
    ///
    /// Sends Close packets to all active links and waits for them to close.
    pub async fn shutdown(&self) {
        log::info!("Closing all links");
        self.close_all_links().await;
        // Give the sockets time to finish closing.
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}
