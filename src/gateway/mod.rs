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

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU16, AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use std::marker::Unpin;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::sync::{Mutex, RwLock};

use crate::configuration::Configuration;
use crate::gateway::buffer_pool::acquire_buffer;
use crate::gateway::compression::{compress_data, decompress_data, MIN_COMPRESS_SIZE};
use crate::gateway::consts::GatewayState;
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

#[derive(Debug, Default)]
pub struct Statistics {
    /// Number of received packets.
    pub packets_in: AtomicU64,
    /// Number of sent packets.
    pub packets_out: AtomicU64,
    /// Total volume of received data (bytes).
    pub bytes_in: AtomicU64,
    /// Total volume of sent data (bytes).
    pub bytes_out: AtomicU64,
    /// Connection establishment time (Unix timestamp in seconds).
    pub connection_time: AtomicI64,
}

impl Statistics {
    /// Creates new statistics with zero values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Reads a consistent-enough snapshot of the counters.
    pub fn snapshot(&self) -> StatisticsSnapshot {
        StatisticsSnapshot {
            packets_in: self.packets_in.load(Ordering::Relaxed),
            packets_out: self.packets_out.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            connection_time: self.connection_time.load(Ordering::Relaxed),
        }
    }
}

/// Plain-value copy of [`Statistics`] for printing/reporting.
#[derive(Debug, Clone, Copy)]
pub struct StatisticsSnapshot {
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

/// Base gateway structure containing common logic and state.
///
/// Used as the inner part of `GatewayClient` and `GatewayServer`
/// via composition to reuse the common logic for managing
/// connections (`Link`), statistics and identifiers.
#[derive(Clone)]
pub struct Gateway {
    /// Application configuration.
    pub config: Configuration,
    /// Current gateway state (changed rarely, lock is fine).
    pub state: Arc<RwLock<GatewayState>>,
    /// Active connections (`Link`) to local applications.
    pub links: Arc<RwLock<HashMap<u16, LinkRef>>>,
    /// Global gateway statistics (lock-free atomics).
    pub statistics: Arc<Statistics>,
    /// Identifier counter for new packets (lock-free, shared with clones).
    pub packet_id_counter: Arc<AtomicU64>,
    /// Identifier counter for new connections (lock-free, shared with clones).
    pub link_id_counter: Arc<AtomicU16>,
    /// Timestamp of the last activity for keep-alive (lock-free, shared).
    pub keep_alive_stamp: Arc<AtomicI64>,
}

impl Gateway {
    /// Creates a new base gateway instance with the given configuration.
    pub fn new(config: Configuration) -> Self {
        Self {
            config,
            state: Arc::new(RwLock::new(GatewayState::Waiting)),
            links: Arc::new(RwLock::new(HashMap::new())),
            statistics: Arc::new(Statistics::new()),
            packet_id_counter: Arc::new(AtomicU64::new(0)),
            link_id_counter: Arc::new(AtomicU16::new(1)),
            keep_alive_stamp: Arc::new(AtomicI64::new(crate::gateway::utils::now_epoch())),
        }
    }

    /// Generates the next unique packet identifier.
    pub fn next_packet_id(&self) -> u64 {
        self.packet_id_counter.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Generates the next connection identifier.
    pub fn next_link_id(&self) -> u16 {
        self.link_id_counter.fetch_add(1, Ordering::Relaxed)
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

        // Build a fresh header from the scalar fields (all Copy) instead of
        // cloning the whole `PacketGW`: compression may change the flag and
        // the payload size.
        let mut send_packet = PacketGW::new(packet.packet_type, packet.packet_id, packet.link_id, 0);
        send_packet.flags = packet.flags;
        let (payload, compressed_flag): (Cow<'_, [u8]>, bool) = match data {
            Some(d) if d.len() >= MIN_COMPRESS_SIZE => {
                let result = compress_data(d);
                if result.is_compressed {
                    send_packet.set_flag(FLAG_COMPRESSED);
                    send_packet.data_size = result.data.len() as u16;
                    (Cow::Owned(result.data), true)
                } else {
                    send_packet.data_size = d.len() as u16;
                    (Cow::Borrowed(d), false)
                }
            }
            Some(d) => {
                send_packet.data_size = d.len() as u16;
                (Cow::Borrowed(d), false)
            }
            None => (Cow::Borrowed(&[]), false),
        };

        let payload_len = payload.len();
        let total_bytes = PACKET_HEADER_SIZE + payload_len;

        // Both header and payload are flushed in a single vectored write
        // whenever possible (one syscall instead of two).
        let mut sock = socket.lock().await;
        let header = send_packet.to_bytes();
        if payload_len > 0 {
            use std::io::IoSlice;
            let bufs = [
                IoSlice::new(&header),
                IoSlice::new(&payload),
            ];
            // write_vectored may be partial; fall back to writing the rest.
            let mut written = sock.write_vectored(&bufs).await?;
            if written < total_bytes {
                for buf in bufs {
                    let start = written.min(buf.len());
                    sock.write_all(&buf[start..]).await?;
                    written += buf.len() - start;
                }
            }
        } else {
            sock.write_all(&header).await?;
        }
        sock.flush().await?;
        drop(sock);

        // Update internal statistics (lock-free)
        self.statistics.packets_out.fetch_add(1, Ordering::Relaxed);
        self.statistics
            .bytes_out
            .fetch_add(total_bytes as u64, Ordering::Relaxed);

        // Update Prometheus metrics (no per-packet allocation: static label)
        let packet_type_str = send_packet.packet_type.as_str();
        metrics::inc_packets_total(packet_type_str, "out");
        metrics::inc_bytes_total(total_bytes as u64, "out");
        if compressed_flag {
            metrics::inc_packets_total(packet_type_str, "compressed");
        }
        metrics::observe_packet_processing(packet_type_str, start.elapsed().as_secs_f64());

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
        let data = if packet.data_size > 0 {
            // Use the buffer pool for reading (zero-copy optimization).
            let mut pooled = acquire_buffer();
            pooled.resize(packet.data_size as usize, 0);
            if sock.read_exact(&mut pooled).await.is_err() {
                return None;
            }
            total_bytes += pooled.len();

            match decompress_data(&pooled, packet.is_compressed()) {
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

        // Update internal statistics (single lock-free update per packet)
        let stats = &self.statistics;
        stats.packets_in.fetch_add(1, Ordering::Relaxed);
        stats
            .bytes_in
            .fetch_add(total_bytes as u64, Ordering::Relaxed);

        // Update Prometheus metrics (no per-packet allocation: static label)
        let packet_type_str = packet.packet_type.as_str();
        metrics::inc_packets_total(packet_type_str, "in");
        metrics::inc_bytes_total(total_bytes as u64, "in");
        metrics::observe_packet_processing(packet_type_str, start.elapsed().as_secs_f64());

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
        let pong_id = self.next_packet_id();
        let pong = PacketGW::new(PacketType::Pong, pong_id, 0, 0);
        let _ = self.send_gw_packet(socket, &pong, None).await;
        log::debug!("Pong sent in reply to packet_id={}", original_packet_id);
    }

    /// Updates the last activity timestamp.
    pub fn update_keepalive(&self) {
        self.keep_alive_stamp.store(crate::gateway::utils::now_epoch(), Ordering::Relaxed);
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

    fn next_packet_id(&self) -> u64 {
        Gateway::next_packet_id(self)
    }

    fn next_link_id(&self) -> u16 {
        Gateway::next_link_id(self)
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
