//! Gateway implementation in client mode.
//!
//! The client connects to a remote gateway server, authenticates
//! and proxies TCP connections between the local application and the remote gateway.
//!
//! ## Client lifecycle:
//! 1. Connect to the remote gateway (`host_gw:port_gw`)
//! 2. Authentication (Challenge-Response -> `GwConnected`/`GwDeny`)
//! 3. Start the Ping loop every `GW_PING_PERIOD` seconds
//! 4. Start the packet exchange loop `gateway_exchange`
//! 5. On link loss: reconnect after `GW_RECONNECT_TIMEOUT` ms

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use log::{debug, error, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use tokio::time;

use crate::configuration::Configuration;
use crate::gateway::auth;
use crate::gateway::consts::{GatewayState, GW_PING_PERIOD, GW_RECONNECT_TIMEOUT, BUFFER_SIZE};
use crate::gateway::link::{ConnectionState, Link, LinkRef};
use crate::gateway::operations::GatewayOperations;
use crate::gateway::packet::{
    PacketGW, PacketType, DisconnectReason, PROTOCOL_VERSION_MAJOR, PACKET_HEADER_SIZE,
};
use crate::gateway::session_store;
use crate::gateway::utils::{format_bytes, now_epoch};
use crate::gateway::Gateway;

/// Gateway in client mode.
pub struct GatewayClient {
    /// Base structure with common logic.
    base: Gateway,
    /// Read half of the gateway connection (used by the exchange loop).
    gw_reader: RwLock<Option<Arc<Mutex<OwnedReadHalf>>>>,
    /// Write half of the gateway connection (used by ping and replies).
    gw_writer: RwLock<Option<Arc<Mutex<OwnedWriteHalf>>>>,
    /// Time of the last activity (epoch seconds).
    last_activity: AtomicI64,
}

impl GatewayClient {
    /// Creates a new client gateway with the given configuration.
    pub fn new(config: Configuration) -> Self {
        Self {
            base: Gateway::new(config),
            gw_reader: RwLock::new(None),
            gw_writer: RwLock::new(None),
            last_activity: AtomicI64::new(now_epoch()),
        }
    }

    /// Connects to the remote gateway server.
    async fn connect_gateway(&self) {
        loop {
            let addr = format!(
                "{}:{}",
                self.base.config.network.host_gw, self.base.config.network.port_gw
            );
            info!("Connecting to gateway {}", addr);

            match TcpStream::connect(&addr).await {
                Ok(socket) => {
                    info!("Gateway connection established");
                    self.on_connection(socket).await;
                    break;
                }
                Err(e) => {
                    error!("Failed to connect to the gateway: {}", e);
                    time::sleep(Duration::from_millis(GW_RECONNECT_TIMEOUT)).await;
                    info!("Retrying connection...");
                }
            }
        }
    }

    /// Handles the established TCP connection to the server.
    ///
    /// Performs authentication, starts the ping loop and the main packet exchange.
    async fn on_connection(&self, socket: TcpStream) {
        // Split the socket into independent read/write halves so that the
        // exchange loop (read) never blocks ping/reply writes on the mutex.
        let (reader, writer) = socket.into_split();
        let reader = Arc::new(Mutex::new(reader));
        let writer = Arc::new(Mutex::new(writer));
        *self.gw_reader.write().await = Some(reader.clone());
        *self.gw_writer.write().await = Some(writer.clone());
        *self.base.state.write().await = GatewayState::Processing;

        // Authentication
        if !self.gw_authenticate(&reader, &writer).await {
            warn!("Authentication failed");
            *self.gw_reader.write().await = None;
            *self.gw_writer.write().await = None;
            return;
        }

        info!("Authentication successful, starting packet exchange");
        *self.base.state.write().await = GatewayState::Connected;
        self.last_activity.store(now_epoch(), Ordering::SeqCst);

        // Start the ping loop in the background
        let writer_clone = writer.clone();
        let base_clone = self.base.clone();
        tokio::spawn(async move {
            Self::start_ping_loop(base_clone, writer_clone).await;
        });

        // Main packet exchange loop
        self.gateway_exchange(reader, writer).await;
    }

    /// Performs Challenge-Response authentication with the server.
    ///
    /// Protocol:
    /// 1. The client waits for `GwAuthChallenge` with a random nonce from the server.
    /// 2. The client computes HMAC-SHA256(key, nonce) and sends `GwAuthResponse`.
    /// 3. The server verifies the response and answers `GwConnected` or `GwDeny`.
    ///
    /// Returns `true` on successful authentication (`GwConnected`).
    async fn gw_authenticate(
        &self,
        reader: &Arc<Mutex<OwnedReadHalf>>,
        writer: &Arc<Mutex<OwnedWriteHalf>>,
    ) -> bool {
        // Step 1: receive the challenge with the nonce from the server.
        let (challenge_id, nonce) = {
            let mut sock = reader.lock().await;
            let mut header_buf = [0u8; PACKET_HEADER_SIZE];
            if sock.read_exact(&mut header_buf).await.is_err() {
                error!("Failed to read the challenge from the server");
                return false;
            }
            let pkt = match PacketGW::from_bytes(&header_buf) {
                Some(p) => p,
                None => {
                    error!("Invalid challenge packet header");
                    return false;
                }
            };
            if pkt.packet_type != PacketType::GwAuthChallenge {
                warn!("Expected GwAuthChallenge, got {:?}", pkt.packet_type);
                return false;
            }
            if pkt.data_size as usize != auth::NONCE_SIZE {
                error!("Invalid nonce size: {}", pkt.data_size);
                return false;
            }
            let mut nonce = vec![0u8; pkt.data_size as usize];
            if sock.read_exact(&mut nonce).await.is_err() {
                error!("Failed to read the nonce");
                return false;
            }
            (pkt.packet_id, nonce)
        };
        debug!("Challenge received (packet_id={})", challenge_id);

        // Step 2: compute the HMAC response and send it.
        let key = self.base.config.general.key.as_bytes();
        let response = auth::compute_response(key, &nonce);
        let packet_id = self.base.next_packet_id().await;
        let resp_packet = PacketGW::new(
            PacketType::GwAuthResponse,
            packet_id,
            0,
            response.len() as u16,
        );

        {
            let mut sock = writer.lock().await;
            if sock.write_all(&resp_packet.to_bytes()).await.is_err() {
                return false;
            }
            if sock.write_all(&response).await.is_err() {
                return false;
            }
        }
        debug!("Authentication response sent");

        // Step 3: wait for the server's verdict.
        let mut header_buf = [0u8; PACKET_HEADER_SIZE];
        let mut sock = reader.lock().await;
        if sock.read_exact(&mut header_buf).await.is_err() {
            return false;
        }

        match PacketGW::from_bytes(&header_buf) {
            Some(pkt) => match pkt.packet_type {
                PacketType::GwConnected => {
                    // Verify the server protocol version (12 bytes: 3 x u32 LE).
                    if pkt.data_size as usize >= 12 {
                        let mut version_buf = vec![0u8; 12];
                        if sock.read_exact(&mut version_buf).await.is_err() {
                            error!("Failed to read the protocol version");
                            return false;
                        }
                        let (sv_major, sv_minor, _sv_patch) = {
                            let major = u32::from_le_bytes(
                                version_buf[0..4].try_into().unwrap(),
                            );
                            let minor = u32::from_le_bytes(
                                version_buf[4..8].try_into().unwrap(),
                            );
                            let patch = u32::from_le_bytes(
                                version_buf[8..12].try_into().unwrap(),
                            );
                            (major, minor, patch)
                        };
                        if sv_major != PROTOCOL_VERSION_MAJOR {
                            error!(
                                "Incompatible server protocol version: {}.{}.{} (expected major={})",
                                sv_major, sv_minor, _sv_patch, PROTOCOL_VERSION_MAJOR
                            );
                            return false;
                        }
                        debug!(
                            "Server protocol version: {}.{}.{}",
                            sv_major, sv_minor, _sv_patch
                        );
                    }
                    info!("The server confirmed authentication");
                    true
                }
                PacketType::GwDeny => {
                    // Read the rejection reason if it is provided.
                    if pkt.data_size > 0 {
                        let mut reason_buf = vec![0u8; pkt.data_size as usize];
                        if sock.read_exact(&mut reason_buf).await.is_ok() {
                            let reason = DisconnectReason::from(reason_buf[0]);
                            warn!("The server rejected authentication: reason {:?}", reason);
                            return false;
                        }
                    }
                    warn!("The server rejected authentication");
                    false
                }
                _ => {
                    warn!("Unexpected packet type: {:?}", pkt.packet_type);
                    false
                }
            },
            None => {
                error!("Invalid packet header");
                false
            }
        }
    }

    /// Loop for periodic Ping packet sending (every `GW_PING_PERIOD` seconds).
    async fn start_ping_loop(base: Gateway, writer: Arc<Mutex<OwnedWriteHalf>>) {
        let mut interval = time::interval(Duration::from_secs(GW_PING_PERIOD));
        loop {
            interval.tick().await;
            let packet_id = base.next_packet_id().await;
            let packet = PacketGW::new(PacketType::Ping, packet_id, 0, 0);
            if base.send_gw_packet(&writer, &packet, None).await.is_err() {
                break;
            }
            base.update_keepalive().await;
            debug!("Ping sent");
        }
    }

    /// Main packet exchange loop with the server.
    ///
    /// Reads packets from the gateway socket and dispatches them by type:
    /// - Ping/Pong — keep-alive
    /// - Open — connect to the local application
    /// - Regular — application data transfer
    /// - Close — connection closing
    async fn gateway_exchange(
        &self,
        reader: Arc<Mutex<OwnedReadHalf>>,
        writer: Arc<Mutex<OwnedWriteHalf>>,
    ) {
        loop {
            match self.base.read_packet(&reader).await {
                Some((packet, data)) => {
                    match packet.packet_type {
                        PacketType::Ping => {
                            self.base.send_pong(&writer, packet.packet_id).await;
                            self.last_activity.store(now_epoch(), Ordering::SeqCst);
                        }
                        PacketType::Pong => {
                            self.last_activity.store(now_epoch(), Ordering::SeqCst);
                            self.base.update_keepalive().await;
                        }
                        PacketType::Open => {
                            let link_id = packet.link_id;
                            self.connect_client(link_id, writer.clone()).await;
                        }
                        PacketType::Close => {
                            let link_id = packet.link_id;
                            if let Some(link) = self.base.remove_link(link_id).await {
                                link.lock().await.close_connection().await;
                            }
                        }
                        PacketType::Regular => {
                            if let Some(payload) = data {
                                self.forward_to_app(packet.link_id, &payload).await;
                            }
                        }
                        PacketType::GwDeny => {
                            error!("The server refused the connection");
                            break;
                        }
                        _ => {
                            warn!("Unexpected packet type: {:?}", packet.packet_type);
                        }
                    }
                }
                None => {
                    info!("Connection to the server lost");
                    break;
                }
            }
        }
    }

    /// Connects to the local application upon receiving an Open packet.
    ///
    /// Sends Opened on successful connection or OpenFail on error.
    async fn connect_client(&self, conn_id: u16, writer: Arc<Mutex<OwnedWriteHalf>>) {
        let app_addr = format!(
            "{}:{}",
            self.base.config.network.host_app, self.base.config.network.port_app
        );

        match TcpStream::connect(&app_addr).await {
            Ok(stream) => {
                let link = Arc::new(Mutex::new(Link::new(stream, conn_id)));
                self.base.add_link(link.clone()).await;

                link.lock().await.set_state(ConnectionState::Connected);
                self.send_opened(conn_id, &writer).await;
                info!("Connection to application #{} established", conn_id);

                // Start data exchange with the application
                let link_clone = link.clone();
                let writer_clone = writer.clone();
                let base = self.base.clone();
                tokio::spawn(async move {
                    Self::app_exchange(base, link_clone, writer_clone).await;
                });
            }
            Err(e) => {
                error!("Error connecting to application #{}: {}", conn_id, e);
                self.send_open_fail(conn_id, &writer).await;
            }
        }
    }

    /// Sends an Opened packet to confirm the connection opening.
    async fn send_opened(&self, link_id: u16, writer: &Arc<Mutex<OwnedWriteHalf>>) {
        let packet_id = self.base.next_packet_id().await;
        let packet = PacketGW::new(PacketType::Opened, packet_id, link_id, 0);
        let _ = self.base.send_gw_packet(writer, &packet, None).await;
    }

    /// Sends an OpenFail packet when connecting to the application fails.
    async fn send_open_fail(&self, link_id: u16, writer: &Arc<Mutex<OwnedWriteHalf>>) {
        let packet_id = self.base.next_packet_id().await;
        let packet = PacketGW::new(PacketType::OpenFail, packet_id, link_id, 0);
        let _ = self.base.send_gw_packet(writer, &packet, None).await;
    }

    /// Forwards data from a Regular packet to the local application.
    async fn forward_to_app(&self, link_id: u16, data: &[u8]) {
        if let Some(link) = self.base.links.read().await.get(&link_id) {
            link.lock().await.send_packet(Some(data)).await;
        } else {
            warn!("Received data for a non-existent connection #{}", link_id);
        }
    }

    /// Bidirectional data exchange between the application and the gateway.
    ///
    /// Reads data from the application socket, packs it into Regular packets and sends it to the server.
    /// On connection close, sends a Close packet.
    async fn app_exchange(
        base: Gateway,
        link: LinkRef,
        writer: Arc<Mutex<OwnedWriteHalf>>,
    ) {
        let link_id = link.lock().await.connection_id;
        let reader = link.lock().await.reader.clone();
        let mut buffer = vec![0u8; BUFFER_SIZE];

        loop {
            // Read via the dedicated reader lock: the whole link lock is NOT
            // held while blocked in read, so control packets stay responsive.
            let read_result = reader.lock().await.read(&mut buffer).await;

            match read_result {
                Ok(0) => {
                    info!("Application #{} closed the connection", link_id);
                    break;
                }
                Ok(n) => {
                    let packet_id = base.next_packet_id().await;
                    let packet = PacketGW::new(PacketType::Regular, packet_id, link_id, n as u16);
                    if base
                        .send_gw_packet(&writer, &packet, Some(&buffer[..n]))
                        .await
                        .is_err()
                    {
                        break;
                    }
                    link.lock().await.add_incoming(n as u64);
                }
                Err(e) => {
                    error!("Error reading from application #{}: {}", link_id, e);
                    break;
                }
            }
        }

        // Send Close and remove the link
        let packet_id = base.next_packet_id().await;
        let close_packet = PacketGW::new(PacketType::Close, packet_id, link_id, 0);
        let _ = base.send_gw_packet(&writer, &close_packet, None).await;
        base.remove_link(link_id).await;
    }

    /// Sends a Ping packet to check connectivity.
    pub async fn send_ping(&self) {
        if let Some(writer) = self.gw_writer.read().await.as_ref() {
            let packet_id = self.base.next_packet_id().await;
            let packet = PacketGW::new(PacketType::Ping, packet_id, 0, 0);
            let _ = self.base.send_gw_packet(writer, &packet, None).await;
            debug!("Ping sent");
        } else {
            warn!("No active connection to the gateway");
        }
    }
}

#[async_trait]
impl crate::gateway::GatewayTrait for GatewayClient {
    /// Main client loop: connection, authentication, operation, reconnection.
    async fn run(&self) {
        // Restore or create a persistent session identifier
        // (Session Migration: survives application restarts).
        let session_id = session_store::load_session_id(None).unwrap_or_else(|| {
            let id = session_store::generate_session_id();
            id
        });
        if let Err(e) = session_store::persist_session_id(session_id, None) {
            warn!("Failed to save the session_id: {}", e);
        }
        info!("Session ID: {}", session_id);

        loop {
            *self.base.state.write().await = GatewayState::Waiting;
            self.connect_gateway().await;
            self.on_disconnection().await;
        }
    }

    /// Returns the current gateway state.
    fn state(&self) -> GatewayState {
        self.base.state.try_read().map(|s| *s).unwrap_or(GatewayState::Waiting)
    }

    /// Prints connection and traffic statistics.
    async fn print_stats(&self) {
        let state = self.state();
        let stats = self.base.statistics.read().await;
        let links = self.base.links.read().await;
        let keep_alive = *self.base.keep_alive_stamp.read().await;

        println!("State: {}", state);
        println!("Statistics:");
        println!("  Packets in: {}", stats.packets_in);
        println!("  Packets out: {}", stats.packets_out);
        println!("  Bytes in: {} ({})", stats.bytes_in, format_bytes(stats.bytes_in));
        println!("  Bytes out: {} ({})", stats.bytes_out, format_bytes(stats.bytes_out));
        println!("Active connections: {}", links.len());
        for (id, link) in links.iter() {
            let lk = link.lock().await;
            let (bytes_in, _bytes_out, link_state) = lk.get_stats();
            println!("  #{}[{:?}]: in={}", id, link_state, format_bytes(bytes_in));
        }
        println!("Last activity: {} (keep-alive: {})",
            self.last_activity.load(Ordering::SeqCst), keep_alive);
    }

    /// Handles the connection loss with the remote gateway.
    ///
    /// Closes all active Links and resets the state to `WAITING`.
    async fn on_disconnection(&self) {
        warn!("Gateway connection lost, closing all Links");
        self.base.close_all_links().await;
        *self.gw_reader.write().await = None;
        *self.gw_writer.write().await = None;
        *self.base.state.write().await = GatewayState::Waiting;
    }

    /// Closes all active connections during graceful shutdown.
    async fn shutdown(&self) {
        info!("Client graceful shutdown");
        self.base.shutdown().await;
        if let Some(writer) = self.gw_writer.write().await.take() {
            let _ = writer.lock().await.shutdown().await;
        }
    }
}
