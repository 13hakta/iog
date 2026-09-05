//! Gateway implementation in server mode.
//!
//! The server accepts incoming connections from gateway clients, authenticates them
//! and proxies TCP connections between clients and the local application.
//!
//! ## Server lifecycle:
//! 1. Listen on the gateway port () for incoming clients
//! 2. Authentication: receive , verify the key, send /
//! 3. Authentication timeout  ms
//! 4. Listen to the local application ()
//! 5. Route packets between clients and the application

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use log::{error, info, warn};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock};
use tokio::time;

use crate::configuration::Configuration;
use crate::gateway::auth;
use crate::gateway::consts::{BUFFER_SIZE, GW_READINESS_TIMEOUT, GatewayState};
use crate::gateway::link::{ConnectionState, Link};
use crate::gateway::packet::{
    PacketGW, PacketType, DisconnectReason, PACKET_HEADER_SIZE, PROTOCOL_VERSION_MAJOR,
    PROTOCOL_VERSION_MINOR, PROTOCOL_VERSION_PATCH,
};
use crate::gateway::operations::GatewayOperations;
use crate::gateway::session_store;
use crate::gateway::session_store::SessionStore;
use crate::gateway::utils::now_epoch;
use crate::gateway::Gateway;
use uuid::Uuid;

/// Information about a connected client.
struct ClientSession {
    /// Write half of the client connection (used for sends and shutdown).
    writer: Arc<Mutex<OwnedWriteHalf>>,
    /// Client IP address (for detecting repeated connections).
    addr: SocketAddr,
    /// Connection time.
    connected_at: i64,
    /// Session identifier (Session Migration).
    session_id: Uuid,
}

/// Gateway in server mode.
pub struct GatewayServer {
    /// Base structure with common logic.
    base: Gateway,
    /// Active client sessions (keyed by IP address).
    client_sessions: Arc<RwLock<HashMap<SocketAddr, ClientSession>>>,
    /// Routing table: link_id -> client address that owns the connection.
    link_clients: Arc<RwLock<HashMap<u16, SocketAddr>>>,
    /// Session store for Session Migration.
    session_store: Arc<SessionStore>,
}

impl GatewayServer {
    /// Creates a new server gateway with the given configuration.
    pub fn new(config: Configuration) -> Self {
        Self {
            base: Gateway::new(config),
            client_sessions: Arc::new(RwLock::new(HashMap::new())),
            link_clients: Arc::new(RwLock::new(HashMap::new())),
            session_store: Arc::new(SessionStore::new()),
        }
    }

    /// Performs Challenge-Response authentication of the client.
    ///
    /// Protocol:
    /// 1. The server sends `GwAuthChallenge` with a random nonce.
    /// 2. The client returns `GwAuthResponse` with HMAC-SHA256(key, nonce).
    /// 3. The server verifies the response and sends `GwConnected` or `GwDeny`.
    ///
    /// The key is never transmitted over the network in plain text.
    async fn authenticate_client(
        base: &Gateway,
        reader: &Arc<Mutex<OwnedReadHalf>>,
        writer: &Arc<Mutex<OwnedWriteHalf>>,
        addr: SocketAddr,
    ) -> bool {
        // Step 1: send the challenge with a random nonce.
        let nonce = auth::generate_nonce();
        let packet_id = base.next_packet_id().await;
        let challenge =
            PacketGW::new(PacketType::GwAuthChallenge, packet_id, 0, nonce.len() as u16);

        {
            let mut sock = writer.lock().await;
            if sock.write_all(&challenge.to_bytes()).await.is_err()
                || sock.write_all(&nonce).await.is_err()
            {
                error!("Failed to send the challenge to client {}", addr);
                return false;
            }
        }

        // Step 2: receive the response from the client (with a timeout).
        let read_result = time::timeout(Duration::from_millis(GW_READINESS_TIMEOUT), async {
            let mut sock = reader.lock().await;
            let mut header_buf = [0u8; PACKET_HEADER_SIZE];
            sock.read_exact(&mut header_buf).await?;
            let pkt = PacketGW::from_bytes(&header_buf).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid header")
            })?;
            if pkt.packet_type != PacketType::GwAuthResponse {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Expected a GwAuthResponse packet",
                ));
            }
            let mut resp_buf = vec![0u8; pkt.data_size as usize];
            sock.read_exact(&mut resp_buf).await?;
            Ok((pkt, resp_buf))
        })
        .await;

        let (response_packet, response_bytes) = match read_result {
            Ok(Ok(pair)) => pair,
            Ok(Err(e)) => {
                error!("Error reading the response from {}: {}", addr, e);
                return false;
            }
            Err(_) => {
                warn!("Timeout waiting for the response from {}", addr);
                return false;
            }
        };

        // Step 3: verify the HMAC response (constant-time comparison).
        let key = base.config.general.key.as_bytes();
        if auth::verify_response(key, &nonce, &response_bytes) {
            info!("Client {} authentication successful", addr);
            // Send GwConnected with the protocol version (12 bytes: 3 x u32 LE).
            let mut version_data = Vec::with_capacity(12);
            version_data.extend_from_slice(&PROTOCOL_VERSION_MAJOR.to_le_bytes());
            version_data.extend_from_slice(&PROTOCOL_VERSION_MINOR.to_le_bytes());
            version_data.extend_from_slice(&PROTOCOL_VERSION_PATCH.to_le_bytes());
            let connected = PacketGW::new(
                PacketType::GwConnected,
                response_packet.packet_id,
                0,
                version_data.len() as u16,
            );
            let mut sock = writer.lock().await;
            sock.write_all(&connected.to_bytes()).await.is_ok()
                && sock.write_all(&version_data).await.is_ok()
        } else {
            warn!("Client {} authentication rejected (invalid HMAC)", addr);
            let deny = PacketGW::new(
                PacketType::GwDeny,
                response_packet.packet_id,
                0,
                1,
            );
            let mut sock = writer.lock().await;
            let _ = sock.write_all(&deny.to_bytes()).await;
            let _ = sock.write_all(&[DisconnectReason::AuthFailed as u8]).await;
            false
        }
    }

    /// Handles an incoming connection from a gateway client.
    ///
    /// Performs authentication, registers the session and spawns the
    /// per-client packet exchange loop (`client_exchange`).
    async fn handle_gateway_connection(
        base: Gateway,
        stream: TcpStream,
        client_sessions: Arc<RwLock<HashMap<SocketAddr, ClientSession>>>,
        link_clients: Arc<RwLock<HashMap<u16, SocketAddr>>>,
        session_store: Arc<SessionStore>,
    ) {
        let addr = stream
            .peer_addr()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)));
        info!("Gateway client connected from {}", addr);

        // Split the socket into independent halves so that the exchange
        // loop (read) never blocks writes on the mutex.
        let (reader, writer) = stream.into_split();
        let reader = Arc::new(Mutex::new(reader));
        let writer = Arc::new(Mutex::new(writer));

        // Close the previous connection from this IP if present.
        {
            let mut sessions = client_sessions.write().await;
            if let Some(old_session) = sessions.remove(&addr) {
                warn!("Closing the previous connection from {}", addr);
                let _ = old_session.writer.lock().await.shutdown().await;
            }
        }

        // Challenge-Response authentication (the key is not transmitted over the network).
        if !Self::authenticate_client(&base, &reader, &writer, addr).await {
            return;
        }

        *base.state.write().await = GatewayState::Connected;

        // Register the client session (Session Migration).
        let session_id = session_store::generate_session_id();
        session_store.save_session(session_id, Vec::new()).await;
        info!("Session {} registered for {}", session_id, addr);

        client_sessions.write().await.insert(
            addr,
            ClientSession {
                writer: writer.clone(),
                addr,
                connected_at: now_epoch(),
                session_id,
            },
        );

        // Spawn the per-client packet exchange loop.
        tokio::spawn(Self::client_exchange(
            base,
            reader,
            addr,
            session_id,
            client_sessions,
            link_clients,
            session_store,
            writer,
        ));
    }

    /// Per-client packet exchange loop.
    ///
    /// Reads packets from the client socket and dispatches them by type:
    /// - `Ping` — reply with `Pong` (keep-alive)
    /// - `Regular` — forward application data into the bound link
    /// - `Opened` — mark the link as connected
    /// - `OpenFail`/`Close` — close and remove the link
    ///
    /// On connection loss, releases all links owned by this client.
    async fn client_exchange(
        base: Gateway,
        reader: Arc<Mutex<OwnedReadHalf>>,
        addr: SocketAddr,
        session_id: Uuid,
        client_sessions: Arc<RwLock<HashMap<SocketAddr, ClientSession>>>,
        link_clients: Arc<RwLock<HashMap<u16, SocketAddr>>>,
        session_store: Arc<SessionStore>,
        writer: Arc<Mutex<OwnedWriteHalf>>,
    ) {
        loop {
            match base.read_packet(&reader).await {
                Some((packet, data)) => {
                    log::debug!(
                        "RX packet type={:?} link={} size={} flags={}",
                        packet.packet_type, packet.link_id, packet.data_size, packet.flags
                    );
                    match packet.packet_type {
                    PacketType::Ping => {
                        base.send_pong(&writer, packet.packet_id).await;
                    }
                    PacketType::Pong => {}
                    PacketType::Regular => {
                        if let Some(payload) = data {
                            let link = base.links.read().await.get(&packet.link_id).cloned();
                            match link {
                                Some(link) => {
                                    link.lock().await.send_packet(Some(&payload)).await;
                                }
                                None => {
                                    warn!(
                                        "Received data for a non-existent connection #{}",
                                        packet.link_id
                                    );
                                }
                            }
                        }
                    }
                    PacketType::Opened => {
                        let link = base.links.read().await.get(&packet.link_id).cloned();
                        if let Some(link) = link {
                            link.lock().await.set_state(ConnectionState::Connected);
                            info!("Connection #{} confirmed by the client", packet.link_id);
                        }
                    }
                    PacketType::OpenFail => {
                        warn!(
                            "The client refused to open connection #{}, closing the link",
                            packet.link_id
                        );
                        Self::remove_link(&base, &link_clients, packet.link_id).await;
                    }
                    PacketType::Close => {
                        info!("The client requested to close connection #{}", packet.link_id);
                        Self::remove_link(&base, &link_clients, packet.link_id).await;
                    }
                    PacketType::GwDeny => {
                        warn!("Client {} received GwDeny, terminating the session", addr);
                        break;
                    }
                    _ => {}
                    }
                }
                None => break,
            }
        }

        warn!("Gateway client {} disconnected", addr);
        Self::cleanup_client(
            &base,
            addr,
            session_id,
            client_sessions,
            link_clients,
            session_store,
        )
        .await;
    }

    /// Closes and removes a link along with its client route.
    async fn remove_link(
        base: &Gateway,
        link_clients: &Arc<RwLock<HashMap<u16, SocketAddr>>>,
        link_id: u16,
    ) {
        link_clients.write().await.remove(&link_id);
        if let Some(link) = base.remove_link(link_id).await {
            link.lock().await.close_connection().await;
        }
    }

    /// Releases all resources of a disconnected client session.
    ///
    /// Closes all links owned by this client, removes their routes,
    /// removes the session from the registry and from the session store.
    async fn cleanup_client(
        base: &Gateway,
        addr: SocketAddr,
        session_id: Uuid,
        client_sessions: Arc<RwLock<HashMap<SocketAddr, ClientSession>>>,
        link_clients: Arc<RwLock<HashMap<u16, SocketAddr>>>,
        session_store: Arc<SessionStore>,
    ) {
        // Collect and remove routes owned by this client.
        let owned_link_ids: Vec<u16> = {
            let mut routes = link_clients.write().await;
            let owned: Vec<u16> = routes
                .iter()
                .filter(|(_, client_addr)| **client_addr == addr)
                .map(|(link_id, _)| *link_id)
                .collect();
            for link_id in &owned {
                routes.remove(link_id);
            }
            owned
        };

        for link_id in owned_link_ids {
            if let Some(link) = base.remove_link(link_id).await {
                link.lock().await.close_connection().await;
            }
        }

        // Remove the session only if it was not replaced by a newer one
        // from the same address (reconnection race).
        {
            let mut sessions = client_sessions.write().await;
            let is_current = sessions
                .get(&addr)
                .map(|s| s.session_id == session_id)
                .unwrap_or(false);
            if is_current {
                sessions.remove(&addr);
            }
        }

        let _links = session_store.restore_session(session_id).await;

        // If there are no clients left, the server is waiting for connections again.
        if client_sessions.read().await.is_empty() {
            *base.state.write().await = GatewayState::Waiting;
        }
    }

    /// Starts listening for incoming connections from gateway clients.
    async fn listen_gateway(
        base: Gateway,
        client_sessions: Arc<RwLock<HashMap<SocketAddr, ClientSession>>>,
        link_clients: Arc<RwLock<HashMap<u16, SocketAddr>>>,
        session_store: Arc<SessionStore>,
    ) {
        let addr = format!("0.0.0.0:{}", base.config.network.port_gw);
        let listener = match TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to bind the gateway port {}: {}", addr, e);
                return;
            }
        };
        info!("Listening for gateway connections on {}", addr);
        *base.state.write().await = GatewayState::Waiting;

        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let base = base.clone();
                    let sessions = client_sessions.clone();
                    let routes = link_clients.clone();
                    let store = session_store.clone();
                    tokio::spawn(Self::handle_gateway_connection(
                        base, stream, sessions, routes, store,
                    ));
                }
                Err(e) => error!("Error accepting a gateway connection: {}", e),
            }
        }
    }

    /// Handles an incoming connection from the local application.
    ///
    /// Binds the new link to an active client session, sends `Open` to that
    /// client and spawns the `app_exchange` loop forwarding application data.
    async fn handle_app_connection(
        base: Gateway,
        stream: TcpStream,
        client_sessions: Arc<RwLock<HashMap<SocketAddr, ClientSession>>>,
        link_clients: Arc<RwLock<HashMap<u16, SocketAddr>>>,
    ) {
        let mut stream = stream;
        let peer = stream
            .peer_addr()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)));
        let link_id = base.next_link_id().await;
        info!("New application connection #{} from {}", link_id, peer);

        // Pick an active client session to route this link to.
        let client = {
            let sessions = client_sessions.read().await;
            sessions
                .values()
                .next()
                .map(|s| (s.writer.clone(), s.addr))
        };

        let (client_writer, _client_addr) = match client {
            Some(pair) => pair,
            None => {
                warn!(
                    "No connected gateway clients, rejecting application connection #{}",
                    link_id
                );
                let _ = stream.shutdown().await;
                return;
            }
        };

        let link = Arc::new(Mutex::new(Link::new(stream, link_id)));
        base.add_link(link.clone()).await;
        link_clients.write().await.insert(link_id, _client_addr);

        // Send Open only to the client this link is routed to.
        let packet_id = base.next_packet_id().await;
        let open = PacketGW::new(PacketType::Open, packet_id, link_id, 0);
        if base
            .send_gw_packet(&client_writer, &open, None)
            .await
            .is_err()
        {
            error!("Failed to send Open for connection #{}", link_id);
        }
        link.lock().await.set_state(ConnectionState::Waiting);

        // Spawn the application data forwarding loop.
        tokio::spawn(Self::app_exchange(
            base,
            link,
            client_writer,
            link_id,
            link_clients,
        ));
    }

    /// Bidirectional data forwarding loop: application -> gateway client.
    ///
    /// Reads data from the application socket, packs it into `Regular`
    /// packets and sends them to the bound client. On connection loss,
    /// sends `Close` to the client and removes the link.
    async fn app_exchange(
        base: Gateway,
        link: crate::gateway::link::LinkRef,
        client_writer: Arc<Mutex<OwnedWriteHalf>>,
        link_id: u16,
        link_clients: Arc<RwLock<HashMap<u16, SocketAddr>>>,
    ) {
        let reader = link.lock().await.reader.clone();
        let mut buf = vec![0u8; BUFFER_SIZE];
        loop {
            // Read via the dedicated reader lock: the whole link lock is NOT
            // held while blocked in read, so control packets stay responsive.
            let n = reader.lock().await.read(&mut buf).await;

            match n {
                Ok(0) => {
                    info!("Application #{} closed the connection", link_id);
                    break;
                }
                Ok(n) => {
                    link.lock().await.add_incoming(n as u64);
                    let packet_id = base.next_packet_id().await;
                    let packet = PacketGW::new(PacketType::Regular, packet_id, link_id, n as u16);
                    if base
                        .send_gw_packet(&client_writer, &packet, Some(&buf[..n]))
                        .await
                        .is_err()
                    {
                        error!("Error sending data of connection #{} to the client", link_id);
                        break;
                    }
                }
                Err(e) => {
                    error!("Error reading from application #{}: {}", link_id, e);
                    break;
                }
            }
        }

        // Notify the client and release the link.
        let packet_id = base.next_packet_id().await;
        let close = PacketGW::new(PacketType::Close, packet_id, link_id, 0);
        let _ = base.send_gw_packet(&client_writer, &close, None).await;
        Self::remove_link(&base, &link_clients, link_id).await;
    }

    /// Starts listening to the local application.
    async fn listen_app(
        base: Gateway,
        client_sessions: Arc<RwLock<HashMap<SocketAddr, ClientSession>>>,
        link_clients: Arc<RwLock<HashMap<u16, SocketAddr>>>,
    ) {
        let addr = format!("0.0.0.0:{}", base.config.network.port_app);
        let listener = match TcpListener::bind(&addr).await {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to bind the application port {}: {}", addr, e);
                return;
            }
        };
        info!("Listening for application connections on {}", addr);

        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let base = base.clone();
                    let sessions = client_sessions.clone();
                    let routes = link_clients.clone();
                    tokio::spawn(Self::handle_app_connection(
                        base, stream, sessions, routes,
                    ));
                }
                Err(e) => error!("Error accepting an application connection: {}", e),
            }
        }
    }
}

#[async_trait]
impl crate::gateway::GatewayTrait for GatewayServer {
    /// Main server loop: listens to the gateway and the application in parallel.
    async fn run(&self) {
        let base = self.base.clone();
        let gw_sessions = self.client_sessions.clone();
        let gw_routes = self.link_clients.clone();
        let gw_store = self.session_store.clone();

        let app_base = self.base.clone();
        let app_sessions = self.client_sessions.clone();
        let app_routes = self.link_clients.clone();

        // Listen to clients and the application in parallel tasks.
        let gw_task = tokio::spawn(async move {
            Self::listen_gateway(base, gw_sessions, gw_routes, gw_store).await;
        });
        let app_task = tokio::spawn(async move {
            Self::listen_app(app_base, app_sessions, app_routes).await;
        });

        let _ = tokio::join!(gw_task, app_task);
    }

    fn state(&self) -> GatewayState {
        self.base.state.try_read().map(|s| *s).unwrap_or(GatewayState::Waiting)
    }

    /// Prints connection and traffic statistics.
    async fn print_stats(&self) {
        let state = self.state();
        let stats = self.base.statistics.read().await;
        let links = self.base.links.read().await;
        let sessions = self.client_sessions.read().await;
        let session_count = self.session_store.session_count().await;

        println!("GW state: {}", state);
        println!(" Since: {}", stats.connection_time);
        println!(" Packets out/in: {}/{}", stats.packets_out, stats.packets_in);
        println!(" Data in/out: {}/{}", stats.bytes_in, stats.bytes_out);
        println!(" Gateway clients: {} (sessions: {})", sessions.len(), session_count);
        for (_addr, session) in sessions.iter() {
            println!("  {}: connected at {} (session {})",
                session.addr, session.connected_at, session.session_id);
        }
        println!(" App connections: {}", links.len());
        for (id, link) in links.iter() {
            let lk = link.lock().await;
            let (bytes_in, bytes_out, link_state) = lk.get_stats();
            println!(" #{}[{}]: {}/{}", id, link_state, bytes_in, bytes_out);
        }
    }

    /// Handles the connection loss with the remote gateway.
    ///
    /// Closes all active Links and resets the state to `WAITING`.
    async fn on_disconnection(&self) {
        warn!("Gateway connection lost, closing all Links");
        self.base.close_all_links().await;
        // Restore (and thereby release) the sessions of disconnected clients.
        let sessions = self.client_sessions.write().await.drain().collect::<Vec<_>>();
        for (_addr, session) in sessions {
            let _links = self.session_store.restore_session(session.session_id).await;
        }
        *self.base.state.write().await = GatewayState::Waiting;
    }

    /// Closes all active connections during graceful shutdown.
    async fn shutdown(&self) {
        info!("Server graceful shutdown");
        self.base.shutdown().await;
        
        // Close all client sessions and remove them from the store.
        let mut sessions = self.client_sessions.write().await;
        for (_addr, session) in sessions.drain() {
            let _ = session.writer.lock().await.shutdown().await;
            self.session_store.remove_session(session.session_id).await;
        }
    }
}
