# I/O Gateway (iog) — Application Architecture

## 1. Overview

**iog** (I/O Gateway) is a high-performance asynchronous network gateway written in Rust using the Tokio framework. The application is designed to proxy TCP connections between client applications and a remote server using a secure binary protocol.

The gateway operates in one of two modes:
- **Client** — connects to a remote gateway server and forwards traffic to a local application
- **Server** — accepts incoming connections from gateway clients and local applications

### Key Features

**Core Functionality:**
- HMAC-SHA256 challenge-response authentication between gateways
- Multiplexing multiple TCP connections over a single channel
- Keep-alive mechanism for connection health monitoring
- Per-connection traffic statistics collection
- Automatic reconnection on connection loss (client mode)

**Advanced Features:**
- **Protocol Versioning** — compatibility support between different gateway versions
- **Graceful Shutdown** — clean shutdown with proper connection closure
- **Traffic Compression** — automatic zstd compression for large packets
- **Buffer Pool** — buffer reuse to minimize memory allocations
- **Session Migration** — restore active connections after reconnection
- **Disconnect Reasons** — detailed diagnostics for connection issues

### Architectural Principles

The project follows modularity and extensibility principles:
- Each component implements a single responsibility
- Shared operations are extracted into the `GatewayOperations` trait
- Helper modules (auth, compression, buffer_pool, session_store) are independent and reusable
- Comprehensive test coverage with 25+ unit and integration tests

---

## 2. System Architecture

### 2.1 Module Structure

```
src/
├── main.rs              # Entry point, argument parsing, graceful shutdown
├── configuration.rs     # Configuration file loading
├── consts.rs            # Global enumerations (AppMode)
└── gateway/
    ├── mod.rs           # Base Gateway structure and GatewayTrait, shutdown()
    ├── client.rs        # GatewayClient implementation with graceful shutdown
    ├── server.rs        # GatewayServer implementation with graceful shutdown
    ├── packet.rs        # PacketGW, packet types, protocol version, DisconnectReason
    ├── link.rs          # Individual TCP connection management (split socket)
    ├── consts.rs        # Gateway constants and statistics structures
    ├── operations.rs    # GatewayOperations trait (shared read/write operations)
    ├── auth.rs          # Challenge-response authentication (HMAC-SHA256)
    ├── compression.rs   # Traffic compression (zstd)
    ├── buffer_pool.rs   # Zero-Copy buffer pool for packet reading
    ├── session_store.rs # Session Migration (UUID sessions, restoration)
    ├── metrics.rs       # Prometheus metrics HTTP server
    └── utils.rs         # Helper functions (time)
```

### 2.2 Main Components

| Component | Description |
|-----------|-------------|
| `Gateway` | Base structure with shared logic for connection management, packet sending, and statistics. Implements `shutdown()` method for graceful termination |
| `GatewayClient` | Inherits from `Gateway`, implements logic for connecting to remote gateway and local application |
| `GatewayServer` | Inherits from `Gateway`, implements listening for incoming connections |
| `GatewayTrait` | Trait defining common interface for client and server (including `shutdown()`) |
| `GatewayOperations` | Trait with shared read/write packet operations, ID generation |
| `Link` | Represents a single TCP connection with an application (separate read/write halves) |
| `PacketGW` | Binary packet for gateway-to-gateway communication |
| `BufferPool` | Buffer pool for memory reuse when reading packets |
| `SessionStore` | Session storage for restoring connections (Session Migration) |
| `auth` | Challenge-response authentication module (HMAC generation/verification) |
| `compression` | Data compression module (zstd) for traffic optimization |
| `metrics` | Prometheus metrics collection and HTTP endpoint |

### 2.3 Component Diagram

```
┌────────────────────────────────────────────────────────────────┐
│                        iog Application                         │
├────────────────────────────────────────────────────────────────┤
│   Configuration (INI/TOML/JSON)      AppMode: client|server    │
├────────────────────────────────────────────────────────────────┤
│                            Gateway                             │
│       state: GatewayState    links: HashMap<u16, LinkRef>      │
│        statistics             config (key, hosts, ports)       │
├────────────────────────────────────────────────────────────────┤
│            GatewayClient              GatewayServer            │
│            - connect_gw()             - listen_gw()            │
│          - gw_exchange              - client_exchange          │
│            - app_exchange             - app_exchange           │
├────────────────────────────────────────────────────────────────┤
│                     Link (per connection)                      │
│    reader: Arc<Mutex<OwnedReadHalf>>  writer: OwnedWriteHalf   │
│     connection_id: u16  state: ConnectionState  statistics     │
├────────────────────────────────────────────────────────────────┤
│     auth  compression  buffer_pool  session_store  metrics     │
└────────────────────────────────────────────────────────────────┘
```

---

## 3. Connection Management

### 3.1 Connection States (ConnectionState)

| State | Value | Description |
|-------|-------|-------------|
| `DISCONNECTED` | 0 | Connection is closed, no data exchange |
| `WAITING` | 1 | Waiting for confirmation from remote side |
| `CONNECTED` | 2 | Connection established, data exchange in progress |

### 3.2 Gateway States (GatewayState)

| State | Description |
|-------|-------------|
| `WAITING` | Waiting for connection to gateway |
| `PROCESSING` | Connection established, awaiting authentication |
| `CONNECTED` | Authentication passed, channel active |

### 3.3 Link Structure

Each TCP connection with an application is represented by a `Link` structure:

```rust
pub struct Link {
    pub reader: Mutex<OwnedReadHalf>,   // Read half
    pub connection_id: u16,              // Unique connection ID
    writer: OwnedWriteHalf,             // Write half
    state: ConnectionState,             // Current state
    statistics: ConnectionStatistics,   // Traffic statistics
    _connection_awaiter: Option<Arc<Notify>>, // Notification on establishment
}
```

TcpStream is split into two independent halves (`OwnedReadHalf` and `OwnedWriteHalf`) to enable parallel reading and writing without locks.

### 3.4 Connection Identifiers

Each connection has a unique identifier:
- Generated by the gateway using a monotonically increasing counter
- 16-bit value (0-65535), wraps around when reaching maximum
- Used in `link_id` field of packets to route data to the correct connection

```
┌────────────────────────────────────────────────────────────────┐
│                        iog Application                         │
├────────────────────────────────────────────────────────────────┤
│                            Gateway                             │
│                  links: HashMap<u16, LinkRef>                  │
│            link_id (u16) assigned per app connection           │
├────────────────────────────────────────────────────────────────┤
│             GatewayClient  <──────>  GatewayServer             │
├────────────────────────────────────────────────────────────────┤
│                     Link (per connection)                      │
│    reader: Arc<Mutex<OwnedReadHalf>>  writer: OwnedWriteHalf   │
│           connection_id: u16  state: ConnectionState           │
│                statistics: bytes_in / bytes_out                │
└────────────────────────────────────────────────────────────────┘
```

---

## 3. Operating Modes

### 3.1 Client Mode (GatewayClient)

In client mode, the application:

1. **Connects to remote gateway server** at `host_gw:port_gw`
2. **Performs authentication** by sending a `GwAuth` packet with the key
3. **Waits for confirmation** (`GwConnected`) or rejection (`GwDeny`)
4. **Processes commands from server**:
   - `Open` — opens a new connection to local application (`host_app:port_app`)
   - `Close` — closes the specified connection
5. **Forwards data** between remote gateway and local application
6. **Automatically reconnects** on connection loss with `GW_RECONNECT_TIMEOUT` (5000 ms) timeout

#### Client Workflow:

```
[Startup] → [Connect to GW] → [GwAuth] → [Wait for GwConnected]
                                                    ↓
                                          [Process packets]
                                            ↙         ↘
                                    [Open]             [Regular]
                                      ↓                    ↓
                            [Connect to App]      [Forward data]
                                      ↓                    ↓
                              [app_exchange] ←→ [send_gw / receive]
```

### 3.2 Server Mode (GatewayServer)

In server mode, the application:

1. **Listens on gateway port** (`host_gw:port_gw`) for incoming client connections
2. **Listens on application port** (`host_app:port_app`) for incoming local application connections
3. **When client connects**:
   - Accepts TCP connection
   - Waits for authentication (`GwAuth`)
   - Verifies key and sends `GwConnected` or `GwDeny`
4. **When application connects**:
   - Assigns connection identifier
   - Sends `Open` packet to client with connection ID
   - Waits for `Opened` confirmation from client
5. **Forwards data** between clients and applications

#### Server Workflow:

```
[Startup] → [listen_gateway()] + [listen_app()]
                ↓                       ↓
        [Accept GW connections]  [Accept App connections]
                ↓                       ↓
        [Wait for GwAuth]      [assign_connection_id]
                ↓                       ↓
        [Verify key]           [Send Open packet]
                ↓                       ↓
    [GwConnected/GwDeny]       [Wait for Opened]
                                       ↓
                               [app_exchange]
```

---

## 4. Communication Protocol

### 4.1 PacketGW Structure

All packets have a fixed **16-byte header** with little-endian encoding:

```
┌─────────────┬──────────────────┬──────────┬───────────┬─────────┬──────────┐
│ packet_type │    packet_id     │ link_id  │ data_size │  flags  │ reserved │
│  (1 byte)   │     (8 bytes)    │ (2 bytes)│ (2 bytes) │ (1 byte)│ (2 bytes)│
└─────────────┴──────────────────┴──────────┴───────────┴─────────┴──────────┘
     0              1                9           11          13        14
```

| Field | Size | Description |
|-------|------|-------------|
| `packet_type` | 1 byte | Packet type (see table below) |
| `packet_id` | 8 bytes | Sequential packet number (for statistics) |
| `link_id` | 2 bytes | Connection identifier (0 for control packets) |
| `data_size` | 2 bytes | Size of payload after header |

Payload of `data_size` bytes follows the header (if `data_size > 0`).

### 4.2 Packet Types

| Value | Type | Direction | Description |
|-------|------|-----------|-------------|
| 0 | `Regular` | Both | Regular application data |
| 1 | `Ping` | Both | Keep-alive request |
| 2 | `Pong` | Both | Keep-alive response |
| 3 | `GwAuth` | Client→Server | Authentication (deprecated, for compatibility) |
| 4 | `GwConnected` | Server→Client | Connection confirmation |
| 5 | `GwDeny` | Server→Client | Connection rejection |
| 6 | `Open` | Server→Client | Request to open connection to application |
| 7 | `Opened` | Client→Server | Confirmation of connection opening |
| 8 | `OpenFail` | Client→Server | Error opening connection |
| 9 | `Close` | Server→Client | Request to close connection |
| 10 | `Closed` | Client→Server | Confirmation of connection closure |
| 11 | `ProtocolVersion` | Both | Protocol version request/response |
| 12 | `GwAuthChallenge` | Server→Client | Challenge for challenge-response authentication |
| 13 | `GwAuthResponse` | Client→Server | Response to challenge |
| 30 | `SessionRestore` | Client→Server | Session restoration after reconnection |

### 4.3 Protocol Version (ProtocolVersion)

Upon connection, client and server exchange protocol versions to ensure compatibility.

**Version format:** `MAJOR.MINOR.PATCH` (e.g., 0.2.0)

**Constants:**
- `PROTOCOL_VERSION_MAJOR = 0`
- `PROTOCOL_VERSION_MINOR = 2`
- `PROTOCOL_VERSION_PATCH = 0`

**Sequence:**
```
Client                              Server
  │──── ProtocolVersion (0.2.0) ─────▶│
  │                                    │ [Compatibility check]
  │◀──── ProtocolVersion (0.2.0) ───── │
  │                                    │
  │════ Versions match ═══════════════ │
```

### 4.4 Disconnect Reasons (DisconnectReason)

| Code | Reason | Description |
|------|--------|-------------|
| 0 | `AuthFailed` | Authentication error |
| 1 | `VersionMismatch` | Incompatible protocol version |
| 2 | `ServerFull` | Server overloaded |
| 3 | `Banned` | Client banned |
| 4 | `RateLimited` | Connection limit exceeded |
| 5 | `SessionNotFound` | Session not found (for Session Migration) |

### 4.5 Challenge-Response Authentication

Instead of transmitting the key in plaintext, HMAC-SHA256 is used:

```
Client                              Server
  │◀──── GwAuthChallenge (nonce) ───── │ [random nonce]
  │                                    │
  │───── GwAuthResponse (HMAC) ──────▶│ [verify HMAC-SHA256(key, nonce)]
  │                                    │
  │◀──── GwConnected ───────────────── │ (or GwDeny + reason)
  │                                    │
  │════ Channel established ══════════ │
```

**Advantages:**
- The key is never transmitted over the network
- Replay attack protection (nonce changes every time)
- Constant-time comparison protects against timing attacks

### 4.6 Traffic Compression

Implemented using the `zstd` library. Packets are compressed before sending if their size exceeds 1 KB.

**Compression flag:** `FLAG_COMPRESSED` = `0x01` (byte 13 of the header)

**Algorithm:**
1. If data size < 1 KB → sent as is
2. If size ≥ 1 KB → data is compressed
3. If compressed size < original → compression flag is set
4. Otherwise → uncompressed data is sent

### 4.7 Buffer Pool

Zero-Copy buffer pool to reduce memory allocations when reading packets:

```rust
pub struct BufferPool {
    inner: Arc<BufferPoolInner>,
    buffer_size: usize,
}
```

**How it works:**
- Pool contains pre-allocated buffers (16 KB by default)
- When reading a packet, a buffer is taken from the pool
- After processing, the buffer is returned to the pool for reuse
- Saves time on memory allocation/deallocation

### 4.8 Session Store (v1.0)

Session storage for restoring connections after reconnection:

```rust
pub struct SessionStore {
    sessions: RwLock<HashMap<SessionId, SessionData>>,
}
```

**How it works:**
1. Client generates unique `session_id` on first connection
2. Server saves state of all active connections for this session
3. On connection loss, client reconnects with the same `session_id`
4. Server restores all active connections from Session Store
5. Client continues working without losing connections

**Advantages:**
- Transparent recovery after network failures
- Preservation of active TCP connection state
- Minimal downtime during reconnection

### 4.3 Authentication Sequence

```
Client                              Server
  │──── TCP Connect ─────────────────▶│
  │                                    │
  │───── GwAuthResponse (HMAC) ──────▶│ [HMAC verification]
  │                                    │
  │◀──── GwConnected ───────────────── │ (or GwDeny)
  │                                    │
  │════ Channel established ══════════ │
```

---

## 5. Connection Management

### 5.1 Link Structure

Each TCP connection with an application is represented by a `Link` structure:

```rust
pub struct Link {
    pub reader: Mutex<OwnedReadHalf>,   // Read half
    pub connection_id: u16,              // Unique connection ID
    writer: OwnedWriteHalf,             // Write half
    state: ConnectionState,             // Current state
    statistics: ConnectionStatistics,   // Traffic statistics
    _connection_awaiter: Option<Arc<Notify>>, // Notification on establishment
}
```

TcpStream is split into two independent halves (`OwnedReadHalf` and `OwnedWriteHalf`) to enable parallel reading and writing without locks.

### 5.2 Connection States (ConnectionState)

| State | Description |
|-------|-------------|
| `DISCONNECTED` | Connection is closed |
| `WAITING` | Waiting for confirmation from remote side |
| `CONNECTED` | Connection established, data is being transmitted |

### 5.3 Gateway States (GatewayState)

| State | Description |
|-------|-------------|
| `WAITING` | Waiting for connection to gateway |
| `PROCESSING` | Connection established, waiting for authentication |
| `CONNECTED` | Authentication passed, channel is active |

### 5.4 Connection Identifiers

- Server assigns ID for each new application connection (auto-increment, starting from 1)
- Client uses ID received from server in `Open` packet
- ID is 0 for control packets (Ping, Pong, GwAuth, etc.)
- Maximum number of simultaneous connections is limited by `u16` type (65535)

---

## 6. Keep-alive Mechanism

### 6.1 How It Works

To maintain connection activity and detect disconnections, a Ping/Pong mechanism is used:

1. **Periodic Ping sending**: every `GW_PING_PERIOD` (30 seconds) a `Ping` packet is sent
2. **Pong response**: upon receiving `Ping`, the side responds with a `Pong` packet
3. **Timestamp update**: upon receiving `Ping` or `Pong`, `keep_alive_stamp` is updated
4. **Timeout control**: if more than `3 × GW_PING_PERIOD` (90 seconds) have passed since the last keep-alive, the connection is considered broken

### 6.2 Timeout Handling

```
[Ping sent] ──▶ [Waiting for Pong]
                      │
     [Pong received within timeout?]
            │yes                │no (>90 sec)
            ▼                    ▼
     [Continue]         [on_disconnection()]
                               │
                               ▼
                     [Close all Links]
                     [Reconnect after GW_RECONNECT_TIMEOUT]
```

---

## 7. Configuration

### 7.1 Configuration File Format

Configuration is loaded from a file in TOML/YAML/JSON format (supported by the `config` library):

```toml
[general]
mode = "client"          # Operating mode: "client" or "server"
key = "secret_key"       # Authentication key for connecting to gateway

[network]
host_gw = "127.0.0.1"   # Gateway address (for client) or listening address (for server)
port_gw = 9000           # Gateway port
host_app = "127.0.0.1"   # Local application address
port_app = 8080          # Local application port
```

### 7.2 Configuration Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `general.mode` | String | Operating mode: `"client"` or `"server"` |
| `general.key` | String | Secret key for authentication between gateways |
| `network.host_gw` | String | Host for connecting to gateway (client) or listening (server) |
| `network.port_gw` | u16 | Gateway port |
| `network.host_app` | String | Local application host |
| `network.port_app` | u16 | Local application port |

---

## 8. Runtime Interface

During operation, the application accepts commands from standard input (stdin):

| Command | Action |
|---------|--------|
| *(empty line)* | Display connection and traffic statistics |
| `p` | Send a Ping packet manually |

### Statistics Output Example:

```
GW state: CONNECTED
 Since: 1716534000
 Keep alive: 1716534120
 Packets out: 1542
 Data in/out: 1048576/2097152
 Last connection: #5
 App connections: 3
 #1[CONNECTED]: 524288/1048576
 #2[CONNECTED]: 262144/524288
 #3[WAITING]: 0/0
```

---

## 9. Constants and Timeouts

| Constant | Value | Description |
|----------|-------|-------------|
| `BUFFER_SIZE` | 16384 (16 KB) | Buffer size for reading data from TCP |
| `GW_PING_PERIOD` | 30 sec | Period for sending Ping packets |
| `GW_RECONNECT_TIMEOUT` | 5000 ms | Timeout before client reconnection |
| `GW_READINESS_TIMEOUT` | 5000 ms | Timeout for waiting for authentication on server |
| `MAX_DATA_SIZE` | 10485760 (10 MB) | Maximum data size in one packet |

---

## 10. Error Handling and Reconnection

### 10.1 Client Reconnection

When connection to gateway server is lost, the client:
1. Calls `on_disconnection()` — closes all active Links, resets counters
2. Transitions to `WAITING` state
3. After `GW_RECONNECT_TIMEOUT` (5 sec), tries to reconnect
4. The cycle repeats indefinitely until successful connection

### 10.2 Server Error Handling

- On authentication error, `GwDeny` is sent and connection is closed
- If client doesn't authenticate within `GW_READINESS_TIMEOUT` (5 sec), connection is closed
- On reconnection from the same IP, previous connection is closed

### 10.3 SIGPIPE Handling

During initialization, a `SIGPIPE` signal handler is set to `SIG_IGN` so that writing to a closed socket doesn't terminate the process.

---

## 11. Statistics

### 11.1 Global Statistics (ConnectionStatistics)

| Field | Description |
|-------|-------------|
| `packets_in` | Number of received packets |
| `packets_out` | Number of sent packets |
| `bytes_in` | Volume of received data (bytes) |
| `bytes_out` | Volume of sent data (bytes) |
| `connection_time` | Time when connection was established (epoch seconds) |

### 11.2 Per-Connection Statistics (Link)

Each Link maintains separate statistics:
- `bytes_in` — bytes received from application
- `bytes_out` — bytes sent to application
- `state` — current connection state

---

## 12. Dependencies

| Library | Purpose |
|---------|---------|
| `tokio` | Async runtime, TCP sockets, timers |
| `serde` | Configuration serialization/deserialization |
| `config` | Loading configuration from files of various formats |
| `async-trait` | Async methods in traits |
| `futures` | Utilities for working with Future |
| `anyhow` / `thiserror` | Error handling |
| `chrono` | Working with time |
| `libc` | System calls (signal, time) |
| `log` / `env_logger` | Logging |
| `clap` | Command-line argument parsing |
| `prometheus` | Prometheus metrics collection and export |
| `axum` | HTTP server for metrics endpoint |
| `hmac` / `sha2` | HMAC-SHA256 for challenge-response authentication |
| `rand` | Cryptographic nonce generation |
| `zstd` | Traffic compression |
| `uuid` | Session identifiers (Session Migration) |
| `once_cell` | Global static buffer pool |

---

## 13. Application Startup

```bash
# Syntax
./iog <path_to_config>

# Example of running in client mode
./iog config-client.ini

# Example of running in server mode
./iog config-server.ini
```

The application requires one mandatory argument — the path to the configuration file.