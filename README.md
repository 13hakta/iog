# I/O Gateway - Inside Out Gateway (iog)

A high-performance, asynchronous TCP gateway written in Rust that proxies TCP connections between client applications and remote servers using a secure binary protocol.

## Features

### Core Functionality
- **TCP Multiplexing** — Multiple TCP connections multiplexed over a single channel
- **Keep-alive Mechanism** — Connection health monitoring and automatic reconnection
- **Traffic Statistics** — Per-connection traffic and packet statistics
- **Automatic Reconnection** — Client automatically reconnects on connection loss

### Security
- **HMAC-SHA256 Authentication** — Challenge-response authentication without transmitting keys over the network
- **Protocol Versioning** — Compatibility support between different gateway versions
- **Disconnect Reason Diagnostics** — Detailed error reporting for connection issues

### Production Features
- **Graceful Shutdown** — Clean shutdown on SIGTERM/Ctrl+C
- **Prometheus Metrics** — HTTP endpoint at `/metrics` for Grafana monitoring
- **Traffic Compression** — Automatic zstd compression for packets larger than 1KB
- **Buffer Pool** — Zero-copy buffer reuse to minimize memory allocations
- **Session Migration** — Restore active connections after reconnection using UUID sessions

## Quick Start

### Build

```bash
cargo build --release
```

### Run

```bash
./target/release/iog <config-file>
```

The application requires one argument — the path to a configuration file (TOML, INI, or JSON).

## Configuration Examples

### Server Mode

```toml
[general]
mode = "server"
key = "your-secret-key"

[network]
port_app = 8080      # Port for local applications
port_gw = 9000       # Port for remote gateway clients
port_metrics = 9090  # Prometheus metrics port
```

### Client Mode

```toml
[general]
mode = "client"
key = "your-secret-key"

[network]
host_gw = "remote-server.example.com"
port_gw = 9000
host_app = "localhost"
port_app = 8080
port_metrics = 9090
```

## Runtime Commands

While running, you can use these commands via standard input:

| Command | Action |
|---------|--------|
| *(empty line)* | Display connection and traffic statistics |
| `p` | Send a Ping packet manually (client mode only) |

### Statistics Output Example

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

## Prometheus Metrics

iog exposes metrics at `http://localhost:9090/metrics`.

### Available Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `iog_packets_total` | Counter | Total packets processed (by type and direction) |
| `iog_bytes_total` | Counter | Total bytes transferred (by direction) |
| `iog_active_connections` | Gauge | Current number of active connections |
| `iog_packet_processing_seconds` | Histogram | Packet processing time |

### Health Check

```bash
curl http://localhost:9090/health
# Output: OK
```

### Prometheus Configuration Example

```yaml
scrape_configs:
  - job_name: 'iog'
    static_configs:
      - targets: ['localhost:9090']
```

## Architecture

iog operates in two modes:

### Server Mode
1. Listens on `port_gw` for incoming gateway client connections
2. Listens on `port_app` for local application connections
3. Authenticates gateway clients using HMAC-SHA256 challenge-response
4. Multiplexes traffic between gateway clients and local applications

### Client Mode
1. Connects to remote gateway server at `host_gw:port_gw`
2. Authenticates using HMAC-SHA256 challenge-response
3. Listens for `Open` commands from the server
4. Forwards traffic between remote gateway and local application

### Binary Protocol

All gateway-to-gateway communication uses a 16-byte header followed by payload:

```
Offset  Size  Field          Description
------  ----  -------------- -----------------------------------
 0      1     packet_type    Packet type (0-10, 11-13, 30)
 1      8     packet_id      Unique packet identifier
 9      2     link_id        Connection ID (0 for control packets)
11      2     data_size      Payload size (0-65535)
13      3     reserved       Reserved (zeros)
```

### Packet Types

| Value | Type | Direction | Description |
|-------|------|-----------|-------------|
| 0 | `Regular` | Both | Application data |
| 1 | `Ping` | Both | Keep-alive request |
| 2 | `Pong` | Both | Keep-alive response |
| 3 | `GwAuth` | Client→Server | Legacy authentication |
| 4 | `GwConnected` | Server→Client | Authentication confirmation |
| 5 | `GwDeny` | Server→Client | Authentication rejection |
| 6 | `Open` | Server→Client | Request to open connection |
| 7 | `Opened` | Client→Server | Connection opened confirmation |
| 8 | `OpenFail` | Client→Server | Connection open failure |
| 9 | `Close` | Server→Client | Request to close connection |
| 10 | `Closed` | Client→Server | Connection closed confirmation |
| 11 | `ProtocolVersion` | Both | Protocol version exchange |
| 12 | `GwAuthChallenge` | Server→Client | Challenge for HMAC authentication |
| 13 | `GwAuthResponse` | Client→Server | Response to challenge |
| 30 | `SessionRestore` | Client→Server | Session restoration after reconnection |

## Development

### Running Tests

```bash
cargo test
```

The project includes 25+ unit and integration tests covering:
- Packet serialization/deserialization
- HMAC authentication
- Buffer pool operations
- Session management
- Compression/decompression
- Metrics functionality

## License

This project is provided by MIT License.
