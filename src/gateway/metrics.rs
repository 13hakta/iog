//! Prometheus metrics for I/O Gateway monitoring (ROADMAP item 4.2).
//!
//! The module provides an HTTP server on a separate port serving Prometheus metrics.
//! Metrics include: counters, gauges, histograms for monitoring traffic,
//! connections and performance.
//!
//! ## Endpoints
//! - `/metrics` — metrics in the Prometheus format
//! - `/health` — health check for Kubernetes
//!
//! ## Metrics
//! - `iog_packets_total` — total number of processed packets (by type and direction)
//! - `iog_bytes_total` — total number of transferred bytes (by direction)
//! - `iog_active_connections` — number of active connections (gauge)
//! - `iog_packet_processing_seconds` — packet processing time (histogram)

use std::net::SocketAddr;
use std::sync::OnceLock;

use axum::{routing::get, Router};
use log::{error, info};
use prometheus::{
    register_counter_vec, register_gauge, register_histogram_vec, CounterVec, Encoder,
    Gauge, HistogramVec, TextEncoder,
};
use tokio::net::TcpListener;

/// Default port for the metrics server.
pub const DEFAULT_METRICS_PORT: u16 = 9090;

/// Metrics initialization (called once at startup).
static METRICS_INITIALIZED: OnceLock<()> = OnceLock::new();

/// Total number of processed packets (by type and direction).
static PACKETS_TOTAL: OnceLock<CounterVec> = OnceLock::new();

/// Total number of transferred bytes (by direction).
static BYTES_TOTAL: OnceLock<CounterVec> = OnceLock::new();

/// Number of active connections (gauge).
static ACTIVE_CONNECTIONS: OnceLock<Gauge> = OnceLock::new();

/// Packet processing time (histogram).
static PACKET_PROCESSING_TIME: OnceLock<HistogramVec> = OnceLock::new();

/// Initializes all Prometheus metrics.
///
/// Must be called once at application startup.
/// Repeated calls are ignored.
pub fn init_metrics() {
    METRICS_INITIALIZED.get_or_init(|| {
        let packets = register_counter_vec!(
            "iog_packets_total",
            "Total number of packets processed",
            &["type", "direction"]
        )
        .expect("Failed to register PACKETS_TOTAL");

        let bytes = register_counter_vec!(
            "iog_bytes_total",
            "Total bytes transferred",
            &["direction"]
        )
        .expect("Failed to register BYTES_TOTAL");

        let connections = register_gauge!(
            "iog_active_connections",
            "Number of active connections"
        )
        .expect("Failed to register ACTIVE_CONNECTIONS");

        let processing_time = register_histogram_vec!(
            "iog_packet_processing_seconds",
            "Time spent processing packets",
            &["type"]
        )
        .expect("Failed to register PACKET_PROCESSING_TIME");

        let _ = PACKETS_TOTAL.set(packets);
        let _ = BYTES_TOTAL.set(bytes);
        let _ = ACTIVE_CONNECTIONS.set(connections);
        let _ = PACKET_PROCESSING_TIME.set(processing_time);

        info!("Prometheus metrics initialized");
    });
}

/// Increments the processed packets counter.
///
/// # Arguments
/// * `packet_type` — packet type (Regular, Ping, Pong, Open, Close, etc.)
/// * `direction` — direction ("in" or "out")
pub fn inc_packets_total(packet_type: &str, direction: &str) {
    if let Some(counter) = PACKETS_TOTAL.get() {
        counter.with_label_values(&[packet_type, direction]).inc();
    }
}

/// Increments the transferred bytes counter.
///
/// # Arguments
/// * `bytes` — number of bytes
/// * `direction` — direction ("in" or "out")
pub fn inc_bytes_total(bytes: u64, direction: &str) {
    if let Some(counter) = BYTES_TOTAL.get() {
        counter.with_label_values(&[direction]).inc_by(bytes as f64);
    }
}

/// Sets the number of active connections.
///
/// # Arguments
/// * `count` — current number of connections
pub fn set_active_connections(count: usize) {
    if let Some(gauge) = ACTIVE_CONNECTIONS.get() {
        gauge.set(count as f64);
    }
}

/// Records the packet processing time into the histogram.
///
/// # Arguments
/// * `packet_type` — packet type
/// * `duration_secs` — processing time in seconds
pub fn observe_packet_processing(packet_type: &str, duration_secs: f64) {
    if let Some(histogram) = PACKET_PROCESSING_TIME.get() {
        histogram
            .with_label_values(&[packet_type])
            .observe(duration_secs);
    }
}

/// Handler for the `/metrics` endpoint.
///
/// Returns all metrics in the Prometheus text format.
async fn metrics_handler() -> String {
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut buffer = Vec::new();

    if let Err(e) = encoder.encode(&metric_families, &mut buffer) {
        error!("Failed to encode metrics: {}", e);
        return String::from("# Error encoding metrics\n");
    }

    String::from_utf8(buffer).unwrap_or_else(|_| String::from("# Error encoding metrics\n"))
}

/// Handler for the `/health` health check endpoint.
async fn health_handler() -> &'static str {
    "OK"
}

/// Starts the HTTP server for Prometheus metrics.
///
/// # Arguments
/// * `port` — port to listen on (default 9090)
pub async fn start_metrics_server(port: u16) {
    init_metrics();

    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler));

    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    match TcpListener::bind(addr).await {
        Ok(listener) => {
            info!("Metrics server listening on http://0.0.0.0:{}/metrics", port);
            if let Err(e) = axum::serve(listener, app).await {
                error!("Metrics server error: {}", e);
            }
        }
        Err(e) => {
            error!("Failed to bind metrics server on port {}: {}", port, e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_initialization() {
        init_metrics();
        assert!(PACKETS_TOTAL.get().is_some());
        assert!(BYTES_TOTAL.get().is_some());
        assert!(ACTIVE_CONNECTIONS.get().is_some());
        assert!(PACKET_PROCESSING_TIME.get().is_some());
    }

    #[test]
    fn test_inc_packets() {
        init_metrics();
        inc_packets_total("Regular", "in");
        inc_packets_total("Ping", "out");
        assert!(PACKETS_TOTAL.get().is_some());
    }

    #[test]
    fn test_inc_bytes() {
        init_metrics();
        inc_bytes_total(1024, "in");
        inc_bytes_total(2048, "out");
        assert!(BYTES_TOTAL.get().is_some());
    }

    #[test]
    fn test_active_connections() {
        init_metrics();
        set_active_connections(5);
        assert!(ACTIVE_CONNECTIONS.get().is_some());
    }

    #[test]
    fn test_observe_processing_time() {
        init_metrics();
        observe_packet_processing("Regular", 0.001);
        observe_packet_processing("Ping", 0.0005);
        assert!(PACKET_PROCESSING_TIME.get().is_some());
    }
}
