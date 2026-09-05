//! Configuration loading module for INI files.
//!
//! The configuration consists of two sections:
//! - `[general]` — operating mode and authentication key.
//! - `[network]` — network settings (ports and hosts).
//!
//! In client mode all `NetworkSettings` fields are used;
//! in server mode only `port_app` is required.

use serde::Deserialize;
use config::{Config, ConfigError};

use crate::consts::AppMode;

/// General settings (`[general]` section).
#[derive(Debug, Deserialize, Clone)]
pub struct GeneralSettings {
    /// Application operating mode (`client` or `server`).
    pub mode: String,
    /// Authentication key used when establishing a connection between gateways.
    pub key: String,
}

/// Network settings (`[network]` section).
#[derive(Debug, Deserialize, Clone)]
pub struct NetworkSettings {
    /// Port the gateway listens on or connects to.
    #[serde(default)]
    pub port_gw: u16,
    /// Port of the local application (or remote one, depending on the mode).
    #[serde(default)]
    pub port_app: u16,
    /// Host of the local application (for the client — where to forward data).
    #[serde(default)]
    pub host_app: String,
    /// Host of the remote gateway (for the client — where to connect).
    #[serde(default)]
    pub host_gw: String,
    /// Port for the Prometheus metrics server (default 9090).
    #[serde(default = "default_metrics_port")]
    pub port_metrics: u16,
}

/// Default value for the metrics port.
fn default_metrics_port() -> u16 {
    9090
}

/// Full application configuration.
#[derive(Debug, Deserialize, Clone)]
pub struct Configuration {
    /// General settings.
    pub general: GeneralSettings,
    /// Network settings.
    pub network: NetworkSettings,
}

impl Configuration {
    /// Loads the configuration from a file.
    ///
    /// Supported formats: `.ini`, `.toml`, `.json` (via the `config` crate).
    ///
    /// # Errors
    ///
    /// Returns `ConfigError` if the file is not found or has format errors.
    pub fn load(filename: &str) -> Result<Self, ConfigError> {
        let s = Config::builder()
            .add_source(config::File::with_name(filename))
            .build()?;

        s.try_deserialize()
    }

    /// Returns the application operating mode as an `AppMode`.
    pub fn app_mode(&self) -> AppMode {
        AppMode::from(self.general.mode.as_str())
    }

    /// Returns the port for the Prometheus metrics server.
    ///
    /// If the port is not set in the configuration, returns the default value (9090).
    pub fn metrics_port(&self) -> Option<u16> {
        if self.network.port_metrics > 0 {
            Some(self.network.port_metrics)
        } else {
            None
        }
    }

    /// Validates the configuration against the selected mode.
    ///
    /// Client mode: requires `host_gw`, `port_gw`, `host_app`, `port_app`.
    /// Server mode: requires `port_app` (and optionally `port_gw`).
    pub fn validate(&self) -> Result<(), String> {
        match self.app_mode() {
            AppMode::Client => {
                if self.network.port_gw == 0 {
                    return Err("Client mode requires 'port_gw' in [network]".to_string());
                }
                if self.network.port_app == 0 {
                    return Err("Client mode requires 'port_app' in [network]".to_string());
                }
                if self.network.host_app.is_empty() {
                    return Err("Client mode requires 'host_app' in [network]".to_string());
                }
                if self.network.host_gw.is_empty() {
                    return Err("Client mode requires 'host_gw' in [network]".to_string());
                }
            }
            AppMode::Server => {
                if self.network.port_app == 0 {
                    return Err("Server mode requires 'port_app' in [network]".to_string());
                }
            }
            AppMode::Undefined => {
                return Err(
                    "Invalid or missing 'mode' in [general]. Must be 'client' or 'server'."
                        .to_string(),
                );
            }
        }
        Ok(())
    }
}

