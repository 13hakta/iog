//! Global enumerations for the application.

use std::fmt;

/// Application operating mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppMode {
    /// Undefined — configuration not loaded or contains an error.
    Undefined,
    /// Client mode — connects to a remote gateway server.
    Client,
    /// Server mode — accepts incoming connections from clients.
    Server,
}

impl fmt::Display for AppMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppMode::Undefined => write!(f, "UNDEFINED"),
            AppMode::Client => write!(f, "CLIENT"),
            AppMode::Server => write!(f, "SERVER"),
        }
    }
}

impl Default for AppMode {
    fn default() -> Self {
        AppMode::Undefined
    }
}

impl From<&str> for AppMode {
    /// Converts a configuration file string into `AppMode`.
    ///
    /// Supports case-insensitive matching: `client`, `Client`, `CLIENT`.
    fn from(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "client" => AppMode::Client,
            "server" => AppMode::Server,
            _ => AppMode::Undefined,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_app_mode_from_str() {
        assert_eq!(AppMode::from("client"), AppMode::Client);
        assert_eq!(AppMode::from("CLIENT"), AppMode::Client);
        assert_eq!(AppMode::from("Client"), AppMode::Client);
        assert_eq!(AppMode::from("server"), AppMode::Server);
        assert_eq!(AppMode::from("SERVER"), AppMode::Server);
        assert_eq!(AppMode::from("unknown"), AppMode::Undefined);
        assert_eq!(AppMode::from(""), AppMode::Undefined);
    }

    #[test]
    fn test_app_mode_display() {
        assert_eq!(AppMode::Client.to_string(), "CLIENT");
        assert_eq!(AppMode::Server.to_string(), "SERVER");
        assert_eq!(AppMode::Undefined.to_string(), "UNDEFINED");
    }
}

