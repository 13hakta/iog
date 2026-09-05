//! Session storage module for Session Migration (ROADMAP item 6.3).
//!
//! Allows the client to reconnect after a link loss while preserving active links.
//!
//! ## How it works:
//! 1. The client generates a `session_id: UUID` on first connection.
//! 2. The server stores the links state for each `session_id`.
//! 3. On reconnection the client sends its `session_id`.
//! 4. The server restores the links if the session is found.
//!
//! ## Persistence:
//! The `session_id` is persisted to a file to survive application restarts.
//!
//! ## Storage file:
//! - The `session_id` is saved to the file `/tmp/iog_session_id` or `./session_id`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use tokio::sync::RwLock;
use uuid::Uuid;

use crate::gateway::link::LinkRef;

/// Session store for Session Migration.
///
/// Stores the state of active links for each `session_id`.
pub struct SessionStore {
    /// Mapping of `session_id` -> links.
    sessions: RwLock<HashMap<Uuid, Vec<(u16, LinkRef)>>>,
}

impl SessionStore {
    /// Creates a new session store.
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Saves the session state.
    ///
    /// # Arguments
    /// * `session_id` - Session identifier.
    /// * `links` - Active links to save.
    pub async fn save_session(&self, session_id: Uuid, links: Vec<(u16, LinkRef)>) {
        let mut sessions = self.sessions.write().await;
        sessions.insert(session_id, links);
    }

    /// Restores the session state.
    ///
    /// # Arguments
    /// * `session_id` - Session identifier to restore.
    ///
    /// # Returns
    /// `Some(links)` if the session is found, `None` otherwise.
    pub async fn restore_session(&self, session_id: Uuid) -> Option<Vec<(u16, LinkRef)>> {
        let mut sessions = self.sessions.write().await;
        sessions.remove(&session_id)
    }

    /// Checks whether a session exists.
    ///
    /// # Arguments
    /// * `session_id` - Session identifier.
    ///
    /// # Returns
    /// `true` if the session exists.
    // Reserved for the full Session Migration implementation.
    #[allow(dead_code)]
    pub async fn has_session(&self, session_id: Uuid) -> bool {
        let sessions = self.sessions.read().await;
        sessions.contains_key(&session_id)
    }

    /// Removes a session.
    ///
    /// # Arguments
    /// * `session_id` - Session identifier to remove.
    pub async fn remove_session(&self, session_id: Uuid) {
        let mut sessions = self.sessions.write().await;
        sessions.remove(&session_id);
    }

    /// Returns the number of active sessions.
    pub async fn session_count(&self) -> usize {
        let sessions = self.sessions.read().await;
        sessions.len()
    }
}

impl Default for SessionStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Generates a new `session_id`.
pub fn generate_session_id() -> Uuid {
    Uuid::new_v4()
}

/// Path to the file used to store the `session_id`.
const SESSION_FILE: &str = "iog_session_id";

/// Persists the `session_id` to a file.
///
/// # Arguments
/// * `session_id` - Session identifier.
/// * `path` - Optional file path.
///
/// # Returns
/// `Ok(())` on success, `Err` on write error.
pub fn persist_session_id(session_id: Uuid, path: Option<&Path>) -> std::io::Result<()> {
    let file_path = path.map(PathBuf::from).unwrap_or_else(|| PathBuf::from(SESSION_FILE));
    std::fs::write(&file_path, session_id.to_string())
}

/// Loads the `session_id` from a file.
///
/// # Arguments
/// * `path` - Optional file path.
///
/// # Returns
/// `Some(session_id)` if the file exists and is valid, `None` otherwise.
pub fn load_session_id(path: Option<&Path>) -> Option<Uuid> {
    let file_path = path.map(PathBuf::from).unwrap_or_else(|| PathBuf::from(SESSION_FILE));
    
    match std::fs::read_to_string(&file_path) {
        Ok(contents) => Uuid::parse_str(contents.trim()).ok(),
        Err(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_session_store_save_restore() {
        let store = SessionStore::new();
        let session_id = generate_session_id();
        
        // Save an empty session.
        store.save_session(session_id, vec![]).await;
        assert!(store.has_session(session_id).await);
        
        // Restore the session.
        let restored = store.restore_session(session_id).await;
        assert!(restored.is_some());
        
        // The session is removed after restoration.
        assert!(!store.has_session(session_id).await);
    }

    #[tokio::test]
    async fn test_session_store_remove() {
        let store = SessionStore::new();
        let session_id = generate_session_id();
        
        store.save_session(session_id, vec![]).await;
        assert!(store.has_session(session_id).await);
        
        store.remove_session(session_id).await;
        assert!(!store.has_session(session_id).await);
    }

    #[test]
    fn test_session_id_persistence() {
        let session_id = generate_session_id();
        let temp_path = std::env::temp_dir().join("test_session_id");
        
        // Save.
        persist_session_id(session_id, Some(&temp_path)).expect("Failed to persist");
        
        // Load.
        let loaded = load_session_id(Some(&temp_path));
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap(), session_id);
        
        // Clean up.
        let _ = std::fs::remove_file(&temp_path);
    }
}
