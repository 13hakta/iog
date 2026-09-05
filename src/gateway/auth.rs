//! Gateway authentication module.
//!
//! Implements the Challenge-Response authentication mechanism (ROADMAP item 3.1).
//! Instead of transmitting the key in plain text, the server sends a random nonce,
//! and the client responds with HMAC-SHA256(key, nonce).

use hmac::{Hmac, Mac};
use sha2::Sha256;
use rand::RngCore;

/// HMAC-SHA256 type used for authentication.
type HmacSha256 = Hmac<Sha256>;

/// Nonce size in bytes.
pub const NONCE_SIZE: usize = 32;

/// HMAC response size in bytes.
pub const RESPONSE_SIZE: usize = 32;

/// Generates a random nonce for challenge-response authentication.
///
/// # Returns
/// An array of 32 random bytes.
pub fn generate_nonce() -> [u8; NONCE_SIZE] {
    let mut nonce = [0u8; NONCE_SIZE];
    rand::thread_rng().fill_bytes(&mut nonce);
    nonce
}

/// Computes the HMAC-SHA256 response for challenge-response authentication.
///
/// # Arguments
/// * `key` - Authentication key
/// * `nonce` - Random nonce from the server
///
/// # Returns
/// An array of 32 bytes of HMAC-SHA256(key, nonce)
pub fn compute_response(key: &[u8], nonce: &[u8]) -> [u8; RESPONSE_SIZE] {
    let mut mac = HmacSha256::new_from_slice(key)
        .expect("HMAC can take key of any size");
    mac.update(nonce);
    mac.finalize().into_bytes().into()
}

/// Verifies the response received from the client.
///
/// # Arguments
/// * `key` - Expected authentication key
/// * `nonce` - Nonce that was sent to the client
/// * `response` - Response received from the client
///
/// # Returns
/// `true` if the response is valid, `false` otherwise
pub fn verify_response(key: &[u8], nonce: &[u8], response: &[u8]) -> bool {
    let expected = compute_response(key, nonce);
    
    // Constant-time comparison to prevent timing attacks
    if response.len() != RESPONSE_SIZE {
        return false;
    }
    
    let mut result = 0u8;
    for (a, b) in expected.iter().zip(response.iter()) {
        result |= a ^ b;
    }
    result == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nonce_generation() {
        let nonce1 = generate_nonce();
        let nonce2 = generate_nonce();
        
        // Nonces must be different (with very high probability)
        assert_ne!(nonce1, nonce2);
        assert_eq!(nonce1.len(), NONCE_SIZE);
    }

    #[test]
    fn test_response_computation() {
        let key = b"test_secret_key";
        let nonce = generate_nonce();
        
        let response = compute_response(key, &nonce);
        assert_eq!(response.len(), RESPONSE_SIZE);
        
        // The same key and nonce must produce the same response
        let response2 = compute_response(key, &nonce);
        assert_eq!(response, response2);
    }

    #[test]
    fn test_response_verification() {
        let key = b"test_secret_key";
        let nonce = generate_nonce();
        
        let response = compute_response(key, &nonce);
        
        // A valid response must pass verification
        assert!(verify_response(key, &nonce, &response));
        
        // A wrong key must fail verification
        let wrong_key = b"wrong_key";
        assert!(!verify_response(wrong_key, &nonce, &response));
        
        // A wrong nonce must fail verification
        let wrong_nonce = generate_nonce();
        assert!(!verify_response(key, &wrong_nonce, &response));
    }

    #[test]
    fn test_invalid_response_length() {
        let key = b"test_key";
        let nonce = generate_nonce();
        let short_response = [0u8; 16]; // Too short
        
        assert!(!verify_response(key, &nonce, &short_response));
    }
}
