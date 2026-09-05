//! Traffic compression module (ROADMAP item 6.1).
//!
//! Uses the zstd algorithm to compress data before sending.
//! Compression is applied only to packets larger than 1 KB (saves CPU).
//!
//! ## Compression flag
//! The `FLAG_COMPRESSED` flag (from `packet.rs`) is set in the packet header
//! if the data was compressed.
//!
//! ## Backward compatibility
//! Packets without the `FLAG_COMPRESSED` flag are processed as usual.
//! If the server does not support compression, it ignores the flag.

use zstd;

/// Minimum data size for compression (1 KB).
/// Data smaller than this is not compressed to save CPU.
pub const MIN_COMPRESS_SIZE: usize = 1024;

/// Default compression level (0-19).
/// Level 3 provides a good balance between speed and compression ratio.
pub const COMPRESSION_LEVEL: i32 = 3;

/// Data compression result.
#[derive(Debug, Clone)]
pub struct CompressionResult {
    /// Compressed data (or the original, if compression is not worthwhile).
    pub data: Vec<u8>,
    /// Compression flag is set.
    pub is_compressed: bool,
}

/// Compresses data when it is worthwhile.
///
/// # Arguments
/// * `data` - Original data to compress
///
/// # Returns
/// A `CompressionResult` with the data and the compression flag.
///
/// # Examples
/// ```
/// let data = vec![0u8; 2048]; // 2 KB of zeros (compresses well)
/// let result = compression::compress_data(&data);
/// assert!(result.is_compressed);
/// assert!(result.data.len() < data.len());
/// ```
pub fn compress_data(data: &[u8]) -> CompressionResult {
    // Do not compress small packets (saves CPU)
    if data.len() < MIN_COMPRESS_SIZE {
        return CompressionResult {
            data: data.to_vec(),
            is_compressed: false,
        };
    }

    // Try to compress the data.
    match zstd::encode_all(data, COMPRESSION_LEVEL) {
        Ok(compressed) => {
            // Check that compression actually reduced the size.
            if compressed.len() < data.len() {
                CompressionResult {
                    data: compressed,
                    is_compressed: true,
                }
            } else {
                // Compression is inefficient, send the original.
                CompressionResult {
                    data: data.to_vec(),
                    is_compressed: false,
                }
            }
        }
        Err(_) => {
            // Compression error, send the original.
            CompressionResult {
                data: data.to_vec(),
                is_compressed: false,
            }
        }
    }
}

/// Decompresses compressed data.
///
/// # Arguments
/// * `data` - Compressed data (or the original, if the flag is not set)
/// * `is_compressed` - Whether the compression flag is set in the packet header
///
/// # Returns
/// The decompressed data or `None` on decompression error.
pub fn decompress_data(data: &[u8], is_compressed: bool) -> Option<Vec<u8>> {
    if !is_compressed {
        return Some(data.to_vec());
    }

    // Decompress the data.
    zstd::decode_all(data).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_small_data_not_compressed() {
        let data = vec![1u8; 512]; // Less than MIN_COMPRESS_SIZE.
        let result = compress_data(&data);
        
        assert!(!result.is_compressed);
        assert_eq!(result.data, data);
    }

    #[test]
    fn test_compressible_data() {
        // Zeros compress well.
        let data = vec![0u8; 4096];
        let result = compress_data(&data);
        
        assert!(result.is_compressed);
        assert!(result.data.len() < data.len());
    }

    #[test]
    fn test_roundtrip() {
        let data = vec![42u8; 4096];
        let compressed = compress_data(&data);
        let decompressed = decompress_data(&compressed.data, compressed.is_compressed);
        
        assert!(decompressed.is_some());
        assert_eq!(decompressed.unwrap(), data);
    }

    #[test]
    fn test_decompress_uncompressed() {
        let data = vec![1u8, 2, 3, 4];
        let result = decompress_data(&data, false);
        
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
    }
}
