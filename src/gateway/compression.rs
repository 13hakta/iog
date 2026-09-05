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

use zstd::bulk::{Compressor, Decompressor};
use zstd::zstd_safe;

/// Minimum data size for compression (1 KB).
/// Data smaller than this is not compressed to save CPU.
pub const MIN_COMPRESS_SIZE: usize = 1024;

/// Default compression level (0-19).
/// Level 3 provides a good balance between speed and compression ratio.
pub const COMPRESSION_LEVEL: i32 = 3;

thread_local! {
    /// Per-thread reusable zstd compression context.
    static COMPRESSOR: std::cell::RefCell<Option<Compressor<'static>>> =
        const { std::cell::RefCell::new(None) };

    /// Per-thread reusable zstd decompression context.
    static DECOMPRESSOR: std::cell::RefCell<Option<Decompressor<'static>>> =
        const { std::cell::RefCell::new(None) };
}

/// Runs `f` with the thread-local compressor, creating it on first use.
///
/// Returns `None` only if the zstd context could not be created.
fn with_compressor<R>(f: impl FnOnce(&mut Compressor<'static>) -> R) -> Option<R> {
    COMPRESSOR.with(|cell| {
        if cell.borrow().is_none() {
            let created = Compressor::new(COMPRESSION_LEVEL).ok();
            *cell.borrow_mut() = created;
        }
        let mut slot = cell.borrow_mut();
        slot.as_mut().map(f)
    })
}

/// Runs `f` with the thread-local decompressor, creating it on first use.
///
/// Returns `None` only if the zstd context could not be created.
fn with_decompressor<R>(f: impl FnOnce(&mut Decompressor<'static>) -> R) -> Option<R> {
    DECOMPRESSOR.with(|cell| {
        if cell.borrow().is_none() {
            let created = Decompressor::new().ok();
            *cell.borrow_mut() = created;
        }
        let mut slot = cell.borrow_mut();
        slot.as_mut().map(f)
    })
}

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

    // Try to compress the data with the reusable thread-local context.
    let compressed = with_compressor(|c| c.compress(data));

    match compressed {
        Some(Ok(compressed)) => {
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
        _ => {
            // Compression error or context unavailable, send the original.
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

    let size = match zstd_safe::get_frame_content_size(data) {
        Ok(Some(size)) => size as usize,
        // Unknown size or malformed frame: fall back to a generous bound.
        _ => data.len() * 16,
    };

    let result = with_decompressor(|d| {
        let mut out = Vec::with_capacity(size);
        d.decompress_to_buffer(data, &mut out)
            .map(|written| {
                // SAFETY: `decompress_to_buffer` reported `written` bytes
                // initialized inside the pre-reserved capacity.
                unsafe { out.set_len(written) };
                out
            })
    });

    match result {
        Some(Ok(out)) => Some(out),
        _ => None,
    }
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
    fn test_roundtrip_reused_context() {
        // Multiple calls exercise the reused thread-local contexts.
        for _ in 0..8 {
            let data = vec![7u8; 16 * 1024];
            let compressed = compress_data(&data);
            assert!(compressed.is_compressed);
            let decompressed = decompress_data(&compressed.data, true);
            assert_eq!(decompressed.unwrap(), data);
        }
    }

    #[test]
    fn test_decompress_uncompressed() {
        let data = vec![1u8, 2, 3, 4];
        let result = decompress_data(&data, false);
        
        assert!(result.is_some());
        assert_eq!(result.unwrap(), data);
    }
}
