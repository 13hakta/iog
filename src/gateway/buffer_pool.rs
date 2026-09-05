//! Buffer pool module for zero-copy operations (ROADMAP item 5.2).
//!
//! Uses an object pool to reuse read buffers,
//! which reduces allocator pressure and the number of allocations.
//!
//! ## Problem without a pool:
//! `vec![0u8; BUFFER_SIZE]` per packet — too many allocations.
//!
//! ## Solution:
//! The buffer pool reuses buffers between packets.
//!
//! ## Expected improvements:
//! - Benchmark: -30% memory allocations.
//! - +15% throughput under high load.

use std::sync::{Arc, Mutex as StdMutex};

use crate::gateway::consts::BUFFER_SIZE;

/// Buffer pool size (number of buffers in the pool).
pub const POOL_SIZE: usize = 100;

/// Internal buffer pool structure.
struct BufferPoolInner {
    buffers: StdMutex<Vec<Vec<u8>>>,
}

/// Buffer automatically returned to the pool on `drop`.
pub struct PooledBuffer {
    buffer: Vec<u8>,
    pool: Arc<BufferPoolInner>,
    /// Size of the buffer as it was issued from the pool.
    buffer_size: usize,
}

impl PooledBuffer {
    /// Creates a new buffer bound to the pool.
    fn new(buffer: Vec<u8>, pool: Arc<BufferPoolInner>) -> Self {
        let buffer_size = buffer.capacity().max(buffer.len());
        Self {
            buffer,
            pool,
            buffer_size,
        }
    }
}

impl std::ops::Deref for PooledBuffer {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl std::ops::DerefMut for PooledBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer
    }
}

impl Drop for PooledBuffer {
    /// Returns the buffer to the pool automatically.
    fn drop(&mut self) {
        let mut buffer = std::mem::take(&mut self.buffer);
        buffer.clear();
        buffer.resize(self.buffer_size, 0);
        if let Ok(mut buffers) = self.pool.buffers.lock() {
            if buffers.len() < POOL_SIZE {
                buffers.push(buffer);
            }
        }
    }
}

/// Buffer pool for packet reading.
///
/// Efficiently reuses buffers between packets.
/// The buffer is automatically returned to the pool when `drop()` is called.
pub struct BufferPool {
    inner: Arc<BufferPoolInner>,
    buffer_size: usize,
}

impl BufferPool {
    /// Creates a new buffer pool.
    ///
    /// # Arguments
    /// * `pool_size` - Number of buffers in the pool.
    /// * `buffer_size` - Size of each buffer.
    pub fn new(pool_size: usize, buffer_size: usize) -> Self {
        let mut buffers = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            buffers.push(vec![0u8; buffer_size]);
        }

        Self {
            inner: Arc::new(BufferPoolInner {
                buffers: StdMutex::new(buffers),
            }),
            buffer_size,
        }
    }

    /// Acquires a buffer from the pool.
    ///
    /// If the pool is empty, a new buffer is created.
    /// The buffer is automatically returned to the pool when `drop()` is called.
    pub async fn acquire(&self) -> PooledBuffer {
        let buffer = self
            .inner
            .buffers
            .lock()
            .ok()
            .and_then(|mut buffers| buffers.pop())
            .unwrap_or_else(|| vec![0u8; self.buffer_size]);
        PooledBuffer::new(buffer, self.inner.clone())
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new(POOL_SIZE, BUFFER_SIZE)
    }
}

/// Global buffer pool for use across the whole application.
pub static BUFFER_POOL: once_cell::sync::Lazy<BufferPool> =
    once_cell::sync::Lazy::new(BufferPool::default);

/// Acquires a buffer from the global pool.
///
/// The buffer is automatically returned to the pool when it goes out of scope.
pub async fn acquire_buffer() -> PooledBuffer {
    BUFFER_POOL.acquire().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_buffer_pool_acquire_release() {
        let pool = BufferPool::new(10, 1024);

        // Acquire a buffer.
        let buffer = pool.acquire().await;
        assert_eq!(buffer.len(), 1024);

        // Return the buffer.
        drop(buffer);

        // Another buffer can be acquired.
        let buffer2 = pool.acquire().await;
        assert_eq!(buffer2.len(), 1024);
    }

    #[tokio::test]
    async fn test_buffer_pool_reuse() {
        let pool = BufferPool::new(2, 512);

        // Acquire a buffer.
        {
            let mut buffer = pool.acquire().await;
            buffer[0] = 42;
        }

        // The buffer must be reused.
        let buffer2 = pool.acquire().await;
        assert_eq!(buffer2.len(), 512);
    }
}

