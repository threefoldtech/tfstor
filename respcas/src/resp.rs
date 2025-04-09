use bytes::Bytes;
use redis_protocol::resp2::types::Frame;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RespError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Protocol error: {0}")]
    Protocol(String),
}

/// Helper functions for Redis RESP protocol
pub struct RespHelper;

impl RespHelper {
    /// Parse a frame from a byte buffer
    pub fn parse_frame(buffer: &[u8]) -> Result<Option<(Frame, usize)>, RespError> {
        if buffer.is_empty() {
            return Ok(None);
        }

        // Convert to Bytes for the redis-protocol crate
        let bytes = Bytes::copy_from_slice(buffer);
        match redis_protocol::resp2::decode::decode(&bytes) {
            Ok(Some((frame, len))) => {
                // Return the frame and how many bytes were consumed
                Ok(Some((frame, len)))
            }
            Ok(None) => {
                // Need more data
                Ok(None)
            }
            Err(e) => {
                if e.to_string().contains("incomplete") {
                    // Need more data
                    Ok(None)
                } else {
                    Err(RespError::Protocol(e.to_string()))
                }
            }
        }
    }

    /// Encode a frame to bytes
    pub fn encode_frame(frame: &Frame) -> Result<Vec<u8>, RespError> {
        // Estimate the frame size - for COMMAND responses, we need a much larger buffer
        let estimated_size = match frame {
            Frame::Array(items) if items.len() > 10 => 4096, // Large arrays like COMMAND response
            _ => 512, // Default size for most responses
        };
        
        // Use Vec<u8> for encoding with a larger capacity
        let mut buffer = Vec::with_capacity(estimated_size);
        buffer.resize(estimated_size, 0);
        
        // Try to encode with the current buffer size
        match redis_protocol::resp2::encode::encode(&mut buffer, 0, frame) {
            Ok(len) => {
                buffer.truncate(len);
                Ok(buffer)
            }
            Err(e) => {
                if e.to_string().contains("Buffer too small") {
                    // If buffer is too small, try with a much larger buffer
                    let mut larger_buffer = Vec::with_capacity(16384); // 16KB should be enough for most responses
                    larger_buffer.resize(16384, 0);
                    let len = redis_protocol::resp2::encode::encode(&mut larger_buffer, 0, frame)
                        .map_err(|e| RespError::Protocol(e.to_string()))?;
                    larger_buffer.truncate(len);
                    Ok(larger_buffer)
                } else {
                    Err(RespError::Protocol(e.to_string()))
                }
            }
        }
    }
}
