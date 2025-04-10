use crate::cmd::{Command, CommandHandler};
use crate::resp::RespHelper;
use crate::storage::MetaStorage;
use anyhow::Result;
use bytes::BytesMut;
use redis_protocol::resp2::types::OwnedFrame as Frame;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};

pub async fn run(addr: String, storage: MetaStorage) -> Result<()> {
    // Create a TCP listener
    let listener = TcpListener::bind(&addr).await?;
    info!("Listening on: {}", addr);

    // Create a shared storage instance
    let storage = Arc::new(storage);

    // Accept connections and process them
    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                info!("Accepted connection from: {}", addr);

                // Clone the storage for this connection
                let storage = Arc::clone(&storage);

                // Spawn a new task to handle this connection
                tokio::spawn(async move {
                    if let Err(e) = process(socket, storage).await {
                        error!("Error processing connection: {}", e);
                    }
                });
            }
            Err(e) => {
                error!("Error accepting connection: {}", e);
            }
        }
    }
}

pub async fn process(mut socket: TcpStream, storage: Arc<MetaStorage>) -> Result<()> {
    // Create a command handler
    let handler = CommandHandler::new(storage);

    // Use BytesMut for zero-copy operations
    let mut buffer = BytesMut::with_capacity(4096);

    // Process commands
    loop {
        // Read data directly into BytesMut buffer
        // This avoids an extra copy compared to using Vec<u8>
        let _n = match socket.read_buf(&mut buffer).await {
            Ok(0) => break, // Connection closed
            Ok(n) => n,
            Err(e) => {
                error!("Error reading from socket: {}", e);
                break;
            }
        };

        // No need to append data as we're reading directly into the buffer
        // Try to parse a frame from the buffer
        let mut pos = 0;
        while pos < buffer.len() {
            // Create a slice starting at the current position
            match RespHelper::parse_frame(&buffer[pos..]) {
                Ok(Some((frame, len))) => {
                    debug!("Received frame: {:?}", frame);
                    pos += len;

                    // Process the frame
                    let response = match Command::from_frame(frame) {
                        Ok(cmd) => handler.execute(cmd).await,
                        Err(e) => {
                            error!("Error parsing command: {}", e);
                            Frame::Error(format!("Error: {}", e))
                        }
                    };

                    // Encode and send the response
                    match RespHelper::encode_frame(&response) {
                        Ok(bytes) => {
                            if let Err(e) = socket.write_all(&bytes).await {
                                error!("Error writing response: {}", e);
                                break;
                            }
                        }
                        Err(e) => {
                            error!("Error encoding response: {}", e);
                            break;
                        }
                    }
                }
                Ok(None) => break, // Need more data
                Err(e) => {
                    error!("Error parsing frame: {}", e);
                    break;
                }
            }
        }

        // Remove processed data using split_to which is zero-copy
        if pos > 0 {
            let _ = buffer.split_to(pos); // Ignore the return value as suggested by the compiler
        }
    }

    debug!("Client disconnected");
    Ok(())
}
