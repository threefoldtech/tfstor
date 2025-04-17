use anyhow::Result;
use bytes::BytesMut;
use redis_protocol::resp2::types::OwnedFrame as Frame;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};

use crate::cmd::{Command, CommandHandler};
use crate::conn::Conn;
use crate::namespace::Namespace;
use crate::resp::RespHelper;
use crate::storage::Storage;

pub async fn run(addr: String, storage: Storage) -> Result<()> {
    // Initialize the default namespace if it doesn't exist
    if let Err(e) = storage.init_namespace() {
        error!("Failed to initialize namespace: {}", e);
        return Err(anyhow::anyhow!("Failed to initialize namespace: {}", e));
    }

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

pub async fn process(socket: TcpStream, storage: Arc<Storage>) -> Result<()> {
    // Create a connection abstraction
    let mut conn = Conn::new(socket);

    // Try to create a namespace for the default namespace
    let namespace = match Namespace::new(storage.clone(), conn.get_namespace()) {
        Ok(namespace) => namespace,
        Err(e) => {
            error!("failed to load default namespace: {}", e);
            // If we can't create the default namespace, try to create it
            debug!("Default namespace not found, attempting to create it");
            match storage.create_namespace(&conn.get_namespace()) {
                Ok(tree) => {
                    // Create a namespace with the newly created tree
                    Namespace { tree }
                }
                Err(create_err) => {
                    // If we can't create the namespace, return an error
                    error!("Failed to create default namespace: {}", create_err);
                    return Err(anyhow::anyhow!(
                        "Failed to initialize default namespace: {}",
                        create_err
                    ));
                }
            }
        }
    };

    // Create a command handler with the connection's namespace
    let mut handler = CommandHandler::new(storage.clone(), namespace);

    // Use BytesMut for zero-copy operations
    let mut buffer = BytesMut::with_capacity(4096);

    // Process commands
    loop {
        // Read data directly into BytesMut buffer
        // This avoids an extra copy compared to using Vec<u8>
        let _n = match conn.read_buf(&mut buffer).await {
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
                        Ok(Command::Select { namespace }) => {
                            // Special handling for SELECT command to set the namespace
                            debug!("Handling SELECT command for namespace: {}", namespace);
                            conn.set_namespace(namespace.clone());

                            // Try to create a new namespace instance
                            match Namespace::new(storage.clone(), conn.get_namespace()) {
                                Ok(namespace) => {
                                    // Update the handler's tree to use the new namespace
                                    handler = CommandHandler::new(storage.clone(), namespace);
                                    Frame::SimpleString("OK".into())
                                }
                                Err(e) => {
                                    // Forward the error message to the client
                                    error!(
                                        "Error selecting namespace {}: {}",
                                        conn.get_namespace(),
                                        e
                                    );
                                    Frame::Error(format!("ERR {}", e))
                                }
                            }
                        }
                        Ok(cmd) => handler.execute(cmd).await,
                        Err(e) => {
                            error!("Error parsing command: {}", e);
                            Frame::Error(format!("Error: {}", e))
                        }
                    };

                    // Encode and send the response
                    match RespHelper::encode_frame(&response) {
                        Ok(bytes) => {
                            if let Err(e) = conn.write_all(&bytes).await {
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
