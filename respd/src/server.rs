use anyhow::Result;
use bytes::BytesMut;
use redis_protocol::resp2::types::OwnedFrame as Frame;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info};

use crate::cmd::{Command, CommandHandler};
use crate::conn::Conn;
use crate::namespace::NamespaceCache;
use crate::resp::RespHelper;
use crate::storage::Storage;

pub async fn run(addr: String, storage: Storage, admin_password: Option<String>) -> Result<()> {
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

    // Create a shared namespace cache
    let namespace_cache = Arc::new(NamespaceCache::new(storage.clone()));

    // Accept connections and process them
    loop {
        match listener.accept().await {
            Ok((socket, addr)) => {
                info!("Accepted connection from: {}", addr);

                // Clone the storage for this connection
                let storage = storage.clone();

                // Clone the namespace cache for this connection
                let namespace_cache = namespace_cache.clone();

                // Clone the admin password for this connection
                let admin_password = admin_password.clone();

                // Spawn a new task to handle this connection
                tokio::spawn(async move {
                    if let Err(e) =
                        process(socket, storage, namespace_cache.clone(), admin_password).await
                    {
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

pub async fn process(
    socket: TcpStream,
    storage: Arc<Storage>,
    namespace_cache: Arc<NamespaceCache>,
    admin_password: Option<String>,
) -> Result<()> {
    // Create a connection abstraction
    // If no admin password is required, all connections are admin by default
    let is_admin = admin_password.is_none();
    let mut conn = Conn::new(socket, is_admin);

    // Try to get or create a namespace for the default namespace using the cache
    let namespace = match namespace_cache.create_if_not_exists(conn.get_namespace()) {
        Ok(namespace) => namespace,
        Err(e) => {
            error!("Failed to initialize default namespace: {}", e);
            return Err(anyhow::anyhow!(
                "Failed to initialize default namespace: {}",
                e
            ));
        }
    };

    // Create a command handler with the connection's namespace and admin status
    let mut handler = CommandHandler::new(storage.clone(), namespace, conn.is_admin());

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
                            match namespace_cache.get_or_create(conn.get_namespace()) {
                                Ok(namespace) => {
                                    // Update the handler's tree to use the new namespace
                                    handler = CommandHandler::new(
                                        storage.clone(),
                                        namespace,
                                        conn.is_admin(),
                                    );
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
                        Ok(Command::Auth { password }) => {
                            // Special handling for AUTH command to authenticate the connection
                            debug!("Handling AUTH command");

                            // Check if authentication is required
                            match &admin_password {
                                Some(admin_pwd) => {
                                    // Admin password is set, verify the provided password
                                    if password == *admin_pwd {
                                        // Password matches, grant admin privileges
                                        conn.set_admin(true);

                                        // Update the handler with the new admin status
                                        // We need to recreate the namespace from the current namespace name
                                        match namespace_cache.get_or_create(conn.get_namespace()) {
                                            Ok(namespace) => {
                                                // Update the handler with the new admin status
                                                handler = CommandHandler::new(
                                                    storage.clone(),
                                                    namespace,
                                                    conn.is_admin(),
                                                );
                                                Frame::SimpleString("OK".into())
                                            }
                                            Err(e) => {
                                                error!(
                                                    "Error recreating namespace after AUTH: {}",
                                                    e
                                                );
                                                Frame::Error(format!("ERR {}", e))
                                            }
                                        }
                                    } else {
                                        // Password doesn't match
                                        Frame::Error("ERR invalid password".into())
                                    }
                                }
                                None => {
                                    // No admin password set, all connections are already admin
                                    Frame::SimpleString("OK".into())
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
