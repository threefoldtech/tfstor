use redis::{Client, Connection};
use std::fs;
use std::net::TcpListener;
use std::sync::Arc;
use std::thread::{self, sleep};
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::oneshot;

struct TestServer {
    port: u16,
    _temp_dir: tempfile::TempDir, // Keep this field to ensure the directory isn't deleted
    _server_handle: thread::JoinHandle<()>,
    shutdown_sender: Option<oneshot::Sender<()>>,
}

impl TestServer {
    fn new() -> Self {
        // Create a temporary directory for the server data
        let temp_dir = tempdir().expect("Failed to create temp directory");
        let data_dir = temp_dir.path().to_path_buf();

        // Make sure the directory exists
        fs::create_dir_all(&data_dir).expect("Failed to create data directory");

        // Find an available port
        let port = Self::find_available_port();
        println!("Starting respd server on port {}", port);

        // Create a shutdown channel
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        
        // Start the server in a separate thread
        let thread_port = port;
        let thread_data_dir = data_dir.clone();
        let server_handle = thread::spawn(move || {
            // Create a new runtime for this thread
            let rt = tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime");
            
            rt.block_on(async {
                // Create a TCP listener
                let addr = format!("127.0.0.1:{}", thread_port);
                let listener = TcpListener::bind(&addr).expect("Failed to bind to address");
                println!("Listening on: {}", addr);
                
                // Create a shared storage instance
                let storage = Arc::new(respd::storage::MetaStorage::new(thread_data_dir, None));
                
                // Convert to tokio TcpListener
                let listener = tokio::net::TcpListener::from_std(listener).expect("Failed to convert listener");
                
                // Create a future that completes when shutdown signal is received
                let shutdown_future = async {
                    let _ = shutdown_receiver.await;
                    println!("Shutdown signal received");
                };
                
                // Accept connections until shutdown signal is received
                tokio::select! {
                    _ = shutdown_future => {
                        println!("Server shutting down");
                        return;
                    }
                    _ = async {
                        loop {
                            match listener.accept().await {
                                Ok((socket, addr)) => {
                                    println!("Accepted connection from: {}", addr);
                                    
                                    // Clone the storage for this connection
                                    let storage = Arc::clone(&storage);
                                    
                                    // Spawn a new task to handle this connection
                                    tokio::spawn(async move {
                                        if let Err(e) = respd::server::process(socket, storage).await {
                                            eprintln!("Error processing connection: {}", e);
                                        }
                                    });
                                }
                                Err(e) => {
                                    eprintln!("Error accepting connection: {}", e);
                                }
                            }
                        }
                    } => {}
                }
            });
        });

        // Give the server some time to start
        sleep(Duration::from_secs(1));

        TestServer {
            port,
            _temp_dir: temp_dir,
            _server_handle: server_handle,
            shutdown_sender: Some(shutdown_sender),
        }
    }

    // Helper function to find an available port
    fn find_available_port() -> u16 {
        // Try to bind to port 0, which will assign a random available port
        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to address");
        let port = listener.local_addr().expect("Failed to get local address").port();
        // The listener will be dropped after this function returns, freeing the port
        port
    }

    fn connect(&self) -> Connection {
        // Try to connect multiple times with backoff
        let url = format!("redis://127.0.0.1:{}", self.port);
        let client = Client::open(url.as_str()).expect("Failed to create Redis client");

        for attempt in 1..=5 {
            match client.get_connection() {
                Ok(conn) => return conn,
                Err(e) => {
                    if attempt == 5 {
                        panic!("Failed to connect to Redis server after 5 attempts: {}", e);
                    }
                    println!("Connection attempt {} failed: {}, retrying...", attempt, e);
                    sleep(Duration::from_millis(500 * attempt));
                }
            }
        }

        panic!("Failed to connect to Redis server");
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        println!("Shutting down test server on port {}", self.port);
        // Take ownership of the sender before sending
        if let Some(sender) = std::mem::take(&mut self.shutdown_sender) {
            let _ = sender.send(());
        }
    }
}

#[test]
fn test_set_get() {
    let server = TestServer::new();
    let mut conn = server.connect();

    // Test SET and GET
    let _: () = redis::cmd("SET")
        .arg("test_key")
        .arg("test_value")
        .query(&mut conn)
        .expect("Failed to set key");
    let value: String = redis::cmd("GET")
        .arg("test_key")
        .query(&mut conn)
        .expect("Failed to get key");

    assert_eq!(value, "test_value");
}

// Run the tests one at a time to avoid port conflicts
#[cfg(test)]
mod test_config {
    use super::*;

    #[test]
    fn test_exists() {
        let server = TestServer::new();
        let mut conn = server.connect();

        // Test EXISTS
        let _: () = redis::cmd("SET")
            .arg("exists_key")
            .arg("value")
            .query(&mut conn)
            .expect("Failed to set key");
        let exists: bool = redis::cmd("EXISTS")
            .arg("exists_key")
            .query(&mut conn)
            .expect("Failed to check if key exists");
        let not_exists: bool = redis::cmd("EXISTS")
            .arg("nonexistent_key")
            .query(&mut conn)
            .expect("Failed to check if key exists");

        assert!(exists);
        assert!(!not_exists);
    }

    #[test]
    fn test_del() {
        let server = TestServer::new();
        let mut conn = server.connect();

        // Test DEL
        let _: () = redis::cmd("SET")
            .arg("del_key")
            .arg("value")
            .query(&mut conn)
            .expect("Failed to set key");
        let exists_before: bool = redis::cmd("EXISTS")
            .arg("del_key")
            .query(&mut conn)
            .expect("Failed to check if key exists");
        let _: () = redis::cmd("DEL")
            .arg("del_key")
            .query(&mut conn)
            .expect("Failed to delete key");
        let exists_after: bool = redis::cmd("EXISTS")
            .arg("del_key")
            .query(&mut conn)
            .expect("Failed to check if key exists");

        assert!(exists_before);
        assert!(!exists_after);
    }

    #[test]
    fn test_ping() {
        let server = TestServer::new();
        let mut conn = server.connect();

        // Test PING
        let pong: String = redis::cmd("PING")
            .query(&mut conn)
            .expect("Failed to ping server");
        let custom_pong: String = redis::cmd("PING")
            .arg("hello")
            .query(&mut conn)
            .expect("Failed to ping server with custom message");

        assert_eq!(pong, "PONG");
        assert_eq!(custom_pong, "hello");
    }

    #[test]
    fn test_command() {
        let server = TestServer::new();
        let mut conn = server.connect();

        // Test COMMAND
        let result: redis::Value = redis::cmd("COMMAND")
            .query(&mut conn)
            .expect("Failed to get command info");

        // COMMAND should return an array of command information
        if let redis::Value::Bulk(commands) = result {
            assert!(!commands.is_empty());
        } else {
            panic!("Expected COMMAND to return a bulk reply");
        }
    }

    #[test]
    fn test_multiple_commands() {
        let server = TestServer::new();
        let mut conn = server.connect();

        // Set multiple keys
        for i in 0..10 {
            let key = format!("key{}", i);
            let value = format!("value{}", i);
            let _: () = redis::cmd("SET")
                .arg(&key)
                .arg(&value)
                .query(&mut conn)
                .expect("Failed to set key");
        }

        // Get multiple keys
        for i in 0..10 {
            let key = format!("key{}", i);
            let expected = format!("value{}", i);
            let value: String = redis::cmd("GET")
                .arg(&key)
                .query(&mut conn)
                .expect("Failed to get key");
            assert_eq!(value, expected);
        }
    }
}
