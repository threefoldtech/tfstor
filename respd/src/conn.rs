use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Connection abstraction for client connections
pub struct Conn {
    /// The TCP socket for the connection
    socket: TcpStream,
    /// Namespace for the connection
    namespace: String,
}

impl Conn {
    /// Create a new connection
    pub fn new(socket: TcpStream) -> Self {
        Self {
            socket,
            namespace: "default".to_string(),
        }
    }

    /// Set the namespace for this connection
    pub fn set_namespace(&mut self, namespace: String) {
        self.namespace = namespace;
    }

    /// Read data from the socket into the provided buffer
    pub async fn read_buf(&mut self, buf: &mut BytesMut) -> std::io::Result<usize> {
        self.socket.read_buf(buf).await
    }

    /// Write data to the socket
    pub async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        self.socket.write_all(buf).await
    }
}
