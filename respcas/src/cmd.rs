use crate::storage::MetaStorage;
use bytes::Bytes;
use redis_protocol::resp2::types::OwnedFrame as Frame;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, error};

#[derive(Debug, Error)]
pub enum CommandError {
    #[error("Unknown command: {0}")]
    UnknownCommand(String),

    #[error("Wrong number of arguments for command: {0}")]
    WrongNumberOfArguments(String),

    #[error("Storage error: {0}")]
    Storage(#[from] metastore::MetaError),

    #[error("Protocol error: {0}")]
    Protocol(String),
}

/// Redis command types supported by our server
#[derive(Debug)]
pub enum Command {
    Get { key: String },
    Set { key: String, value: Bytes },
    Ping { message: Option<String> },
    Info,
    Del { key: String },
    Exists { key: String },
    // Add more commands as needed
}

impl Command {
    /// Parse a Redis protocol frame into a command
    pub fn from_frame(frame: Frame) -> Result<Self, CommandError> {
        match frame {
            Frame::Array(array) => {
                if array.is_empty() {
                    return Err(CommandError::WrongNumberOfArguments(
                        "empty command".to_string(),
                    ));
                }

                // Extract the command name from the first element
                let command_name = match &array[0] {
                    Frame::BulkString(bytes) => String::from_utf8_lossy(bytes).to_uppercase(),
                    _ => {
                        return Err(CommandError::Protocol(
                            "Command name must be a bulk string".to_string(),
                        ))
                    }
                };

                // Parse the command based on its name
                match command_name.as_str() {
                    "COMMAND" => {
                        // Return the Info variant
                        Ok(Command::Info)
                    }
                    "DEL" => {
                        if array.len() != 2 {
                            return Err(CommandError::WrongNumberOfArguments("DEL".to_string()));
                        }

                        let key = match &array[1] {
                            Frame::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                            _ => {
                                return Err(CommandError::Protocol(
                                    "DEL key must be a bulk string".to_string(),
                                ))
                            }
                        };

                        Ok(Command::Del { key })
                    }
                    "EXISTS" => {
                        if array.len() != 2 {
                            return Err(CommandError::WrongNumberOfArguments("EXISTS".to_string()));
                        }

                        let key = match &array[1] {
                            Frame::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                            _ => {
                                return Err(CommandError::Protocol(
                                    "EXISTS key must be a bulk string".to_string(),
                                ))
                            }
                        };

                        Ok(Command::Exists { key })
                    }
                    "GET" => {
                        if array.len() != 2 {
                            return Err(CommandError::WrongNumberOfArguments("GET".to_string()));
                        }
                        let key = match &array[1] {
                            Frame::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                            _ => {
                                return Err(CommandError::Protocol(
                                    "GET key must be a bulk string".to_string(),
                                ))
                            }
                        };
                        Ok(Command::Get { key })
                    }
                    "SET" => {
                        if array.len() < 3 {
                            return Err(CommandError::WrongNumberOfArguments("SET".to_string()));
                        }
                        let key = match &array[1] {
                            Frame::BulkString(bytes) => String::from_utf8_lossy(bytes).to_string(),
                            _ => {
                                return Err(CommandError::Protocol(
                                    "SET key must be a bulk string".to_string(),
                                ))
                            }
                        };
                        let value = match &array[2] {
                            Frame::BulkString(bytes) => Bytes::from(bytes.clone()),
                            _ => {
                                return Err(CommandError::Protocol(
                                    "SET value must be a bulk string".to_string(),
                                ))
                            }
                        };
                        Ok(Command::Set { key, value })
                    }
                    "PING" => {
                        let message = if array.len() > 1 {
                            match &array[1] {
                                Frame::BulkString(bytes) => {
                                    Some(String::from_utf8_lossy(bytes).to_string())
                                }
                                _ => {
                                    return Err(CommandError::Protocol(
                                        "PING message must be a bulk string".to_string(),
                                    ))
                                }
                            }
                        } else {
                            None
                        };
                        Ok(Command::Ping { message })
                    }
                    _ => Err(CommandError::UnknownCommand(command_name)),
                }
            }
            _ => Err(CommandError::Protocol(
                "Command must be an array".to_string(),
            )),
        }
    }
}

/// Handler for Redis commands
pub struct CommandHandler {
    storage: Arc<MetaStorage>,
}

impl CommandHandler {
    /// Create a new command handler
    pub fn new(storage: Arc<MetaStorage>) -> Self {
        Self { storage }
    }

    /// Execute a command and return the response frame
    pub async fn execute(&self, cmd: Command) -> Frame {
        match cmd {
            Command::Get { key } => self.handle_get(key).await,
            Command::Set { key, value } => self.handle_set(key, value).await,
            Command::Ping { message } => self.handle_ping(message),
            Command::Info => self.handle_command(),
            Command::Del { key } => self.handle_del(key).await,
            Command::Exists { key } => self.handle_exists(key).await,
        }
    }

    /// Handle GET command
    async fn handle_get(&self, key: String) -> Frame {
        debug!("Handling GET command for key: {}", key);
        match self.storage.get(&key).await {
            Ok(Some(value)) => Frame::BulkString(value.to_vec()),
            Ok(None) => Frame::Null,
            Err(e) => {
                error!("Error getting key {}: {}", key, e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle SET command
    async fn handle_set(&self, key: String, value: Bytes) -> Frame {
        debug!("Handling SET command for key: {}", key);
        match self.storage.set(&key, value.to_vec()).await {
            Ok(()) => Frame::SimpleString("OK".into()),
            Err(e) => {
                error!("Error setting key {}: {}", key, e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle PING command
    fn handle_ping(&self, message: Option<String>) -> Frame {
        match message {
            Some(msg) => Frame::BulkString(msg.into_bytes()),
            None => Frame::SimpleString("PONG".into()),
        }
    }

    /// Handle COMMAND command
    /// Returns information about the supported commands in a format compatible with redis-cli
    fn handle_command(&self) -> Frame {
        // For redis-cli compatibility, we need to return a specific format
        // Based on the Redis protocol specification, the COMMAND response should be:
        // 1. An array where each element is information about a command
        // 2. Each command info is an array with specific elements

        // Format: [name, arity, flags, first_key, last_key, step]
        let command_info = vec![
            // GET command
            Frame::Array(vec![
                Frame::BulkString("get".into()), // name
                Frame::Integer(2),               // arity (command + 1 arg)
                Frame::Array(vec![
                    // flags
                    Frame::BulkString("readonly".into()),
                    Frame::BulkString("fast".into()),
                ]),
                Frame::Integer(1), // first key position
                Frame::Integer(1), // last key position
                Frame::Integer(1), // step
            ]),
            // SET command
            Frame::Array(vec![
                Frame::BulkString("set".into()), // name
                Frame::Integer(3),               // arity (command + 2 args)
                Frame::Array(vec![
                    // flags
                    Frame::BulkString("write".into()),
                    Frame::BulkString("denyoom".into()),
                ]),
                Frame::Integer(1), // first key position
                Frame::Integer(1), // last key position
                Frame::Integer(1), // step
            ]),
            // DEL command
            Frame::Array(vec![
                Frame::BulkString("del".into()), // name
                Frame::Integer(2),               // arity (command + 1 arg)
                Frame::Array(vec![
                    // flags
                    Frame::BulkString("write".into()),
                ]),
                Frame::Integer(1), // first key position
                Frame::Integer(1), // last key position
                Frame::Integer(1), // step
            ]),
            // EXISTS command
            Frame::Array(vec![
                Frame::BulkString("exists".into()), // name
                Frame::Integer(2),                  // arity (command + 1 arg)
                Frame::Array(vec![
                    // flags
                    Frame::BulkString("readonly".into()),
                    Frame::BulkString("fast".into()),
                ]),
                Frame::Integer(1), // first key position
                Frame::Integer(1), // last key position
                Frame::Integer(1), // step
            ]),
            // PING command
            Frame::Array(vec![
                Frame::BulkString("ping".into()), // name
                Frame::Integer(-1),               // arity (variable args)
                Frame::Array(vec![
                    // flags
                    Frame::BulkString("fast".into()),
                ]),
                Frame::Integer(0), // first key position
                Frame::Integer(0), // last key position
                Frame::Integer(0), // step
            ]),
            // COMMAND command
            Frame::Array(vec![
                Frame::BulkString("command".into()), // name
                Frame::Integer(0),                   // arity (just the command)
                Frame::Array(vec![
                    // flags
                    Frame::BulkString("readonly".into()),
                    Frame::BulkString("random".into()),
                ]),
                Frame::Integer(0), // first key position
                Frame::Integer(0), // last key position
                Frame::Integer(0), // step
            ]),
        ];

        Frame::Array(command_info)
    }

    /// Handle DEL command
    async fn handle_del(&self, key: String) -> Frame {
        debug!("Handling DEL command for key: {}", key);
        match self.storage.delete(&key).await {
            Ok(()) => Frame::Integer(1), // Successfully deleted 1 key
            Err(e) => {
                error!("Error deleting key {}: {}", key, e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }

    /// Handle EXISTS command
    async fn handle_exists(&self, key: String) -> Frame {
        debug!("Handling EXISTS command for key: {}", key);
        match self.storage.exists(&key).await {
            Ok(exists) => {
                if exists {
                    Frame::Integer(1) // Key exists
                } else {
                    Frame::Integer(0) // Key does not exist
                }
            }
            Err(e) => {
                error!("Error checking if key {} exists: {}", key, e);
                Frame::Error(format!("ERR {}", e))
            }
        }
    }
}
