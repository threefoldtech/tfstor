use redis_protocol::resp2::types::OwnedFrame as Frame;
use respd::cmd::{Command, CommandError};

#[test]
fn test_select_command_parsing() {
    // Test SELECT with just namespace (2 arguments)
    let frame = Frame::Array(vec![
        Frame::BulkString(b"SELECT".to_vec()),
        Frame::BulkString(b"test_namespace".to_vec()),
    ]);

    let cmd = Command::from_frame(frame).unwrap();
    match cmd {
        Command::Select {
            namespace,
            password,
        } => {
            assert_eq!(namespace, "test_namespace");
            assert_eq!(password, None);
        }
        _ => panic!("Expected SELECT command"),
    }

    // Test SELECT with namespace and password (3 arguments)
    let frame = Frame::Array(vec![
        Frame::BulkString(b"SELECT".to_vec()),
        Frame::BulkString(b"test_namespace".to_vec()),
        Frame::BulkString(b"secret_password".to_vec()),
    ]);

    let cmd = Command::from_frame(frame).unwrap();
    match cmd {
        Command::Select {
            namespace,
            password,
        } => {
            assert_eq!(namespace, "test_namespace");
            assert_eq!(password, Some("secret_password".to_string()));
        }
        _ => panic!("Expected SELECT command"),
    }

    // Test SELECT with too few arguments
    let frame = Frame::Array(vec![Frame::BulkString(b"SELECT".to_vec())]);

    let result = Command::from_frame(frame);
    assert!(matches!(
        result,
        Err(CommandError::WrongNumberOfArguments(_))
    ));

    // Test SELECT with too many arguments
    let frame = Frame::Array(vec![
        Frame::BulkString(b"SELECT".to_vec()),
        Frame::BulkString(b"test_namespace".to_vec()),
        Frame::BulkString(b"secret_password".to_vec()),
        Frame::BulkString(b"extra_arg".to_vec()),
    ]);

    let result = Command::from_frame(frame);
    assert!(matches!(
        result,
        Err(CommandError::WrongNumberOfArguments(_))
    ));
}
