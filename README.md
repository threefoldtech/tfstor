# TFStor

This project provides storage solutions using content-addressed storage:

1. **[s3cas](./s3cas/README.md)** - An S3-compatible API server using content-addressed storage. [Read more about s3cas →](./s3cas/README.md)
2. **[respd](./respd/README.md)** - A Redis-compatible server using metastore as backend. [Read more about respd →](./respd/README.md)

## Building

To build the project, use the standard Rust tools:

```bash
git clone https://github.com/threefoldtech/tfstor
cd tfstor

# Build all components
cargo build --release

# Or build individual components
cargo build --release -p s3cas
cargo build --release -p respd
```
With this feature, the data blocks will be deleted when they aren't used anymore.

## respd Features

The respd server implements some basic Redis commands and also provides additional commands for namespace and data management that are not part of the standard Redis protocol.

## Known issues

### s3cas
- Only the basic S3 API is implemented (no copy between servers)
- Single key only, no support to add multiple keys with different permissions

### respd
- Limited subset of Redis commands implemented
