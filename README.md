# TFStor

This project provides storage solutions using content-addressed storage:  

1. **s3cas** - An S3-compatible API server using content-addressed storage  
2. **respd** - A Redis-compatible server using metastore as backend

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

## Running s3cas

```console
s3cas server --access-key=MY_KEY --secret-key=MY_SECRET --fs-root=/tmp/s3/fs --meta-root=/tmp/s3/meta
```

## Running respd

```console
respd --data-dir=/tmp/respd/data
```

By default, respd listens on 127.0.0.1:6379 and can be accessed using any Redis client.

## s3cas Features

### Inline metadata

Objects smaller than or equal to a configurable threshold can be stored directly in their metadata records,
improving performance for small objects.

Configure this feature using the command-line option:
```console
--inline-metadata-size <size>    # omit to disable inlining
```

When the size is set:
- If the size of object data + metadata smaller than or equal to the threshold, the object data is stored in the metadata,
  otherwise use the standard block storage
- Setting size to 0 or omitting the option disables inlining completely

Currently, objects uploaded using the multipart method will never be inlined
because they are assumed to be large objects.

### Reference Counting

The `refcount` feature adds reference counting to data blocks.
With this feature, the data blocks will be deleted when they aren't used anymore.

## respd Features

The respd server implements basic Redis commands including:
- PING
- SET
- GET
- DEL
- EXISTS

## Known issues

### s3cas
- Only the basic S3 API is implemented (no copy between servers)
- Single key only, no support to add multiple keys with different permissions

### respd
- Limited subset of Redis commands implemented
