# S3-CAS

A simple POC implementation of the (basic) S3 API using content addresses storage. The current implementation
has been running in production for 1.5 years storing some 250M objects.

There is also `refcount` feature which adds reference counting to data blocks.
With this feature, the data blocks will be deleted when they aren't used anymore.


## Building

To build it yourself, clone the repo and then use the standard rust tools.
The `vendored` feature can be used if a static binary is needed.

```
git clone https://github.com/leesmet/s3-cas
cd s3-cas
cargo build --release --features binary
```

## Running

```console
s3-cas server --access-key=MY_KEY --secret-key=MY_SECRET --fs-root=/tmp/s3/fs --meta-root=/tmp/s3/meta
```

## Inline metadata

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

## Known issues

- Only the basic API is implemented, and even then it is not entirely implemented (for instance copy
  between servers is not implemented).
- Single key only, no support to add multiple keys with different permissions.
