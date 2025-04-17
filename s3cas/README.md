# s3cas

`s3cas` is an S3-compatible API server that uses content-addressed storage for efficient and reliable data management.

## Running s3cas

```console
s3cas server --access-key=MY_KEY --secret-key=MY_SECRET --fs-root=/tmp/s3/fs --meta-root=/tmp/s3/meta
```

## Features

### Inline Metadata
Objects smaller than or equal to a configurable threshold can be stored directly in their metadata records, improving performance for small objects.

Configure this feature using the command-line option:
```console
--inline-metadata-size <size>    # omit to disable inlining
```

- If the size of object data + metadata is smaller than or equal to the threshold, the object data is stored in the metadata; otherwise, standard block storage is used.
- Setting size to 0 or omitting the option disables inlining completely.
- Objects uploaded using the multipart method will never be inlined because they are assumed to be large objects.

### Reference Counting
The `refcount` feature adds reference counting to data blocks.

---

For project overview and build instructions, see the [main README](../README.md).
