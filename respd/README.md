# respd

`respd` is a Redis-compatible server using metastore as its backend. It is designed to work seamlessly with Redis clients and supports a subset of Redis commands, along with namespace management features.

## Running respd

```console
respd --data-dir=/tmp/respd/data
```

With admin authentication:

```console
respd --data-dir=/tmp/respd/data --admin=mypassword
```

By default, respd listens on `127.0.0.1:6379` and can be accessed using any Redis client.

## Supported Commands

| Command           | Description                              | Example Usage                      |
|-------------------|------------------------------------------|------------------------------------|
| GET <key>         | Get the value of a key                   | `GET mykey`                        |
| MGET <key>...     | Get the values of multiple keys          | `MGET key1 key2`                   |
| SET <key> <value> | Set the value of a key                   | `SET mykey somevalue`              |
| DEL <key>         | Delete a key                             | `DEL mykey`                        |
| EXISTS <key>      | Check if a key exists                    | `EXISTS mykey`                     |
| PING [message]    | Ping the server (optionally with message)| `PING` or `PING hello`             |
| CHECK <key>       | Verify data integrity for a key          | `CHECK mykey`                      |
| LENGTH <key>      | Get the size (in bytes) of a key's value, returns nil if key doesn't exist | `LENGTH mykey`                     |
| KEYTIME <key>     | Get the last-update timestamp of a key (Unix time), returns nil if key doesn't exist | `KEYTIME mykey`                    |
| AUTH <password>   | Authenticate as admin                    | `AUTH mypassword`                  |
| SELECT <namespace>| Switch to a different namespace          | `SELECT mynamespace`               |
| NSNEW <n>      | Create a new namespace (admin only)      | `NSNEW mynamespace`                |
| NSINFO <n>     | Show info about a namespace              | `NSINFO mynamespace`               |
| NSLIST           | List all available namespaces            | `NSLIST`                           |
| DBSIZE           | Get the number of keys in the current namespace (approximate) | `DBSIZE`                          |

- All commands are case-insensitive.
- Namespace commands (`SELECT`, `NSNEW`, `NSINFO`, `NSLIST`) allow multi-tenant data separation.
- Authentication with `AUTH` is only applicable if the server was started with the `--admin` parameter.
- Some commands (like `NSNEW`) require admin privileges.

## Features
- Redis protocol compatibility (subset)
- Namespace support
- Data integrity checking
- Simple to run and integrate

## Known Limitations
- The `NSLIST` command currently collects all namespace names before sending the response. Future improvements will implement streaming responses to handle large numbers of namespaces more efficiently.

---

For project overview and build instructions, see the [main README](../README.md).
