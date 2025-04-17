# respd

`respd` is a Redis-compatible server using metastore as its backend. It is designed to work seamlessly with Redis clients and supports a subset of Redis commands, along with namespace management features.

## Running respd

```console
respd --data-dir=/tmp/respd/data
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
| SELECT <namespace>| Switch to a different namespace          | `SELECT mynamespace`               |
| NSNEW <name>      | Create a new namespace                   | `NSNEW mynamespace`                |
| NSINFO <name>     | Show info about a namespace              | `NSINFO mynamespace`               |

- All commands are case-insensitive.
- Namespace commands (`SELECT`, `NSNEW`, `NSINFO`) allow multi-tenant data separation.

## Features
- Redis protocol compatibility (subset)
- Namespace support
- Data integrity checking
- Simple to run and integrate

---

For project overview and build instructions, see the [main README](../README.md).
