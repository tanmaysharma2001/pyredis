# PyRedis

PyRedis is a lightweight, in-memory key-value store server, written in Python, that partially implements the Redis protocol. Below are the features currently supported by PyRedis.

## Features

### SET Command

- **Description**: Stores a key-value pair in the memory. If the key already exists, its value is overwritten.
- **Usage**: `SET <key> <value> [PX <milliseconds>]`
- **Extended Feature**: The `SET` command supports an optional `PX` argument that allows setting an expiry time for the key in milliseconds. After the specified duration, the key will automatically be removed from the store.

### GET Command

- **Description**: Retrieves the value of the specified key. If the key does not exist, a null bulk string is returned.
- **Usage**: `GET <key>`
- **Note**: If a key has been set with an expiry time using the `PX` argument in the `SET` command and the key has expired, a null bulk string (`$-1
`) is returned.

### ECHO Command

- **Description**: Echoes the given string back to the client.
- **Usage**: `ECHO <message>`
- **Note**: This command is useful for testing and debugging client-server communication.

### Key Expiry

- **Description**: Keys can be set with an expiry time in milliseconds using the `PX` argument in the `SET` command. Expired keys are automatically removed from the store and are not retrievable with the `GET` command.
- **Usage**: Included as part of the `SET` command with the `PX` argument.

## Running the Server

To start the PyRedis server, ensure you have Python installed on your machine, navigate to the project directory in your terminal, and run:

```bash
python server.py
```

The server will start listening for connections on `localhost:6379`.

## Connecting to the Server

You can connect to the PyRedis server using any Redis client by connecting to `localhost` on port `6379`. For example, using `redis-cli`:

```bash
redis-cli -h localhost -p 6379
```

## Note

PyRedis is a simplified version of Redis and does not support the full set of Redis features or security measures.