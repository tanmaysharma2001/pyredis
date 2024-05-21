import asyncio
import base64
import socket
import threading
from threading import Thread
import time
import re
import argparse

# Lock for thread safety
lock = threading.Lock()

parser = argparse.ArgumentParser()
parser.add_argument("-p", "--port", help="Port Number", type=int)
parser.add_argument("--replicaof", nargs=1, help="Replicate to another redis instance")

# In-memory key-value store
key_value_store = {}
master_replid = '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb'
master_repl_offset = 0

# Master HOST and PORT
master_host = ''
master_port = ''
replicas = []

# MASTER SOCKET for Replica
master_socket = None
role = "master"
is_replica = False
replica_offset = 0

# Empty RDB file content (base64 representation)
EMPTY_RDB_FILE = b"UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="


def handle_set_command(parsed_command):
    global is_replica, master_host, master_port
    key, value = parsed_command[1], parsed_command[2]
    expiry = None
    if "px" in parsed_command:
        px_index = parsed_command.index("px")
        if px_index + 1 < len(parsed_command):
            expiry = int(time.time() * 1000) + int(
                parsed_command[px_index + 1])  # Convert to milliseconds and add to current time
    with lock:
        key_value_store[key] = (value, expiry)


def handle_get_command(parsed_command):
    key = parsed_command[1]
    with lock:
        value, expiry = key_value_store.get(key, (None, None))
    if value is not None:
        # Check if the key has expired
        if expiry is not None and int(time.time() * 1000) > expiry:
            with lock:
                del key_value_store[key]  # Remove expired key
            return b"$-1\r\n"
        else:
            response = f"${len(value)}\r\n{value}\r\n"
            return response.encode()
    else:
        return b"$-1\r\n"


def handle_info_command(parsed_command):
    global is_replica, master_replid, master_repl_offset
    if parsed_command[1] == 'replication':
        if is_replica:
            return b"$10\r\nrole:slave\r\n"
        else:
            return f"${2 + 11 + len(master_replid) + len('master_replid:') + len(str(master_repl_offset)) + len('master_repl_offset:')}\r\nrole:master,master_replid:{master_replid},master_repl_offset:{master_repl_offset}\r\n".encode()


def propogate_to_slave(replica_socket, message):
    replica_socket.write(message)
    replica_socket.drain()


def handle_command(parsed_command, writer=None):
    global is_replica
    responses = []

    if parsed_command[0] == 'set':
        handle_set_command(parsed_command)
        if not is_replica:
            responses.append(b"+OK\r\n")
            with lock:
                for replica_socket in replicas:
                    propogate_to_slave(replica_socket,
                                       f"*3\r\n$3\r\nSET\r\n${len(parsed_command[1])}\r\n{parsed_command[1]}\r\n${len(parsed_command[2])}\r\n{parsed_command[2]}\r\n".encode())

    # Handling GET command
    elif parsed_command[0] == 'get' and len(parsed_command) == 2:
        responses.append(handle_get_command(parsed_command))

    # Handling the INFO command
    elif parsed_command[0] == 'info' and len(parsed_command) == 2:
        responses.append(handle_info_command(parsed_command))

    elif parsed_command[0] == 'wait' and len(parsed_command) > 1:
        responses.append(f":{len(replicas)}\r\n".encode())

    # Handling the echo command
    elif parsed_command[0] == 'echo' and len(parsed_command) > 1:
        response_message = f"+{parsed_command[1]}\r\n".encode()
        # writer.write(response_message.encode())
        responses.append(response_message)

    elif parsed_command[0] == 'replconf' and len(parsed_command) > 1:
        responses.append(b'+OK\r\n')

    elif parsed_command[0] == 'psync' and parsed_command[1] == '?' and parsed_command[2] == '-1':
        # Send FULLRESYNC response
        response = f"+FULLRESYNC {master_replid} 0\r\n".encode()
        responses.append(response)
        if not is_replica:
            with lock:
                replicas.append(writer)
        # Send empty RDB file
        rdb_content = base64.b64decode(EMPTY_RDB_FILE)
        rdb_length = len(rdb_content)
        responses.append(f"${rdb_length}\r\n".encode() + rdb_content)
    else:
        responses.append(b"+PONG\r\n")

    return responses


# Parser function for Redis protocol
def parse_redis_protocol(data):
    # data = data.decode("utf-8")
    command_list = data.split("\r\n")
    if command_list[-1] == "":
        command_list.pop()
    first_arg = command_list.pop(0)
    arg_num_list = re.findall(pattern="\*\d", string=first_arg)
    if len(arg_num_list) != 1:
        return "-Wrong command1\r\n"
    arg_num = int(arg_num_list[0].replace("*", ""))
    if arg_num not in range(1, 6):
        return "-Wrong command2\r\n"
    command_list = list(
        filter(
            lambda x: True if "$" not in x else False,
            command_list,
        )
    )
    command_list[0] = command_list[0].lower()
    return command_list


async def on_new_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    global is_replica, replicas

    try:
        print(f"Reader: {reader}")

        while True:
            data = await reader.read(4096)
            if not data:
                print(f"No data received from {reader}")
                break

            data = data.decode('utf-8')

            # Parsing the Data according to the Redis Protocol
            parsed_commands = parse_redis_protocol(data)

            if not parsed_commands:
                continue

            responses = handle_command(parsed_commands, writer)
            for response in responses:
                writer.write(response)

        await writer.drain()

    except Exception as err:
        print(f"Connection closed {err}")



def get_from_master(master_socket: socket.socket):
    global replica_offset
    while True:
        try:
            # print("next master ")
            data = master_socket.recv(4039)  # .decode("utf-8")
            if data:
                print(data)
                pattern = r'\*[\d]+\r\n(?:\$[\d]+\r\n[^\r\n]+\r\n)+'
                command_list = re.findall(pattern, data.decode('utf-8'))
                print(command_list)
                for command in command_list:
                    if command:
                        # if command.startswith(b'+FULLRESYNC') or command.startswith(b'*\r\n'):
                        #     continue
                        parsed_commands = parse_redis_protocol(command)
                        if parsed_commands[0] == 'replconf' and parsed_commands[1] == "GETACK":
                            master_socket.sendall(
                                f"*3\r\n${len('replconf')}\r\nREPLCONF\r\n${len('ack')}\r\nACK\r\n${len(str(replica_offset))}\r\n{replica_offset}\r\n".encode())
                        else:
                            handle_command(parsed_commands)

                        replica_offset = replica_offset + len(command)

        except Exception as err:
            print(f"Connection with master {master_socket}: {err}")


async def create_server(master_host, master_port, port_number):
    global role, is_replica, master_socket
    if role == "slave":
        try:
            slave_socket = socket.socket()
            slave_socket.connect((master_host, master_port))
            is_replica = True

            # Part 1 of the handshake
            slave_socket.sendall(b"*1\r\n$4\r\nPING\r\n")
            # Wait for response to PING
            data = slave_socket.recv(1024)
            if data != b"+PONG\r\n":
                print("Failed to receive response to PING")
                return

            # Part 2 of the handshake
            slave_socket.sendall(f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{port_number}\r\n".encode())
            slave_socket.sendall(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")

            # Wait for response to REPLCONF
            for _ in range(2):
                data = slave_socket.recv(1024)
                if data != b"+OK\r\n":
                    print("Failed to receive response to REPLCONF")
                    return

            # Third Part of the handshake
            # Sending PSYNC
            slave_socket.sendall(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
            for _ in range(2):
                data = slave_socket.recv(2048)

            # Put receiving the empty file from master to different thread
            Thread(target=get_from_master, args=(slave_socket,)).start()

        except Exception as e:
            print("Error in replication:", e)
    server = await asyncio.start_server(on_new_client, "localhost", port_number)
    async with server:
        await server.serve_forever()


def main():
    global role, master_host, master_port
    print("Logs from your program will appear here!")

    port_number = 6379
    args = parser.parse_args()
    if args.port:
        port_number = args.port

    role = "master"

    if args.replicaof:
        master_address = args.replicaof
        master_address = master_address[0].split(" ")
        master_host = master_address[0]
        master_port = int(master_address[1])
        role = "slave"

    print(f"Create redis server as {role} on port {port_number}")
    asyncio.run(create_server(master_host, master_port, port_number))


if __name__ == "__main__":
    main()
