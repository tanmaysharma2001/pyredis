import base64
import socket
import threading
import time

# Arguement Parser
import argparse
parser = argparse.ArgumentParser()
parser.add_argument("-p", "--port", help="Port Number")
parser.add_argument("--replicaof", nargs=2, help="Replicate to another redis instance")

# In-memory key-value store
key_value_store = {}
is_replica = False
master_replid = '8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb'
master_repl_offset = 0

# Empty RDB file content (base64 representation)
EMPTY_RDB_BASE64 = b"UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

# Parser function for Redis protocol
def parse_redis_protocol(data):
    lines = data.split(b'\r\n')
    command = []
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith(b'*'):
            # The number of arguments
            pass
        elif line.startswith(b'$'):
            # The length of the next argument
            length = int(line[1:])
            i += 1  # Move to the argument
            argument = lines[i][:length]  # Get the argument with the specified length
            command.append(argument.decode())  # Assuming UTF-8 encoding
        i += 1
    return command


def handle_set_command(parsed_command):
    key, value = parsed_command[1], parsed_command[2]
    expiry = None
    if "px" in parsed_command:
        px_index = parsed_command.index("px")
        if px_index + 1 < len(parsed_command):
            expiry = int(time.time() * 1000) + int(parsed_command[px_index + 1])  # Convert to milliseconds and add to current time
    key_value_store[key] = (value, expiry)


def handle_get_command(clientsocket, parsed_command):
    key = parsed_command[1]
    value, expiry = key_value_store.get(key, (None, None))
    if value is not None:
        # Check if the key has expired
        if expiry is not None and int(time.time() * 1000) > expiry:
            del key_value_store[key]  # Remove expired key
            clientsocket.sendall(b"$-1\r\n")
        else:
            response = f"${len(value)}\r\n{value}\r\n"
            clientsocket.sendall(response.encode())
    else:
        clientsocket.sendall(b"$-1\r\n")


def handle_info_command(clientsocket, parsed_command):
    global is_replica, master_replid, master_repl_offset
    if parsed_command[1] == 'replication':
        if is_replica:
            clientsocket.sendall(b"$10\r\nrole:slave\r\n")
        else:
            clientsocket.sendall(f"${2 + 11 + len(master_replid) + len('master_replid:') + len(str(master_repl_offset)) + len('master_repl_offset:')}\r\nrole:master,master_replid:{master_replid},master_repl_offset:{master_repl_offset}\r\n".encode())

def on_new_client(clientsocket, addr):
    print(f"Connected by {addr}")
    while True:
        data = clientsocket.recv(1024)       
        if not data:
            break

        # Parsing the Data according to the Redis Protocol
        parsed_command = parse_redis_protocol(data)

        if not parsed_command:
            continue

        # Handling the set command
        # Handling SET command with optional PX for expiry
        if parsed_command[0].lower() == 'set':
            handle_set_command(parsed_command)
            clientsocket.sendall(b"+OK\r\n")
        
        # Handling GET command
        elif parsed_command[0].lower() == 'get' and len(parsed_command) == 2:
            handle_get_command(clientsocket, parsed_command)
        
        # Handling the INFO command
        elif parsed_command[0].lower() == 'info' and len(parsed_command) == 2:
            handle_info_command(clientsocket, parsed_command)
        
        # Handling the echo command
        elif parsed_command[0] == 'ECHO' and len(parsed_command) > 1:
            response_message = f"+{parsed_command[1]}\r\n"
            clientsocket.sendall(response_message.encode())
        
        else:
            clientsocket.sendall(b"+PONG\r\n")
    clientsocket.close()


def replicate_to_master(master_host, master_port, portNumber):
    global is_replica
    while True:
        try:
            master_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            master_socket.connect((master_host, master_port))
            is_replica = True

            # Part 1 of the handshake
            master_socket.sendall(b"*1\r\n$4\r\nPING\r\n")
            # Wait for response to PING
            data = master_socket.recv(1024)
            if data != b"+PONG\r\n":
                print("Failed to receive response to PING")
                return

            # Part 2 of the handshake
            master_socket.sendall(b"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n")
            master_socket.sendall(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")

            # Wait for response to REPLCONF
            for _ in range(2):
                data = master_socket.recv(1024)
                if data != b"+OK\r\n":
                    print("Failed to receive response to REPLCONF")
                    return
            
            # Third Part of the handshake
            # Sending PSYNC
            master_socket.sendall(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
            
            # data = master_socket.recv(1024)
            # if data != b"+PONG\r\n":
            #     print("Failed to receive response to PING")
            #     return
            # Send empty RDB file
            rdb_content = base64.b64decode(EMPTY_RDB_BASE64)
            rdb_length = len(rdb_content)
            master_socket.sendall(b"$" + str(rdb_length).encode() + b"\r\n" + rdb_content)


        except Exception as e:
            print("Error in replication:", e)

def main():
    print("Logs from your program will appear here!")

    portNumber = 6379
    args = parser.parse_args()
    if args.port:
        portNumber = int(args.port)

    if args.replicaof:
        master_host, master_port = args.replicaof
        master_port = int(master_port)
        threading.Thread(target=replicate_to_master, args=(master_host, master_port, portNumber)).start()


    try:
        server_socket = socket.create_server(("localhost", portNumber), reuse_port=True)
        
        while True:
            conn, addr = server_socket.accept() # wait for client
            client_thread = threading.Thread(target=on_new_client, args=(conn, addr))
            client_thread.start()
    except:
        print("Except an error occured")
    
    server_socket.close()

if __name__ == "__main__":
    main()
