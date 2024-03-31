import socket
import threading
import time

# In-memory key-value store
key_value_store = {}

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
        
        # Handling the echo command
        elif parsed_command[0] == 'echo' and len(parsed_command) > 1:
            response_message = f"+{parsed_command[1]}\r\n"
            clientsocket.sendall(response_message.encode())
        
        else:
            clientsocket.sendall(b"+PONG\r\n")
    clientsocket.close()

def main():
    print("Logs from your program will appear here!")

    try:
        server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
        
        while True:
            conn, addr = server_socket.accept() # wait for client
            client_thread = threading.Thread(target=on_new_client, args=(conn, addr))
            client_thread.start()
    except:
        print("Except an error occured")
    
    server_socket.close()

if __name__ == "__main__":
    main()
