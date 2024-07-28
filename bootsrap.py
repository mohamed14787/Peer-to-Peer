import socket
import threading
import json

# Sample list of peers (could be dynamically updated)
PEERS = [
    {"name": "ghobashy ", "host": "127.0.0.1", "port": 1223},
    {"name": "shaarawy", "host": "127.0.0.1", "port": 3145},
    {"name": "3osa", "host": "127.0.0.1", "port": 9321}
]

def handle_client(conn):
    try:
        data = conn.recv(1024).decode()
        if data == "GET_PEERS":
            response = json.dumps(PEERS)
            conn.sendall(response.encode())
    except Exception as e:
        print(f"Error handling client: {e}")
    finally:
        conn.close()

def start_bootstrap_service(host='127.0.0.1', port=2050):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"Bootstrapping service started on {host}:{port}")

    while True:
        conn, addr = server_socket.accept()
        threading.Thread(target=handle_client, args=(conn,), daemon=True).start()

if __name__ == "__main__":
    start_bootstrap_service()
