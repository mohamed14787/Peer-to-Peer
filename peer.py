import uuid
import socket
import threading
from message import Message
import json
import hashlib
import time
import random
from collections import deque
import asyncio




class Peer:
    def __init__(self, name,degree=5,cacheSize=10, host='127.0.0.1', port=5000):
        self.name = name
        self.host = host
        self.port = port
        self.degree=degree
        self.cacheSize=cacheSize
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.connections = {}  # Dictionary to store connected peers
        self.degree=degree
        self.rate_limit_window = 60  # 60 seconds time window
        self.message_limit = 10  # limit to 10 messages per window
        self.message_timestamps = deque()  # to store timestamps of sent messages


        # Start listening for incoming connections
        threading.Thread(target=self.listen_for_connections, daemon=True).start()
        
        
  

    def listen_for_connections(self):
        try:
            self.socket.bind((self.host, self.port))
            print(f"{self.name} is listening on {self.host}:{self.port}")

            while True:
                conn, addr = self.socket.recvfrom(1024)
                threading.Thread(target=self.handle_client, args=(conn,), daemon=True).start()
        except Exception as e:
            print(f"Error in listen_for_connections: {e}")

    def handle_client(self, conn):
      
            try:
                data = conn.decode()
                if data:
                    try:
                        message, ttl = data.split("|")
                        print(f"{self.name} received message: {message} with TTL: {ttl}")
                    except ValueError:
                        print(f"Received malformed message: {data}")
            except Exception as e:
                print(f"Error handling client: {e}")
            

    # def receive_messages(self, peer_name):
    #     while True:
    #         try:
    #             data, addr = self.socket.recvfrom(1024)
    #             if data:
    #                 try:
    #                     message, ttl = data.decode().split("|")
    #                     print(f"{self.name} received message from {peer_name}: {message} with TTL: {ttl}")

    #                 except ValueError:
    #                     print(f"Received malformed message from {peer_name}: {data}")
    #         except Exception as e:
    #             print(f" {self.name}Error receiving message from {peer_name}: {e}")
    #             break
           

    def connect(self, peer_name, peer_host, peer_port):
        if (peer_name, peer_host, peer_port) not in [(name, p_host, p_port) for name, (p_host, p_port) in self.connections.items()]:
            try:
                
                self.connections[peer_name] = (peer_host, peer_port)
                print(f"{self.name} connected to peer: {peer_name} at {peer_host}:{peer_port}")

            except Exception as e:
                print(f"Failed to connect to peer {peer_name} at {peer_host}:{peer_port}: {e}")

    def send_message(self, message, ttl):
        if not self.check_rate_limit():
            print(f"Rate limit exceeded. Message to {recipient_host}:{recipient_port} not sent.")
            return
        if len( self.connections)>0:
            try:
                for peer_name, (recipient_host, recipient_port) in self.connections.items():
                    full_message = f"{message}|{ttl}"
                    self.socket.sendto(full_message.encode(), (recipient_host, recipient_port))
                    print(f"{self.name} sent message to {recipient_host}:{recipient_port}: {message} with TTL: {ttl}")
                    self.message_timestamps.append(time.time())

            except Exception as e:
                print(f"Failed to send message to {recipient_port}: {e}")
        else:
            print(f"Peer {recipient_port} is not connected to {self.name}")
            
    def check_rate_limit(self):
        current_time = time.time()

        # Remove timestamps older than the rate limit window
        while self.message_timestamps and self.message_timestamps[0] < current_time - self.rate_limit_window:
            self.message_timestamps.popleft()

        if len(self.message_timestamps) < self.message_limit:
            return True
        else:
            print(f"Rate limit exceeded. Message not sent.")
            return False


# class Peer:
#     def __init__(self, name, host='127.0.0.1', port=3000):
#         self.name = name
#         self.id = str(uuid.uuid4())  # Unique identifier for the peer
#         self.host = host
#         self.port = port
#         self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#         self.connections = {}  # Dictionary to keep track of connected peers
        
#         self.bootstrap_host = '127.0.0.1'
#         self.bootstrap_port = 2050
#         self.connect_to_bootstrap_service()
#         # Start listening for incoming connections
#         threading.Thread(target=self.listen_for_connections, daemon=True).start()
        
#     def connect_to_bootstrap_service(self):
#         try:
#             with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#                 s.connect((self.bootstrap_host, self.bootstrap_port))
#                 s.sendall("GET_PEERS".encode())
#                 data = s.recv(1024).decode()
#                 peers = json.loads(data)
#                 print(f"Initial peers obtained: {peers}")
#                 for peer_info in peers:
#                     self.connect_to_peer(peer_info["host"], peer_info["port"])
#         except Exception as e:
#             print(f"Failed to connect to bootstrapping service: {e}")

#     # def connect_to_peer(self, host, port):
#     #     if (host, port) not in [(p.host, p.port) for p in self.connections.values()]:
#     #         try:
#     #             peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     #             peer_socket.connect((host, port))
#     #             peer_name = f"{host}:{port}"  # Use a unique identifier for the peer
#     #             self.connections[peer_name] = peer_socket
#     #             print(f"{self.name} connected to peer at {host}:{port}")
#     #             conn, addr = self.socket.accept()


#     #             threading.Thread(target=self.receive_messages, args=(conn), daemon=True).start()
#     #         except Exception as e:
#     #             print(f"Failed to connect to peer at {host}:{port}: {e}")
#     def connect_to_peer(self, host, port):
#        if (host, port) not in [(p.host, p.port) for p in self.connections.values()]: 
         
#             try:
#                 name = f"{host}:{port}" 
#                 peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#                 peer_socket.connect((host, port))
#                 self.connections[name] = peer_socket
#                 print(f"{self.name} connected to {name}")
#             except Exception as e:
#                 print(f"Failed to connect to {name}: {e}")
                
#     def connect(self, peer):
        
#          if peer not in self.connections:
#             try:
#                 peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#                 peer_socket.connect((peer.host, peer.port))
#                 self.connections[peer.name] = peer_socket
#                 print(f"{self.name} connected to {peer.name}")
#             except Exception as e:
#                 print(f"Failed to connect to {peer.name}: {e}")
                
#     def listen_for_connections(self):
#         try:
#             self.socket.bind((self.host, self.port))
#             self.socket.listen(5)
#             print(f"{self.name} is listening on {self.host}:{self.port}")

#             while True:
#                 conn, addr = self.socket.accept()
#                 threading.Thread(target=self.receive_messages, args=(conn,), daemon=True).start()
#         except Exception as e:
#             print(f"Error in listen_for_connections: {e}")

#     def receive_messages(self, conn):
#         while True:
#             try:
#                 data = conn.recv(1024).decode()
#                 if data:
#                     message, ttl = data.split("|")
#                     print(f"{self.name} received message from : {message} with TTL:{ttl} ")
#             except Exception as e:
#                 print(f"Error receiving message from : {e}")
#                 break
#     def send_message(self, message, recipient_name,ttl):
#         print(recipient_name)
#         if recipient_name in self.connections:
#             try:
#                full_message = f"{message}|{ttl}"
#                self.connections[recipient_name].sendall(full_message.encode())
#             except Exception as e:
#                 print(f"Failed to send message to {recipient_name}: {e}")
#         else:
#             print(f"{recipient_name} is not connected to {self.name}")
        
        
