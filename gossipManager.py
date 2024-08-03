import socket
import threading
import json
from peer import Peer
import hashlib
import random
import time
import asyncio

class GossipManager:
    def __init__(self, bootstrap_host='127.0.0.1', bootstrap_port=2050,degree=3,cacheSize=5):
        self.bootstrap_host = bootstrap_host
        self.bootstrap_port = bootstrap_port
        self.peers = {}

        # Start the bootstrapping service connection
        self.connect_to_bootstrap_service()

    def connect_to_bootstrap_service(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.bootstrap_host, self.bootstrap_port))
                s.sendall("GET_PEERS".encode())
                data = s.recv(1024).decode()
                peers_info = json.loads(data)
                print(f"Initial peers obtained: {peers_info}")
                
                for peer_info in peers_info:
                     self.add_peer(peer_info["name"], peer_info["host"], peer_info["port"])
                for peer in self.peers:
                    for peer2 in self.peers:
                        if peer != peer2:
                            self.peers[peer].connect(self.peers[peer2].name, self.peers[peer2].host, self.peers[peer2].port),
                            self.peers[peer2].connect(self.peers[peer].name, self.peers[peer].host, self.peers[peer].port)
                print(self.peers)
        except Exception as e:
            print(f"Failed to connect to bootstrapping service: {e}")

    def add_peer(self, name, host, port):
        if name not in self.peers:
            try:
                challenge, difficulty = self.generate_pow_challenge(3)
                new_peer = Peer(name, host=host, port=port)
                x=self.solve_pow_challenge(challenge, difficulty)
                if self.verify_pow_solution(x, challenge, difficulty)== True:
                    
                  self.peers[name] = new_peer
            
                
                  print(f"Added new peer: {name} at {host}:{port}")
            except Exception as e:
                print(f"Failed to add peer {name}: {e}")
                
                
                
    def generate_pow_challenge(self,difficulty=4):
        nonce = random.getrandbits(32)
        timestamp = int(time.time())
        challenge = f"{nonce}:{timestamp}"
        return challenge, difficulty
   
    
    def verify_pow_solution(self,solution, challenge, difficulty):
        hash = hashlib.sha256(solution.encode()).hexdigest()
        prefix = '0' * difficulty
        return hash.startswith(prefix)
    
    def solve_pow_challenge(self,challenge, difficulty):
        counter = 0
        prefix = '0' * difficulty
        while True:
            attempt = f"{challenge}:{counter}"
            hash = hashlib.sha256(attempt.encode()).hexdigest()
            if hash.startswith(prefix):
                return attempt
            counter += 1
    async def send_message(self, sender_name, message, ttl):
        try:
            sender=self.peers[sender_name]
            
       # if any(sender_name == peer.name for peer in self.peers.values())  and any(recipient_name == peer.name for peer in self.peers.values):
            await sender.send_message(message, ttl)
       # else:
        except Exception as e:
          print(f"One or both peers not found: {sender_name} , {e}")

