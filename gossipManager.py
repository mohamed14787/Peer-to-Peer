import socket
import threading
import json
from peer import Peer
import hashlib
import random
import time
import asyncio
import argparse

class GossipManager:
    def __init__(self, bootstrap_host='127.0.0.1', bootstrap_port=2050,degree=3,cacheSize=5):
        self.bootstrap_host = bootstrap_host
        self.bootstrap_port = bootstrap_port
        self.peers = {}

        # Start the bootstrapping service connection

    async def connect_to_bootstrap_service(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.bootstrap_host, self.bootstrap_port))
                s.sendall("GET_PEERS".encode())
                data = s.recv(1024).decode()
                peers_info = json.loads(data)
                print(f"Initial peers obtained: {peers_info}")
                
                for peer_info in peers_info:
                     await self.add_peer(peer_info["name"], peer_info["host"], peer_info["port"])
                # for peer in self.peers:
                #     for peer2 in self.peers:
                #         if peer != peer2:
                #             await self.peers[peer].connect(self.peers[peer2].name, self.peers[peer2].host, self.peers[peer2].port),
                #             await self.peers[peer2].connect(self.peers[peer].name, self.peers[peer].host, self.peers[peer].port)
                # print(self.peers)
        except Exception as e:
            print(f"Failed to connect to bootstrapping service: {e}")

    async def add_peer(self, name, host, port):
        if name not in self.peers:
            try:
                challenge, difficulty = self.generate_pow_challenge(3)
                new_peer = Peer(name, host=host, port=port)
                await asyncio.sleep(1)
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
          
          
          
    async def main(self):
            print("Starting Gossip Manager...")
            await self.connect_to_bootstrap_service()
            while True:
                command = input("> ").split()  # Get command from user
                if command[0] == "add_peer" and len(command) == 4:
                    try:
                        name, host, port = command[1:]
                        port = int(port)  # Convert port to integer
                        await self.add_peer(name, host, port)
                    except ValueError:
                        print("Invalid port number. Please enter an integer.")
                elif command[0] == "send_message" and len(command) >= 4:
                    try:
                        sender_name, message = command[1:3]
                        ttl = int(command[3]) if len(command) > 3 else 1  # Default TTL to 1
                        await self.send_message(sender_name, message, ttl)
                    except ValueError:
                        print("Invalid TTL. Please enter an integer.")
                elif command[0] == "exit":
                    break
                else:
                    print("Invalid command. Use 'add_peer <name> <host> <port>', 'send_message <sender> <recipient> <message> [ttl]', or 'exit'.")
                await asyncio.sleep(1)  # Adjust as needed
                
                
               
if __name__ == "__main__":
    usage_string = ("Run a GOSSIP module mockup with local info exchange.\n\n"
                    + "Multiple API clients can connect to this same instance.")

    cmd = argparse.ArgumentParser(description=usage_string)
    cmd.add_argument("-a", "--address", 
 default="127.0.0.1",  
                     help="Bind server to this address")
    cmd.add_argument("-p", "--port", type=int, default=2050, 
                     help="Bind server to this port")
    args = cmd.parse_args()
    
    

    gossip_manager = GossipManager(bootstrap_host=args.address, bootstrap_port=args.port)
    asyncio.run(gossip_manager.main()) 