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
import struct
import consul
import os
import uuid
import cryptography
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
import base64


GOSSIP_ANNOUNCE = 500
GOSSIP_NOTIFY = 501
GOSSIP_NOTIFICATION = 502
GOSSIP_VALIDATION = 503
from util import bad_packet, read_message, handle_client








class Peer:
    def __init__(self, name,degree=5,cacheSize=10, host='0.0.0.0', port=7000):
        self.name = name
        self.host = host
        self.port = port
        self.degree=degree
        self.cacheSize=cacheSize
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setblocking(False)
        self.private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )
        self.public_key = self.private_key.public_key()

        self.connections = {}  # Dictionary to store connected peers
        self.degree=degree
        self.rate_limit_window = 60  # 60 seconds time window
        self.message_limit = 10  # limit to 10 messages per window
        self.message_timestamps = deque()  # to store timestamps of sent messages
        self.dataTypes_to_modules = {}
        self.mids = []
        self.known_peers_publicKeys={}
        print(f"Peer {self.name} created at {socket.gethostbyname(self.host)}:{self.port}",flush=True)

        self.consul = consul.Consul(host='consul', port=8500)
        
        loop = asyncio.get_event_loop()
        # loop.create_task(self.register_with_consul())
        # loop.create_task(self.start_listening())
        # loop.create_task(self.discover_peers()) 
        asyncio.gather(self.register_with_consul(),self.listen_for_connections(),self.listen_for_modules(),self.discover_peers())
    
        # try:
        #     # Run the event loop
        #     asyncio.get_event_loop().run_forever()
        # except KeyboardInterrupt:
        #     print("Shutting down...")
        # finally:
        #     print("Cleanup if needed")

        # print('done')
        
        


        # Start listening for incoming connections
        # self.loop = asyncio.get_event_loop()
        # self.loop.create_task(self.register_with_consul())
        # self.loop.create_task(self.start_listening())
        # self.loop.create_task(self.discover_peers())
        
    async def  continue_running(self):  
        try:
            # Run the event loop
            asyncio.get_event_loop().run_forever()
        except KeyboardInterrupt:
            print("Shutting down...")
        finally:
            print("Cleanup if needed")
            

    def get_public_key_pem(self):
        """
        Returns the PEM-encoded representation of the peer's public key.
        """
        return self.public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo 


        )

    def get_public_key_base64(self):
        """
        Returns the Base64-encoded representation of the peer's public key.
        """
        pem_data = self.get_public_key_pem()
        return base64.b64encode(pem_data).decode('utf-8')   
    
    def load_public_key_from_base64(self, base64_key):
        """
        Loads a public key from its Base64-encoded representation.
        """
        pem_data = base64.b64decode(base64_key.encode('utf-8'))
        return serialization.load_pem_public_key(pem_data)
    
    
    def sign_message(self, message):
        """
        Signs a message using the peer's private key.
        """
        signature = self.private_key.sign(
            message.encode(),
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )
        return signature

    def verify_signature(self, message, signature, sender_public_key):
        """
        Verifies the signature of a message using the sender's public key.
        """
        try:
                    sender_public_key.verify(
                    signature,
                    message.encode(),  # The message must be encoded to bytes
                    padding.PSS(
                        mgf=padding.MGF1(hashes.SHA256()),
                        salt_length=padding.PSS.MAX_LENGTH
                    ),
                    hashes.SHA256()
                )
                    print("Signature verification successful",flush=True)
                    return True
        # If no exception was raised, the signature is valid
         
           
        except Exception as e:
        # If an exception is raised, the signature is invalid
            print(f"Signature verification failed: {e}")
            return False
    
    async def register_with_consul(self):
        """Register the peer with Consul."""
        try:
            print(self.public_key,flush=True)

            self.consul.agent.service.register(
                'peer',
                service_id=self.name,
                address=self.host,
                port=self.port,
               
                tags=['peer']
            )

        except Exception as e:
            print(f"Failed to register with Consul: {e}")
        print(f"{self.name} registered with Consul at {self.host}:{self.port}")
        
        
        

    async def discover_peers(self):
        """Discover other peers using Consul."""
        if await self.perform_pow():
            while True:
                index, services = self.consul.catalog.service('peer')
                for service in services:
                    peer_name = service['ServiceName']
                    peer_host = service['ServiceAddress']
                    peer_port = service['ServicePort']
                    print("hehehe",flush=True)
                    
                    
                    

                    
                    
                    

                    await self.connect(peer_name, peer_host, peer_port)
                    # try:
                    #  await self.send_message(message=1, ttl=2)
                    #  print(self.connections,flush=True)
                    # except Exception as e:
                    #     print(f"Failed to send message to {peer_name} at {peer_host}:{peer_port}: {e}")
                    
                    # await asyncio.sleep(0.5)
                    # print(f"Discovered peer for {self.name}: {peer_name} at {peer_host}:{peer_port}",flush=True)

                await asyncio.sleep(10)  # Poll every 10 seconds 
                print("20 seconds check ",flush=True)       
            
    async def start_listening(self):
       await asyncio.gather(self.listen_for_connections(), self.listen_for_modules())
    
        
    async def perform_pow(self):
        """Performs a simple Proof of Work challenge."""
        difficulty = 0  # Adjust the difficulty as needed
        challenge = os.urandom(16).hex()  # Generate a random challenge
        print(f"Performing Proof of Work with challenge: {challenge}")

        start_time = time.time()
        while True:
            nonce = random.randint(0, 2**32 - 1)
            attempt = hashlib.sha256(f"{challenge}{nonce}".encode()).hexdigest()
            if attempt.startswith('0' * difficulty):  # Check if the hash meets the difficulty
                elapsed_time = time.time() - start_time
                print(f"Solved challenge in {elapsed_time:.2f} seconds with nonce: {nonce}")
                return True  # Proof of Work successful

            await asyncio.sleep(0.01)  # Avoid blocking the event loop
   
        
  

    async def listen_for_modules(self):
            handler = lambda r, w: handle_client(r, w, self.handle_modules_message)
            server = await asyncio.start_server(handler, host=self.host,
                                                port=12345, family=socket.AF_INET, reuse_address=True, 
                                                reuse_port=True)
            print(f"[+] GOSSIP mockup listening for modules on {self.host}:12345")

            async with server:
                await server.serve_forever()
        
    
    
    
    async def listen_for_connections(self):
        self.socket.bind((self.host, self.port))
        print(f"{self.name} is listening on {self.host}:{self.port}",flush=True)

        while True:
            try:
                conn, addr = await asyncio.get_event_loop().run_in_executor(None, self.socket.recvfrom, 1024)
                print(f"Received data from {addr}:")  # Print received data for debugging
                asyncio.create_task(self.handle_message(conn,addr))
            except BlockingIOError:
                await asyncio.sleep(0.1)


  
    
    async def handle_message(self, conn,address):
        try:
             # Deserialize using your Message class
            data=conn.decode()
            
            sender_host, sender_port = address
            if data:
                    try:
                        
                        parts = data.split("|&",4)
                        print(parts,flush=True)
                        if len(parts) == 5:
                            mid, message, dtype, ttl, sig = parts
                             # Decode the signature from base64
                        else:
                            # Handle the case where the message is still malformed even after the adjustment
                            print("Error: Message is still malformed"
                        )
                        escaped_sig = base64.b64decode(sig.encode('utf-8'))
                        if mid!="100":
                            
                            sender_public_key=self.known_peers_publicKeys[sender_host]
                            if  not self.verify_signature(message,escaped_sig,sender_public_key):
                                
                                print("Invalid signature",flush=True)
                                return
                            
                        if mid=="100":
                            if sender_host not in self.known_peers_publicKeys:
                                self.known_peers_publicKeys[sender_host]=self.load_public_key_from_base64(message)
                                print(self.known_peers_publicKeys,flush=True)
                                print(f"public key received from {mid} is {message}")
                                await self.send_public_key(sender_host,sender_port)
                                
                        else:
                            
                                ttl = int(ttl)
                                if ttl > 0 and mid not in self.mids:
                                    self.mids.append(mid)
                                    ttl -= 1
                                    await self.send_message(message, mid,dtype, ttl)
                                    print("ego",flush=True)
                                    print(message,dtype,mid,flush=True)
                                    await self.send_to_modules(message,dtype,mid)
                                    print(f"{self.name} received message: {message} with TTL: {ttl}, {dtype}",flush=True)

                    except ValueError:
                        print(f"Received malformed message: {data}")
        except Exception as e:
                        print(f"Error handling client: {e}")           
            
    
    async def send_to_modules(self, data, dtype, mid):
       print(dtype,flush=True)
       print("in this")
       print(self.dataTypes_to_modules,flush=True)
       dtype=int(dtype)
       
       if dtype in self.dataTypes_to_modules:
        
        
            for conn in self.dataTypes_to_modules[dtype]:
                try:
                    reader, writer = conn
                    raddr, rport = writer.get_extra_info('socket').getpeername()

                    mid = random.randint(0, 65535)
                    msize = 8 + len(data)
                    msg = struct.pack(">HHHH",
                                    msize,
                                    GOSSIP_NOTIFICATION,
                                    mid,
                                    dtype)
                    msg += data
                    
                    print("to be sent to modukles",flush=True)
                    print(data,flush=True)

                    await writer.drain()
                    if not writer.is_closing():
                        writer.write(msg)
                        await writer.drain()
                        print(f"[+] {raddr}:{rport} <<< GOSSIP_NOTIFICATION("
                            + f"{mid}, {dtype}, {data})")
                    
                    
                except Exception as e:
                                print(f"Failed to send message to module: {e}")
         
       else:
           print(f"No subscribers for data type {dtype}")
                            
                            
    async def handle_gossip_announce(self,buf, reader, writer):
        raddr, rport = writer.get_extra_info('socket').getpeername()
        header = buf[:4]
        body = buf[4:]

        msize = struct.unpack(">HH", header)[0]

        if msize <= 8 or len(buf) != msize:
            await bad_packet(reader, writer,
                            reason='GOSSIP_ANNOUNCE with datasize = 0 received',
                            data=buf,
                            )
            return False

        ttl, mres, dtype = struct.unpack(">BBH", body[:4])
        mdata = body[4:].decode('utf-8')

        mid = self.generate_message_id()
        self.mids.append(mid)
        await  self.send_message(mdata, mid,dtype, ttl)
        
        


        print(f"[+] {raddr}:{rport} >>> GOSSIP_ANNOUNCE: ({dtype}:{mdata})")

        # await gossip_to_subscribers(dtype, mdata, (reader,writer))
        return True
    
    
    async def handle_gossip_notify(self,buf, reader, writer):
            raddr, rport = writer.get_extra_info('socket').getpeername()
            header = buf[:4]
            body = buf[4:]

            msize = struct.unpack(">HH", header)[0]

            if msize != 8 or msize != len(buf):
                await bad_packet(reader, writer,
                                reason="Invalid size for GOSSIP_NOTIFY",
                                data=buf,
                                )
                return False

            res, dtype = struct.unpack(">HH", body)
            await self.add_subscription((reader,writer), dtype)

            print(f"[+] {raddr}:{rport} >>> GOSSIP_NOTIFY registered: ({dtype})")
            return True
     
    async def add_subscription(self, conn, dtype):
        dtype=int(dtype)
        if dtype not in self.dataTypes_to_modules:
            self.dataTypes_to_modules[dtype] = []  # Initialize an empty list for new data types

        if conn not in self.dataTypes_to_modules[dtype]:
            self.dataTypes_to_modules[dtype].append(conn) 
    async def handle_gossip_validation(self,buf, reader, writer):
        raddr, rport = writer.get_extra_info('socket').getpeername()
        header = buf[:4]
        body = buf[4:]

        msize = struct.unpack(">HH", header)[0]

        if msize != 8 or msize != len(buf):
            await bad_packet(reader, writer,
                            reason="Invalid size for GOSSIP_VALIDATION",
                            data=buf,
                            )
            return False

        mid, res, valid = struct.unpack(">HBB", body)
        valid = True if valid > 0 else False

        print(f"[+] {raddr}:{rport} >>> GOSSIP_VALIDATION: ({mid}:{valid})")

        # if not await update_validation_state((reader,writer), mid, valid):
        #     await bad_packet(reader, writer,
        #                     reason="GOSSIP_VALIDATION for nonexisting subscription",
        #                     data=buf,
        #                     cleanup_func=clear_state)
        #     return False

        return True

    async def handle_modules_message(self,buf, reader, writer):
        ret = False
        header = buf[:4]
        body = buf[4:]
       

        mtype = struct.unpack(">HH", header)[1]
        print(" got sooooooooooooooooomething ",flush=True)
        print(mtype,flush=True)
        if mtype == GOSSIP_ANNOUNCE:
            ret = await self.handle_gossip_announce(buf, reader, writer)
        elif mtype == GOSSIP_NOTIFY:
            ret = await self.handle_gossip_notify(buf, reader, writer)
        elif mtype == GOSSIP_NOTIFICATION:
            await bad_packet(reader, writer,
                            f"Received illegal GOSSIP_NOTIFICATION from client.",
                            header)
        elif mtype == GOSSIP_VALIDATION:
            ret = await self.handle_gossip_validation(buf, reader, writer)
        else:
            await bad_packet(reader, writer,
                            f"Unknown message type {mtype} received",
                            header)
        return ret

    
           


    async def connect(self, peer_name, peer_host, peer_port):
        if peer_host != self.host:
            if (peer_host, peer_host, peer_port) not in [(name, p_host, p_port) for name, (p_host, p_port) in self.connections.items()]:
                try:
                    
                    self.connections[peer_host] = (peer_host, peer_port)
                    await self.send_public_key(peer_host,peer_port)
                    print(f"{[self.host]} connected to peer: {peer_host} at {socket.gethostbyname(peer_host)}:{peer_port}")

                except Exception as e:
                    print(f"Failed to connect to peer {peer_name} at {peer_host}:{peer_port}: {e}")

    async def send_message(self, message,mid,dtype, ttl):
        if not self.check_rate_limit():
            print(f"Rate limit exceeded. Message cannot be sent.")
            return
        
        sig=self.sign_message(message)
        escaped_sig = base64.b64encode(sig).decode('utf-8')

        if len( self.connections)>0:
            try:
                for peer_name, (recipient_host, recipient_port) in self.connections.items():
                    host =socket.gethostbyname(recipient_host)
                    
                    full_message = f"{mid}|&{message}|&{dtype}|&{ttl}|&{escaped_sig}"
                    self.socket.sendto(full_message.encode(), (recipient_host, recipient_port))

                    print(f"{self.name} sent message to {recipient_host}:{recipient_port}: {message} with TTL: {ttl}",flush=True)
                    self.message_timestamps.append(time.time())

            except Exception as e:
                print(f"Failed to send message to {recipient_port}: {e}")
        else:
            print(f"Peer {recipient_port} is not connected to {self.name}")
            
    async def send_public_key(self, reciepient_host,recipient_port):
        try:
            public_key = self.get_public_key_base64()
            mid = 100
            message=self.get_public_key_base64()
            dtype=999
            ttl=0
            sig=self.sign_message(message)
            escaped_sig = base64.b64encode(sig).decode('utf-8')
            
            print("okaaaay",flush=True)
            full_message = f"{mid}|&{message}|&{dtype}|&{ttl}|&{escaped_sig}"
            self.socket.sendto(full_message.encode(), (reciepient_host, recipient_port))
            await asyncio.sleep(0.5)
            print(f"{self.name} sent {self.load_public_key_from_base64(self.get_public_key_base64())} {reciepient_host}:{recipient_port}")
        except Exception as e:
            print(f"Failed to send public key to {reciepient_host}:{recipient_port}: {e}")
            
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
        
    def generate_message_id(self):
        return  uuid.uuid4()
        
    
    
    
async def main():
    # Read environment variables
    name = os.getenv('PEER_NAME', 'peer')
    host = os.getenv('HOST', '127.0.0.1')
    port = int(os.getenv('PORT', 5000))
    degree = int(os.getenv('DEGREE', 5))
    cacheSize = int(os.getenv('CACHE_SIZE', 10))

    peer = Peer(name=name, degree=5, cacheSize=2, host=host, port=port)
    await asyncio.Event().wait()
    

    # try:
    #     # Run the event loop
    #     asyncio.get_event_loop().run_forever()
    # except KeyboardInterrupt:
    #     print("Shutting down...")
    # finally:
    #     print("Cleanup if needed")

    

if __name__ == "__main__":
     asyncio.run(main())    