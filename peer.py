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
import nanoid
import struct
import consul
import os


GOSSIP_ANNOUNCE = 500
GOSSIP_NOTIFY = 501
GOSSIP_NOTIFICATION = 502
GOSSIP_VALIDATION = 503
from util import bad_packet, read_message, handle_client








class Peer:
    def __init__(self, name,degree=5,cacheSize=10, host='0.0.0.0', port=5000):
        self.name = name
        self.host = host
        self.port = port
        self.degree=degree
        self.cacheSize=cacheSize
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setblocking(False)

        self.connections = {}  # Dictionary to store connected peers
        self.degree=degree
        self.rate_limit_window = 60  # 60 seconds time window
        self.message_limit = 10  # limit to 10 messages per window
        self.message_timestamps = deque()  # to store timestamps of sent messages
        self.dataTypes_to_modules = {}
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
            

        
    async def register_with_consul(self):
        """Register the peer with Consul."""
        print("lolo")
        try:

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
        while True:
            index, services = self.consul.catalog.service('peer')
            for service in services:
                print(f"Service: {service}",flush=True)
                peer_name = service['ServiceName']
                peer_host = service['ServiceAddress']
                peer_port = service['ServicePort']
                

                await self.connect(peer_name, peer_host, peer_port)
                print("connected",flush=True)
                try:
                 await self.send_message(message=1, ttl=2)
                except Exception as e:
                    print(f"Failed to send message to {peer_name} at {peer_host}:{peer_port}: {e}")
                
                # await asyncio.sleep(0.5)
                print("sent",flush=True)
                # print(f"Discovered peer for {self.name}: {peer_name} at {peer_host}:{peer_port}",flush=True)

            await asyncio.sleep(20)  # Poll every 10 seconds        
            
    async def start_listening(self):
       await asyncio.gather(self.listen_for_connections(), self.listen_for_modules())
    
        
        
        
  

    async def listen_for_modules(self):
            handler = lambda r, w: handle_client(r, w, self.handle_modules_message)
            server = await asyncio.start_server(handler, host=self.host,
                                                port=12345, family=socket.AF_INET, reuse_address=True, 
                                                reuse_port=True)
            print(f"[+] GOSSIP mockup listening for modules on {self.host}:12345")

            async with server:
                await server.serve_forever()
        
    
    # def listen_for_connections(self):
    #     try:
    #         self.socket.bind((self.host, self.port))
    #         print(f"{self.name} is listening on {self.host}:{self.port}")

    #         while True:
    #             conn, addr = self.socket.recvfrom(1024)
    #             threading.Thread(target=self.handle_client, args=(conn,), daemon=True).start()
                
            
    #     except Exception as e:
    #         print(f"Error in listen_for_connections: {e}")
    
    async def listen_for_connections(self):
        self.socket.bind((self.host, self.port))
        print(f"{self.name} is listening on {self.host}:{self.port}",flush=True)

        while True:
            try:
                conn, addr = await asyncio.get_event_loop().run_in_executor(None, self.socket.recvfrom, 1024)
                print(f"Received data from {addr}:")  # Print received data for debugging
                asyncio.create_task(self.handle_message(conn,))
            except BlockingIOError:
                await asyncio.sleep(0.1)


    # def handle_client(self, conn):
      
    #         try:
    #             data = conn.decode()
    #             if data:
    #                 try:
    #                     message, ttl = data.split("|")
    #                     print(f"{self.name} received message: {message} with TTL: {ttl}")
    #                 except ValueError:
    #                     print(f"Received malformed message: {data}")
    #         except Exception as e:
    #             print(f"Error handling client: {e}")
    
    async def handle_message(self, conn):
        try:
             # Deserialize using your Message class
            data=conn.decode()
            if data:
                    try:
                        
                        message, ttl = data.split("|")
                        print(f"{self.name} received message: {message} with TTL: {ttl}",flush=True)
                    except ValueError:
                        print(f"Received malformed message: {data}")
        except Exception as e:
                        print(f"Error handling client: {e}")           
            

    async def handle_gossip_announce(buf, reader, writer):
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
        mdata = body[4:]
        


        print(f"[+] {raddr}:{rport} >>> GOSSIP_ANNOUNCE: ({dtype}:{mdata})")

        # await gossip_to_subscribers(dtype, mdata, (reader,writer))
        return True
    
    
    async def handle_gossip_notify(buf, reader, writer):
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
            # await add_subscription((reader,writer), dtype)

            print(f"[+] {raddr}:{rport} >>> GOSSIP_NOTIFY registered: ({dtype})")
            return True
        
    async def handle_gossip_validation(buf, reader, writer):
        raddr, rport = writer.get_extra_info('socket').getpeername()
        header = buf[:4]
        body = buf[4:]

        msize = struct.unpack(">HH", header)[0]

        if msize != 8 or msize != len(buf):
            await bad_packet(reader, writer,
                            reason="Invalid size for GOSSIP_VALIDATION",
                            data=buf,
                            cleanup_func=clear_state)
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
        if (peer_name, peer_host, peer_port) not in [(name, p_host, p_port) for name, (p_host, p_port) in self.connections.items()]:
            try:
                
                self.connections[peer_name] = (peer_host, peer_port)
                print(f"{self.name} connected to peer: {peer_name} at {peer_host}:{peer_port}")

            except Exception as e:
                print(f"Failed to connect to peer {peer_name} at {peer_host}:{peer_port}: {e}")

    async def send_message(self, message, ttl):
        print("sending message",flush=True)
        if not self.check_rate_limit():
            print(f"Rate limit exceeded. Message cannot be sent.")
            return
        if len( self.connections)>0:
            try:
                for peer_name, (recipient_host, recipient_port) in self.connections.items():
                    host =socket.gethostbyname(recipient_host)
                    print(f"host:{host}")
                    full_message = f"{message}|{ttl}"
                    self.socket.sendto(full_message.encode(), (host, recipient_port))

                    print(f"{self.name} sent message to {recipient_host}:{recipient_port}: {message} with TTL: {ttl}",flush=True)
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
        
    def generate_mid(message):
        return nanoid.generate(size=10,alphabet=message)
    
    
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