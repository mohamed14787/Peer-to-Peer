from gossipManager import GossipManager
import time

def create_network():
    # Initialize PeerManager
    manager = GossipManager()

    # Wait for peers to be connected
    time.sleep(5)

    return manager

def run_simulation(manager):
    # Example messages between peers
    
    manager.send_message("3osa", message=1, ttl=2)
    time.sleep(1)
    manager.send_message("shaarawy", message=2, ttl=2)
    time.sleep(1)
    manager.send_message("ghobashy", message=3, ttl=2)
    time.sleep(1)
    manager.send_message("3osa", message=4, ttl=2)
    time.sleep(1)
    
   
   
    

def main():
    manager = create_network()
    run_simulation(manager)

if __name__ == "__main__":
    main()
