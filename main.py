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
    
    manager.send_message("ghobashy", message=1, ttl=2)
    manager.send_message("shaarawy", message=3, ttl=4)
    manager.send_message("3osa", message=5, ttl=6)
    time.sleep(5)
   
   
   
    
    
   
   



    
    
   
   
    

def main():
    manager = create_network()
    run_simulation(manager)

if __name__ == "__main__":
    main()
