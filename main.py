import asyncio
from gossipManager import GossipManager

async def create_network():
    manager = GossipManager()
    await asyncio.sleep(3)  # Wait for connections
    return manager

async def run_simulation(manager):
    # Simulate message sending between peers
    await manager.send_message("ghobashy", message=1, ttl=2)
    
    await asyncio.sleep(1)  # Adjust as needed

    await manager.send_message("shaarawy", message=3, ttl=4)
    await asyncio.sleep(1)  # Adjust as needed

    await manager.send_message("3osa", message=5, ttl=6)

    # Give some time for message propagation
    await asyncio.sleep(1)  # Adjust as needed

async def main():
    manager = await create_network()
    await run_simulation(manager)

if __name__ == "__main__":
    asyncio.run(main())
