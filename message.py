 
 

class Message:
    def __init__(self, content):
        self.content = content
        self.ttl = 4  # List to keep track of connected peers

    def decreaseTtl(self):
        self.ttl -= 1

    def _message(self, message, recipient):
        if recipient in self.connections:
            print(f"{self.name} sends message to {recipient.name}: {message}")
            recipient.receive_message(self.name, message)
        else:
            print(f"{recipient.name} is not connected to {self.name}")

    def receive_message(self, sender, message):
        print(f"{self.name} received message from {sender}: {message}")
        
        
