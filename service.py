import sys, os, re, socket
from mediverse_key_value_store.hashtable import HashTable
from threading import Thread


# Initialize Hash Table service
class HashTableService:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port
        self.ht = HashTable()
    
    # handle commands that write to the table
    def handle_commands(self, msg):
        # regex that receives setter and getter
        set_ht = re.match('^set ([a-zA-Z0-9]+) ([a-zA-Z0-9]+)$', msg)
        get_ht = re.match('^get ([a-zA-Z0-9]+)', msg)
        
        if set_ht:
            key, value = set_ht.groups()
            res = self.ht.set(key=key, value=value)
            output = "Inserted"
        elif get_ht:
            key = get_ht.groups()[0]
            output = self.ht.get(key=key)
            
            if output is None:
                output = 'Error: The specific key does not exist'
        else:
            output = 'Error: Invalid command!'
        
        return output
    
    def process_request(self, conn):
        while True:
            try:
                msg = conn.recv(2048).decode()
                print("f{msg} received")
                output = self.handle_commands(msg=msg)
                
                conn.send(output.encode())
            except Exception as e:
                print("Error processing message from client")
                print(e)
    