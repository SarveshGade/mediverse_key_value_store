import sys, os, re, socket, select
from hashtable import HashTable
from threading import Thread, Lock
import time
import mmh3
from random import randint, random, shuffle
from queue import Queue

def run_thread(fn, args):
    my_thread = Thread(target=fn, args=args)
    my_thread.daemon = True
    my_thread.start()
    return my_thread
    
# this is FULLY SYNCHRONOUS REPLICATION-- all servers are immediately updated, prioritizing consistency    


def send_and_receive(msg, servers, socket_locks, i, res=None, timeout=-1):
    """
    res -- a Queue
    servers -- a list of servers, 2nd index is socket
    """
    resp = None
    # Could not connect is bc of the following:
    # 1. Server is not ready
    # 2. Server is busy
    # 3. Server crashed
    while True:
        try:
            if servers[i][2] is None:
                # server has not been connected yet, if server not ready it throws error
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                ip, port, _ = servers[i]
                sock.connect((str(ip), int(port)))
                servers[i][2] = sock
            with socket_locks[i]:
                conn = servers[i][2]
                # if server crashes, this throws error
                conn.send(msg.encode())
                if timeout > 0:
                    ready = select.select([conn], [], [], timeout)
                    if ready[0]:
                        resp = conn.recv(2048).decode()
                else:
                    resp = conn.recv(2048).decode()
                break
        except Exception as e:
            print(e)
            # if server ceashes but isn't marked
            if servers[i][2] is not None:
                sock = servers[i][2]
                sock.close()
                servers[i][2] = None
            time.sleep(0.5)
    if res is not None:
        res.put(resp)
    return resp

def broadcast_write(msg, cluster, socket_locks):
    # Add the outputs from sending to replicas in a multithreaded queue
    res = Queue()
    n = len(cluster)
    
    if n == 1:
        # No replica found
        return True
    
    for i in range(n):
        if cluster[i] is not None:
            # send messages to replicas in parallel threads
            run_thread(send_and_receive, args=(msg, cluster, socket_locks, i, res))
    cnts = 0
    
    while True:
        try:
           # wait for all replicas to respond
            out = res.get(block=True)
            if out and out == 'ok':
               cnts += 1
               # exclude the leader because the leader is already updated
               if cnts == n - 1:
                   return True
            else:
                return False 
        except Exception as e:
            print(e)
            return False
        
def broadcast_join(msg, conns, lock, socket_locks):
    # Add the outputs from sending to replicas in a multithreaded queue
    res = Queue()
    
    with lock:
        n = len(conns)
        
    for i in range(n):
        # send message to all leaders in parallel threads
        run_thread(send_and_receive(msg, conns[i], socket_locks[i], 0, res))
    cnts = 0
    
    while True:
        try:
            out = res.get(block=True)
            if out and out == 'ok':
                cnts += 1
                # All leaders received the join request
                if cnts == n:
                    True
            else:
                return False
        except Exception as e:
            print(e)
            return False
    

# Initialize Hash Table service
class HashTableService:
    def __init__(self, ip, port, partitions):
        self.ip = ip
        self.port = port
        self.ht = HashTable()
        self.partitions = eval(partitions)
        self.conns = [[None]*len(self.partitions[i]) for i in range(len(self.partitions))]
        self.is_leader = False
        self.cluster_index = -1
        self.cluster_lock = Lock()
        self.socket_locks = [[Lock() for j in range(len(self.partitions[i]))] for i in range(len(self.partitions))]
        
        for i in range(len(self.partitions)):
            cluster = self.partitions[i]
            for j in range(len(cluster)):
                
                ip, port = cluster[j].split(':')
                port = int(port)
            
                if (ip, port) == (self.ip, self.port):
                    self.cluster_index = i
                    if j == 0:
                        self.is_leader = True
                else:
                    # 3rd element is the socket object
                    self.conns[i][j] = [ip, port, None]
        run_thread(fn=self.join_replica, args=())
                    
        print("Ready...")

    def join_replica(self):
        # Replic asks leaders to add itself
        if self.is_leader is False:
            # send message to all leaders beacuse during 'get' some leader other than own leader
            msg = f"join {self.ip} {self.port} {self.cluster_index}"
            resp = broadcast_join(msg, self.conns, self.cluster_lock, self.socket_locks)
    
            assert resp == True
    
    # handle commands that write to the table
    def handle_commands(self, msg, conn):
        # regex that receives setter and getter
        set_ht = re.match('^set ([a-zA-Z0-9]+) ([a-zA-Z0-9]+) ([0-9]+)$', msg)
        get_ht = re.match('^get ([a-zA-Z0-9]+) ([0-9]+)$', msg)
        replica_join = re.match('^join ([0-9\.]+) ([0-9]+) ([0-9]+)$', msg)
        
        if replica_join:
            # Add new replica if not already applied
            ip, port, index = replica_join.groups()
            ip_str = f"{ip}:{port}"
            index = int(index)
            
            with self.cluster_lock:
                # Add new replica if it is leader and not already added
                if self.is_leader and index < len(self.partitions) and ip_str not in self.partitions:
                    port = int(port)
                    self.partitions[index].append(ip_str)
                    self.conns[index].append([ip, port, None])
                    self.socket_locks[index].append(Lock())
                    output = "ok"
                    
                else:
                    if index >= len(self.partitions):
                        output = "ko"
                    else:
                        output = "ok"
        else:
            output = "Error: Invalid command"
        
        if set_ht:
            n = len(self.partitions)
            key, value, req_id = set_ht.groups()
            req_id = int(req_id)
            node = mmh3.hash(key, signed=False) % n
            if self.cluster_index == node:
                # the key is intended for the current cluster
                if self.is_leader:
                    # replicate if this is the leader server and req_id is the latest one corresponding to key
                    replicated = broadcast_write(msg, self.conns[node], self.socket_locks[node])
                    if replicated:
                        ret = self.ht.set(key=key, value=value, req_id=req_id)
                        output = "ok"
                    else:
                        output= "ko"
                else:
                    ret = self.ht.set(key=key, value=value, req_id=req_id)
                    output = "ok" 
            else:
                # forward to relevant cluster if key is not intended for this cluster
                output = send_and_receive(msg, self.conns[node], self.socket_locks[node], 0)
                if output is None:
                    output = "ko"
        elif get_ht:
            n = len(self.partitions)
            key, _= get_ht.groups()
            node = mmh3.hash(key, signed=False) % n
            
            if self.cluster_index == node:
                # The key is intended for the current cluster
                output = str(self.ht.get(key=key))
            else:
                # Forward the get request to a random replica in the currect cluster
                indices = list(range(len(self.partitions[node])))
                shuffle(indices)
                
                # Loop over multiple indices because some replicas may be unresponsive and thus time out
                for j in indices:
                    output = send_and_receive(msg, self.conns[node], self.socket_locks[node], j, timeout=10)
                    if output:
                        break
            if output is None:
                output = 'Error: Non existent key'
        else:
            output = 'Error: Invalid command!'
        
        return output
    

    
    def process_request(self, conn):
        while True:
            try:
                msg = conn.recv(2048).decode()
                print(f"{msg} received")
                output = self.handle_commands(msg=msg, conn=conn)
                
                conn.send(output.encode())
            except Exception as e:
                print("Error processing message from client")
                print(e)
                conn.close()
                break    
                
    def listen_to_clients(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(('0.0.0.0', int(self.port)))
        sock.listen(50)
        
        while True:
            try:
                client_socket, client_address = sock.accept()
                
                print(f"Connected to new client address {client_address}")
                my_thread = Thread(target=self.process_request, args=(client_socket,))
                my_thread.daemon = True
                my_thread.start()
            except:
                print("Error accepting connection")
                
                
if __name__ == '__main__':
    ip_address = str(sys.argv[1])
    port = int(sys.argv[2])
    partitions = str(sys.argv[3])
    
    dht = HashTableService(ip=ip_address, port=port, partitions=partitions)
    dht.listen_to_clients()
    

