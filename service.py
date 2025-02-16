import sys, os, re, socket, select
from hashtable import HashTable
from threading import Thread, Lock
import time
import mmh3
from random import randint, random, shuffle
from queue import Queue
from pathlib import Path
from commit_log import CommitLog
from consistent_hashing import ConsistentHashing

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
            # if server crashes but isn't marked
            if servers[i][2] is not None:
                sock = servers[i][2]
                sock.close()
                
                servers[i][2] = None

            time.sleep(0.5)
    if res is not None:
        res.put(resp)
    return resp

def broadcast_write(msg, cluster, lock, socket_locks):
    # Add the outputs from sending to replicas in a multithreaded queue
    res = Queue()
    with lock:
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
        
def broadcast_join(msg, conns, lock, socket_locks, exclude=None):
    # Add the outputs from sending to replicas in a multithreaded queue
    res = Queue()
    
    with lock:
        n = len(conns)
        
    for i in range(n):
        # send message to all leaders in parallel threads
        run_thread(send_and_receive, args=(msg, conns[i], socket_locks[i], 0, res))
    cnts = 0
    
    while True:
        try:
            out = res.get(block=True)
            if out and out == 'ok':
                cnts += 1
                # All leaders received the join request
                if (exclude is None and cnts == n) or (exclude is not None and cnts == n-1):
                    return True
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
        self.commit_log = CommitLog(file=f"commit-log-{self.ip}-{self.port}.txt")
        self.chash = ConsistentHashing()
        self.socket_locks = [[Lock() for j in range(len(self.partitions[i]))] for i in range(len(self.partitions))]
        self.commit_temp = {}
        self.commit_temp_lock = Lock()

        
        commit_logfile = Path(self.commit_log.file)
        commit_logfile.touch(exist_ok=True)
        
        
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
        self.consistent_hash_join()
        run_thread(fn=self.join_replica, args=())
        run_thread(fn=self.join_cluster, args=())
                    
        print("Ready...")
    
    def consistent_hash_join(self):
        # Add leader nodes to consistent hashing
        for i in range(len(self.partitions)):
            added = self.chash.add_node_hash(str(i))
            assert added == 1

    def join_replica(self):
        # Replica asks leaders to add itself
        if self.is_leader is False:
            # send message to all leaders because during 'get' some leader other than own leader
            # might need to forward request to this replica
            msg = f"join {self.ip} {self.port} {self.cluster_index}"
            resp = broadcast_join(msg, self.conns, self.cluster_lock, self.socket_locks)
    
            assert resp == True
            
            self.commit_log.truncate()
            
            # Get commitlog from own leader and update own ht
            # Wait for own leader to be ready
            while True:
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    sock.connect((str(self.conns[self.cluster_index][0][0]),
                                  int(self.conns[self.cluster_index][0][1])))
                    self.commit_log.write_log_from_sock(sock)
                    sock.close()
                    break
                except Exception as e:
                    print(e)
                    time.sleep(0.5)
            # Get commands in memory from log file and insert into own ht
            commands = self.commit_log.read_log()
            for cmd in commands:
                parts = cmd.split(" ")
                if len(parts) == 4:
                    # set operation
                    op, key, value, req_id = parts
                else:
                    # delete operations
                    op, key, req_id = parts
                req_id = int(req_id)
                if op == 'set':
                    self.ht.set(key=key, value=value, req_id=req_id)
                else:
                    self.ht.delete(key=key, req_id=req_id)

                    
    def join_cluster(self):
        # Leader asks other leaders to add itself
        if self.is_leader:
            # Send message to all leaders other than itself
            msg = f"join {self.ip} {self.port} {self.cluster_index}"
            resp = broadcast_join(msg, self.conns, self.cluster_lock, self.socket_locks, self.cluster_index)
            assert resp == True
            
            # Get all next nodes in consistent hash ring. Some keys that were mapped to these nodes
            # will now be mapped to this new leader node.
            nodes = self.chash.get_next_nodes_from_node(str(self.cluster_index))
            
            for next_node in nodes:
                next_node = int(next_node)
                
                self.commit_log_temp = CommitLog(f"commit-log-temp-{self.ip}-{self.port}.txt")
                
                commit_logfile = Path(self.commit_log_temp.file)
                commit_logfile.touch(exist_ok=True)
                
                # Get commitlog from leader and update own ht
                while True:
                    try:
                        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                        sock.connect((str(self.conns[next_node][0][0]), 
                                    int(self.conns[next_node][0][1])))
                        
                        self.commit_log_temp.write_log_from_sock(sock)
                        sock.close()
                        break
                    except Exception as e:
                        print(e)
                        time.sleep(0.5)
                
                # Update commit log with only those keys for which are before current node
                # in the consistent hash ring and not all keys.
                # Create temp log file for these.
                commands = self.commit_log_temp.read_log()
                
                for cmd in commands:
                    parts = cmd.split(" ")
                    if len(parts) == 4:
                        # set operation
                        op, key, value, req_id = parts
                    else:
                        # delete operations
                        op, key, req_id = parts
                        
                    nxt = int(self.chash.get_next_node(key))
                    
                    if nxt == self.cluster_index:
                        req_id = int(req_id)
                        if op == 'set':
                            ret = self.ht.set(key=key, value=value, req_id=req_id)
                        else:
                            ret = self.ht.delete(key=key, req_id=req_id)
                        
                        if ret == 1:
                            self.commit_log.log(cmd)
                        
                        # send delete message for moved keys
                        msg = f"del-no-fwd {key} {req_id}"
                        resp = send_and_receive(msg, self.conns[next_node], self.socket_locks[next_node], 0)
                        assert resp == "ok"
                
                os.remove(self.commit_log_temp.file)            

    # handle commands that write to the table
    def handle_commands(self, msg, conn):
        # regex that receives setter and getter
        set_ht = re.match('^set ([a-zA-Z0-9:]+) ([a-zA-Z0-9]+) ([0-9]+)$', msg)
        get_ht = re.match('^get ([a-zA-Z0-9:]+) ([0-9]+)$', msg)
        del_ht = re.match('^del ([a-zA-Z0-9:]+) ([0-9]+)$', msg)
        del_ht_no_fwd = re.match('^del-no-fwd ([a-zA-Z0-9]+) ([0-9]+)$', msg)
        replica_join = re.match('^join ([0-9\.]+) ([0-9]+) ([0-9]+)$', msg)
        committxn = re.match('^committxn ([a-zA-Z\-]+) ([a-zA-Z0-9]+) ([0-9]+)$', msg)
        
        log = re.match('^commitlog$', msg)
        
        if set_ht:
            output = "ko"
            
            try:
                key, value, req_id = set_ht.groups()
                req_id = int(req_id)
                node = int(self.chash.get_next_node(key))
                
                if self.cluster_index == node:
                    # The key is intended for current cluster
                    
                    # Prevent reading key while it is being updated and replicated
                    with self.commit_temp_lock:
                        if key not in self.commit_temp:
                            self.commit_temp[key] = {}
                        
                        # Same key might come in for multiple req_id's
                        self.commit_temp[key][req_id] = value
                    
                    if self.is_leader:
                        # Replicate if this is leader server and req_id is the latest one corresponding to key
                        replicated = broadcast_write(msg, self.conns[node], self.cluster_lock, self.socket_locks[node])
                        
                        if replicated:
                            commited = broadcast_write(f"committxn set {key} {req_id}", self.conns[node], self.cluster_lock, self.socket_locks[node])
                            
                            if commited:
                                ret = self.ht.set(key=key, value=value, req_id=req_id)
                                if ret == 1:
                                    self.commit_log.log(msg)
                                
                                # req_id commit is completed
                                self.commit_temp[key].pop(req_id)
                                output = "ok"
                    else:
                        output = "ok"
                else:
                    # Forward to relevant cluster if key is not intended for this cluster
                    output = send_and_receive(msg, self.conns[node], self.socket_locks[node], 0)
                    if output is None:
                        output = "ko"
                        
            except Exception as e:
                print(e)
   
        elif get_ht:
            output = "ko"
            
            try:
                key, _ = get_ht.groups()
                node = int(self.chash.get_next_node(key))
                
                if self.cluster_index == node:
                    # The key is intended for current cluster
                    
                    while True:
                        # Key is being updated and replicated
                        if key not in self.commit_temp or len(self.commit_temp[key]) == 0:
                            break
                        
                        # retry if update is not yet commited
                        time.sleep(0.5)
                        
                    output = self.ht.get(key=key)
                    if output:
                        output = str(output)
                        
                else:
                    # Forward the get request to a random node in the correct cluster
                    with self.cluster_lock:
                        indices = list(range(len(self.partitions[node])))
                    
                    shuffle(indices)
                    # Loop over multiple indices because some replica might be unresponsive and times out
                    for j in indices:
                        output = send_and_receive(msg, self.conns[node], self.socket_locks[node], j, timeout=10)
                        if output:
                            break
                    
                if output is None:
                    output = 'Error: Non existent key'
                    
            except Exception as e:
                print(e)
        
        elif del_ht:
            output = "ko"
            
            try:
                key, req_id = del_ht.groups()
                req_id = int(req_id)
                node = int(self.chash.get_next_node(key))
                
                if self.cluster_index == node:
                    # The key is intended for current cluster
                    
                    # Prevent reading key while it is being updated and replicated
                    with self.commit_temp_lock:
                        if key not in self.commit_temp:
                            self.commit_temp[key] = {}
                        
                        # Same key might come in for multiple req_id's
                        self.commit_temp[key][req_id] = None
                    
                    if self.is_leader:
                        # Replicate if this is leader server and req_id is the latest one corresponding to key
                        replicated = broadcast_write(msg, self.conns[node], self.cluster_lock, self.socket_locks[node])
                        
                        if replicated:
                            commited = broadcast_write(f"committxn del {key} {req_id}", self.conns[node], self.cluster_lock, self.socket_locks[node])
                            if commited:
                                ret = self.ht.delete(key=key, req_id=req_id)
                                if ret == 1:
                                    self.commit_log.log(msg)
                                
                                # req_id commit is completed
                                self.commit_temp[key].pop(req_id)
                                output = "ok"
                    else:
                        output = "ok"
                else:
                    # Forward to relevant cluster if key is not intended for this cluster
                    output = send_and_receive(msg, self.conns[node], self.socket_locks[node], 0)
                    if output is None:
                        output = "ko"
                        
            except Exception as e:
                print(e)
        
        elif del_ht_no_fwd:
            output = "ko"
            
            try:
                key, req_id = del_ht_no_fwd.groups()
                req_id = int(req_id)
                
                # Prevent reading key while it is being updated and replicated
                with self.commit_temp_lock:
                    if key not in self.commit_temp:
                        self.commit_temp[key] = {}
                    
                    # Same key might come in for multiple req_id's
                    self.commit_temp[key][req_id] = None
                
                # The key is intended for current cluster
                if self.is_leader:
                    # Replicate if this is leader server and req_id is the latest one corresponding to key
                    replicated = broadcast_write(msg, self.conns[self.cluster_index], self.cluster_lock, self.socket_locks[self.cluster_index])
                    
                    if replicated:
                        commited = broadcast_write(f"committxn del-no-fwd {key} {req_id}", self.conns[self.cluster_index], self.cluster_lock, self.socket_locks[self.cluster_index])
                        if commited:
                            ret = self.ht.delete(key=key, req_id=req_id)
                            if ret == 1:
                                self.commit_log.log(msg)
                            
                            # req_id commit is completed
                            self.commit_temp[key].pop(req_id)
                            output = "ok"
                else:
                    output = "ok"
                    
            except Exception as e:
                print(e)
        
        elif replica_join:
            # Add new replica if not already added
            output = "ko"
            
            try:
                ip, port, index = replica_join.groups()
                ip_str = f"{ip}:{port}"
                index  = int(index)
                
                with self.cluster_lock:
                    # Add new cluster
                    if index >= len(self.partitions):
                        port = int(port)
                        self.partitions.append([ip_str])
                        self.conns.append([[ip, port, None]])
                        self.socket_locks.append([Lock()])
                        self.chash.add_node_hash(str(index))
                        output = "ok"
                    
                    # Add new replica if it is leader and not already added
                    elif self.is_leader and ip_str not in self.partitions[index]:
                        port = int(port)
                        self.partitions[index].append(ip_str)
                        self.conns[index].append([ip, port, None])
                        self.socket_locks[index].append(Lock())
                        output = "ok"
                            
                    else:
                        output = "ok"
                        
            except Exception as e:
                print(e)
        
        elif log:
            output = "ko"
            
            try:
                # Send commit log file
                self.commit_log.send_log_to_sock(conn)
                output = ""
                conn.close()
            except Exception as e:
                print(e)
            
        elif committxn:
            output = "ko"
            
            try:
                op, key, req_id = committxn.groups()
                req_id = int(req_id)
                
                val = self.commit_temp[key][req_id]
                
                if op == "set":
                    ret = self.ht.set(key=key, value=val, req_id=req_id)
                    if ret == 1:
                        self.commit_log.log(f"set {key} {val} {req_id}")
                        
                elif op == "del":
                    ret = self.ht.delete(key=key, req_id=req_id)
                    if ret == 1:
                        self.commit_log.log(f"del {key} {req_id}")
                
                elif op == "del-no-fwd":
                    ret = self.ht.delete(key=key, req_id=req_id)
                    if ret == 1:
                        self.commit_log.log(f"del-no-fwd {key} {req_id}")
                else:
                    raise Exception("Invalid operator")
                
                self.commit_temp[key].pop(req_id)
                output = "ok"
                        
            except Exception as e:
                print(e)
                
        else:
            output = "Error: Invalid command"
        
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
    

