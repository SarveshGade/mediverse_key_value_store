from fastapi import FastAPI, HTTPException
from hashtable import HashTable
from consistent_hashing import ConsistentHashing
import sys, os, re, socket, select
import json

app = FastAPI()
ht = HashTable()
chash = ConsistentHashing()


NODES = [
    ("127.0.0.1", 5001),
    ("127.0.0.1", 5002),
    ("127.0.0.1", 5003),
]

for node in NODES:
    chash.add_node_hash(f"{node[0]}:{node[1]}")
    
    
def send_request(node, command):
    """send a command to a designated node"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        s.connect(node)
        s.sendall(command.encode())
        
        response = s.recv(2048).decode()
        s.close()
        return response
    except Exception as e:
        print(e)
    
@app.get("/patient/{patient_id}")
def get_patient_data(patient_id: str):
    """Retrieve patient data"""
    node_id = chash.get_next_node(patient_id)
    if not node_id:
        raise HTTPException(status_code=500, detail="No available nodes")
    ip, port = node_id.split(":")
    response = send_request((ip, int(port)), f"get patient:{patient_id}")
    if not response:
        raise HTTPException(status_code=500, detail="No such node or patient")
    data = json.dumps(response)
    return {"patient_id": patient_id, "data": data}


@app.post("/patient/{patient_id}")
def add_patient_data(patient_id: str, data: dict):
    """Add or update patient data"""
    node_id = chash.get_next_node(patient_id)
    if not node_id:
        raise HTTPException(status_code=500, detail="No available nodes")
    ip, port = node_id.split(":")
    for key, value in data.items():
        command = f"set patient:{patient_id}:{key} {value} 1"
        response = send_request((ip, int(port)), command)
        if not response or response != "ok":
            raise HTTPException(status_code=500, detail="Failed write to node")
        
    return {"message":"Patient data updated"}


@app.delete("/patient/{patient_id}")
def delete_patient_data(patient_id: str, key):
    """Delete a specific field for a patient."""
    node_id = chash.get_next_node(patient_id)
    if not node_id:
        raise HTTPException(status_code=500, detail="No available nodes")
    ip, port = node_id.split(":")
    command = f"delete patient:{patient_id}:{key} 1"
    response = send_request((ip, int(port)), command)
    if not response or response != "ok":
            raise HTTPException(status_code=500, detail="Failed delete")
    return {"message": f"{key} deleted"}