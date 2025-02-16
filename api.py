from fastapi import FastAPI, HTTPException
from hashtable import HashTable

app = FastAPI()
ht = HashTable()

@app.get("/patient/{patient_id}")
def get_patient_data(patient_id: str):
    """Retrieve patient data"""
    keys = [key for key in ht.get_copy().keys() if key.startswith(f"patient:{patient_id}")]
    if not keys:
        raise HTTPException(status_code=404, detail="Patient not found.")
    data = {key.split(":")[-1]: ht.get(key)[0] for key in keys}
    return {"patient_id": patient_id, "data": data}
@app.post("/patient/{patient_id}")
def add_patient_data(patient_id: str, data: dict):
    """Add or update patient data"""
    req_id = 1
    for key, value in data.items():
        ht.set(f"patient:{patient_id}:{key}", value, req_id)
        req_id += 1
    return {"message":"Patient data updated"}
@app.delete("/patient/{patient_id}")
def delete_patient_data(patient_id: str, key):
    """Delete a specific field for a patient."""
    req_id = 1
    success = ht.delete(f"patient:{patient_id}:{key}", req_id)
    if success == -1:
        raise HTTPException(status_code=404, detail="Key not found")
    return {"message": f"{key} deleted"}