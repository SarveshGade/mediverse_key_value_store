import { useState } from "react";
import { getPatient, addPatientData, deletePatientField } from "../utils/api";

export default function Home() {
  const [patientId, setPatientId] = useState("");
  const [patientData, setPatientData] = useState(null);
  const [newData, setNewData] = useState({ key: "", value: "" });

  const fetchPatient = async () => {
    const data = await getPatient(patientId);
    setPatientData(data ? data.data : null);
  };

  const updatePatient = async () => {
    if (!newData.key || !newData.value) return;
    await addPatientData(patientId, { [newData.key]: newData.value });
    setNewData({ key: "", value: "" });
    fetchPatient();
  };

  const deleteField = async (key) => {
    await deletePatientField(patientId, key);
    fetchPatient();
  };

  return (
    <div style={{ maxWidth: "600px", margin: "50px auto", textAlign: "center" }}>
      <h1>Medical Records</h1>

      <input
        type="text"
        placeholder="Enter Patient ID"
        value={patientId}
        onChange={(e) => setPatientId(e.target.value)}
      />
      <button onClick={fetchPatient}>Search</button>

      {patientData && (
        <>
          <h2>Patient Data</h2>
          <table border="1" style={{ width: "100%", marginTop: "20px" }}>
            <thead>
              <tr>
                <th>Key</th>
                <th>Value</th>
                <th>Action</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(patientData).map(([key, value]) => (
                <tr key={key}>
                  <td>{key}</td>
                  <td>{value}</td>
                  <td>
                    <button onClick={() => deleteField(key)}>Delete</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}

      <h2>Add/Update Data</h2>
      <input
        type="text"
        placeholder="Field"
        value={newData.key}
        onChange={(e) => setNewData({ ...newData, key: e.target.value })}
      />
      <input
        type="text"
        placeholder="Value"
        value={newData.value}
        onChange={(e) => setNewData({ ...newData, value: e.target.value })}
      />
      <button onClick={updatePatient}>Submit</button>
    </div>
  );
}
