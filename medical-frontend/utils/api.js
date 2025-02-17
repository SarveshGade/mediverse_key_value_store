import axios from "axios";

const API_BASE_URL = "http://127.0.0.1:8000"; // FastAPI server

export const getPatient = async (patientId) => {
  try {
    const response = await axios.get(`${API_BASE_URL}/patient/${patientId}`);
    return response.data;
  } catch (error) {
    return null;
  }
};

export const addPatientData = async (patientId, data) => {
  try {
    await axios.post(`${API_BASE_URL}/patient/${patientId}`, data);
    return { success: true };
  } catch (error) {
    return { success: false };
  }
};

export const deletePatientField = async (patientId, field) => {
  try {
    await axios.delete(`${API_BASE_URL}/patient/${patientId}/${field}`);
    return { success: true };
  } catch (error) {
    return { success: false };
  }
};
