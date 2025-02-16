import requests

# Define the base URL (use the correct port)
BASE_URL = "http://127.0.0.1:5001"  # or whichever port your API is running on

try:
    # Add patient data
    response = requests.post(f"{BASE_URL}/patient/123", json={
        "name": "John Doe",
        "age": "40",
        "blood_type": "O+"
    })
    print("POST response:", response.json())

    # Get patient data
    response = requests.get(f"{BASE_URL}/patient/123")
    print("GET response:", response.json())

    # Delete a specific field
    response = requests.delete(f"{BASE_URL}/patient/123", params={"key": "age"})
    print("DELETE response:", response.json())

except requests.exceptions.ConnectionError:
    print("Connection Error: Make sure the API server is running and the port is correct")
except requests.exceptions.RequestException as e:
    print(f"Request failed: {e}")
except Exception as e:
    print(f"An error occurred: {e}")