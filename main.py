from fastapi import FastAPI
import time

app = FastAPI()

@app.post('/flight_details')
async def root():
    # Simulate a delay of 2 seconds
    time.sleep(2)

    # Return a JSON object with flight details
    return {
        "flight_number": "123",
        "departure_city": "New York",
        "arrival_city": "Los Angeles",
        "departure_time": "2024-03-05 10:00:00",
        "arrival_time": "2024-03-05 13:00:00",
        "status": "on time",
        "gate": "A1"
    }