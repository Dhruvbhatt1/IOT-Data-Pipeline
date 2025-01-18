import time
import json
import random
from datetime import datetime
from azure.eventhub import EventHubProducerClient, EventData


EVENT_HUB_CONNECTION_STR = "Y"
EVENT_HUB_NAME = ""

def generate_device_data(device_id):
    """
    Simulates IoT device data generation.
    """
    temperature = round(random.uniform(-180.0, 135.0), 2)  
    humidity = round(random.uniform(1.0, 80.0), 2)     
    timestamp = datetime.utcnow().isoformat()           
    data = {
        "DeviceID": device_id,
        "Temperature": temperature,
        "Humidity": humidity,
        "Timestamp": timestamp,
    }
    return data

def send_to_event_hub(data):
    """
    Sends generated data to Azure Event Hub.
    """
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR,
        eventhub_name=EVENT_HUB_NAME
    )
    try:
        # Create a batch
        event_data_batch = producer.create_batch()
        event_data_batch.add(EventData(json.dumps(data)))

        # Send the batch of events
        producer.send_batch(event_data_batch)
        print(f"Data sent to Event Hub: {data}")
    except Exception as e:
        print(f"Error sending data to Event Hub: {e}")
    finally:
        producer.close()

if _name_ == "_main_":
    DEVICE_ID = "mySimulatedDevice"  # Unique Device ID

    # Infinite loop to simulate continuous signal generation
    try:
        while True:
            # Generate data
            device_data = generate_device_data(DEVICE_ID)

            # Send to Azure Event Hub
            send_to_event_hub(device_data)

            # Wait for 5 seconds before sending the next signal
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nSimulation stopped.")