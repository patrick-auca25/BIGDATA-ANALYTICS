import paho.mqtt.client as mqtt
import json
import time
import random
from datetime import datetime
import math

# --- Configuration ---
BROKER = "localhost"
PORT = 1883  # Matches your YAML
TOTAL_METERS = 500 
TOPIC_PREFIX = "energy/meters"

# Connect to the EMQX Broker
client = mqtt.Client()
try:
    client.connect(BROKER, PORT)
    print(f"Connected to Broker at {BROKER}:{PORT}")
except Exception as e:
    print(f"Failed to connect: {e}")
    exit()

def generate_realistic_power(hour):
    """Simulates daily peaks: Morning (8 AM) and Evening (7 PM)"""
    # Base + Morning Peak + Evening Peak + Random noise
    base = 0.5
    morning = 2.0 * math.exp(-((hour - 8)**2)/2)
    evening = 3.0 * math.exp(-((hour - 19)**2)/4)
    return round(base + morning + evening + random.uniform(0, 0.3), 2)

# Generate 500 unique 10-digit IDs
meter_ids = [1000000000 + i for i in range(TOTAL_METERS)]

print(f"Starting simulation for {TOTAL_METERS} meters...")

while True:
    current_hour = datetime.now().hour
    
    for m_id in meter_ids:
        power = generate_realistic_power(current_hour)
        
        # Data structure matching your SQL schema
        payload = {
            "meter_id": m_id,
            "timestamp": datetime.now().isoformat(),
            "power": power,
            "voltage": round(random.uniform(220, 240), 1),
            "current": round((power * 1000) / 230, 2),
            "frequency": 50.0,
            "energy": round(random.uniform(10.0, 100.0), 4)
        }
        
        # Publish to the specific topic required by the spec
        topic = f"{TOPIC_PREFIX}/{m_id}"
        client.publish(topic, json.dumps(payload))
    
    print(f"Successfully published {TOTAL_METERS} readings at {datetime.now().strftime('%H:%M:%S')}")
    
    # Wait 5 minutes (300 seconds) for the next reporting interval
    time.sleep(300)