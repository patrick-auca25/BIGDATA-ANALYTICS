import paho.mqtt.client as mqtt
import json
import time
import random
from datetime import datetime
import math

# --- Configuration ---
# These settings match with our docker-compose.yml
BROKER = "localhost"
PORT = 1883  # The MQTT protocol port from  YAML
TOTAL_METERS = 500 
TOPIC_PREFIX = "energy/meters"

# Connect to the EMQX Broker
client = mqtt.Client()
try:
    client.connect(BROKER, PORT)
    client.loop_start()  # FIX: Start network loop
    time.sleep(1)  # Give it a moment to connect
    print(f" Connected to EMQX Broker at {BROKER}:{PORT}")
except Exception as e:
    print(f" Failed to connect to Broker: {e}")
    exit()

def generate_realistic_power(hour):
    """
    Simulates daily energy peaks: 
    - Morning (around 8 AM)
    - Evening (around 7 PM)
    """
    base_load = 2.0  # FIX: More realistic base (was 0.5)
    
    # Math to create peaks using Gaussian distribution
    morning_peak = 2.0 * math.exp(-((hour - 8)**2)/2)
    evening_peak = 3.0 * math.exp(-((hour - 19)**2)/4)
    noise = random.uniform(-0.3, 0.3)  # Can go slightly negative
    
    # Ensure power is never negative
    power = max(0.1, base_load + morning_peak + evening_peak + noise)
    
    return round(power, 3)

# FIX: Generate 500 unique 10-digit IDs as STRINGS
meter_ids = [str(1000000000 + i) for i in range(TOTAL_METERS)]

print(f" Starting simulation for {TOTAL_METERS} meters...")
print(" Reporting interval: 5 minutes")
print(" Press Ctrl+C to stop")
print("-" * 60)

try:
    cycle_count = 0
    while True:
        current_hour = datetime.now().hour
        start_time = time.time()
        
        for m_id in meter_ids:
            power = generate_realistic_power(current_hour)
            
            # FIX: Calculate voltage and current consistently
            voltage = round(random.uniform(220, 240), 2)
            current = round((power * 1000) / voltage, 3)
            
            # Frequency should be stable around 50 Hz
            frequency = round(random.uniform(49.8, 50.2), 2)
            
            # FIX: Calculate energy from power (NOT random!)
            # Energy (kWh) = Power (kW) × Time (hours)
            # 5 minutes = 5/60 hours = 1/12 hours
            energy = round(power / 12, 4)
            
          
            payload = {
                "meter_id": m_id,  
                "timestamp": datetime.now().isoformat(),
                "power": power,
                "voltage": voltage,
                "current": current,
                "frequency": frequency,
                "energy": energy  
            }
            
            # Publish to topic: energy/meters/{meter_id}
            topic = f"{TOPIC_PREFIX}/{m_id}"
            result = client.publish(topic, json.dumps(payload))
            
            # Check if publish succeeded
            if result.rc != 0:
                print(f" Failed to publish for meter {m_id}")
        
        cycle_count += 1
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"Cycle {cycle_count}: Published {TOTAL_METERS} readings at {timestamp}")
        
        # Requirement: Report every 5 minutes (300 seconds)
        # We subtract the time taken to publish to keep the interval accurate
        elapsed = time.time() - start_time
        sleep_time = max(0, 300 - elapsed)
        
        if sleep_time > 0:
            print(f" Waiting {sleep_time:.1f} seconds until next cycle...")
        else:
            print(f" Warning: Publishing took {elapsed:.1f}s (longer than 5 min interval)")
        
        time.sleep(sleep_time)
        
except KeyboardInterrupt:
    print("\n" + "="*60)
    print(f" Simulation stopped by user")
    print(f" Total cycles completed: {cycle_count}")
    print(f" Total messages published: {cycle_count * TOTAL_METERS}")
    print("="*60)
    client.loop_stop()
    client.disconnect()
    print("Disconnected gracefully")

except Exception as e:
    print(f"\n Unexpected error: {e}")
    client.loop_stop()
    client.disconnect()