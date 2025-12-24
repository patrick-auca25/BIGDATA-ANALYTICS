import paho.mqtt.client as mqtt
import psycopg2
import json
from datetime import datetime

# Database connection
conn = psycopg2.connect(
    host="localhost",
    database="energy",
    user="postgres",
    password="Biko@1010"
)
cur = conn.cursor()

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload)
        cur.execute("""
            INSERT INTO energy_readings 
            (meter_id, timestamp, power, voltage, current, frequency, energy)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            data['meter_id'],
            data['timestamp'],
            data['power'],
            data['voltage'],
            data['current'],
            data['frequency'],
            data['energy']
        ))
        conn.commit()
        print(f"Stored: {data['meter_id']} at {data['timestamp']}")
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()

client = mqtt.Client()
client.on_message = on_message
client.connect("localhost", 1883, 60)
client.subscribe("energy/meters/#")
client.loop_forever()