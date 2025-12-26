import paho.mqtt.client as mqtt
import psycopg2
import json
from datetime import datetime
import sys
import os

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'database': os.getenv('DB_NAME', 'energy'),  
    'user': os.getenv('DB_USER', 'postgres'),
    'password': os.getenv('DB_PASSWORD', 'Biko@1010')  
}

# MQTT configuration
MQTT_BROKER = 'localhost'
MQTT_PORT = 1883
MQTT_TOPIC = 'energy/meters/#'

# Global variables
conn = None
cur = None
message_count = 0
error_count = 0

def connect_database():
    """Connect to PostgreSQL database with error handling"""
    global conn, cur
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Test the connection
        cur.execute("SELECT version();")
        version = cur.fetchone()
        
        print("="*60)
        print(" Connected to PostgreSQL Database")
        print(f" Host: {DB_CONFIG['host']}")
        print(f" Database: {DB_CONFIG['database']}")
        print(f" PostgreSQL version: {version[0].split(',')[0]}")
        print("="*60)
        
        # Verify table exists
        cur.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = 'energy_readings'
        """)
        
        if cur.fetchone()[0] == 0:
            print("  WARNING: Table 'energy_readings' does not exist!")
            print("   Run the setup SQL first")
            return False
        else:
            # Get current row count
            cur.execute("SELECT COUNT(*) FROM energy_readings")
            existing_rows = cur.fetchone()[0]
            print(f" Existing rows in database: {existing_rows:,}")
            print("="*60)
        
        return True
        
    except psycopg2.OperationalError as e:
        print(f" Database connection failed: {e}")
        print("\n Troubleshooting:")
        print("   1. Check if TimescaleDB container is running: docker ps")
        print("   2. Check database name matches docker-compose.yml")
        print("   3. Verify password is correct")
        return False
    except Exception as e:
        print(f"❌ Unexpected database error: {e}")
        return False

def on_connect(client, userdata, flags, rc):
    """Callback when connected to MQTT broker"""
    if rc == 0:
        print("\n Connected to MQTT Broker")
        print(f" Broker: {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(MQTT_TOPIC)
        print(f"Subscribed to: {MQTT_TOPIC}")
        print("\n Waiting for messages...\n")
    else:
        print(f" MQTT Connection failed with code: {rc}")
        error_codes = {
            1: "Incorrect protocol version",
            2: "Invalid client identifier",
            3: "Server unavailable",
            4: "Bad username or password",
            5: "Not authorized"
        }
        print(f"   Reason: {error_codes.get(rc, 'Unknown error')}")

def on_disconnect(client, userdata, rc):
    """Callback when disconnected from MQTT broker"""
    if rc != 0:
        print(f"\n  Unexpected MQTT disconnection (code: {rc})")
        print("   Attempting to reconnect...")

def on_message(client, userdata, msg):
    """Callback when message received from MQTT"""
    global message_count, error_count
    
    try:
        # Parse JSON payload
        data = json.loads(msg.payload.decode())
        
        # Validate required fields
        required_fields = ['meter_id', 'timestamp', 'power', 'voltage', 
                          'current', 'frequency', 'energy']
        missing_fields = [f for f in required_fields if f not in data]
        
        if missing_fields:
            print(f"  Missing fields: {missing_fields}")
            error_count += 1
            return
        
        # Insert into database
        cur.execute("""
            INSERT INTO energy_readings 
            (meter_id, timestamp, power, voltage, current, frequency, energy)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            str(data['meter_id']),  
            data['timestamp'],
            float(data['power']),
            float(data['voltage']),
            float(data['current']),
            float(data['frequency']),
            float(data['energy'])
        ))
        
        conn.commit()
        message_count += 1
        
        # Print progress every 100 messages (not every message!)
        if message_count % 100 == 0:
            print(f" Stored: {message_count:,} readings "
                  f"(Latest: Meter {data['meter_id']} at {data['timestamp'][:19]})")
        
        # Print summary every 500 messages
        if message_count % 500 == 0:
            cur.execute("SELECT COUNT(*) FROM energy_readings")
            total_in_db = cur.fetchone()[0]
            print(f" Summary: {total_in_db:,} total rows in database | "
                  f"Errors: {error_count}")
        
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON from topic {msg.topic}: {e}")
        error_count += 1
        
    except KeyError as e:
        print(f"❌ Missing field in data: {e}")
        print(f"   Data received: {data}")
        error_count += 1
        
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        print(f"   Rolling back transaction...")
        conn.rollback()
        error_count += 1
        
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        print(f"   Topic: {msg.topic}")
        print(f"   Payload: {msg.payload[:100]}...")  # First 100 chars
        conn.rollback()
        error_count += 1

def main():
    """Main function to run the ingestor"""
    global conn, cur
    
    print("="*60)
    print(" MQTT TO POSTGRESQL INGESTOR")
    print("   Energy Grid Monitoring System")
    print("="*60)
    
    # Connect to database
    if not connect_database():
        sys.exit(1)
    
    # Setup MQTT client
    client = mqtt.Client(client_id="energy_ingestor")
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    
    try:
        # Connect to MQTT broker
        print(f"\n Connecting to MQTT Broker at {MQTT_BROKER}:{MQTT_PORT}...")
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        
        # Start the loop (blocking - runs forever)
        print(" Starting message loop...\n")
        client.loop_forever()
        
    except KeyboardInterrupt:
        print("\n\n" + "="*60)
        print(" SHUTDOWN INITIATED")
        print("="*60)
        
        # Get final statistics
        try:
            cur.execute("SELECT COUNT(*) FROM energy_readings")
            total_rows = cur.fetchone()[0]
            
            cur.execute("""
                SELECT MIN(timestamp), MAX(timestamp) 
                FROM energy_readings 
                WHERE timestamp >= NOW() - INTERVAL '1 hour'
            """)
            time_range = cur.fetchone()
            
            print(f" Final Statistics:")
            print(f"   Messages processed: {message_count:,}")
            print(f"   Errors encountered: {error_count}")
            print(f"   Total rows in database: {total_rows:,}")
            
            if time_range[0]:
                print(f"   Latest data range: {time_range[0]} to {time_range[1]}")
                
        except Exception as e:
            print(f"  Could not fetch final statistics: {e}")
        
        print("="*60)
        
    except ConnectionRefusedError:
        print(f"\n Cannot connect to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
        print("\n Troubleshooting:")
        print("   1. Check if EMQX is running: docker ps")
        print("   2. Verify port 1883 is not blocked")
        print("   3. Check docker-compose logs: docker-compose logs emqx")
        
    except Exception as e:
        print(f"\n Unexpected error: {e}")
        
    finally:
        # Cleanup
        print("\n Cleaning up...")
        
        if cur:
            cur.close()
            print("   Database cursor closed")
            
        if conn:
            conn.close()
            print("    Database connection closed")
            
        client.disconnect()
        print("    MQTT client disconnected")
        
        print("\n Ingestor stopped gracefully\n")

if __name__ == "__main__":
    main()