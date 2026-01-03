import csv
import random
import math
from datetime import datetime, timedelta
import sys

def generate_realistic_power(hour, day_of_week, meter_variation=0):
    
    base = 2.0 + meter_variation
    
    # Weekend effect
    if day_of_week >= 5:
        base *= 1.15
    
    # Morning peak (7-9 AM)
    morning = 3.0 * math.exp(-((hour - 8)**2) / 2)
    
    # Evening peak (6-9 PM)
    evening = 4.0 * math.exp(-((hour - 19)**2) / 2)
    
    # Night reduction
    if 23 <= hour or hour <= 5:
        base *= 0.4
    
    noise = random.uniform(-0.5, 0.5)
    power = max(0.1, base + morning + evening + noise)
    
    return round(power, 3)

print("="*60)
print(" HISTORICAL DATA GENERATOR")
print("="*60)

# Configuration
NUM_METERS = 500
DAYS = 14
INTERVAL_MINUTES = 5

# Calculate totals
readings_per_hour = 60 // INTERVAL_MINUTES
readings_per_day = readings_per_hour * 24
total_readings = NUM_METERS * readings_per_day * DAYS

print(f"\n  Configuration:")
print(f"   Meters: {NUM_METERS}")
print(f"   Duration: {DAYS} days")
print(f"   Interval: {INTERVAL_MINUTES} minutes")
print(f"   Expected rows: {total_readings:,}")

# Generate meter IDs
meter_ids = [str(1000000000 + i) for i in range(NUM_METERS)]
meter_variations = {m_id: random.uniform(0, 1.0) for m_id in meter_ids}

# Date range
start_date = datetime.now() - timedelta(days=DAYS)
end_date = datetime.now()

print(f"\n Date Range:")
print(f"   From: {start_date.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"   To:   {end_date.strftime('%Y-%m-%d %H:%M:%S')}")

# Generate CSV
filename = 'historical_data.csv'
print(f"\n  Writing to: {filename}")
print("-"*60)

with open(filename, 'w', newline='') as f:
    writer = csv.writer(f)
    
    current_time = start_date
    count = 0
    last_percent = 0
    
    while current_time < end_date:
        hour = current_time.hour
        day_of_week = current_time.weekday()
        
        for meter_id in meter_ids:
            power = generate_realistic_power(hour, day_of_week, meter_variations[meter_id])
            voltage = round(random.uniform(220, 240), 2)
            current = round((power * 1000) / voltage, 3)
            frequency = round(random.uniform(49.8, 50.2), 2)
            energy = round(power / 12, 4)
            
            writer.writerow([
                meter_id,
                current_time.isoformat(),
                power,
                voltage,
                current,
                frequency,
                energy
            ])
            
            count += 1
        
        # Progress
        percent = int((count / total_readings) * 100)
        if percent >= last_percent + 5:
            print(f"  Progress: {percent:3d}% ({count:,} / {total_readings:,} rows)")
            last_percent = percent
        
        current_time += timedelta(minutes=INTERVAL_MINUTES)

print("-"*60)
print(f"\n Generation complete!")
print(f" Total rows: {count:,}")
print(f" File: {filename}")
print(f" Approx size: ~{round(count * 0.0001, 1)} MB")

