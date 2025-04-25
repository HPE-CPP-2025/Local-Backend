import sys
import json
from datetime import datetime
import random
import psycopg2
import time
from threading import Event
import os
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DB_URL")

def connect_to_db():
    try:
        conn = psycopg2.connect(DB_URL)
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}", file=sys.stderr)
        return None

def generate_energy_reading(device_id):
    # Generate values similar to those in the image
    base_voltage = 238.5
    base_current = 0.45
    base_power = 109.5
    
    return {
        "device_id": device_id,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S+00"),
        "voltage": round(base_voltage + random.uniform(-0.2, 0.2), 2),
        "current": round(base_current + random.uniform(-0.01, 0.01), 2),
        "power": round(base_power + random.uniform(-0.1, 0.1), 2),
        "energy": 0.005,  # Constant as shown in image
        "frequency": 50.0,  # Constant as shown in image
        "power_factor": 1.00,  # Constant as shown in image
    }

def insert_energy_readings(conn, readings):
    try:
        cursor = conn.cursor()
        for reading in readings:
            cursor.execute("""
                INSERT INTO energy_readings (device_id, timestamp, voltage, current, power, energy, frequency, power_factor)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                reading["device_id"],
                reading["timestamp"],
                reading["voltage"],
                reading["current"],
                reading["power"],
                reading["energy"],
                reading["frequency"],
                reading["power_factor"]
            ))
        conn.commit()
        print(f"Energy readings inserted successfully for device(s): {[r['device_id'] for r in readings]}", file=sys.stderr)
    except Exception as e:
        print(f"Error inserting energy readings: {e}", file=sys.stderr)
        conn.rollback()
    finally:
        if 'cursor' in locals():
            cursor.close()

def print_reading_to_console(reading):
    """Print the reading in the format shown in the image"""
    print(f"Custom Address:{reading['device_id']}")
    print(f"Voltage: {reading['voltage']}V")
    print(f"Current: {reading['current']}A")
    print(f"Power: {reading['power']}W")
    print(f"Energy: {reading['energy']}kWh")
    print(f"Frequency: {reading['frequency']}Hz")
    print(f"PF: {reading['power_factor']:.2f}")
    print()  # Add empty line between readings

def generate_and_insert_data(device_ids, stop_event):
    print(f"[CRON] Starting data generation for devices: {device_ids}", file=sys.stderr)
    
    while not stop_event.is_set():
        # Generate data only for the specified device IDs
        readings = [generate_energy_reading(device_id) for device_id in device_ids]
        
        # Connect to the database
        conn = connect_to_db()
        if conn:
            # Insert the generated data into the database
            insert_energy_readings(conn, readings)
            conn.close()
        else:
            print("Failed to connect to the database.", file=sys.stderr)

        # Output the generated data in the same format as the image
        for reading in readings:
            print_reading_to_console(reading)
        
        # Wait for 1 second before next iteration
        time.sleep(1)
        
        # Check if stop event is set
        if stop_event.is_set():
            print(f"[CRON] Stopping data generation for devices: {device_ids}", file=sys.stderr)
            break

def main(device_ids):
    if not device_ids:
        print("Error: No device IDs provided", file=sys.stderr)
        sys.exit(1)
        
    print(f"Starting data generation for devices: {', '.join(device_ids)}", file=sys.stderr)
    print("Press Ctrl+C to stop...", file=sys.stderr)
    
    stop_event = Event()
    
    try:
        generate_and_insert_data(device_ids, stop_event)
    except KeyboardInterrupt:
        print("\nStopping data generation...", file=sys.stderr)
        stop_event.set()

if __name__ == "__main__":
    # Parse device IDs from command-line arguments
    device_ids = sys.argv[1:]
    main(device_ids)