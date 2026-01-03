import psycopg2
import time
import subprocess
import sys
import os
import ctypes

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'energy',
    'user': 'postgres',
    'password': 'Biko@1010'
}

def is_admin():
    """Check if running as Administrator"""
    try:
        return ctypes.windll.shell32.IsUserAnAdmin() != 0
    except:
        return False

def restart_postgres():
    """Restart PostgreSQL service (Windows)"""
    print(" Restarting PostgreSQL for cold cache...")
    
    SERVICE_NAME = 'postgresql-x64-18'  # Your PostgreSQL 18 service
    
    try:
        # Stop PostgreSQL
        print("   Stopping PostgreSQL...")
        subprocess.run(['net', 'stop', SERVICE_NAME], 
                      shell=True, 
                      capture_output=True,
                      text=True,
                      check=False)
        time.sleep(5)
        
        # Start PostgreSQL
        print("   Starting PostgreSQL...")
        result = subprocess.run(['net', 'start', SERVICE_NAME], 
                               shell=True,
                               capture_output=True,
                               text=True,
                               check=True)
        time.sleep(10)
        
        print(" PostgreSQL restarted\n")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f" Failed to restart PostgreSQL")
        print(f"   Error: {e.stderr if hasattr(e, 'stderr') else 'Unknown error'}")
        print("\n  Make sure you run this script as Administrator!")
        print("   Right-click Python/CMD → 'Run as Administrator'\n")
        return False

def run_query(name, sql):
    """Execute query and return execution time"""
    print(f" Running: {name}")
    print("-" * 60)
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Measure execution time
        start = time.time()
        cur.execute(sql)
        results = cur.fetchall()
        end = time.time()
        
        exec_time = (end - start) * 1000  # Convert to milliseconds
        
        print(f"  Execution Time: {exec_time:.2f} ms")
        print(f" Rows Returned: {len(results)}")
        print(f" Completed\n")
        
        cur.close()
        conn.close()
        
        return exec_time, len(results)
        
    except Exception as e:
        print(f" Error: {e}\n")
        return None, None

def main():
    print("="*60)
    print("BASELINE PERFORMANCE TEST")
    print("Hypertable: energy_readings (1-day chunks)")
    print("Platform: Windows (PostgreSQL 18)")
    print("="*60)
    print()
    
    # Check admin privileges
    if not is_admin():
        print("  WARNING: Not running as Administrator!")
        print("   PostgreSQL restart will likely fail.")
        print()
        print(" To run as Administrator:")
        print("   1. Right-click on Command Prompt/PowerShell")
        print("   2. Select 'Run as Administrator'")
        print("   3. Navigate to your project directory")
        print("   4. Run: python src/run_baseline_queries.py")
        print()
        response = input("Continue anyway? (yes/no): ")
        if response.lower() != 'yes':
            print("\nExiting... Please restart as Administrator.")
            sys.exit(1)
        print()
    
    # Define the 4 baseline queries
    queries = {
        "Query 1: Hourly avg today": """
            SELECT time_bucket('1 hour', timestamp) AS hour,
                   AVG(power) as avg_power
            FROM energy_readings
            WHERE timestamp >= DATE_TRUNC('day', NOW())
            GROUP BY hour ORDER BY hour;
        """,
        
        "Query 2: Peak periods (7 days)": """
            SELECT time_bucket('15 minutes', timestamp) AS period,
                   AVG(power) as avg_power
            FROM energy_readings
            WHERE timestamp >= NOW() - INTERVAL '7 days'
            GROUP BY period ORDER BY avg_power DESC LIMIT 10;
        """,
        
        "Query 3: Monthly per meter": """
            SELECT meter_id,
                   TO_CHAR(DATE_TRUNC('month', timestamp), 'YYYY-MM') as month,
                   SUM(energy) as total_energy
            FROM energy_readings
            GROUP BY meter_id, month
            ORDER BY month, total_energy DESC;
        """,
        
        "Query 4: Full scan": """
            SELECT COUNT(*), AVG(power), MAX(power), MIN(power)
            FROM energy_readings;
        """
    }
    
    results = {}
    
    # Run each query with cold cache
    for name, sql in queries.items():
        # Restart PostgreSQL for cold cache
        if not restart_postgres():
            print("\n Cannot continue without PostgreSQL restart capability")
            print("   Please run this script as Administrator\n")
            sys.exit(1)
        
        # Run the query
        exec_time, rows = run_query(name, sql)
        
        if exec_time:
            results[name] = {'time': exec_time, 'rows': rows}
        
        # Brief pause between queries
        time.sleep(2)
    
    # Display summary
    print("="*60)
    print("BASELINE RESULTS SUMMARY (1-day chunks)")
    print("="*60)
    print()
    print(f"{'Query':<35} {'Time (ms)':<15} {'Rows':<10}")
    print("-"*60)
    
    for name, data in results.items():
        print(f"{name:<35} {data['time']:<15.2f} {data['rows']:<10}")
    
    print()
    
    # Save results to file
    os.makedirs('results', exist_ok=True)
    filename = 'results/baseline_1day_chunks.txt'
    
    with open(filename, 'w') as f:
        f.write("BASELINE PERFORMANCE - 1 DAY CHUNKS\n")
        f.write("="*60 + "\n")
        f.write(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Platform: Windows (PostgreSQL 18 Local)\n")
        f.write(f"Dataset: ~2M rows (14 days, 500 meters)\n")
        f.write(f"Chunk Interval: 1 day\n")
        f.write(f"Cold Cache: YES (PostgreSQL restarted before each query)\n")
        f.write("="*60 + "\n\n")
        
        for name, data in results.items():
            f.write(f"{name}\n")
            f.write(f"  Execution Time: {data['time']:.2f} ms\n")
            f.write(f"  Rows Returned: {data['rows']}\n\n")
    
    print(f" Results saved to: {filename}")
   

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n Testing interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n Unexpected error: {e}")
        sys.exit(1)