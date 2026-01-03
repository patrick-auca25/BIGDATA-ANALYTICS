import psycopg2
import time
import subprocess
import sys
import ctypes

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'energy',
    'user': 'postgres',
    'password': 'Biko@1010'
}

def is_admin():
    try:
        return ctypes.windll.shell32.IsUserAnAdmin() != 0
    except:
        return False

def restart_postgres():
    """Restart PostgreSQL for cold cache"""
    print(" Restarting PostgreSQL...")
    
    try:
        subprocess.run(['net', 'stop', 'postgresql-x64-18'], 
                      shell=True, capture_output=True, check=False)
        time.sleep(5)
        
        subprocess.run(['net', 'start', 'postgresql-x64-18'], 
                       shell=True, capture_output=True, check=True)
        time.sleep(10)
        
        print(" Restarted\n")
        return True
    except:
        print(" Restart failed")
        return False

def run_query(table_name, query_name, sql):
    """Execute query and return time"""
    try:
        conn = psycopg2.connect(**DB_CONFIG, connect_timeout=10)
        cur = conn.cursor()
        
        start = time.time()
        cur.execute(sql)
        results = cur.fetchall()
        end = time.time()
        
        exec_time = (end - start) * 1000
        
        cur.close()
        conn.close()
        
        return exec_time
    except Exception as e:
        print(f"Error: {e}")
        return None

def main():
    print("="*60)
    print("COMPRESSION IMPACT MEASUREMENT")
    print("="*60)
    print()
    
    if not is_admin():
        print("  Run as Administrator!")
        response = input("Continue? (yes/no): ")
        if response.lower() != 'yes':
            sys.exit(1)
    
    # Query 2 and Query 3 only
    queries = {
        "Query 2": """
            SELECT time_bucket('15 minutes', timestamp)::timestamp as period,
                   ROUND(AVG(power)::numeric, 2) as avg_power
            FROM {table}
            WHERE timestamp >= NOW() - INTERVAL '7 days'
            GROUP BY time_bucket('15 minutes', timestamp)
            ORDER BY avg_power DESC LIMIT 10;
        """,
        
        "Query 3": """
            SELECT meter_id,
                   TO_CHAR(DATE_TRUNC('month', timestamp), 'YYYY-MM') as month,
                   ROUND(SUM(energy)::numeric, 2) as total_energy
            FROM {table}
            GROUP BY meter_id, DATE_TRUNC('month', timestamp)
            ORDER BY DATE_TRUNC('month', timestamp), total_energy DESC;
        """
    }
    
    tables = {
        '1-day chunks': 'energy_readings',
        '3-hour chunks': 'energy_readings_3h',
        '1-week chunks': 'energy_readings_week'
    }
    
    # Store results
    results_before = {}
    
    print("MEASURING BEFORE COMPRESSION\n")
    
    for table_desc, table_name in tables.items():
        print(f"Testing: {table_name}")
        results_before[table_desc] = {}
        
        for query_name, query_template in queries.items():
            # Restart for cold cache
            if not restart_postgres():
                sys.exit(1)
            
            sql = query_template.format(table=table_name)
            
            print(f"  {query_name}... ", end='', flush=True)
            exec_time = run_query(table_name, query_name, sql)
            
            if exec_time:
                results_before[table_desc][query_name] = exec_time
                print(f"{exec_time:.2f} ms")
            else:
                results_before[table_desc][query_name] = 0
                print("FAILED")
            
            time.sleep(1)
        
        print()
    
    # Display results
    print("="*60)
    print("BEFORE COMPRESSION RESULTS")
    print("="*60)
    print()
    print(f"{'Table':<20} {'Query 2 (ms)':<15} {'Query 3 (ms)':<15}")
    print("-"*60)
    
    for table_desc in ['1-day chunks', '3-hour chunks', '1-week chunks']:
        q2_time = results_before[table_desc].get('Query 2', 0)
        q3_time = results_before[table_desc].get('Query 3', 0)
        print(f"{table_desc:<20} {q2_time:<15.2f} {q3_time:<15.2f}")
    
    print()
    
    # Save to file
    import os
    os.makedirs('results', exist_ok=True)
    
    with open('results/compression_after.txt', 'w') as f:
        f.write("QUERY PERFORMANCE AFTER COMPRESSION\n")
        f.write("="*60 + "\n")
        f.write(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write(f"{'Table':<20} {'Query 2 (ms)':<15} {'Query 3 (ms)':<15}\n")
        f.write("-"*60 + "\n")
        
        for table_desc in ['1-day chunks', '3-hour chunks', '1-week chunks']:
            q2 = results_before[table_desc].get('Query 2', 0)
            q3 = results_before[table_desc].get('Query 3', 0)
            f.write(f"{table_desc:<20} {q2:<15.2f} {q3:<15.2f}\n")
    
  

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n Interrupted")
        sys.exit(1)