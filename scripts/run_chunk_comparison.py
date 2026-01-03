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
    print(" Restarting PostgreSQL...")
    
    SERVICE_NAME = 'postgresql-x64-18'
    
    try:
        subprocess.run(['net', 'stop', SERVICE_NAME], 
                      shell=True, 
                      capture_output=True,
                      text=True,
                      check=False)
        time.sleep(5)
        
        subprocess.run(['net', 'start', SERVICE_NAME], 
                       shell=True,
                       capture_output=True,
                       text=True,
                       check=True)
        time.sleep(10)
        
        print(" Restarted\n")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f" Restart failed")
        return False

def run_query(table_name, sql):
    """Execute query and return execution time"""
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
        
        return exec_time, len(results)
        
    except Exception as e:
        print(f" Error: {e}")
        return None, None

def main():
    print("="*60)
    print("CHUNK INTERVAL COMPARISON")
    print("="*60)
    print()
    
    if not is_admin():
        print("  Run as Administrator!")
        response = input("Continue? (yes/no): ")
        if response.lower() != 'yes':
            sys.exit(1)
    
    # Query templates
    query_templates = {
        "1": """
            SELECT time_bucket('1 hour', timestamp)::timestamp as hour,
                   ROUND(AVG(power)::numeric, 2) as avg_power
            FROM {table}
            WHERE timestamp >= DATE_TRUNC('day', NOW())
            GROUP BY time_bucket('1 hour', timestamp)
            ORDER BY hour;
        """,
        
        "2": """
            SELECT time_bucket('15 minutes', timestamp)::timestamp as period,
                   ROUND(AVG(power)::numeric, 2) as avg_power
            FROM {table}
            WHERE timestamp >= NOW() - INTERVAL '7 days'
            GROUP BY time_bucket('15 minutes', timestamp)
            ORDER BY avg_power DESC LIMIT 10;
        """,
        
        "3": """
            SELECT meter_id,
                   TO_CHAR(DATE_TRUNC('month', timestamp), 'YYYY-MM') as month,
                   ROUND(SUM(energy)::numeric, 2) as total_energy
            FROM {table}
            GROUP BY meter_id, DATE_TRUNC('month', timestamp)
            ORDER BY DATE_TRUNC('month', timestamp), total_energy DESC;
        """,
        
        "4": """
            SELECT COUNT(*) as total_rows,
                   ROUND(AVG(power)::numeric, 2) as avg_power,
                   ROUND(MAX(power)::numeric, 2) as max_power,
                   ROUND(MIN(power)::numeric, 2) as min_power
            FROM {table};
        """
    }
    
    
    tables = {
        '3-hour chunks': 'energy_readings_3h',
        '1-day chunks': 'energy_readings',
        '1-week chunks': 'energy_readings_week'
    }
    
    
    results = {
        '1': {},
        '2': {},
        '3': {},
        '4': {}
    }
    
    
    for table_desc, table_name in tables.items():
        print(f"\nTesting: {table_name}")
        print("-"*60)
        
        for query_num, query_template in query_templates.items():
            
            if not restart_postgres():
                sys.exit(1)
            
            sql = query_template.format(table=table_name)
            
            print(f"Query {query_num}... ", end='', flush=True)
            exec_time, rows = run_query(table_name, sql)
            
            if exec_time is not None:
                results[query_num][table_desc] = exec_time
                print(f"{exec_time:.2f} ms")
            else:
                results[query_num][table_desc] = 0
                print("FAILED")
            
            time.sleep(1)
    
    
    print("\n" + "="*60)
    print("RESULTS")
    print("="*60)
    print()
    print(f"{'Query':<10} {'3-hour chunks':<20} {'1-day chunks':<20} {'1-week chunks':<20}")
    print("-"*60)
    
    for query_num in ['1', '2', '3', '4']:
        print(f"{query_num:<10} ", end='')
        print(f"{results[query_num].get('3-hour chunks', 0):<20.2f} ", end='')
        print(f"{results[query_num].get('1-day chunks', 0):<20.2f} ", end='')
        print(f"{results[query_num].get('1-week chunks', 0):<20.2f}")
    
    print()
    
    # Save to file
    os.makedirs('results', exist_ok=True)
    
    with open('results/chunk_comparison.txt', 'w') as f:
        f.write("CHUNK INTERVAL COMPARISON\n")
        f.write("="*60 + "\n")
        f.write(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write(f"{'Query':<10} {'3-hour chunks':<20} {'1-day chunks':<20} {'1-week chunks':<20}\n")
        f.write("-"*60 + "\n")
        
        for query_num in ['1', '2', '3', '4']:
            f.write(f"{query_num:<10} ")
            f.write(f"{results[query_num].get('3-hour chunks', 0):<20.2f} ")
            f.write(f"{results[query_num].get('1-day chunks', 0):<20.2f} ")
            f.write(f"{results[query_num].get('1-week chunks', 0):<20.2f}\n")
    
    
   

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n Interrupted")
        sys.exit(1)