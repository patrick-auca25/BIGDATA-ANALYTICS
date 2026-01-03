import psycopg2
import time
import subprocess
import sys
import ctypes
import os

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
    print("🔄 Restarting PostgreSQL...")
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
        print(" Failed\n")
        return False

def run_query(name, sql):
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
        
        return exec_time, len(results)
    except Exception as e:
        print(f" Error: {e}")
        return None, None

def main():
    print("="*60)
    print("CONTINUOUS AGGREGATE PERFORMANCE COMPARISON")
    print("="*60)
    print()
    
    if not is_admin():
        print("  Run as Administrator!")
        response = input("Continue? (yes/no): ")
        if response.lower() != 'yes':
            sys.exit(1)
    
    # Test queries - comparing raw vs aggregated data
    test_cases = [
        {
            'name': '(15-min buckets)',
            'raw': """
                SELECT meter_id, 
                       time_bucket('15 minutes', timestamp) AS bucket,
                       AVG(power) as avg_power
                FROM energy_readings
                WHERE timestamp >= NOW() - INTERVAL '1 day'
                  AND meter_id = '1000000002'
                GROUP BY meter_id, bucket
                ORDER BY bucket;
            """,
            'aggregate': """
                SELECT meter_id, bucket, avg_power
                FROM energy_readings_15min
                WHERE bucket >= NOW() - INTERVAL '1 day'
                  AND meter_id = '1000000002'
                ORDER BY bucket;
            """
        }
    ]
    
    results = []
    
    for i, test in enumerate(test_cases, 1):
        print(f"\nTest {i}/3: {test['name']}")
        print("-"*60)
        
        # Test raw data query
        if not restart_postgres():
            sys.exit(1)
        
        print("  Raw data... ", end='', flush=True)
        raw_time, raw_rows = run_query('raw', test['raw'])
        if raw_time:
            print(f"{raw_time:.2f} ms ({raw_rows} rows)")
        else:
            print("FAILED")
            raw_time = 0
        
        time.sleep(2)
        
        # Test aggregate query
        if not restart_postgres():
            sys.exit(1)
        
        print("  Aggregate... ", end='', flush=True)
        agg_time, agg_rows = run_query('aggregate', test['aggregate'])
        if agg_time:
            print(f"{agg_time:.2f} ms ({agg_rows} rows)")
        else:
            print("FAILED")
            agg_time = 0
        
        # Calculate speedup
        speedup = 0
        if agg_time > 0:
            speedup = raw_time / agg_time
            print(f"   Speedup: {speedup:.1f}x faster")
        
        results.append({
            'name': test['name'],
            'raw_time': raw_time,
            'agg_time': agg_time,
            'speedup': speedup
        })
    
    # Summary
    print("\n" + "="*60)
    print("RESULTS")
    print("="*60)
    print()
    print(f"{'Test':<25} {'Raw (ms)':<12} {'Agg (ms)':<12} {'Speedup':<10}")
    print("-"*60)
    
    for r in results:
        print(f"{r['name']:<25} {r['raw_time']:<12.2f} {r['agg_time']:<12.2f} {r['speedup']:<10.1f}x")
    
    valid_speedups = [r['speedup'] for r in results if r['speedup'] > 0]
    avg_speedup = sum(valid_speedups) / len(valid_speedups) if valid_speedups else 0
    print()
    print(f"Average Speedup: {avg_speedup:.1f}x faster")
    
    # Save
    os.makedirs('results', exist_ok=True)
    
    with open('results/continuous_aggregate_comparison.txt', 'w') as f:
        f.write("CONTINUOUS AGGREGATE PERFORMANCE COMPARISON\n")
        f.write("="*60 + "\n")
        f.write(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        f.write(f"{'Test':<25} {'Raw (ms)':<12} {'Agg (ms)':<12} {'Speedup':<10}\n")
        f.write("-"*60 + "\n")
        
        for r in results:
            f.write(f"{r['name']:<25} {r['raw_time']:<12.2f} {r['agg_time']:<12.2f} {r['speedup']:<10.1f}x\n")
        
        f.write(f"\nAverage Speedup: {avg_speedup:.1f}x\n")
    
   
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n Interrupted")
        sys.exit(1)