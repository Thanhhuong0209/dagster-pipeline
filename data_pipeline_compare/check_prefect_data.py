"""
Script to check if data is working in Prefect pipeline
"""
import requests
import json
from datetime import datetime, timedelta

VM_URL = "http://localhost:8429"  # Prefect VictoriaMetrics port

def check_prefect_data():
    """Check data in Prefect's VictoriaMetrics"""
    print("=" * 60)
    print("CHECKING PREFECT PIPELINE DATA")
    print("=" * 60)
    
    # Check if VictoriaMetrics is accessible
    try:
        health = requests.get(f"{VM_URL}/health", timeout=5)
        if health.status_code == 200:
            print(f"[OK] VictoriaMetrics is accessible at {VM_URL}")
        else:
            print(f"[ERROR] VictoriaMetrics health check failed: {health.status_code}")
            return
    except Exception as e:
        print(f"[ERROR] Cannot connect to VictoriaMetrics at {VM_URL}")
        print(f"  Error: {str(e)}")
        print(f"\n  Make sure Prefect pipeline is running:")
        print(f"    cd prefect_pipeline")
        print(f"    podman-compose up -d")
        return
    
    # Get all metric names
    print("\n1. Available Metrics:")
    try:
        r = requests.get(f"{VM_URL}/api/v1/label/__name__/values")
        if r.status_code == 200:
            metrics = r.json().get("data", [])
            if metrics:
                print(f"   Found {len(metrics)} metrics:")
                for metric in metrics:
                    print(f"   - {metric}")
            else:
                print("   No metrics found yet")
                print("   Run the Prefect pipeline to generate data:")
                print("     cd prefect_pipeline")
                print("     python main_flow.py")
                print("     (Make sure to set VICTORIAMETRICS_URL=http://localhost:8429)")
                return
        else:
            print(f"   Error querying metrics: {r.status_code}")
            return
    except Exception as e:
        print(f"   Error: {str(e)}")
        return
    
    # Check temperature metrics
    print("\n2. Temperature Metrics (Latest):")
    try:
        r = requests.get(f"{VM_URL}/api/v1/query?query=temperature_celsius")
        if r.status_code == 200:
            data = r.json()
            results = data.get("data", {}).get("result", [])
            if results:
                print(f"   Found {len(results)} temperature series:")
                for result in results:
                    metric = result.get("metric", {})
                    value = result.get("value", [0, 0])
                    print(f"   - {metric.get('sensor_id', 'unknown')}: {value[1]} C")
            else:
                print("   No temperature data found")
        else:
            print(f"   Error: {r.status_code}")
    except Exception as e:
        print(f"   Error: {str(e)}")
    
    # Check humidity metrics
    print("\n3. Humidity Metrics (Latest):")
    try:
        r = requests.get(f"{VM_URL}/api/v1/query?query=humidity_percent")
        if r.status_code == 200:
            data = r.json()
            results = data.get("data", {}).get("result", [])
            if results:
                print(f"   Found {len(results)} humidity series:")
                for result in results:
                    metric = result.get("metric", {})
                    value = result.get("value", [0, 0])
                    print(f"   - {metric.get('sensor_id', 'unknown')}: {value[1]}%")
            else:
                print("   No humidity data found")
        else:
            print(f"   Error: {r.status_code}")
    except Exception as e:
        print(f"   Error: {str(e)}")
    
    # Check data range
    print("\n4. Data Range (Last 1 hour):")
    try:
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=1)
        
        r = requests.get(
            f"{VM_URL}/api/v1/query_range",
            params={
                "query": "temperature_celsius",
                "start": int(start_time.timestamp()),
                "end": int(end_time.timestamp()),
                "step": 60
            },
            timeout=10
        )
        if r.status_code == 200:
            data = r.json()
            results = data.get("data", {}).get("result", [])
            if results:
                print(f"   Found {len(results)} series with data points")
                total_points = sum(len(r.get("values", [])) for r in results)
                print(f"   Total data points: {total_points}")
            else:
                print("   No data in the last hour")
        else:
            print(f"   Error: {r.status_code}")
    except Exception as e:
        print(f"   Error: {str(e)}")
    
    print("\n" + "=" * 60)
    print("Summary:")
    print("  - VictoriaMetrics URL: http://localhost:8429")
    print("  - To run Prefect pipeline: cd prefect_pipeline && python main_flow.py")
    print("  - To check Prefect UI: http://localhost:4200")
    print("=" * 60)

if __name__ == "__main__":
    check_prefect_data()

