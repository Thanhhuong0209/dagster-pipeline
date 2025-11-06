"""
Quick script to check data in both Prefect UI and VictoriaMetrics
"""
import requests
import json

def check_prefect_runs():
    """Check runs in Prefect UI"""
    print("=" * 60)
    print("CHECKING PREFECT UI RUNS")
    print("=" * 60)
    
    try:
        # Get recent flow runs
        r = requests.get("http://localhost:4200/api/flow_runs?limit=10")
        if r.status_code == 200:
            runs = r.json()
            if runs:
                print(f"\nFound {len(runs)} recent runs:")
                for run in runs[:5]:
                    name = run.get("name", "N/A")
                    state = run.get("state_type", "N/A")
                    flow_name = run.get("flow", {}).get("name", "N/A") if isinstance(run.get("flow"), dict) else "N/A"
                    print(f"  - Flow: {flow_name}")
                    print(f"    Run: {name}")
                    print(f"    Status: {state}")
                    print()
            else:
                print("\nNo runs found yet")
                print("Run the flow from Prefect UI or CLI to create runs")
        else:
            print(f"\nError: {r.status_code}")
    except Exception as e:
        print(f"\nCannot connect to Prefect API: {str(e)}")
        print("Make sure Prefect server is running at http://localhost:4200")

def check_victoriametrics_data():
    """Check data in VictoriaMetrics"""
    print("=" * 60)
    print("CHECKING VICTORIAMETRICS DATA")
    print("=" * 60)
    
    VM_URL = "http://localhost:8429"
    
    try:
        # Health check
        health = requests.get(f"{VM_URL}/health", timeout=5)
        if health.status_code == 200:
            print(f"\n[OK] VictoriaMetrics accessible at {VM_URL}")
        else:
            print(f"\n[ERROR] Health check failed: {health.status_code}")
            return
    except Exception as e:
        print(f"\n[ERROR] Cannot connect to VictoriaMetrics: {str(e)}")
        return
    
    # Get metrics
    try:
        r = requests.get(f"{VM_URL}/api/v1/label/__name__/values")
        if r.status_code == 200:
            metrics = r.json().get("data", [])
            if metrics:
                print(f"\nAvailable metrics: {', '.join(metrics)}")
                
                # Query temperature
                r2 = requests.get(f"{VM_URL}/api/v1/query?query=temperature_celsius")
                if r2.status_code == 200:
                    data = r2.json()
                    results = data.get("data", {}).get("result", [])
                    print(f"Temperature series: {len(results)}")
                
                # Query humidity
                r3 = requests.get(f"{VM_URL}/api/v1/query?query=humidity_percent")
                if r3.status_code == 200:
                    data = r3.json()
                    results = data.get("data", {}).get("result", [])
                    print(f"Humidity series: {len(results)}")
            else:
                print("\nNo metrics found in VictoriaMetrics")
                print("Run the Prefect flow to generate data")
        else:
            print(f"Error querying metrics: {r.status_code}")
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    check_prefect_runs()
    print()
    check_victoriametrics_data()
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print("1. Prefect UI: http://localhost:4200")
    print("   - Go to 'Runs' tab to see flow runs")
    print("   - Go to 'Deployments' tab to run flows")
    print()
    print("2. VictoriaMetrics: http://localhost:8429")
    print("   - Use check_prefect_data.py for detailed info")
    print("   - Or query directly via API")
    print("=" * 60)

