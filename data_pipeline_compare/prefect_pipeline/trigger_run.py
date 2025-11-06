"""
Script to trigger a Prefect flow run that will appear in Prefect UI
"""
import os
import sys
from main_flow import parquet_to_victoriametrics_flow

# Set Prefect API URL to connect to server
os.environ["PREFECT_API_URL"] = "http://localhost:4200/api"

if __name__ == "__main__":
    print("=" * 60)
    print("TRIGGERING PREFECT FLOW RUN")
    print("=" * 60)
    print(f"Prefect API URL: {os.environ.get('PREFECT_API_URL')}")
    print("\nRunning flow...")
    print("This run will appear in Prefect UI at http://localhost:4200/runs")
    print("\n" + "=" * 60)
    
    try:
        # Run the flow - this will create a run in Prefect UI
        result = parquet_to_victoriametrics_flow()
        
        print("\n" + "=" * 60)
        print("FLOW RUN COMPLETED")
        print("=" * 60)
        print(f"Total metrics: {result.get('total_metrics', 'N/A')}")
        print(f"Successful batches: {result.get('successful_batches', 'N/A')}")
        print(f"Failed batches: {result.get('failed_batches', 'N/A')}")
        print("\nCheck Prefect UI at http://localhost:4200/runs to see this run!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\nError running flow: {str(e)}")
        print("\nMake sure:")
        print("  1. Prefect server is running (podman-compose up -d)")
        print("  2. Flow is being served (python run_with_server.py)")
        sys.exit(1)

