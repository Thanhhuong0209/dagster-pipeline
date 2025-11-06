"""
Run Prefect flow connected to Prefect server
This will keep running and make the flow available on Prefect UI
"""
import os
from main_flow import parquet_to_victoriametrics_flow

if __name__ == "__main__":
    # Set Prefect API URL to connect to server
    if not os.getenv("PREFECT_API_URL"):
        os.environ["PREFECT_API_URL"] = "http://localhost:4200/api"
        print(f"Set PREFECT_API_URL to {os.environ['PREFECT_API_URL']}")
    
    print("Serving flow on Prefect server...")
    print("Flow will be available at: http://localhost:4200/flows")
    print("Press Ctrl+C to stop")
    
    # Serve the flow - this makes it available on Prefect server
    # Flow will keep running and be visible in UI
    parquet_to_victoriametrics_flow.serve(
        name="parquet-to-victoriametrics",
        limit=1
    )

