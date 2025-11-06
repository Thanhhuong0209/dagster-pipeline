"""
Deploy Prefect flow to Prefect server using Prefect 3.x API
"""
from main_flow import parquet_to_victoriametrics_flow

if __name__ == "__main__":
    print("Deploying flow to Prefect server...")
    print("Make sure PREFECT_API_URL is set to http://localhost:4200/api")
    print("\nUsing flow.serve() to make flow available on server...")
    
    # Serve the flow - this makes it available on Prefect server
    parquet_to_victoriametrics_flow.serve(
        name="parquet-to-victoriametrics",
        limit=1  # Maximum concurrent runs
    )
    
    print("\nFlow is now available in Prefect UI!")
    print("Go to: http://localhost:4200/flows")
    print("You should see the flow 'parquet-to-victoriametrics'")

