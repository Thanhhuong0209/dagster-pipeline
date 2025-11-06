"""
Dagster project for loading Parquet data to VictoriaMetrics
"""
from dagster import Definitions, load_assets_from_modules
from .assets import parquet_loader
from .jobs import load_to_victoria
from .resources import victoria_resource

# Load all assets
assets = load_assets_from_modules([parquet_loader])

# Create definitions
defs = Definitions(
    assets=assets,
    jobs=[load_to_victoria.parquet_to_victoriametrics_job],
    resources={"victoria_metrics": victoria_resource.victoria_metrics_resource},
)

