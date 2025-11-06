"""
Jobs for loading data to VictoriaMetrics
"""
from dagster import define_asset_job
from ..assets import parquet_loader

# Define job for loading Parquet to VictoriaMetrics
parquet_to_victoriametrics_job = define_asset_job(
    name="parquet_to_victoriametrics_job",
    selection=[
        parquet_loader.read_parquet_data,
        parquet_loader.write_to_victoriametrics
    ],
)

