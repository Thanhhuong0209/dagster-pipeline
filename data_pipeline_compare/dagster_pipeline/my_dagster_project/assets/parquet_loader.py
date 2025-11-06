"""
Assets for loading Parquet data
"""
from dagster import (
    asset,
    AssetExecutionContext,
    MaterializeResult,
    Config,
)
from typing import List, Dict
import pandas as pd
import os
from pathlib import Path


class VictoriaMetricsConfig(Config):
    """Configuration for VictoriaMetrics connection"""
    # Default to localhost:18428 for Windows host, or use container name if in container
    default_vm_url = os.getenv("VICTORIAMETRICS_URL", "http://localhost:18428")
    vm_url: str = default_vm_url
    batch_size: int = 1000


@asset(
    description="Read Parquet file and convert to DataFrame"
)
def read_parquet_data(context: AssetExecutionContext) -> pd.DataFrame:
    """
    Asset to read data from Parquet file
    
    Expected Parquet schema:
    - timestamp: datetime or timestamp column
    - metric_name: string column with metric name
    - value: float column with metric value
    - Additional columns will be treated as labels
    """
    # Path to Parquet file (relative to project root)
    parquet_path = Path(__file__).parent.parent.parent.parent.parent / "data" / "sensor_data.parquet"
    
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {parquet_path}")
    
    context.log.info(f"Reading Parquet file: {parquet_path}")
    
    # Read Parquet file
    df = pd.read_parquet(parquet_path)
    
    context.log.info(f"Read {len(df)} rows from Parquet file")
    context.log.info(f"Columns: {df.columns.tolist()}")
    
    return df


@asset(
    description="Transform DataFrame to VictoriaMetrics format and write to VictoriaMetrics",
    deps=[read_parquet_data]
)
def write_to_victoriametrics(
    context: AssetExecutionContext,
    read_parquet_data: pd.DataFrame,
    config: VictoriaMetricsConfig
) -> MaterializeResult:
    """
    Asset to transform DataFrame and write to VictoriaMetrics
    
    Assumes the DataFrame has:
    - timestamp column (will be converted to milliseconds)
    - metric_name column (or uses default)
    - value column
    - Other columns as labels
    """
    from ..resources.victoria_resource import VictoriaMetricsWriter
    
    # Override with environment variable if set
    vm_url = os.getenv("VICTORIAMETRICS_URL", config.vm_url)
    context.log.info(f"Using VictoriaMetrics URL: {vm_url}")
    # Get the DataFrame from previous asset
    df = read_parquet_data
    
    context.log.info(f"Processing {len(df)} rows for VictoriaMetrics")
    
    # Initialize writer with logger for better integration
    writer = VictoriaMetricsWriter(vm_url=vm_url, logger=context.log)
    
    # Convert DataFrame to metrics format
    metrics = []
    
    # Required columns
    required_cols = ['timestamp', 'value']
    
    # Check if required columns exist
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Required column '{col}' not found in DataFrame")
    
    # Determine metric name column
    metric_name_col = 'metric_name' if 'metric_name' in df.columns else None
    default_metric_name = 'parquet_metric'
    
    # Get label columns (all columns except timestamp, value, metric_name)
    label_cols = [col for col in df.columns 
                  if col not in ['timestamp', 'value', 'metric_name']]
    
    # Convert timestamp to milliseconds if needed
    if pd.api.types.is_datetime64_any_dtype(df['timestamp']):
        df['timestamp_ms'] = df['timestamp'].astype('int64') // 10**6
    else:
        # Assume already in milliseconds or seconds
        df['timestamp_ms'] = df['timestamp'].astype('int64')
        # If values are too small, assume seconds and convert
        if df['timestamp_ms'].max() < 10**12:
            df['timestamp_ms'] = df['timestamp_ms'] * 1000
    
    # Convert DataFrame rows to metrics
    for _, row in df.iterrows():
        metric_name = row[metric_name_col] if metric_name_col else default_metric_name
        
        # Build labels
        labels = {}
        for col in label_cols:
            if pd.notna(row[col]):
                labels[col] = str(row[col])
        
        metric = {
            'name': str(metric_name),
            'value': float(row['value']),
            'timestamp': int(row['timestamp_ms']),
            'labels': labels
        }
        
        metrics.append(metric)
    
    context.log.info(f"Converted {len(metrics)} rows to metrics format")
    
    # Write to VictoriaMetrics in batches
    batch_size = config.batch_size
    total_batches = (len(metrics) + batch_size - 1) // batch_size
    successful_batches = 0
    failed_batches = 0
    
    context.log.info(f"Starting to write {len(metrics)} metrics in {total_batches} batches (batch_size={batch_size})")
    
    for i in range(0, len(metrics), batch_size):
        batch = metrics[i:i + batch_size]
        batch_num = i // batch_size + 1
        
        context.log.info(f"Writing batch {batch_num}/{total_batches} ({len(batch)} metrics)...")
        
        try:
            success = writer.write_metrics(batch)
            
            if success:
                successful_batches += 1
                context.log.info(f"[OK] Successfully wrote batch {batch_num}/{total_batches}")
            else:
                failed_batches += 1
                context.log.error(f"[ERROR] Failed to write batch {batch_num}/{total_batches} to {vm_url} after retries")
                context.log.error(f"VM URL: {vm_url}, Insert URL: {writer.insert_url}")
        except Exception as e:
            failed_batches += 1
            context.log.error(f"[EXCEPTION] Exception writing batch {batch_num}/{total_batches}: {str(e)}")
            context.log.error(f"VM URL: {vm_url}, Insert URL: {writer.insert_url}")
            import traceback
            context.log.error(traceback.format_exc())
    
    context.log.info(
        f"Write complete: {successful_batches} successful batches, "
        f"{failed_batches} failed batches"
    )
    
    if failed_batches > 0:
        raise Exception(f"Failed to write {failed_batches} batches to VictoriaMetrics")
    
    return MaterializeResult(
        metadata={
            "total_metrics": len(metrics),
            "successful_batches": successful_batches,
            "failed_batches": failed_batches,
            "vm_url": vm_url
        }
    )

