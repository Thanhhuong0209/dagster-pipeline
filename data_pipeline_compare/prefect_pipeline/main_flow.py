"""
Prefect pipeline to read Parquet files and save data to VictoriaMetrics
"""
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
from typing import List, Dict
import pandas as pd
import requests
import time
import os
from pathlib import Path


class VictoriaMetricsWriter:
    """Helper class to write data to VictoriaMetrics"""
    
    def __init__(self, vm_url: str):
        self.vm_url = vm_url.rstrip('/')
        self.insert_url = f"{self.vm_url}/api/v1/import/prometheus"
    
    def write_metrics(self, metrics: List[Dict]) -> bool:
        """
        Write metrics to VictoriaMetrics using Prometheus format
        
        Args:
            metrics: List of metric dicts with format:
                {
                    'name': 'metric_name',
                    'value': 123.45,
                    'timestamp': 1234567890,
                    'labels': {'label1': 'value1', 'label2': 'value2'}
                }
        
        Returns:
            True if successful, False otherwise
        """
        try:
            lines = []
            for metric in metrics:
                name = metric['name']
                value = metric['value']
                timestamp_ms = metric.get('timestamp', int(time.time() * 1000))
                
                labels_str = ""
                if metric.get('labels'):
                    label_pairs = [f'{k}="{v}"' for k, v in metric['labels'].items()]
                    labels_str = "{" + ",".join(label_pairs) + "}"
                
                line = f"{name}{labels_str} {value} {timestamp_ms}"
                lines.append(line)
            
            response = requests.post(
                self.insert_url,
                data='\n'.join(lines),
                headers={'Content-Type': 'text/plain'}
            )
            
            return response.status_code in [200, 204]
                
        except Exception as e:
            print(f"Error writing metrics: {str(e)}")
            return False


@task(
    name="read_parquet_data",
    description="Read Parquet file and convert to DataFrame",
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1)
)
def read_parquet_data(parquet_path: str = None) -> pd.DataFrame:
    """
    Task to read data from Parquet file
    
    Expected Parquet schema:
    - timestamp: datetime or timestamp column
    - metric_name: string column with metric name
    - value: float column with metric value
    - Additional columns will be treated as labels
    """
    logger = get_run_logger()
    
    # Default path relative to project root
    if parquet_path is None:
        parquet_path = Path(__file__).parent.parent / "data" / "sensor_data.parquet"
    else:
        parquet_path = Path(parquet_path)
    
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {parquet_path}")
    
    logger.info(f"Reading Parquet file: {parquet_path}")
    
    # Read Parquet file
    df = pd.read_parquet(parquet_path)
    
    logger.info(f"Read {len(df)} rows from Parquet file")
    logger.info(f"Columns: {df.columns.tolist()}")
    
    return df


@task(
    name="transform_to_metrics",
    description="Transform DataFrame to VictoriaMetrics format"
)
def transform_to_metrics(df: pd.DataFrame) -> List[Dict]:
    """
    Task to transform DataFrame to metrics format
    
    Assumes the DataFrame has:
    - timestamp column (will be converted to milliseconds)
    - metric_name column (or uses default)
    - value column
    - Other columns as labels
    """
    logger = get_run_logger()
    logger.info(f"Transforming {len(df)} rows to metrics format")
    
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
    
    logger.info(f"Converted {len(metrics)} rows to metrics format")
    return metrics


@task(
    name="write_to_victoriametrics",
    description="Write metrics to VictoriaMetrics in batches"
)
def write_to_victoriametrics(
    metrics: List[Dict],
    vm_url: str | None = None,
    batch_size: int = 1000
) -> Dict:
    """
    Task to write metrics to VictoriaMetrics in batches
    
    Args:
        metrics: List of metric dictionaries
        vm_url: VictoriaMetrics URL (defaults to environment variable or localhost)
        batch_size: Number of metrics per batch
    
    Returns:
        Dictionary with write statistics
    """
    logger = get_run_logger()
    
    # Get VM URL from environment or use default
    if vm_url is None:
        vm_url = os.getenv("VICTORIAMETRICS_URL", "http://localhost:8429")  # Prefect uses port 8429
    
    logger.info(f"Writing {len(metrics)} metrics to VictoriaMetrics at {vm_url}")
    
    # Initialize writer
    writer = VictoriaMetricsWriter(vm_url=vm_url)
    
    # Write to VictoriaMetrics in batches
    successful_batches = 0
    failed_batches = 0
    
    for i in range(0, len(metrics), batch_size):
        batch = metrics[i:i + batch_size]
        success = writer.write_metrics(batch)
        
        if success:
            successful_batches += 1
            logger.info(f"Batch {i // batch_size + 1}: Successfully wrote {len(batch)} metrics")
        else:
            failed_batches += 1
            logger.warning(f"Batch {i // batch_size + 1}: Failed to write {len(batch)} metrics")
    
    logger.info(
        f"Write complete: {successful_batches} successful batches, "
        f"{failed_batches} failed batches"
    )
    
    if failed_batches > 0:
        raise Exception(f"Failed to write {failed_batches} batches to VictoriaMetrics")
    
    return {
        "total_metrics": len(metrics),
        "successful_batches": successful_batches,
        "failed_batches": failed_batches,
        "vm_url": vm_url
    }


@flow(
    name="parquet_to_victoriametrics",
    description="Prefect flow to read Parquet and write to VictoriaMetrics",
    log_prints=True
)
def parquet_to_victoriametrics_flow(
    parquet_path: str | None = None,
    vm_url: str | None = None,
    batch_size: int = 1000
) -> Dict:
    """
    Prefect flow to read Parquet file and write data to VictoriaMetrics
    
    Args:
        parquet_path: Path to Parquet file (defaults to ../data/sensor_data.parquet)
        vm_url: VictoriaMetrics URL (optional, uses env var or localhost)
        batch_size: Number of metrics per batch
    
    Returns:
        Dictionary with execution results
    """
    logger = get_run_logger()
    logger.info("Starting Prefect pipeline: Parquet to VictoriaMetrics")
    
    # Step 1: Read Parquet file
    df = read_parquet_data(parquet_path)
    
    # Step 2: Transform to metrics format
    metrics = transform_to_metrics(df)
    
    # Step 3: Write to VictoriaMetrics
    result = write_to_victoriametrics(metrics, vm_url=vm_url, batch_size=batch_size)
    
    logger.info("Prefect pipeline completed successfully")
    return result


if __name__ == "__main__":
    # Run the flow
    result = parquet_to_victoriametrics_flow()
    print(f"\nPipeline completed: {result}")

