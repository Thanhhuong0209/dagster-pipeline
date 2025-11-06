"""
Script to generate timeseries data and save to VictoriaMetrics
"""
import time
import random
import requests
from datetime import datetime, timedelta
from typing import List, Dict
import json


class VictoriaMetricsWriter:
    """Helper class to write data to VictoriaMetrics"""
    
    def __init__(self, vm_url: str = "http://localhost:8428"):
        """
        Initialize VictoriaMetrics writer
        
        Args:
            vm_url: VictoriaMetrics URL (default: http://localhost:8428)
        """
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
            # Format metrics in Prometheus format
            lines = []
            for metric in metrics:
                name = metric['name']
                value = metric['value']
                timestamp_ms = metric.get('timestamp', int(time.time() * 1000))
                
                # Format labels
                labels_str = ""
                if metric.get('labels'):
                    label_pairs = [f'{k}="{v}"' for k, v in metric['labels'].items()]
                    labels_str = "{" + ",".join(label_pairs) + "}"
                
                # Prometheus format: metric_name{labels} value timestamp_ms
                line = f"{name}{labels_str} {value} {timestamp_ms}"
                lines.append(line)
            
            # Send to VictoriaMetrics
            response = requests.post(
                self.insert_url,
                data='\n'.join(lines),
                headers={'Content-Type': 'text/plain'}
            )
            
            if response.status_code in [200, 204]:
                print(f"Successfully wrote {len(metrics)} metrics to VictoriaMetrics")
                return True
            else:
                print(f"Error writing metrics: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            print(f"Exception writing metrics: {str(e)}")
            return False


def generate_timeseries_data(
    start_time: datetime,
    end_time: datetime,
    interval_seconds: int = 60,
    metric_name: str = "sample_metric",
    labels: Dict[str, str] = None
) -> List[Dict]:
    """
    Generate synthetic timeseries data
    
    Args:
        start_time: Start datetime
        end_time: End datetime
        interval_seconds: Interval between data points in seconds
        metric_name: Name of the metric
        labels: Dictionary of labels to attach to metrics
    
    Returns:
        List of metric dictionaries
    """
    metrics = []
    current_time = start_time
    
    while current_time <= end_time:
        # Generate random value (can be customized)
        value = random.uniform(10.0, 100.0)
        
        # Convert datetime to timestamp in milliseconds
        timestamp_ms = int(current_time.timestamp() * 1000)
        
        metric = {
            'name': metric_name,
            'value': value,
            'timestamp': timestamp_ms,
            'labels': labels or {}
        }
        
        metrics.append(metric)
        current_time += timedelta(seconds=interval_seconds)
    
    return metrics


def main():
    """Main function to generate and write timeseries data"""
    print("Generating timeseries data...")
    
    # Configuration
    vm_url = "http://localhost:8428"
    metric_name = "temperature_celsius"
    
    # Generate data for last 24 hours, 1 minute intervals
    end_time = datetime.now()
    start_time = end_time - timedelta(hours=24)
    
    # Generate multiple metrics with different labels
    all_metrics = []
    
    # Temperature from different sensors
    for sensor_id in ['sensor_01', 'sensor_02', 'sensor_03']:
        labels = {
            'sensor_id': sensor_id,
            'location': 'room_a',
            'type': 'temperature'
        }
        metrics = generate_timeseries_data(
            start_time=start_time,
            end_time=end_time,
            interval_seconds=60,
            metric_name=metric_name,
            labels=labels
        )
        all_metrics.extend(metrics)
    
    # Humidity metrics
    for sensor_id in ['sensor_01', 'sensor_02']:
        labels = {
            'sensor_id': sensor_id,
            'location': 'room_a',
            'type': 'humidity'
        }
        metrics = generate_timeseries_data(
            start_time=start_time,
            end_time=end_time,
            interval_seconds=60,
            metric_name='humidity_percent',
            labels=labels
        )
        all_metrics.extend(metrics)
    
    print(f"Generated {len(all_metrics)} data points")
    
    # Write to VictoriaMetrics
    writer = VictoriaMetricsWriter(vm_url=vm_url)
    
    # Write in batches to avoid overwhelming the server
    batch_size = 1000
    for i in range(0, len(all_metrics), batch_size):
        batch = all_metrics[i:i + batch_size]
        writer.write_metrics(batch)
        time.sleep(0.1)  # Small delay between batches
    
    print(f"\nSuccessfully generated and wrote {len(all_metrics)} metrics to VictoriaMetrics")
    print(f"  Time range: {start_time} to {end_time}")
    print(f"  Metrics: {metric_name}, humidity_percent")
    print(f"  Sensors: sensor_01, sensor_02, sensor_03")


if __name__ == "__main__":
    main()

