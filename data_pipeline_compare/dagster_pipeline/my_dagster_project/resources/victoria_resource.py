"""
VictoriaMetrics resource for Dagster
"""
from dagster import ConfigurableResource, Config
from typing import List, Dict
import requests
import time
import os


class VictoriaMetricsWriter:
    """Helper class to write data to VictoriaMetrics"""
    
    def __init__(self, vm_url: str, logger=None):
        self.vm_url = vm_url.rstrip('/')
        self.insert_url = f"{self.vm_url}/api/v1/import/prometheus"
        self.logger = logger  # Optional logger for better integration
    
    def _log(self, message: str, level: str = "info"):
        """Log message using logger if available, otherwise print"""
        if self.logger:
            if level == "error":
                self.logger.error(message)
            elif level == "warning":
                self.logger.warning(message)
            else:
                self.logger.info(message)
        else:
            print(message)
    
    def write_metrics(self, metrics: List[Dict], max_retries: int = 3) -> bool:
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
            max_retries: Maximum number of retry attempts
        
        Returns:
            True if successful, False otherwise
        """
        for attempt in range(max_retries):
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
                    headers={'Content-Type': 'text/plain'},
                    timeout=30  # Increased timeout
                )
                
                if response.status_code in [200, 204]:
                    return True
                else:
                    error_msg = f"HTTP {response.status_code}: {response.text[:200] if response.text else 'No response body'}"
                    if attempt < max_retries - 1:
                        self._log(f"VictoriaMetrics error (attempt {attempt + 1}/{max_retries}): {error_msg}, retrying...", "warning")
                        time.sleep(1 * (attempt + 1))  # Exponential backoff
                    else:
                        self._log(f"VictoriaMetrics error (final attempt): {error_msg}", "error")
                        return False
                    
            except requests.exceptions.ConnectionError as e:
                if attempt < max_retries - 1:
                    self._log(f"VictoriaMetrics connection error (attempt {attempt + 1}/{max_retries}) to {self.insert_url}: {str(e)}, retrying...", "warning")
                    time.sleep(1 * (attempt + 1))
                else:
                    self._log(f"VictoriaMetrics connection error (final attempt) to {self.insert_url}: {str(e)}", "error")
                    return False
            except requests.exceptions.Timeout as e:
                if attempt < max_retries - 1:
                    self._log(f"VictoriaMetrics timeout (attempt {attempt + 1}/{max_retries}) to {self.insert_url}: {str(e)}, retrying...", "warning")
                    time.sleep(1 * (attempt + 1))
                else:
                    self._log(f"VictoriaMetrics timeout (final attempt) to {self.insert_url}: {str(e)}", "error")
                    return False
            except Exception as e:
                if attempt < max_retries - 1:
                    self._log(f"VictoriaMetrics unexpected error (attempt {attempt + 1}/{max_retries}): {str(e)}, retrying...", "warning")
                    time.sleep(1 * (attempt + 1))
                else:
                    self._log(f"VictoriaMetrics unexpected error (final attempt): {str(e)}", "error")
                    return False
        
        return False


class VictoriaMetricsConfig(Config):
    """Configuration for VictoriaMetrics connection"""
    # Default to localhost:18428 for Windows host, or use container name if in container
    default_vm_url = os.getenv("VICTORIAMETRICS_URL", "http://localhost:18428")
    vm_url: str = default_vm_url
    batch_size: int = 1000


class VictoriaMetricsResource(ConfigurableResource):
    """Dagster resource for VictoriaMetrics"""
    
    # Default to localhost:18428 for Windows host, or use container name if in container
    default_vm_url = os.getenv("VICTORIAMETRICS_URL", "http://localhost:18428")
    vm_url: str = default_vm_url
    
    def get_writer(self) -> VictoriaMetricsWriter:
        """Get VictoriaMetrics writer instance"""
        return VictoriaMetricsWriter(self.vm_url)


# Create resource instance
victoria_metrics_resource = VictoriaMetricsResource()

