"""
Performance Benchmark Script: Dagster vs Prefect
Runs multiple iterations and compares performance metrics
"""
import time
import subprocess
import sys
import os
import statistics
import json
from datetime import datetime
from pathlib import Path
import psutil


class PerformanceBenchmark:
    """Class to benchmark Dagster and Prefect pipelines"""
    
    def __init__(self, iterations=5):
        self.iterations = iterations
        self.results = {
            "dagster": [],
            "prefect": []
        }
        self.project_root = Path(__file__).parent
        self.dagster_dir = self.project_root / "dagster_pipeline"
        self.prefect_dir = self.project_root / "prefect_pipeline"
    
    def run_dagster_pipeline(self, iteration):
        """Run Dagster pipeline and measure performance"""
        print(f"\n[Dagster] Running iteration {iteration + 1}/{self.iterations}...")
        
        start_time = time.time()
        process = psutil.Process()
        start_memory = process.memory_info().rss / 1024 / 1024  # MB
        start_cpu = psutil.cpu_percent(interval=None)
        start_network = psutil.net_io_counters()
        
        try:
            # Check if running from root dagster_pipeline.py or project structure
            root_pipeline = self.project_root.parent / "dagster_pipeline.py"
            
            if root_pipeline.exists():
                # Use root dagster_pipeline.py
                cmd = [
                    sys.executable, "-m", "dagster",
                    "asset", "materialize",
                    "--select", "read_parquet_data,write_to_victoriametrics",
                    "-f", str(root_pipeline)
                ]
                cwd = str(self.project_root.parent)
            else:
                # Use project structure
                cmd = [
                    sys.executable, "-m", "dagster",
                    "job", "execute",
                    "-j", "parquet_to_victoriametrics_job",
                    "-m", "my_dagster_project"
                ]
                cwd = str(self.dagster_dir)
            
            result = subprocess.run(
                cmd,
                cwd=cwd,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
                timeout=300,
                env={**os.environ, "VICTORIAMETRICS_URL": "http://localhost:18428"}
            )
            
            end_time = time.time()
            end_memory = process.memory_info().rss / 1024 / 1024  # MB
            end_cpu = psutil.cpu_percent(interval=None)
            end_network = psutil.net_io_counters()
            
            execution_time = end_time - start_time
            memory_used = end_memory - start_memory
            cpu_avg = (start_cpu + end_cpu) / 2
            
            # Network stats (bytes sent/received)
            bytes_sent = end_network.bytes_sent - start_network.bytes_sent
            bytes_recv = end_network.bytes_recv - start_network.bytes_recv
            network_total = bytes_sent + bytes_recv
            
            if result.returncode == 0:
                print(f"  [OK] Completed in {execution_time:.2f}s (CPU: {cpu_avg:.1f}%, Network: {network_total/1024:.1f}KB)")
                return {
                    "success": True,
                    "execution_time": execution_time,
                    "memory_used_mb": memory_used,
                    "cpu_percent": cpu_avg,
                    "network_bytes_sent": bytes_sent,
                    "network_bytes_recv": bytes_recv,
                    "network_total_bytes": network_total,
                    "iteration": iteration + 1
                }
            else:
                error_msg = result.stderr[:200] if result.stderr else result.stdout[:200]
                print(f"  [ERROR] Failed: {error_msg.encode('ascii', errors='replace').decode('ascii')}")
                return {
                    "success": False,
                    "execution_time": execution_time,
                    "error": error_msg
                }
                
        except subprocess.TimeoutExpired:
            print(f"  [ERROR] Timeout after 300s")
            return {"success": False, "error": "Timeout"}
        except Exception as e:
            error_msg = str(e)[:100].encode('ascii', errors='replace').decode('ascii')
            print(f"  [ERROR] Error: {error_msg}")
            return {"success": False, "error": str(e)[:200]}
    
    def run_prefect_pipeline(self, iteration):
        """Run Prefect pipeline and measure performance"""
        print(f"\n[Prefect] Running iteration {iteration + 1}/{self.iterations}...")
        
        # Set environment
        env = os.environ.copy()
        env["PREFECT_API_URL"] = "http://localhost:4200/api"
        env["VICTORIAMETRICS_URL"] = "http://localhost:8429"
        
        start_time = time.time()
        process = psutil.Process()
        start_memory = process.memory_info().rss / 1024 / 1024  # MB
        start_cpu = psutil.cpu_percent(interval=None)
        start_network = psutil.net_io_counters()
        
        try:
            cmd = [sys.executable, "trigger_run.py"]
            
            result = subprocess.run(
                cmd,
                cwd=str(self.prefect_dir),
                env=env,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
                timeout=300
            )
            
            end_time = time.time()
            end_memory = process.memory_info().rss / 1024 / 1024  # MB
            end_cpu = psutil.cpu_percent(interval=None)
            end_network = psutil.net_io_counters()
            
            execution_time = end_time - start_time
            memory_used = end_memory - start_memory
            cpu_avg = (start_cpu + end_cpu) / 2
            
            # Network stats (bytes sent/received)
            bytes_sent = end_network.bytes_sent - start_network.bytes_sent
            bytes_recv = end_network.bytes_recv - start_network.bytes_recv
            network_total = bytes_sent + bytes_recv
            
            if result.returncode == 0:
                print(f"  [OK] Completed in {execution_time:.2f}s (CPU: {cpu_avg:.1f}%, Network: {network_total/1024:.1f}KB)")
                return {
                    "success": True,
                    "execution_time": execution_time,
                    "memory_used_mb": memory_used,
                    "cpu_percent": cpu_avg,
                    "network_bytes_sent": bytes_sent,
                    "network_bytes_recv": bytes_recv,
                    "network_total_bytes": network_total,
                    "iteration": iteration + 1
                }
            else:
                error_msg = result.stderr[:200] if result.stderr else result.stdout[:200]
                print(f"  [ERROR] Failed: {error_msg.encode('ascii', errors='replace').decode('ascii')}")
                return {
                    "success": False,
                    "execution_time": execution_time,
                    "error": error_msg
                }
                
        except subprocess.TimeoutExpired:
            print(f"  [ERROR] Timeout after 300s")
            return {"success": False, "error": "Timeout"}
        except Exception as e:
            error_msg = str(e)[:100].encode('ascii', errors='replace').decode('ascii')
            print(f"  [ERROR] Error: {error_msg}")
            return {"success": False, "error": str(e)[:200]}
    
    def run_benchmark(self):
        """Run complete benchmark"""
        print("=" * 80)
        print("PERFORMANCE BENCHMARK: DAGSTER vs PREFECT")
        print("=" * 80)
        print(f"Iterations: {self.iterations}")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        # Check prerequisites
        print("\nChecking prerequisites...")
        root_pipeline = self.project_root.parent / "dagster_pipeline.py"
        dagster_ok = root_pipeline.exists() or (self.dagster_dir / "my_dagster_project").exists()
        prefect_ok = (self.prefect_dir / "main_flow.py").exists()
        data_ok = (self.project_root / "data" / "sensor_data.parquet").exists()
        
        print(f"  Dagster pipeline: {'[OK]' if dagster_ok else '[ERROR]'}")
        print(f"  Prefect pipeline: {'[OK]' if prefect_ok else '[ERROR]'}")
        print(f"  Data file: {'[OK]' if data_ok else '[ERROR]'}")
        
        if not (dagster_ok and prefect_ok and data_ok):
            print("\nERROR: Missing prerequisites!")
            if not dagster_ok:
                print("  - Dagster pipeline not found (check dagster_pipeline.py or project structure)")
            if not prefect_ok:
                print("  - Prefect pipeline not found")
            if not data_ok:
                print(f"  - Data file not found at: {self.project_root / 'data' / 'sensor_data.parquet'}")
            return
        
        print("\nNOTE: Running warm-up iteration to ensure fair comparison...")
        
        # Warm-up run for Dagster
        print("\n[Dagster] Warm-up run...")
        warmup_dagster = self.run_dagster_pipeline(-1)
        
        # Warm-up run for Prefect
        print("\n[Prefect] Warm-up run...")
        warmup_prefect = self.run_prefect_pipeline(-1)
        
        print("\nWarm-up completed. Starting actual benchmark...")
        time.sleep(3)  # Cool down
        
        # Run Dagster benchmark
        print("\n" + "=" * 80)
        print("RUNNING DAGSTER BENCHMARK")
        print("=" * 80)
        
        for i in range(self.iterations):
            result = self.run_dagster_pipeline(i)
            if result["success"]:
                self.results["dagster"].append(result)
            time.sleep(3)  # Wait between runs to avoid interference
        
        # Wait before Prefect
        print("\nWaiting 5 seconds before Prefect benchmark...")
        time.sleep(5)
        
        # Run Prefect benchmark
        print("\n" + "=" * 80)
        print("RUNNING PREFECT BENCHMARK")
        print("=" * 80)
        
        for i in range(self.iterations):
            result = self.run_prefect_pipeline(i)
            if result["success"]:
                self.results["prefect"].append(result)
            time.sleep(3)  # Wait between runs to avoid interference
        
        # Generate report
        self.generate_report()
    
    def generate_report(self):
        """Generate performance comparison report"""
        print("\n" + "=" * 80)
        print("PERFORMANCE COMPARISON REPORT")
        print("=" * 80)
        
        # Calculate statistics
        dagster_times = [r["execution_time"] for r in self.results["dagster"] if r.get("success")]
        prefect_times = [r["execution_time"] for r in self.results["prefect"] if r.get("success")]
        
        if not dagster_times:
            print("\nERROR: No successful Dagster runs!")
            print("Check that Dagster containers are running and accessible.")
            return
        
        if not prefect_times:
            print("\nERROR: No successful Prefect runs!")
            print("Check that Prefect containers are running and accessible.")
            return
        
        # Dagster stats
        dagster_avg = statistics.mean(dagster_times)
        dagster_min = min(dagster_times)
        dagster_max = max(dagster_times)
        dagster_std = statistics.stdev(dagster_times) if len(dagster_times) > 1 else 0
        dagster_memory = statistics.mean([r["memory_used_mb"] for r in self.results["dagster"] if r.get("success")])
        dagster_cpu = statistics.mean([r.get("cpu_percent", 0) for r in self.results["dagster"] if r.get("success")])
        dagster_network = statistics.mean([r.get("network_total_bytes", 0) for r in self.results["dagster"] if r.get("success")])
        
        # Prefect stats
        prefect_avg = statistics.mean(prefect_times)
        prefect_min = min(prefect_times)
        prefect_max = max(prefect_times)
        prefect_std = statistics.stdev(prefect_times) if len(prefect_times) > 1 else 0
        prefect_memory = statistics.mean([r["memory_used_mb"] for r in self.results["prefect"] if r.get("success")])
        prefect_cpu = statistics.mean([r.get("cpu_percent", 0) for r in self.results["prefect"] if r.get("success")])
        prefect_network = statistics.mean([r.get("network_total_bytes", 0) for r in self.results["prefect"] if r.get("success")])
        
        # Print results
        print("\nEXECUTION TIME (seconds):")
        print("-" * 80)
        print(f"{'Metric':<20} {'Dagster':<20} {'Prefect':<20} {'Winner':<20}")
        print("-" * 80)
        print(f"{'Average':<20} {dagster_avg:<20.2f} {prefect_avg:<20.2f} {'Dagster' if dagster_avg < prefect_avg else 'Prefect'}")
        print(f"{'Min':<20} {dagster_min:<20.2f} {prefect_min:<20.2f} {'Dagster' if dagster_min < prefect_min else 'Prefect'}")
        print(f"{'Max':<20} {dagster_max:<20.2f} {prefect_max:<20.2f} {'Dagster' if dagster_max < prefect_max else 'Prefect'}")
        print(f"{'Std Dev':<20} {dagster_std:<20.2f} {prefect_std:<20.2f} {'Dagster' if dagster_std < prefect_std else 'Prefect'}")
        
        print("\nMEMORY USAGE (MB):")
        print("-" * 80)
        print(f"{'Average':<20} {dagster_memory:<20.2f} {prefect_memory:<20.2f} {'Dagster' if dagster_memory < prefect_memory else 'Prefect'}")
        
        print("\nCPU USAGE (%):")
        print("-" * 80)
        print(f"{'Average':<20} {dagster_cpu:<20.2f} {prefect_cpu:<20.2f} {'Dagster' if dagster_cpu < prefect_cpu else 'Prefect'}")
        
        print("\nNETWORK USAGE (KB):")
        print("-" * 80)
        print(f"{'Average':<20} {dagster_network/1024:<20.2f} {prefect_network/1024:<20.2f} {'Dagster' if dagster_network < prefect_network else 'Prefect'}")
        
        # Calculate performance difference
        time_diff = abs(dagster_avg - prefect_avg)
        faster = "Dagster" if dagster_avg < prefect_avg else "Prefect"
        slower = "Prefect" if faster == "Dagster" else "Dagster"
        
        if dagster_avg < prefect_avg:
            speedup = prefect_avg / dagster_avg
            slower_pct = ((prefect_avg - dagster_avg) / dagster_avg) * 100
        else:
            speedup = dagster_avg / prefect_avg
            slower_pct = ((dagster_avg - prefect_avg) / prefect_avg) * 100
        
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Faster: {faster} ({speedup:.2f}x faster)")
        print(f"Time difference: {time_diff:.2f} seconds")
        print(f"{slower} is {slower_pct:.1f}% slower than {faster}")
        print(f"Successful runs: Dagster={len(dagster_times)}/{self.iterations}, Prefect={len(prefect_times)}/{self.iterations}")
        
        # Save results to JSON
        report = {
            "timestamp": datetime.now().isoformat(),
            "iterations": self.iterations,
            "dagster": {
                "average_time": dagster_avg,
                "min_time": dagster_min,
                "max_time": dagster_max,
                "std_dev": dagster_std,
                "average_memory_mb": dagster_memory,
                "average_cpu_percent": dagster_cpu,
                "average_network_kb": dagster_network / 1024,
                "successful_runs": len(dagster_times),
                "all_times": dagster_times
            },
            "prefect": {
                "average_time": prefect_avg,
                "min_time": prefect_min,
                "max_time": prefect_max,
                "std_dev": prefect_std,
                "average_memory_mb": prefect_memory,
                "average_cpu_percent": prefect_cpu,
                "average_network_kb": prefect_network / 1024,
                "successful_runs": len(prefect_times),
                "all_times": prefect_times
            },
            "comparison": {
                "faster": faster,
                "speedup": speedup,
                "time_difference": time_diff,
                "slower_percentage": slower_pct
            }
        }
        
        report_file = self.project_root / "performance_report.json"
        with open(report_file, "w") as f:
            json.dump(report, f, indent=2)
        
        print(f"\nDetailed report saved to: {report_file}")
        print("=" * 80)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Benchmark Dagster vs Prefect")
    parser.add_argument(
        "--iterations",
        type=int,
        default=5,
        help="Number of iterations to run (default: 5)"
    )
    
    args = parser.parse_args()
    
    benchmark = PerformanceBenchmark(iterations=args.iterations)
    benchmark.run_benchmark()
