"""
Create visualization charts from performance benchmark results
"""
import json
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

def load_results(json_file="performance_report.json"):
    """Load performance results from JSON file"""
    with open(json_file, 'r') as f:
        return json.load(f)

def create_execution_time_chart(data, output_file="execution_time_comparison.png"):
    """Create execution time comparison chart"""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    
    # Bar chart - averages
    frameworks = ['Dagster', 'Prefect']
    averages = [data['dagster']['average_time'], data['prefect']['average_time']]
    colors = ['#8B5CF6', '#14B8A6']
    
    bars = ax1.bar(frameworks, averages, color=colors, alpha=0.8, edgecolor='black', linewidth=1.5)
    ax1.set_ylabel('Execution Time (seconds)', fontsize=12, fontweight='bold')
    ax1.set_title('Average Execution Time Comparison', fontsize=14, fontweight='bold')
    ax1.grid(axis='y', alpha=0.3)
    
    # Add value labels on bars
    for bar, avg in zip(bars, averages):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{avg:.2f}s',
                ha='center', va='bottom', fontsize=11, fontweight='bold')
    
    # Line chart - all runs
    dagster_times = data['dagster']['all_times']
    prefect_times = data['prefect']['all_times']
    iterations = range(1, len(dagster_times) + 1)
    
    ax2.plot(iterations, dagster_times, 'o-', label='Dagster', color='#8B5CF6', linewidth=2, markersize=8)
    ax2.plot(iterations, prefect_times, 's-', label='Prefect', color='#14B8A6', linewidth=2, markersize=8)
    ax2.set_xlabel('Iteration', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Execution Time (seconds)', fontsize=12, fontweight='bold')
    ax2.set_title('Execution Time Across All Iterations', fontsize=14, fontweight='bold')
    ax2.legend(fontsize=11)
    ax2.grid(alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Saved: {output_file}")
    plt.close()

def create_comprehensive_chart(data, output_file="comprehensive_comparison.png"):
    """Create comprehensive comparison chart with all metrics"""
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Dagster vs Prefect: Comprehensive Performance Comparison', 
                 fontsize=16, fontweight='bold', y=0.995)
    
    frameworks = ['Dagster', 'Prefect']
    colors = ['#8B5CF6', '#14B8A6']
    
    # 1. Execution Time
    ax = axes[0, 0]
    times = [data['dagster']['average_time'], data['prefect']['average_time']]
    bars = ax.bar(frameworks, times, color=colors, alpha=0.8, edgecolor='black', linewidth=1.5)
    ax.set_ylabel('Time (seconds)', fontsize=11, fontweight='bold')
    ax.set_title('Average Execution Time', fontsize=12, fontweight='bold')
    ax.grid(axis='y', alpha=0.3)
    for bar, t in zip(bars, times):
        ax.text(bar.get_x() + bar.get_width()/2., bar.get_height(),
                f'{t:.2f}s', ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    # 2. CPU Usage
    ax = axes[0, 1]
    cpu = [data['dagster'].get('average_cpu_percent', 0), 
           data['prefect'].get('average_cpu_percent', 0)]
    bars = ax.bar(frameworks, cpu, color=colors, alpha=0.8, edgecolor='black', linewidth=1.5)
    ax.set_ylabel('CPU Usage (%)', fontsize=11, fontweight='bold')
    ax.set_title('Average CPU Usage', fontsize=12, fontweight='bold')
    ax.grid(axis='y', alpha=0.3)
    for bar, c in zip(bars, cpu):
        ax.text(bar.get_x() + bar.get_width()/2., bar.get_height(),
                f'{c:.2f}%', ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    # 3. Network Usage
    ax = axes[1, 0]
    network = [data['dagster'].get('average_network_kb', 0), 
               data['prefect'].get('average_network_kb', 0)]
    bars = ax.bar(frameworks, network, color=colors, alpha=0.8, edgecolor='black', linewidth=1.5)
    ax.set_ylabel('Network (KB)', fontsize=11, fontweight='bold')
    ax.set_title('Average Network Usage', fontsize=12, fontweight='bold')
    ax.grid(axis='y', alpha=0.3)
    for bar, n in zip(bars, network):
        ax.text(bar.get_x() + bar.get_width()/2., bar.get_height(),
                f'{n:.1f}KB', ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    # 4. Consistency (Std Dev)
    ax = axes[1, 1]
    std_devs = [data['dagster']['std_dev'], data['prefect']['std_dev']]
    bars = ax.bar(frameworks, std_devs, color=colors, alpha=0.8, edgecolor='black', linewidth=1.5)
    ax.set_ylabel('Standard Deviation (seconds)', fontsize=11, fontweight='bold')
    ax.set_title('Execution Time Consistency', fontsize=12, fontweight='bold')
    ax.grid(axis='y', alpha=0.3)
    for bar, std in zip(bars, std_devs):
        ax.text(bar.get_x() + bar.get_width()/2., bar.get_height(),
                f'{std:.3f}s', ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Saved: {output_file}")
    plt.close()

def create_time_series_chart(data, output_file="time_series_comparison.png"):
    """Create time series comparison chart"""
    fig, ax = plt.subplots(figsize=(12, 6))
    
    dagster_times = data['dagster']['all_times']
    prefect_times = data['prefect']['all_times']
    iterations = range(1, len(dagster_times) + 1)
    
    ax.plot(iterations, dagster_times, 'o-', label='Dagster', color='#8B5CF6', 
            linewidth=2.5, markersize=10, alpha=0.8)
    ax.plot(iterations, prefect_times, 's-', label='Prefect', color='#14B8A6', 
            linewidth=2.5, markersize=10, alpha=0.8)
    
    # Add average lines
    dagster_avg = data['dagster']['average_time']
    prefect_avg = data['prefect']['average_time']
    ax.axhline(y=dagster_avg, color='#8B5CF6', linestyle='--', alpha=0.5, 
               label=f'Dagster Avg: {dagster_avg:.2f}s')
    ax.axhline(y=prefect_avg, color='#14B8A6', linestyle='--', alpha=0.5, 
               label=f'Prefect Avg: {prefect_avg:.2f}s')
    
    ax.set_xlabel('Iteration Number', fontsize=12, fontweight='bold')
    ax.set_ylabel('Execution Time (seconds)', fontsize=12, fontweight='bold')
    ax.set_title('Execution Time: Dagster vs Prefect (All Iterations)', 
                 fontsize=14, fontweight='bold')
    ax.legend(fontsize=11, loc='best')
    ax.grid(alpha=0.3)
    ax.set_xticks(iterations)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"Saved: {output_file}")
    plt.close()

if __name__ == "__main__":
    print("=" * 60)
    print("CREATING VISUALIZATION CHARTS")
    print("=" * 60)
    
    # Load data
    report_file = Path(__file__).parent / "performance_report.json"
    if not report_file.exists():
        print(f"Error: {report_file} not found!")
        print("Run performance_benchmark.py first to generate results.")
        exit(1)
    
    data = load_results(str(report_file))
    
    print(f"\nLoaded results from: {report_file}")
    print(f"Iterations: {data['iterations']}")
    print(f"Timestamp: {data['timestamp']}")
    
    # Create charts
    print("\nGenerating charts...")
    create_execution_time_chart(data)
    create_comprehensive_chart(data)
    create_time_series_chart(data)
    
    print("\n" + "=" * 60)
    print("ALL CHARTS CREATED SUCCESSFULLY!")
    print("=" * 60)
    print("\nGenerated files:")
    print("  - execution_time_comparison.png")
    print("  - comprehensive_comparison.png")
    print("  - time_series_comparison.png")
    print("=" * 60)

