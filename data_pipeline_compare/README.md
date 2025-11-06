# Data Pipeline Comparison: Dagster vs Prefect

Project này so sánh performance giữa Dagster và Prefect trong việc đọc Parquet files và ghi vào VictoriaMetrics.

## Cấu trúc Project

```
data_pipeline_compare/
├── data/
│   └── sensor_data.parquet          # File data chung cho cả hai pipeline
├── dagster_pipeline/
│   ├── workspace.yaml               # Dagster workspace config
│   ├── docker-compose.yml           # Docker compose cho Dagster
│   ├── requirements.txt            # Dependencies cho Dagster
│   └── my_dagster_project/
│       ├── __init__.py              # Definitions
│       ├── jobs/
│       │   └── load_to_victoria.py # Job definitions
│       ├── assets/
│       │   └── parquet_loader.py    # Asset definitions
│       └── resources/
│           └── victoria_resource.py # Resource definitions
├── prefect_pipeline/
│   ├── docker-compose.yml           # Docker compose cho Prefect
│   ├── requirements.txt             # Dependencies cho Prefect
│   └── main_flow.py                 # Prefect flow chính
└── README.md                        # File này
```

## Cài đặt

### Dagster Pipeline

```bash
cd dagster_pipeline
pip install -r requirements.txt
```

### Prefect Pipeline

```bash
cd prefect_pipeline
pip install -r requirements.txt
```

## Sử dụng

### Dagster Pipeline

#### Chạy Dagster UI:
```bash
cd dagster_pipeline
dagster dev
```

Truy cập: http://localhost:3000

#### Chạy từ command line:
```bash
cd dagster_pipeline
dagster asset materialize --select "read_parquet_data,write_to_victoriametrics" -m my_dagster_project
```

### Prefect Pipeline

#### Chạy Prefect flow:
```bash
cd prefect_pipeline
python main_flow.py
```

#### Chạy Prefect UI (optional):
```bash
cd prefect_pipeline
prefect server start
```

Truy cập: http://localhost:4200

## Data File

File data chung: `data/sensor_data.parquet`

Cả hai pipeline đều đọc từ cùng một file này:
- Dagster: `../data/sensor_data.parquet` (relative từ dagster_pipeline/)
- Prefect: `../data/sensor_data.parquet` (relative từ prefect_pipeline/)

## Sử dụng Podman

### Dagster Pipeline

```bash
cd dagster_pipeline
podman-compose up -d
```

Sau khi chạy:
- VictoriaMetrics: http://localhost:18428 (mapped từ container port 8428)
- Dagster UI: http://localhost:3000

**Lưu ý:** Nếu port 8428 đã bị chiếm, container sẽ map sang port 18428 để tránh conflict.

### Prefect Pipeline

```bash
cd prefect_pipeline
podman-compose up -d
```

Sau khi chạy:
- VictoriaMetrics: http://localhost:8429 (khác port để tránh conflict)
- Prefect UI: http://localhost:4200

### Quản lý containers

```bash
# Xem containers
podman ps

# Xem logs
podman logs dagster-vic
podman logs prefect-vic

# Dừng
podman-compose down

# Hoặc dừng từng container
podman stop dagster-vic victoriametrics-dagster
podman stop prefect-vic victoriametrics-prefect
```

## Xem Data trong Prefect UI

### Xem Flow Runs

1. **Truy cập Prefect UI**: http://localhost:4200
2. **Vào tab "Runs"** để xem lịch sử các lần chạy
3. **Click vào một run** để xem:
   - Logs của từng task
   - Timeline
   - Metadata (số metrics đã ghi, batches, etc.)

### Chạy Flow để tạo Run trong UI

**Cách 1: Chạy từ UI (Khuyến nghị)**
1. Vào tab **"Deployments"**
2. Click vào deployment `parquet-to-victoriametrics`
3. Click nút **"Run"** hoặc **"Quick run"**
4. Xem kết quả trong tab "Runs"

**Cách 2: Chạy từ CLI**
```bash
cd prefect_pipeline
$env:PREFECT_API_URL="http://localhost:4200/api"
prefect deployment run 'parquet_to_victoriametrics/parquet-to-victoriametrics'
```

**Cách 3: Chạy bằng script**
```bash
cd prefect_pipeline
$env:PREFECT_API_URL="http://localhost:4200/api"
python trigger_run.py
```

### Kiểm tra Data nhanh

```bash
# Check Prefect runs và VictoriaMetrics data
cd data_pipeline_compare
python quick_check_data.py

# Hoặc check chi tiết VictoriaMetrics
python check_prefect_data.py
```

## So sánh Performance

Để so sánh performance giữa Dagster và Prefect:

```bash
cd data_pipeline_compare
python performance_benchmark.py --iterations 5
```

Script này sẽ:
- Chạy cả Dagster và Prefect pipelines với số iterations chỉ định
- Đo execution time, CPU usage, network usage
- Tạo báo cáo chi tiết trong `performance_report.json`
- Hiển thị kết quả so sánh

### Tạo Visualization Charts

```bash
cd data_pipeline_compare
python create_visualization.py          # Basic charts
python create_dagster_focused_charts.py # Dagster-focused charts
```

Charts sẽ được lưu trong folder `data_pipeline_compare/`:
- `execution_time_comparison.png`
- `comprehensive_comparison.png`
- `time_series_comparison.png`
- `consistency_comparison.png`
- `radar_comparison.png`
- `feature_comparison.png`
- `dagster_advantages.png`

## Cấu hình

### VictoriaMetrics URL

Mặc định: `http://localhost:8428`

Có thể thay đổi qua environment variable:
```bash
export VICTORIAMETRICS_URL=http://your-vm-url:8428
```

## Requirements

- Python 3.11+
- **Podman** (được khuyến nghị sử dụng)
- VictoriaMetrics (chạy qua podman-compose)

## Notes

- Cả hai pipeline đọc cùng file `data/sensor_data.parquet`
- Cả hai pipeline ghi vào cùng VictoriaMetrics instance
- Batch size mặc định: 1000 metrics per batch

