# Dagster vs Prefect Performance Comparison

Project này so sánh performance và features giữa Dagster và Prefect trong việc đọc Parquet files và ghi vào VictoriaMetrics.

## Cấu trúc Project

```
dagster_vic/
├── data_pipeline_compare/          # Main comparison project
│   ├── dagster_pipeline/           # Dagster pipeline
│   ├── prefect_pipeline/           # Prefect pipeline
│   ├── data/                      # Shared data files
│   ├── performance_benchmark.py   # Benchmark script
│   ├── create_visualization.py    # Visualization charts
│   ├── create_dagster_focused_charts.py  # Dagster-focused charts
│   └── README.md                  # Detailed documentation
├── dagster_pipeline.py            # Root Dagster pipeline (for Windows host)
├── generate_timeseries.py         # Generate timeseries data
└── README.md                       # This file
```

## Sử dụng

### 1. Generate Timeseries Data và lưu vào VictoriaMetrics

```bash
python generate_timeseries.py
```

Script này sẽ:
- Generate timeseries data cho 24 giờ qua (interval 1 phút)
- Tạo metrics cho temperature và humidity từ nhiều sensors
- Ghi trực tiếp vào VictoriaMetrics tại `http://localhost:8428`

### 2. Tạo Sample Parquet File

```bash
python create_sample_parquet.py
```

File Parquet sẽ được tạo tại `data/timeseries_data.parquet`

### 3. Chạy Dagster Pipeline

#### Sử dụng Podman:

**Windows (PowerShell):**
```powershell
.\run_dagster.ps1
```

**Linux/Mac:**
```bash
chmod +x run_dagster.sh
./run_dagster.sh
```

Sau đó truy cập http://localhost:3000 để xem và chạy pipeline.

#### Chạy trực tiếp (không dùng container):
```bash
dagster dev
```

Sau đó truy cập http://localhost:3000 để xem và chạy pipeline.

#### Chạy từ command line:
```bash
dagster asset materialize --select "read_parquet_data,write_to_victoriametrics" -m dagster_pipeline
```

### 4. Chạy Prefect Pipeline (So sánh với Dagster)

#### Chạy Prefect pipeline:
```bash
python run_prefect_comparison.py
```

Hoặc chạy trực tiếp:
```bash
python prefect_pipeline.py
```

#### So sánh Performance giữa Dagster và Prefect:
```bash
python performance_comparison.py
```

Script này sẽ:
- Chạy Dagster pipeline và đo thời gian
- Chạy Prefect pipeline và đo thời gian
- So sánh và hiển thị kết quả

## Cấu hình VictoriaMetrics

Mặc định pipeline sử dụng VictoriaMetrics tại `http://localhost:8428`.

Để thay đổi URL, bạn có thể:
1. Sửa trong code
2. Hoặc sử dụng Dagster config (trong UI hoặc code)

## Parquet File Format

File Parquet cần có các cột:
- `timestamp`: datetime hoặc timestamp
- `value`: giá trị số (float)
- `metric_name`: tên metric (optional, default: 'parquet_metric')
- Các cột khác sẽ được dùng làm labels

## Assets

Pipeline có 2 assets:
1. `read_parquet_data`: Đọc file Parquet
2. `write_to_victoriametrics`: Transform và ghi vào VictoriaMetrics

## Schedules và Sensors

- **Daily Schedule**: Chạy hàng ngày lúc nửa đêm
- **File Sensor**: Detect file Parquet mới (mặc định tắt)

## VictoriaMetrics Setup

### Sử dụng Podman:

**Windows (PowerShell):**
```powershell
.\run_victoriametrics.ps1
```

**Linux/Mac:**
```bash
chmod +x run_victoriametrics.sh
./run_victoriametrics.sh
```

**Hoặc chạy trực tiếp:**
```bash
podman run -d \
  --name victoriametrics \
  -p 8428:8428 \
  -v victoriametrics-data:/victoria-metrics-data \
  victoriametrics/victoria-metrics:latest \
  --storageDataPath=/victoria-metrics-data \
  --httpListenAddr=:8428
```

**Khởi động cả hai services (VictoriaMetrics + Dagster):**

**Windows (PowerShell):**
```powershell
.\run_all.ps1
```

**Linux/Mac:**
```bash
chmod +x run_all.sh
./run_all.sh
```

**Sử dụng podman-compose:**
```bash
podman-compose up -d
```

### Quản lý container:
```bash
# Xem logs
podman logs victoriametrics

# Dừng container
podman stop victoriametrics

# Khởi động lại
podman start victoriametrics

# Xóa container
podman rm victoriametrics
```

## Query Data trong VictoriaMetrics

Sau khi ghi data, bạn có thể query bằng cách:

```bash
# Query metrics
curl 'http://localhost:8428/api/v1/query?query=temperature_celsius'

# Hoặc sử dụng Grafana để visualize
```

