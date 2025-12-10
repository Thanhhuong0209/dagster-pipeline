# Dagster Pipeline với VictoriaMetrics

Project này sử dụng Dagster để đọc Parquet files và ghi dữ liệu vào VictoriaMetrics.

## Cấu trúc Project

```
dagster_vic/
├── data_pipeline_compare/          # Dagster pipeline project
│   ├── dagster_pipeline/           # Dagster pipeline
│   ├── data/                       # Data files
│   └── README.md                   # Detailed documentation
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

### 2. Chạy Dagster Pipeline

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
