FROM python:3.9-slim

# Thiết lập biến môi trường
ENV PYTHONUNBUFFERED=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home

# Tạo thư mục ứng dụng
WORKDIR /app

# Copy các file cần thiết
COPY requirements.txt .
COPY dagster_pipeline.py .
COPY data ./data

# Cài đặt các thư viện Python
RUN pip install --no-cache-dir -r requirements.txt

# Tạo thư mục Dagster home
RUN mkdir -p $DAGSTER_HOME

# Expose port cho gRPC server (nếu dùng) hoặc webserver
EXPOSE 3030

# Command mặc định để chạy code user-deployment (gRPC server)
# Dagster Daemon/Webserver sẽ kết nối tới đây để lấy định nghĩa code
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "3030", "-f", "dagster_pipeline.py"]

