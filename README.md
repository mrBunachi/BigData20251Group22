# Big Data for IT Jobs
### 1. Thông tin nhóm
* Nguyễn Khổng Duy Hoàng
* Hoàng Đức Khải
* Trịnh Hoàng Chi
* Trần Thế Khiêm
* Phạm Minh Toàn
### 2. Sơ đồ Kiến trúc Hệ thống
![Sơ đồ luồng dữ liệu](images/workflow.png)

### 3. Đáp ứng tiêu chí
* Data Ingestion → Producer App (Crawler) + Kafka/Redpanda.
* Data Processing → Spark Batch (Ingestion & ETL Python Code).
* Stream Processing → Realtime Metrics (Web Dashboard via MongoDB).
* Data Storage → MinIO DataLake (JSON/Parquet) + MongoDB.
* System Integration → Trino SQL + BI Tool (Superset) + Web Dashboard.
* Performance Optimization → Chuẩn hóa Parquet, Partition theo ngày/nguồn.
* Monitoring → Log số lượng tin tuyển dụng, Kafka latency.
* Scaling → Thêm nhiều nguồn (TopCV, Vietnamworks, ITviec).
* Data Quality & Testing → Chuẩn hóa lương, làm sạch HTML, lọc trùng lặp.
* Security & Governance → Phân quyền truy cập (Data Engineer, Analyst).
* Fault Tolerance → Backup Raw Data (MinIO), Kafka offset replay.