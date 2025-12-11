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

# Setup K3s

Hướng dẫn thiết lập cụm Kubernetes (K3s) và MinIO trên máy ảo Ubuntu.

### Bước 1: Cấu hình mạng (Netplan)
Thực hiện trên **TẤT CẢ** các máy (Master và Worker).
Chỉnh sửa file netplan:
```bash
sudo nano /etc/netplan/01-netcfg.yaml
```
Nội dung các folder đó:
```yaml
network:
  ethernets:
    enp0s3:             # Card NAT (Internet)
      dhcp4: true
    enp0s8:             # Card Host-only (Nội bộ)
      dhcp4: false
      addresses:
        - 192.168.56.101/24  # <--- SỬA SỐ NÀY CHO TỪNG MÁY
        #Với máy master thì để là 101, các máy agent để số cuối cùng là số máy +3
        #Ví dụ: datanode1 thì để ip là 192.168.56.104/24
      # QUAN TRỌNG: Không điền gateway 4 ở đây để tránh mất internet
  version: 2
```

### Bước 2: Cài đặt curl
Chạy trên tất cả các máy:
```bash
sudo apt update && sudo apt install curl -y
```

### Bước 3: Cài đặt Master Node
Chạy trên máy master 192.168.56.101
```bash
curl -sfL [https://get.k3s.io](https://get.k3s.io) | INSTALL_K3S_EXEC="server --node-ip 192.168.56.101 --flannel-iface enp0s8" sh -
```
Lấy token để kết nối với Woker
```bash
sudo cat /var/lib/rancher/k3s/server/node-token
```

### Bước 4: Cài đặt Worker Node
Chạy trên các máy Worker (VD: .104,.105,.106). Thay K3S_TOKEN bằng chuỗi token vừa lấy ở bước 3
```bash
# Ví dụ cho máy 104 (Thay IP agent --node-ip tương ứng cho từng máy)
curl -sfL [https://get.k3s.io](https://get.k3s.io) | K3S_URL=[https://192.168.56.101:6443](https://192.168.56.101:6443) K3S_TOKEN="<DÁN_TOKEN_VÀO_ĐÂY>" INSTALL_K3S_EXEC="agent --node-ip 192.168.56.104 --flannel-iface enp0s8" sh -
```

### Bước 5: Kiểm tra Nodes
Trên máy Master, kiểm tra danh sách node:
```bash
sudo kubectl get nodes -o wide
```