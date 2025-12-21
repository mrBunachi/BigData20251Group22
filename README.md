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

# Setup MinIO
Khi bạn chạy 4 Replicas, Kubernetes sẽ rải đều mỗi máy 1 Pod.

Máy 101 (Master): Tốn 9GB đĩa cứng.

Máy 104 (Agent 1): Tốn 9GB đĩa cứng.

Máy 105 (Agent 2): Tốn 9GB đĩa cứng.

Máy 106 (Agent 3): Tốn 9GB đĩa cứng.

Tổng cộng: 36GB được cấp phát, nhưng nhờ cơ chế Erasure Coding, bạn sẽ có 18GB dung lượng thực tế có thể sử dụng và hệ thống chịu lỗi được ngay cả khi 2 máy bất kỳ bị sập.

## 🚀 Cấu hình 4 Replicas - Điều hướng về Node 
Chúng ta sẽ cấu hình để Master tham gia lưu trữ, nhưng mọi thông tin điều hướng (Redirect) sẽ trỏ về IP của máy 104.
### Bước 1: Thực hiện trên nút Master: Cho phép Master chạy Pod dữ liệu
Mặc định K3s có thể chặn không cho Pod chạy trên Master (Taint). Hãy chạy lệnh này để chắc chắn Master sẵn sàng nhận Pod:
```bash
kubectl taint nodes ubuntu24-nn-virtualbox node-role.kubernetes.io/master:NoSchedule-
kubectl taint nodes ubuntu24-nn-virtualbox node-role.kubernetes.io/control-plane:NoSchedule-
```
### Bước 2: Tạo file YAML cấu hình 4 bản ghi
```bash
nano minio-distributed-4nodes.yaml
```
Nội dung:
```YAML
apiVersion: v1
kind: Namespace
metadata:
  name: bigdata
---
apiVersion: v1
kind: Service
metadata:
  name: minio-service
  namespace: bigdata
spec:
  type: NodePort
  selector:
    app: minio
  ports:
    - name: api
      port: 9000
      targetPort: 9000
      nodePort: 30000
    - name: console
      port: 9001
      targetPort: 9001
      nodePort: 30001
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: minio
  namespace: bigdata
spec:
  serviceName: minio-service
  replicas: 4 # Phân tán ra 4 máy (101, 104, 105, 106)
  selector:
    matchLabels:
      app: minio
  template:
    metadata:
      labels:
        app: minio
    spec:
      # Rải đều Pod: Mỗi máy chỉ chứa duy nhất 1 bản MinIO
      affinity:
        podAntiAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            - labelSelector:
                matchExpressions:
                  - key: app
                    operator: In
                    values: ["minio"]
              topologyKey: "kubernetes.io/hostname"
      containers:
      - name: minio
        image: minio/minio:latest
        args:
        - server
        - http://minio-{0...3}.minio-service.bigdata.svc.cluster.local/data
        - --console-address
        - :9001
        env:
        - name: MINIO_ROOT_USER
          value: "admin"
        - name: MINIO_ROOT_PASSWORD
          value: "password123"
        # --- THIẾT LẬP NODE 104 LÀM QUẢN LÝ CHÍNH ---
        - name: MINIO_BROWSER_REDIRECT_URL
          value: "http://192.168.56.104:30001"
        volumeMounts:
        - name: data
          mountPath: /data
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: [ "ReadWriteOnce" ]
      storageClassName: "local-path"
      resources:
        requests:
          storage: 9Gi
```
### Bước 3: Triển khai và xác nhận
Chạy lệnh ```kubectl apply -f minio-distributed-4nodes.yaml```

Kiểm tra các Pod đã rải đều chưa:
```bash
kubectl get pods -n bigdata -o wide
```
Bạn sẽ thấy 4 Pods chạy trên 4 Node khác nhau: Master (.101), datanode1 (.104), datanode2 (.105), datanode3 (.106).

Giờ bạn chỉ cần vào địa chỉ ```http://192.168.56.104:30001``` là xong, trêm bất cứ máy nào trong cụm, hoặc trên Window (nếu dùng Host-only Adapter).