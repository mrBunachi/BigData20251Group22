# 🛠️ Cheat Sheet: Các lệnh quản trị K3s thường dùng

### Chạy các lệnh này tại máy Master.

### **1. Kiểm tra trạng thái hệ thống**

```bash
# Xem danh sách các node và trạng thái (Ready/NotReady)
kubectl get nodes

# Xem TOÀN BỘ các Pods, IP và vị trí máy đang chạy
kubectl get pods -n bigdata -o wide

# Xem danh sách Services và Port mở ra (NodePort)
kubectl get svc -n bigdata
```

---

### **2. Triển khai & Cập nhật**

```bash
# Áp dụng cấu hình từ file YAML (Tạo mới hoặc Cập nhật)
kubectl apply -f minio-deploy.yaml
kubectl apply -f kafka-worker.yaml
kubectl apply -f spark-deploy.yaml
```

---

### **3. Debug & Sửa lỗi**

```bash
# Xem chi tiết cấu hình và lỗi của Pod (Dùng khi Pod bị Pending hoặc ImagePullBackOff)
kubectl describe pod -n bigdata <tên-pod>

# Xem log in ra màn hình của Pod (Dùng khi Pod bị CrashLoopBackOff hoặc Error)
kubectl logs -f -n bigdata <tên-pod>

# Xóa Pod để K3s tự tạo lại (Dùng để restart nhanh)
kubectl delete pod -n bigdata <tên-pod>

# Xóa toàn bộ Deployment (Để dọn dẹp sạch sẽ trước khi apply lại)
kubectl delete deployment -n bigdata spark-master spark-worker
```

---
