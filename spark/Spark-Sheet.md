# 🛠️ Cheat Sheet: Xử lý Spark

## **Lần đầu chạy Spark**

### **BƯỚC 1: Tạo nội dung cho file** `spark_job.py`

---

### **Bước 2: Apply file vào Pod (tạo mới/ghi đè)**

### Chạy 2 lệnh này tại terminal máy Master để cập nhật code mới vào bên trong Pod:

```bash
# 1. Lấy tên Pod hiện tại
export MASTER_POD=$(kubectl get pods -n bigdata -l app=spark-master -o jsonpath='{.items[0].metadata.name}')

# 2. Copy ghi đè file vào trong Pod
kubectl cp spark_job.py bigdata/$MASTER_POD:/opt/spark/spark_job.py
```

`(Nếu lệnh chạy im lặng không báo lỗi gì là thành công).`

---

### **BƯỚC 3: Chạy Job (Spark Submit)**

### Chui vào Pod và chạy Submit:

```bash
# 1. Chui vào Pod
kubectl exec -it $MASTER_POD -n bigdata -- bash

# 2. Dán lệnh này để CHẠY (Bên trong Pod)
/opt/spark/bin/spark-submit \
  --master spark://spark-master-svc:7077 \
  --deploy-mode client \
  --name "IT Jobs Splitting" \
  --conf spark.driver.host=$(hostname -i) \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --driver-memory 512M \
  --executor-memory 2G \
  --executor-cores 2 \
  /opt/spark/spark_job.py
```

## **Update Code**

Nếu cần sửa lại code, hãy làm đúng thứ tự này:

- **Dừng Job**: Tại màn hình log đang chạy, bấm `Ctrl + C`.
- **Sửa File**: Sửa file `spark_job.py` ở máy Master
- **Copy lại**: Chạy lại **Bước 2**
- **Chạy lại**: Chạy lại **Bước 3**
