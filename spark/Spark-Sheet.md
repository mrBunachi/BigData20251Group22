# 🛠️ Cheat Sheet: Xử lý Spark

## **Lần đầu chạy Spark**

### **BƯỚC 1: Tạo nội dung cho file** `spark_job.py`

---

### **Bước 2: Apply file vào Pod (tạo mới/ghi đè)**

### Chạy 2 lệnh này tại terminal máy Master để cập nhật code mới vào bên trong Pod:

```bash
# 1. Lấy tên Pod hiện tại
export SPARK_POD=$(kubectl get pods -n bigdata -l app=spark-master -o jsonpath='{.items[0].metadata.name}')

# 2. Copy ghi đè file vào trong Pod
kubectl cp spark_job.py bigdata/$SPARK_POD:/opt/spark/work-dir/spark_job.py
```

`(Nếu lệnh chạy im lặng không báo lỗi gì là thành công).`

---

### **BƯỚC 3: Chạy Job (Spark Submit)**

### Chui vào Pod và chạy Submit:

```bash
# 1. Chui vào Pod
kubectl exec -it -n bigdata $SPARK_POD -- /bin/bash

# 2. Dán lệnh này để CHẠY (Bên trong Pod)
/opt/spark/bin/spark-submit \
--conf spark.jars.ivy=/tmp/.ivy \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.4 \
--driver-memory 800m \
--executor-memory 1g \
/opt/spark/work-dir/spark_job.py
```

## **Update Code**

Nếu cần sửa lại code, hãy làm đúng thứ tự này:

- **Dừng Job**: Tại màn hình log đang chạy, bấm `Ctrl + C`.
- **Sửa File**: Sửa file `spark_job.py` ở máy Master
- **Copy lại**: Chạy lại **Bước 2**
- **Chạy lại**: Chạy lại **Bước 3**
