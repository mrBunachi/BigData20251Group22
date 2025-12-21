# 🛠️ Cheat Sheet: Xử lý Spark

## **Lần đầu chạy Spark**

### **BƯỚC 1: Tạo nội dung cho file** `spark_job.py`

---

### **Bước 2: Apply file vào Pod (tạo mới/ghi đè)**

### Chạy 2 lệnh này tại terminal máy Master để cập nhật code mới vào bên trong Pod:

```bash
# Lấy tên Master Pod và Worker Pod
export MASTER_POD=$(kubectl get pods -n bigdata -l app=spark-master -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it $MASTER_POD -n bigdata -- ls -l /opt/spark/jars/ | grep -E "aws-java-sdk-bundle|commons-pool2|hadoop-aws|kafka-clients|spark-sql-kafka|spark-token-provider"

export WORKER_POD=$(kubectl get pods -n bigdata -l app=spark-worker -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it $WORKER_POD -n bigdata -- ls -l /opt/spark/jars/ | grep -E "aws-java-sdk-bundle|commons-pool2|hadoop-aws|kafka-clients|spark-sql-kafka|spark-token-provider"

# 1. Copy ghi đè file vào trong Pod
# a. Job 1 (Đọc ghi MinIO)
kubectl cp spark_job.py bigdata/$MASTER_POD:/opt/spark/spark_job.py

# b. Job 2 (Xử lý batch)
kubectl cp spark_job_batch.py bigdata/$MASTER_POD:/opt/spark/spark_job_batch.py

# c. Job 3 (Xử lý Stream)
kubectl cp spark_job_stream.py bigdata/$MASTER_POD:/opt/spark/spark_job_stream.py
```

`(Nếu lệnh chạy im lặng không báo lỗi gì là thành công).`

---

### **BƯỚC 3: Chạy Job (Spark Submit)**

### Chui vào Pod và chạy Submit:

```bash
# QUAN TRỌNG: Cấp quyền ghi cho spark worker (tại máy cài spark)
sudo chmod -R 777 /tmp/spark-worker-data

# 1. Vào Pod Master
export MASTER_POD=$(kubectl get pods -n bigdata -l app=spark-master -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it $MASTER_POD -n bigdata -- bash

# 2. Khai báo biến chứa danh sách Jars
JARS="/opt/spark/jars/kafka-clients-3.4.1.jar,\
/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,\
/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,\
/opt/spark/jars/commons-pool2-2.11.1.jar,\
/opt/spark/jars/hadoop-aws-3.3.4.jar,\
/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"

# 3. Chạy lệnh Submit với tham số --jars
/opt/spark/bin/spark-submit \
  --master spark://spark-master-svc:7077 \
  --deploy-mode client \
  --name "IT Jobs Splitting" \
  --jars $JARS \
  --conf spark.driver.host=$(hostname -i) \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --driver-memory 512M \
  --executor-memory 2G \
  --executor-cores 2 \
  /opt/spark/spark_job.py

# 4. Submit Job Batch (Chỉ dùng 1 Core)
/opt/spark/bin/spark-submit \
  --master spark://spark-master-svc:7077 \
  --deploy-mode client \
  --name "IT Jobs Batch ETL" \
  --jars $JARS \
  --conf spark.driver.host=$(hostname -i) \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --driver-memory 512M \
  --executor-memory 2G \
  --executor-cores 1 \
  /opt/spark/spark_job_batch.py

# 5. Submit Job Streaming
/opt/spark/bin/spark-submit \
  --master spark://spark-master-svc:7077 \
  --deploy-mode client \
  --name "IT Jobs Stream (MinIO + Mongo)" \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 \
  --jars $JARS \
  --conf spark.driver.host=$(hostname -i) \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --driver-memory 512M \
  --executor-memory 2G \
  --executor-cores 2 \
  /opt/spark/spark_job_stream.py
```

## **Update Code**

Nếu cần sửa lại code, hãy làm đúng thứ tự này:

- **Dừng Job**: Tại màn hình log đang chạy, bấm `Ctrl + C`.
- **Sửa File**: Sửa file `spark_job.py` ở máy Master
- **Copy lại**: Chạy lại **Bước 2**
- **Chạy lại**: Chạy lại **Bước 3**
