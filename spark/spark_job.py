import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StringType

# 1. Schema dữ liệu (TIẾNG VIỆT - Khớp 100% với Kafka)
job_schema = StructType() \
    .add("Tên công việc", StringType()) \
    .add("Tên công ty", StringType()) \
    .add("Mức lương", StringType()) \
    .add("Địa điểm", StringType()) \
    .add("Kinh nghiệm", StringType()) \
    .add("Mô tả công việc", StringType()) \
    .add("Yêu cầu ứng viên", StringType()) \
    .add("Quyền lợi", StringType()) \
    .add("Địa điểm làm việc", StringType()) \
    .add("Địa điểm làm việc(đã được cập nhật theo Danh mục Hành chính mới)", StringType()) \
    .add("Thời gian làm việc", StringType()) \
    .add("Cách thức ứng tuyển", StringType()) \
    .add("ingest_time", StringType())

# 2. Khởi tạo Spark
spark = SparkSession.builder \
    .appName("IT Jobs Ingestion") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio-service.bigdata.svc:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 3. Đọc từ Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-service:9092") \
    .option("subscribe", "itjobs_history,itjobs_live") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 500) \
    .load()

# 4. Parse & Rename (QUAN TRỌNG: Đổi tên cột tại đây)
df_parsed = df_kafka.select(
    col("topic"),
    from_json(col("value").cast("string"), job_schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select(
    col("topic"),
    col("data.`Tên công việc`").alias("job_title"),
    col("data.`Tên công ty`").alias("company_name"),
    col("data.`Mức lương`").alias("salary"),
    col("data.`Địa điểm`").alias("location"),
    col("data.`Kinh nghiệm`").alias("experience"),
    col("data.`Mô tả công việc`").alias("job_description"),
    col("data.`Yêu cầu ứng viên`").alias("requirements"),
    col("data.`Quyền lợi`").alias("benefits"),
    # Xử lý trường hợp có 2 loại cột địa điểm (ưu tiên cột mới nếu có)
    f.coalesce(col("data.`Địa điểm làm việc`"), col("data.`Địa điểm làm việc(đã được cập nhật theo Danh mục Hành chính mới)`")).alias("workplace"),
    col("data.`Thời gian làm việc`").alias("working_time"),
    col("data.`Cách thức ứng tuyển`").alias("apply_method"),
    col("kafka_timestamp")
)

# 5. Tách luồng và ghi MinIO

# (Batch History) - Chạy hết data hiện có rồi tự dừng
query_history = df_parsed.filter(col("topic") == "itjobs_history") \
    .writeStream \
    .format("json") \
    .option("path", "s3a://bucket1/batch/") \
    .option("checkpointLocation", "s3a://bucket1/checkpoints/history_batch/") \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .start()

# (Live Streaming) - Chạy vĩnh viễn
query_live = df_parsed.filter(col("topic") == "itjobs_live") \
    .writeStream \
    .format("json") \
    .option("path", "s3a://bucket1/streaming/") \
    .option("checkpointLocation", "s3a://bucket1/checkpoints/live_streaming/") \
    .outputMode("append") \
    .start()

print("--> Đang chạy Ingestion (Spark Job)...")

# Chờ luồng Live chạy
try:
    query_live.awaitTermination()
except Exception as e:
    print(f"Error: {e}")