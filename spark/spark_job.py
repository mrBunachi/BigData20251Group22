from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

# 1. Schema dữ liệu
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
    .add("Cách thức ứng tuyển", StringType()) \
    .add("ingest_time", StringType()) 

# 2. Khởi tạo Spark
spark = SparkSession.builder \
    .appName("IT Jobs Splitting") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio-service.bigdata.svc:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 3. Đọc từ Kafka
TOPICS = "itjobs_history,itjobs_live"

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-service:9092") \
    .option("subscribe", TOPICS) \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 200) \
    .load()

# 4. Parse JSON
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
    col("kafka_timestamp")
)

# 5. Tách luồng dữ liệu (Branching)

# Luồng 1: Dữ liệu lịch sử -> Folder /batch
df_history = df_parsed.filter(col("topic") == "itjobs_history").drop("topic")

# Luồng 2: Dữ liệu live -> Folder /streaming
df_live = df_parsed.filter(col("topic") == "itjobs_live").drop("topic")

print(f"Spark đang lắng nghe topics [{TOPICS}] và phân loại vào batch/streaming...")

# 6. Ghi xuống MinIO (Chạy 2 query song song)

# Query 1: Ghi History vào folder batch
query_history = df_history.writeStream \
    .format("json") \
    .option("path", "s3a://bucket1/batch/") \
    .option("checkpointLocation", "s3a://bucket1/checkpoints/history_batch/") \
    .outputMode("append") \
    .start()

# Query 2: Ghi Live vào folder streaming
query_live = df_live.writeStream \
    .format("json") \
    .option("path", "s3a://bucket1/streaming/") \
    .option("checkpointLocation", "s3a://bucket1/checkpoints/live_streaming/") \
    .outputMode("append") \
    .start()

# In kết quả ra màn hình log của Spark Driver
# query = df_parsed.writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .option("truncate", "false") \
#     .start()

# Chờ cả 2 luồng
spark.streams.awaitAnyTermination()