from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StringType

# 1. Schema dữ liệu
job_schema = StructType() \
    .add("Tên công ty", StringType()) \
    .add("Mức lương", StringType()) \
    .add("Địa điểm", StringType()) \
    .add("Kinh nghiệm", StringType()) \
    .add("Mô tả công việc", StringType()) \
    .add("Yêu cầu ứng viên", StringType()) \
    .add("Quyền lợi", StringType()) \
    .add("Địa điểm làm việc", StringType()) \
    .add("Cách thức ứng tuyển", StringType()) \
    .add("data_type", StringType()) \
    .add("ingest_time", StringType()) # Trường này do Producer thêm vào

# 2. Khởi tạo Spark
spark = SparkSession.builder \
    .appName("IT Jobs Processing") \
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
    .load()

# 4. Parse JSON
df_parsed = df_kafka.select(
    from_json(col("value").cast("string"), job_schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select(
    col("data.`Tên công ty`").alias("company_name"),
    col("data.`Mức lương`").alias("salary"),
    col("data.`Địa điểm`").alias("location"),
    col("data.`Kinh nghiệm`").alias("experience"),
    col("data.`Mô tả công việc`").alias("job_description"),
    col("data.data_type"),
    col("kafka_timestamp")
)

# 5. Ghi kết quả
# In kết quả ra màn hình log của Spark Driver
query = df_parsed.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()

print(f"Spark đang lắng nghe topics [{TOPICS}] và ghi ra Console...")
query.awaitTermination()