import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, from_json
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, LongType
import re

# 1. LOGIC LÀM SẠCH
def clean_salary_logic(salary_str):
    if salary_str is None or str(salary_str).strip() == "" or salary_str == "Thoả thuận":
        return None, None, "VND"
    s = str(salary_str).lower().replace(",", "").replace(".", "")
    currency = "USD" if ("usd" in s or "$" in s) else "VND"
    numbers = [int(n) for n in re.findall(r'\d+', s)]
    min_sal, max_sal = 0, 0
    if len(numbers) >= 2:
        min_sal, max_sal = numbers[0], numbers[1]
    elif len(numbers) == 1:
        if "tới" in s or "up to" in s: max_sal = numbers[0]
        elif "từ" in s: min_sal = numbers[0]
        else: max_sal = numbers[0]
    if currency == "VND":
        if 0 < min_sal < 1000: min_sal *= 1000000
        if 0 < max_sal < 1000: max_sal *= 1000000
    final_min = int(min_sal) if min_sal > 0 else None
    final_max = int(max_sal) if max_sal > 0 else None
    return final_min, final_max, currency

def clean_experience_logic(exp_str):
    if exp_str is None or "không yêu cầu" in str(exp_str).lower():
        return 0
    numbers = re.findall(r'\d+', str(exp_str))
    return int(numbers[0]) if numbers else 0

salary_schema = StructType([
    StructField("min_salary", LongType(), True),
    StructField("max_salary", LongType(), True),
    StructField("currency", StringType(), True)
])
clean_salary_udf = udf(clean_salary_logic, salary_schema)
clean_exp_udf = udf(clean_experience_logic, IntegerType())

# 2. CẤU HÌNH MONGODB
MONGO_URI = "mongodb+srv://bigData:bigGroup22@bigdata.uaojt2r.mongodb.net/?retryWrites=true&w=majority&connectTimeoutMS=30000&socketTimeoutMS=30000&serverSelectionTimeoutMS=30000"
MONGO_DB = "serving"
MONGO_COLLECTION = "jobs_realtime"

def create_spark_session():
    builder = SparkSession.builder \
        .appName("IT Jobs Kafka -> MinIO & MongoDB") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio-service.bigdata.svc:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.mongodb.connection.uri", MONGO_URI) \
        .config("spark.mongodb.database", MONGO_DB) \
        .config("spark.mongodb.collection", MONGO_COLLECTION)
    return builder.getOrCreate()

# 3. HÀM GHI ĐA ĐÍCH (FOREACHBATCH)
def write_to_sinks(batch_df, batch_id):
    """
    Hàm này sẽ được gọi cho mỗi Micro-Batch.
    Tại đây chúng ta ghi DataFrame vào cả MinIO và MongoDB.
    """
    # Cache lại vì chúng ta sẽ dùng DF này 2 lần (cho 2 đích đến)
    batch_df.persist()
    
    count = batch_df.count()
    if count > 0:
        print(f"--- Processing Batch ID: {batch_id} with {count} records ---")
        
        # 1. Ghi vào MinIO (Parquet)
        try:
            batch_df.write \
                .format("parquet") \
                .mode("append") \
                .save("s3a://bucket2/streaming/")
            print("   [MinIO] Write Success.")
        except Exception as e:
            print(f"   [MinIO] Write Failed: {e}")

        # 2. Ghi vào MongoDB
        try:
            batch_df.write \
                .format("mongodb") \
                .mode("append") \
                .option("database", MONGO_DB) \
                .option("uri", MONGO_URI) \
                .option("collection", MONGO_COLLECTION) \
                .save()
            print("   [MongoDB] Write Success.")
        except Exception as e:
            print(f"   [MongoDB] Write Failed: {e}")
            
    batch_df.unpersist()

# 4. MAIN
def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Schema (Kafka Raw)
    raw_schema = StructType() \
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

    print(">>> [STREAMING] Listening Kafka -> MinIO + MongoDB...")

    # Đọc Kafka
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka-service:9092") \
        .option("subscribe", "itjobs_live") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 500) \
        .load()

    # Parse & Rename
    df_parsed = df_kafka.select(
        from_json(col("value").cast("string"), raw_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select(
        col("data.`Tên công việc`").alias("job_title"),
        col("data.`Tên công ty`").alias("company"),
        col("data.`Mức lương`").alias("raw_salary"),
        col("data.`Địa điểm`").alias("location"),
        col("data.`Kinh nghiệm`").alias("raw_experience"),
        col("data.`Mô tả công việc`").alias("job_description"),
        col("data.`Yêu cầu ứng viên`").alias("requirements"),
        col("data.`Quyền lợi`").alias("benefits"),
        f.coalesce(col("data.`Địa điểm làm việc`"), col("data.`Địa điểm làm việc(đã được cập nhật theo Danh mục Hành chính mới)`")).alias("workplace"),
        col("data.`Thời gian làm việc`").alias("working_time"),
        col("data.`Cách thức ứng tuyển`").alias("apply_method"),
        col("kafka_timestamp")
    )

    # Làm sạch (Transform)
    df_cleaned = df_parsed.withColumn("salary_info", clean_salary_udf(col("raw_salary"))) \
                          .withColumn("clean_exp", clean_exp_udf(col("raw_experience")))

    # Chọn cột cuối cùng
    final_df = df_cleaned.select(
        col("job_title"),
        col("company"),
        col("location"),
        col("salary_info.min_salary").alias("min_salary"),
        col("salary_info.max_salary").alias("max_salary"),
        col("salary_info.currency").alias("currency"),
        col("clean_exp").alias("years_of_experience"),
        col("job_description"),
        col("requirements"),
        col("benefits"),
        col("workplace"),
        col("working_time"),
        col("apply_method"),
        col("raw_salary"),
        col("raw_experience"),
        col("kafka_timestamp").alias("ingested_at")
    )

    # KÍCH HOẠT STREAM VỚI FOREACHBATCH
    query = final_df.writeStream \
        .foreachBatch(write_to_sinks) \
        .option("checkpointLocation", "s3a://bucket2/checkpoints/streaming_kafka_mongo_v1/") \
        .trigger(processingTime='60 seconds') \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()