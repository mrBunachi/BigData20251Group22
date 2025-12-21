import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, LongType
import re

# ================= 1. LOGIC LÀM SẠCH (UDF) - GIỐNG HỆT BATCH =================
def clean_salary_logic(salary_str):
    if salary_str is None or str(salary_str).strip() == "" or salary_str == "Thoả thuận":
        return 0, 0, "VND"
    
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
            
    return int(min_sal), int(max_sal), currency

def clean_experience_logic(exp_str):
    if exp_str is None or "không yêu cầu" in str(exp_str).lower():
        return 0
    numbers = re.findall(r'\d+', str(exp_str))
    return int(numbers[0]) if numbers else 0

# Đăng ký UDF
salary_schema = StructType([
    StructField("min_salary", LongType(), False),
    StructField("max_salary", LongType(), False),
    StructField("currency", StringType(), False)
])

clean_salary_udf = udf(clean_salary_logic, salary_schema)
clean_exp_udf = udf(clean_experience_logic, IntegerType())

# ================= 2. KHỞI TẠO SPARK =================
def create_spark_session():
    builder = SparkSession.builder \
        .appName("IT Jobs Streaming Processor") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio-service.bigdata.svc:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.parquet.compression.codec", "snappy")
    return builder.getOrCreate()

# ================= 3. MAIN =================
def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # ĐỊNH NGHĨA SCHEMA CHO DỮ LIỆU ĐẦU VÀO (BẮT BUỘC VỚI STREAMING)
    # Phải khớp với các cột tiếng Anh mà spark_job.py đã tạo ra trong bucket1
    input_schema = StructType([
        StructField("job_title", StringType(), True),
        StructField("company_name", StringType(), True),
        StructField("salary", StringType(), True),
        StructField("location", StringType(), True),
        StructField("experience", StringType(), True),
        StructField("job_description", StringType(), True),
        StructField("requirements", StringType(), True),
        StructField("benefits", StringType(), True),
        StructField("workplace", StringType(), True),
        StructField("working_time", StringType(), True),
        StructField("apply_method", StringType(), True),
        StructField("kafka_timestamp", StringType(), True)
    ])

    print(">>> [STREAMING JOB] Đang lắng nghe file mới từ Bucket 1 (Streaming)...")

    # 1. Đọc Stream từ MinIO Bucket 1
    raw_df = spark.readStream \
        .schema(input_schema) \
        .json("s3a://bucket1/streaming/")

    # 2. Transform (Làm sạch) - Logic y hệt Batch
    df_cleaned = raw_df.withColumn("salary_info", clean_salary_udf(col("salary"))) \
                       .withColumn("clean_exp", clean_exp_udf(col("experience")))

    final_df = df_cleaned.select(
        col("job_title"),
        col("company_name").alias("company"),
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
        col("salary").alias("raw_salary"),
        col("experience").alias("raw_experience"),
        col("kafka_timestamp").alias("ingested_at")
    )

    # 3. Ghi Stream xuống Bucket 2 (Parquet)
    # Sử dụng Checkpoint khác để không xung đột với job nào khác
    print(">>> [STREAMING JOB] Đang xử lý và ghi sang Bucket 2...")
    
    query = final_df.writeStream \
        .format("parquet") \
        .option("path", "s3a://bucket2/streaming/") \
        .option("checkpointLocation", "s3a://bucket2/checkpoints/streaming_processing/") \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()