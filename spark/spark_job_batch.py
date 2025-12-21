import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, LongType
import re

# 1. LOGIC LÀM SẠCH (UDF)
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

# 2. KHỞI TẠO SPARK 
def create_spark_session():
    builder = SparkSession.builder \
        .appName("IT Jobs Batch ETL") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio-service.bigdata.svc:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.sql.parquet.compression.codec", "snappy")
    return builder.getOrCreate()

# 3. MAIN
def main():
    spark = create_spark_session()
    print(">>> [BATCH JOB] Đang đọc dữ liệu từ MinIO Bucket 1...")

    try:
        # Đọc dữ liệu từ Bucket 1 (Lúc này dữ liệu ĐÃ LÀ TIẾNG ANH do spark_job.py tạo ra)
        raw_df = spark.read.json("s3a://bucket1/batch/*.json")
    except Exception as e:
        print(f"!!! Lỗi đọc file: {e}")
        spark.stop()
        return

    print(">>> [BATCH JOB] Đang xử lý và làm sạch dữ liệu...")
    
    # 1. Áp dụng UDF làm sạch (Dùng cột tiếng Anh: salary, experience)
    df_cleaned = raw_df.withColumn("salary_info", clean_salary_udf(col("salary"))) \
                       .withColumn("clean_exp", clean_exp_udf(col("experience")))

    # 2. Chọn cột output cuối cùng (Chuẩn hóa Schema)
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
        col("workplace"),      # Cột này đã được spark_job.py tạo ra
        col("working_time"),
        col("apply_method"),
        col("salary").alias("raw_salary"),
        col("experience").alias("raw_experience"),
        col("kafka_timestamp").alias("ingested_at")
    )

    # 3. Ghi xuống Bucket 2
    dest_path = "s3a://bucket2/batch/"
    print(f">>> [BATCH JOB] Đang ghi dữ liệu sạch xuống {dest_path}...")
    final_df.write.mode("overwrite").parquet(dest_path)
    
    print(">>> [BATCH JOB] HOÀN TẤT!")
    spark.stop()

if __name__ == "__main__":
    main()