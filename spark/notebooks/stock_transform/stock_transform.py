# README:
# SPARK_APPLICATION_ARGS contains stock-market/AAPL/prices.json
# SPARK_APPLICATION_ARGS will be passed to the Spark application as an argument -e when running the Spark application from Airflow
# - Sometimes the script can stay stuck after "Passing arguments..."
# - Sometimes the script can stay stuck after "Successfully stopped SparkContext"
# - Sometimes the script can show "WARN TaskSchedulerImpl: Initial job has not accepted any resources; check your cluster UI to ensure that workers are registered and have sufficient resources"
# The easiest way to solve that is to restart your Airflow instance
# astro dev kill && astro dev start
# Also, make sure you allocated at least 8gb of RAM to Docker Desktop
# Go to Docker Desktop -> Preferences -> Resources -> Advanced -> Memory

# Import the SparkSession module
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, arrays_zip, from_unixtime
from pyspark.sql.types import DateType
import os
import sys

def main():
    try:
        # 从环境变量中获取 S3 路径
        s3_path = os.getenv("SPARK_APPLICATION_ARGS")  # e.g., "stock-market/NVDA"
        if not s3_path:
            raise ValueError("环境变量 SPARK_APPLICATION_ARGS 未设置")

        print(f"✅ 读取路径: s3a://{s3_path}")

        # 初始化 SparkSession
        spark = SparkSession.builder.appName("FormatStock") \
            .config("fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minio")) \
            .config("fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minio123")) \
            .config("fs.s3a.endpoint", os.getenv("ENDPOINT", "http://host.docker.internal:9000")) \
            .config("fs.s3a.connection.ssl.enabled", "false") \
            .config("fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.connection.timeout", "10000") \
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000") \
            .getOrCreate()

        # 读取原始 JSON 文件
        df = spark.read.option("multiline", "true").json(f"s3a://{s3_path}")
        print("✅ 完成 JSON 读取")

        # 提取并展开 timestamp 与 quote 数据
        df_exploded = df.select("timestamp", explode("indicators.quote").alias("quote")) \
            .select("timestamp", "quote.*")

        df_zipped = df_exploded.select(arrays_zip("timestamp", "close", "high", "low", "open", "volume").alias("zipped"))
        df_flat = df_zipped.select(explode("zipped").alias("row")).select(
            "row.timestamp", "row.close", "row.high", "row.low", "row.open", "row.volume"
        )
        df_final = df_flat.withColumn("date", from_unixtime("timestamp").cast(DateType())) \
                          .drop("timestamp") \
                          .select("date", "open", "high", "low", "close", "volume")

        print("✅ 数据清洗完成，开始写入 CSV")

        df_final.write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(f"s3a://{s3_path}/formatted_prices")

        print("✅ CSV 写入完成：s3a://{s3_path}/formatted_prices")
        spark.stop()
    except Exception as e:
        print(f"❌ Spark 任务失败: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
