from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, date_format, regexp_replace
import os

ES_HOST = os.getenv("ES_HOST", "elasticsearch")
ES_INDEX_BATCH = "weather_batch"

def main():
    spark = SparkSession.builder \
        .appName("Weather-Batch-Layer") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1") \
        .getOrCreate()
    
    # Đọc dữ liệu lịch sử từ file JSONL
    df = spark.read.json("*.jsonl")
    
    # Thêm cột Location từ resolvedAddress, loại bỏ "Vi\u1ec7t Nam"
    df = df.withColumn("Location", regexp_replace(col("resolvedAddress"), ", Việt Nam", ""))
    
    # Đổi tên cột datetime thành Local_Time để match
    df = df.withColumnRenamed("datetime", "Local_Time")
    
    # Tính trung bình temp hàng ngày theo location
    batch_df = df.groupBy(
        date_format(col("Local_Time"), "yyyy-MM-dd").alias("date"),
        col("Location")
    ).agg(avg("temp").alias("avg_temp"))
    
    # Ghi batch views vào ES
    batch_df.write \
        .format("org.elasticsearch.spark.sql") \
        .mode("overwrite") \
        .option("es.nodes", ES_HOST) \
        .option("es.port", "9200") \
        .option("es.resource", ES_INDEX_BATCH) \
        .save()

if __name__ == "__main__":
    main()