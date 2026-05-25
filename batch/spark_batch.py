from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql.functions import (
    broadcast, col, avg, min, max, sum as spark_sum,
    date_format, regexp_replace, to_timestamp,
    to_date, when, lit, lag, row_number, datediff, coalesce,
    udf
)
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
import os

#--- ENV CONFIF --- 
MINIO_USER = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "password123")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://35.240.199.161:9000")
ES_HOST = os.getenv("ES_HOST", "elasticsearch")
ES_INDEX_DAILY = "weather_batch_daily"
ES_INDEX_STATS = "weather_batch_stats"
HEATWAVE_THRESHOLD = 30.0


def main():
    print("Khởi động Batch Job: Kéo dữ liệu từ MinIO")
    print(f"Batch MinIO endpoint: {MINIO_ENDPOINT}")
    # 1. KHỞI TẠO SPARK VÀ CẤU HÌNH MINIO S3
    spark = SparkSession.builder \
        .appName("Weather-Batch-Layer") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", "200")

    @udf(StringType())
    def heat_level(avg_temp):
        if avg_temp is None:
            return "unknown"
        if avg_temp >= 40:
            return "cực kỳ nóng"
        if avg_temp >= 35:
            return "rất nóng"
        if avg_temp >= 30:
            return "nóng"
        if avg_temp >= 25:
            return "ấm"
        return "mát"

    # 2. ĐỌC DỮ LIỆU TỪ MINIO
    paths_to_read = [
        "s3a://raw-weather-data/topics/weather_data/*/*.json", # Data Streaming
        "s3a://raw-weather-data/historical/*"                          # Data Lịch sử (.jsonl)
    ]
    print(f"Batch input paths: {paths_to_read}")
    df = spark.read.option("mode", "DROPMALFORMED").json(paths_to_read)


    #Nếu không có address thì tìm resolvedAddress, nếu có cả 2 thì ưu tiên address
    if "address" in df.columns and "resolvedAddress" in df.columns:
        loc_col = coalesce(col("address"), col("resolvedAddress"))
    elif "address" in df.columns:
        loc_col = col("address")
    else:
        loc_col = col("resolvedAddress")
        
    if "currentConditions" in df.columns:
        df = df.select(
            loc_col.alias("resolvedAddress"),
            coalesce(col("currentConditions.datetime"), col("datetime")).alias("datetime"),
            coalesce(col("currentConditions.temp"), col("temp")).alias("temp"),
            coalesce(col("currentConditions.humidity"), col("humidity")).alias("humidity"),
            coalesce(col("currentConditions.precip"), col("precip")).alias("precip")
        )
    else:
        df = df.select(
            loc_col.alias("resolvedAddress"),
            col("datetime"),
            col("temp"),
            col("humidity"),
            col("precip")
        )

        
    # Chuẩn hóa cột thời gian và location
    df = df.withColumn("Local_Time", to_timestamp(col("datetime")))
    df = df.withColumn("Location", regexp_replace(col("resolvedAddress"), ", Việt Nam", ""))
    df = df.withColumn("date", date_format(col("Local_Time"), "yyyy-MM-dd"))

    province_mapping = [
        ("Hà Nội", "Hà Nội"),
        ("Thành phố Hồ Chí Minh", "TP HCM"),
        ("Sài Gòn", "TP HCM"),
        ("Hải Phòng", "Hải Phòng"),
        ("Đà Nẵng", "Đà Nẵng"),
    ]
    province_map_df = spark.createDataFrame(province_mapping, ["old_province", "new_province"])
    df = df.join(
        broadcast(province_map_df),
        df.Location == province_map_df.old_province,
        how="left"
    ).withColumn("Location", coalesce(col("new_province"), col("Location"))).drop("old_province", "new_province")

    # Lọc dữ liệu không hợp lệ
    df = df.filter(col("Local_Time").isNotNull() & col("Location").isNotNull())
    raw_count = df.count()
    print(f"Batch valid raw rows: {raw_count}")

    # Tạo daily aggregates để lưu theo ngày
    daily_df = df.groupBy("Location", "date").agg(
        avg("temp").alias("avg_temp"),
        min("temp").alias("min_temp"),
        max("temp").alias("max_temp"),
        avg("humidity").alias("avg_humidity"),
        spark_sum("precip").alias("total_precip"),
    ).persist(StorageLevel.MEMORY_AND_DISK)

    daily_df = daily_df.withColumn("temp_category", heat_level(col("avg_temp")))

    daily_count = daily_df.count()
    print(f"Batch daily rows: {daily_count}")

    daily_df.write \
        .format("org.elasticsearch.spark.sql") \
        .mode("overwrite") \
        .option("es.nodes", ES_HOST) \
        .option("es.port", "9200") \
        .option("es.resource", ES_INDEX_DAILY) \
        .save()

    pivot_df = daily_df.groupBy("Location").pivot("temp_category", ["cực kỳ nóng", "rất nóng", "nóng", "ấm", "mát"]).agg(spark_sum(lit(1)).alias("count")).fillna(0)

    unpivot_df = pivot_df.selectExpr(
        "Location",
        "stack(5, 'cực kỳ nóng', `cực kỳ nóng`, 'rất nóng', `rất nóng`, 'nóng', `nóng`, 'ấm', `ấm`, 'mát', `mát`) as (temp_category, category_count)"
    ).filter(col("category_count") > 0)
    unpivot_count = unpivot_df.count()
    print(f"Pivot -> Unpivot rows: {unpivot_count}")

    trend_df = daily_df.groupBy("Location").agg(
        avg("avg_temp").alias("avg_of_avg_temp"),
        spark_sum("total_precip").alias("sum_precip"),
    )
    sort_merge_df = daily_df.hint("merge").join(trend_df.hint("merge"), on="Location", how="inner")
    sort_merge_count = sort_merge_df.count()
    print(f"Sort-merge join rows: {sort_merge_count}")

    # Tính summary gồm nóng nhất, lạnh nhất, và đợt nóng dài nhất theo location
    window_loc = Window.partitionBy("Location").orderBy("date")
    daily_ordered = daily_df.withColumn("prev_date", lag("date").over(window_loc))
    daily_ordered = daily_ordered.withColumn(
        "continued_day",
        when(datediff(to_date(col("date")), to_date(col("prev_date"))) == 1, lit(1)).otherwise(lit(0)),
    )
    daily_ordered = daily_ordered.withColumn(
        "heat_day",
        when(col("avg_temp") >= HEATWAVE_THRESHOLD, lit(1)).otherwise(lit(0)),
    )
    daily_ordered = daily_ordered.withColumn(
        "heat_group",
        when(col("heat_day") == 1, when((col("heat_day") == 1) & (lag(col("heat_day")).over(window_loc) == 1) & (datediff(to_date(col("date")), to_date(col("prev_date"))) == 1), lit(0)).otherwise(lit(1))).otherwise(lit(0)),
    )
    daily_ordered = daily_ordered.withColumn(
        "group_id",
        spark_sum("heat_group").over(Window.partitionBy("Location").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)),
    )

    heatwave_df = daily_ordered.filter(col("heat_day") == 1).groupBy("Location", "group_id").agg(
        min("date").alias("start_date"),
        max("date").alias("end_date"),
        spark_sum("heat_day").alias("length_days"),
        max("max_temp").alias("max_temp"),
    )

    rank_hot = Window.partitionBy("Location").orderBy(col("max_temp").desc(), col("length_days").desc())
    heatwave_ranked = heatwave_df.withColumn("rank", row_number().over(rank_hot)).filter(col("rank") == 1)

    hottest = daily_df.withColumn("rank", row_number().over(Window.partitionBy("Location").orderBy(col("max_temp").desc(), col("avg_temp").desc()))).filter(col("rank") == 1).select(
        col("Location"),
        col("date").alias("hottest_date"),
        col("max_temp").alias("hottest_temp"),
    )

    coldest = daily_df.withColumn("rank", row_number().over(Window.partitionBy("Location").orderBy(col("min_temp").asc(), col("avg_temp").asc()))).filter(col("rank") == 1).select(
        col("Location"),
        col("date").alias("coldest_date"),
        col("min_temp").alias("coldest_temp"),
    )

    latest = daily_df.withColumn("rank", row_number().over(Window.partitionBy("Location").orderBy(col("date").desc()))).filter(col("rank") == 1).select(
        col("Location"),
        col("date").alias("latest_date"),
        col("avg_temp").alias("latest_avg_temp"),
    )

    summary_df = hottest.join(coldest, on="Location").join(latest, on="Location").join(
        heatwave_ranked.select(
            col("Location"),
            col("length_days").alias("longest_heatwave_days"),
            col("start_date").alias("heatwave_start"),
            col("end_date").alias("heatwave_end"),
            col("max_temp").alias("heatwave_max_temp"),
        ), on="Location", how="left",
    ).fillna({"longest_heatwave_days": 0, "heatwave_max_temp": 0.0})

    summary_count = summary_df.count()
    print(f"Batch summary rows: {summary_count}")

    summary_df.write \
        .format("org.elasticsearch.spark.sql") \
        .mode("overwrite") \
        .option("es.nodes", ES_HOST) \
        .option("es.port", "9200") \
        .option("es.resource", ES_INDEX_STATS) \
        .save()


if __name__ == "__main__":
    main()
