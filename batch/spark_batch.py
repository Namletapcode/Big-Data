from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    min,
    max,
    sum as spark_sum,
    date_format,
    regexp_replace,
    to_timestamp,
    to_date,
    when,
    lit,
    lag,
    row_number,
    datediff,
)
from pyspark.sql.window import Window
import os

ES_HOST = os.getenv("ES_HOST", "elasticsearch")
ES_INDEX_DAILY = "weather_batch_daily"
ES_INDEX_STATS = "weather_batch_stats"
HEATWAVE_THRESHOLD = 30.0


def main():
    spark = SparkSession.builder \
        .appName("Weather-Batch-Layer") \
        .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1") \
        .getOrCreate()

    # Đọc dữ liệu từ Master Dataset (Data Lake)
    df = spark.read.json("/data_lake/*/*.jsonl")

    # Chuẩn hóa cột thời gian và location
    df = df.withColumn("Local_Time", to_timestamp(col("datetime")))
    df = df.withColumn("Location", regexp_replace(col("resolvedAddress"), ", Việt Nam", ""))
    df = df.withColumn("date", date_format(col("Local_Time"), "yyyy-MM-dd"))

    # Lọc dữ liệu không hợp lệ
    df = df.filter(col("Local_Time").isNotNull() & col("Location").isNotNull())

    # Tạo daily aggregates để lưu theo ngày
    daily_df = df.groupBy("Location", "date").agg(
        avg("temp").alias("avg_temp"),
        min("temp").alias("min_temp"),
        max("temp").alias("max_temp"),
        avg("humidity").alias("avg_humidity"),
        spark_sum("precip").alias("total_precip"),
    )

    daily_df.write \
        .format("org.elasticsearch.spark.sql") \
        .mode("overwrite") \
        .option("es.nodes", ES_HOST) \
        .option("es.port", "9200") \
        .option("es.resource", ES_INDEX_DAILY) \
        .save()

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

    summary_df.write \
        .format("org.elasticsearch.spark.sql") \
        .mode("overwrite") \
        .option("es.nodes", ES_HOST) \
        .option("es.port", "9200") \
        .option("es.resource", ES_INDEX_STATS) \
        .save()


if __name__ == "__main__":
    main()