import logging
import os

from elasticsearch import Elasticsearch
from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    add_months,
    array,
    array_union,
    avg,
    broadcast,
    coalesce,
    col,
    countDistinct,
    date_format,
    datediff,
    explode,
    lag,
    lit,
    max,
    min,
    regexp_replace,
    row_number,
    sum as spark_sum,
    to_date,
    to_timestamp,
    trim,
    udf,
    when,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from pyspark.sql.window import Window


MINIO_USER = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "password123")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://35.240.139.79:9000")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "raw-weather-data")
HISTORICAL_PREFIX = os.getenv("HISTORICAL_PREFIX", "historical")
HISTORICAL_FOLDERS = [
    folder.strip()
    for folder in os.getenv("HISTORICAL_FOLDERS", "34,29").split(",")
    if folder.strip()
]
LOG_BATCH_COUNTS = os.getenv("LOG_BATCH_COUNTS", "false").lower() == "true"
PROVINCE_MERGE_CUTOFF_DATE = os.getenv("PROVINCE_MERGE_CUTOFF_DATE", "2025-07-01")
ES_HOST = os.getenv("ES_HOST", "elasticsearch")
ES_INDEX_DAILY = "weather_batch_daily"
ES_INDEX_STATS = "weather_batch_stats"
ES_INDEX_YOY = "weather_batch_yoy"
HEATWAVE_THRESHOLD = 30.0
DEFAULT_PROVINCE_MAPPING_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "province_mapping_63_to_34.json"
)

DAILY_HEAT_SCHEMA = StructType(
    [
        StructField("temp_category", StringType(), False),
        StructField("heat_alert_level", StringType(), False),
    ]
)
RAW_WEATHER_SCHEMA = StructType(
    [
        StructField("address", StringType(), True),
        StructField("resolvedAddress", StringType(), True),
        StructField("datetime", StringType(), True),
        StructField("temp", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("precip", DoubleType(), True),
        StructField(
            "currentConditions",
            StructType(
                [
                    StructField("datetime", StringType(), True),
                    StructField("temp", DoubleType(), True),
                    StructField("humidity", DoubleType(), True),
                    StructField("precip", DoubleType(), True),
                ]
            ),
            True,
        ),
    ]
)


def classify_daily_heat(avg_temp, max_temp, avg_humidity):
    if avg_temp is None:
        temp_category = "unknown"
    elif avg_temp >= 40:
        temp_category = "cực kỳ nóng"
    elif avg_temp >= 35:
        temp_category = "rất nóng"
    elif avg_temp >= 30:
        temp_category = "nóng"
    elif avg_temp >= 25:
        temp_category = "ấm"
    else:
        temp_category = "mát"

    if avg_temp is None or max_temp is None:
        alert = "unknown"
    elif max_temp >= 40 or (avg_temp >= 35 and avg_humidity is not None and avg_humidity >= 70):
        alert = "extreme"
    elif max_temp >= 35 or avg_temp >= 30:
        alert = "high"
    elif avg_temp >= 27 and avg_humidity is not None and avg_humidity >= 70:
        alert = "caution"
    else:
        alert = "normal"

    return temp_category, alert


classify_daily_heat_udf = udf(classify_daily_heat, DAILY_HEAT_SCHEMA)


def normalize_location_column(location_col):
    without_country = regexp_replace(location_col, r",\s*(VN|Việt Nam)\s*$", "")
    without_prefix = regexp_replace(without_country, r"^(Tỉnh|Thành phố)\s+", "")
    return trim(regexp_replace(without_prefix, r"\s+", " "))


def load_province_mapping(spark: SparkSession, mapping_path: str) -> DataFrame:
    source_df = spark.read.option("multiline", "true").json(mapping_path)
    required_columns = {"old_province", "canonical_province", "aliases"}
    if not required_columns.issubset(source_df.columns):
        raise ValueError(f"Province mapping must contain columns: {sorted(required_columns)}")

    old_count = source_df.select("old_province").distinct().count()
    canonical_count = source_df.select("canonical_province").distinct().count()
    if old_count != 63 or canonical_count != 34:
        raise ValueError(
            f"Province mapping must contain 63 old and 34 canonical provinces; "
            f"found {old_count} and {canonical_count}"
        )

    mapping_df = source_df.select(
        explode(array_union(array(col("old_province")), col("aliases"))).alias("source_province"),
        col("old_province"),
        col("canonical_province"),
    ).dropDuplicates(["source_province", "canonical_province"])
    ambiguous_sources = (
        mapping_df.groupBy("source_province")
        .agg(countDistinct("canonical_province").alias("target_count"))
        .filter(col("target_count") > 1)
        .count()
    )
    if ambiguous_sources:
        raise ValueError("Province mapping aliases must resolve to only one canonical province")
    return mapping_df


def apply_province_mapping(raw_df: DataFrame, mapping_df: DataFrame, cutoff_date: str = PROVINCE_MERGE_CUTOFF_DATE) -> DataFrame:
    normalized_df = raw_df.withColumn(
        "_normalized_province", normalize_location_column(col("resolvedAddress"))
    )
    joined_df = normalized_df.join(
        broadcast(mapping_df),
        normalized_df["_normalized_province"] == mapping_df["source_province"],
        how="left",
    )
    if LOG_BATCH_COUNTS:
        unmatched_count = (
            joined_df.filter(col("canonical_province").isNull())
            .select("_normalized_province")
            .distinct()
            .count()
        )
        logger.info("Batch unmatched normalized locations: %d", unmatched_count)

    old_location = coalesce(col("old_province"), col("_normalized_province"))
    canonical_location = coalesce(col("canonical_province"), col("_normalized_province"))
    if "date" in raw_df.columns:
        location_col = when(to_date(col("date")) < to_date(lit(cutoff_date)), old_location).otherwise(canonical_location)
        province_view_col = when(
            to_date(col("date")) < to_date(lit(cutoff_date)),
            lit("pre_merge_63"),
        ).otherwise(lit("post_merge_34"))
    else:
        location_col = canonical_location
        province_view_col = lit("post_merge_34")

    return (
        joined_df.withColumn("Location", location_col)
        .withColumn("old_location", old_location)
        .withColumn("canonical_location", canonical_location)
        .withColumn("province_view", province_view_col)
        .drop("_normalized_province", "source_province", "old_province", "canonical_province")
    )


def build_daily_aggregates(valid_df: DataFrame) -> DataFrame:
    group_columns = ["Location", "date"]
    if "province_view" in valid_df.columns:
        group_columns.append("province_view")

    daily_df = valid_df.groupBy(*group_columns).agg(
        avg("temp").alias("avg_temp"),
        min("temp").alias("min_temp"),
        max("temp").alias("max_temp"),
        avg("humidity").alias("avg_humidity"),
        spark_sum("precip").alias("total_precip"),
    )
    classified_df = daily_df.withColumn(
        "_heat", classify_daily_heat_udf(col("avg_temp"), col("max_temp"), col("avg_humidity"))
    )
    return (
        classified_df.withColumn("temp_category", col("_heat.temp_category"))
        .withColumn("heat_alert_level", col("_heat.heat_alert_level"))
        .drop("_heat")
    )


def build_yoy_comparison(daily_df: DataFrame) -> DataFrame:
    current_daily = daily_df.select(
        "Location",
        to_date(col("date")).alias("_join_date"),
        col("avg_temp"),
        col("avg_humidity"),
        col("total_precip"),
        col("heat_alert_level"),
    )
    previous_year_daily = (
        daily_df.select(
            "Location",
            col("date").alias("previous_date"),
            to_date(col("date")).alias("_previous_date"),
            add_months(to_date(col("date")), 12).alias("_join_date"),
            col("avg_temp").alias("previous_avg_temp"),
            col("avg_humidity").alias("previous_avg_humidity"),
            col("total_precip").alias("previous_total_precip"),
        )
        .filter(date_format(col("_previous_date"), "MM-dd") == date_format(col("_join_date"), "MM-dd"))
        .drop("_previous_date")
    )
    joined_df = current_daily.hint("merge").join(
        previous_year_daily.hint("merge"), on=["Location", "_join_date"], how="inner"
    )
    return joined_df.select(
        "Location",
        date_format(col("_join_date"), "yyyy-MM-dd").alias("date"),
        "previous_date",
        "avg_temp",
        "avg_humidity",
        "total_precip",
        "heat_alert_level",
        "previous_avg_temp",
        "previous_avg_humidity",
        "previous_total_precip",
        (col("avg_temp") - col("previous_avg_temp")).alias("delta_avg_temp"),
        (col("avg_humidity") - col("previous_avg_humidity")).alias("delta_avg_humidity"),
        (col("total_precip") - col("previous_total_precip")).alias("delta_total_precip"),
    )


def ensure_elasticsearch_index(index: str) -> None:
    es_url = ES_HOST if ES_HOST.startswith(("http://", "https://")) else f"http://{ES_HOST}:9200"
    client = Elasticsearch(es_url)
    if not client.indices.exists(index=index):
        client.indices.create(index=index)
    client.cluster.health(index=index, wait_for_status="yellow", timeout="30s")


def write_to_elasticsearch(df: DataFrame, index: str) -> None:
    # ES-Hadoop can race automatic index creation on the first distributed write.
    ensure_elasticsearch_index(index)
    (
        df.write.format("org.elasticsearch.spark.sql")
        .mode("overwrite")
        .option("es.nodes", ES_HOST)
        .option("es.port", "9200")
        .option("es.resource", index)
        .save()
    )


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("spark_batch")


def main():
    logger.info("========== Khởi động Batch Job: Kéo dữ liệu từ MinIO ==========")
    logger.info("MinIO endpoint  : %s", MINIO_ENDPOINT)
    logger.info("MinIO user      : %s", MINIO_USER)
    spark = (
        SparkSession.builder.appName("Weather-Batch-Layer")
        # hadoop-aws JAR: bắt buộc khi chạy local (trong Docker đã được bundled sẵn)
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    logger.info("SparkSession khởi tạo thành công (app: %s)", spark.sparkContext.appName)

    paths_to_read = [
        f"s3a://{MINIO_BUCKET}/{HISTORICAL_PREFIX}/{folder}/*"
        for folder in HISTORICAL_FOLDERS
    ]
    logger.info("Bắt đầu đọc dữ liệu từ MinIO — %d path(s):", len(paths_to_read))
    for p in paths_to_read:
        logger.info("  • %s", p)

    try:
        df = spark.read.schema(RAW_WEATHER_SCHEMA).option("mode", "DROPMALFORMED").json(paths_to_read)
    except Exception as exc:  # noqa: BLE001
        logger.error("[MinIO READ] ✗ Không thể đọc dữ liệu từ MinIO. Lỗi: %s", exc)
        logger.error(
            "  → Kiểm tra lại: endpoint (%s), credentials, bucket/path tồn tại hay không.",
            MINIO_ENDPOINT,
        )
        raise

    if LOG_BATCH_COUNTS:
        total_rows = df.count()
        logger.info("[MinIO READ] Tổng số row sau khi đọc tất cả path: %d", total_rows)
    else:
        logger.info("[MinIO READ] Đã tạo DataFrame từ MinIO; bỏ qua count nguồn để job chạy nhanh hơn.")

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
            coalesce(col("currentConditions.precip"), col("precip")).alias("precip"),
        )
    else:
        df = df.select(
            loc_col.alias("resolvedAddress"),
            col("datetime"),
            col("temp"),
            col("humidity"),
            col("precip"),
        )

    df = df.withColumn("Local_Time", to_timestamp(col("datetime")))
    df = df.withColumn("date", date_format(col("Local_Time"), "yyyy-MM-dd"))
    mapping_path = os.getenv("PROVINCE_MAPPING_PATH", DEFAULT_PROVINCE_MAPPING_PATH)
    df = apply_province_mapping(df, load_province_mapping(spark, mapping_path))

    valid_df = df.filter(col("Local_Time").isNotNull() & col("Location").isNotNull())
    if LOG_BATCH_COUNTS:
        valid_count = valid_df.count()
        logger.info("Batch valid raw rows (sau filter): %d", valid_count)

    daily_df = build_daily_aggregates(valid_df).persist(StorageLevel.MEMORY_AND_DISK)
    daily_count = daily_df.count()
    logger.info("Batch daily aggregate rows: %d", daily_count)
    write_to_elasticsearch(daily_df, ES_INDEX_DAILY)
    logger.info("Đã ghi daily data vào ES index '%s'.", ES_INDEX_DAILY)

    yoy_df = build_yoy_comparison(daily_df)
    yoy_count = yoy_df.count()
    logger.info("Batch YoY rows: %d", yoy_count)
    write_to_elasticsearch(yoy_df, ES_INDEX_YOY)
    logger.info("Đã ghi YoY data vào ES index '%s'.", ES_INDEX_YOY)

    summary_group = ["Location"]
    if "province_view" in daily_df.columns:
        summary_group.append("province_view")

    window_loc = Window.partitionBy(*summary_group).orderBy("date")
    daily_ordered = daily_df.withColumn("prev_date", lag("date").over(window_loc))
    daily_ordered = daily_ordered.withColumn(
        "heat_day", when(col("avg_temp") >= HEATWAVE_THRESHOLD, lit(1)).otherwise(lit(0))
    )
    daily_ordered = daily_ordered.withColumn(
        "heat_group",
        when(
            col("heat_day") == 1,
            when(
                (lag(col("heat_day")).over(window_loc) == 1)
                & (datediff(to_date(col("date")), to_date(col("prev_date"))) == 1),
                lit(0),
            ).otherwise(lit(1)),
        ).otherwise(lit(0)),
    )
    daily_ordered = daily_ordered.withColumn(
        "group_id",
        spark_sum("heat_group").over(
            Window.partitionBy("Location")
            .orderBy("date")
            .rowsBetween(Window.unboundedPreceding, 0)
        ),
    )

    heatwave_df = daily_ordered.filter(col("heat_day") == 1).groupBy(*summary_group, "group_id").agg(
        min("date").alias("start_date"),
        max("date").alias("end_date"),
        spark_sum("heat_day").alias("length_days"),
        max("max_temp").alias("max_temp"),
    )
    rank_hot = Window.partitionBy(*summary_group).orderBy(col("max_temp").desc(), col("length_days").desc())
    heatwave_ranked = heatwave_df.withColumn("rank", row_number().over(rank_hot)).filter(col("rank") == 1)

    hottest = (
        daily_df.withColumn(
            "rank",
            row_number().over(Window.partitionBy(*summary_group).orderBy(col("max_temp").desc(), col("avg_temp").desc())),
        )
        .filter(col("rank") == 1)
        .select(*[col(c) for c in summary_group], col("date").alias("hottest_date"), col("max_temp").alias("hottest_temp"))
    )
    coldest = (
        daily_df.withColumn(
            "rank",
            row_number().over(Window.partitionBy(*summary_group).orderBy(col("min_temp").asc(), col("avg_temp").asc())),
        )
        .filter(col("rank") == 1)
        .select(*[col(c) for c in summary_group], col("date").alias("coldest_date"), col("min_temp").alias("coldest_temp"))
    )
    latest = (
        daily_df.withColumn("rank", row_number().over(Window.partitionBy(*summary_group).orderBy(col("date").desc())))
        .filter(col("rank") == 1)
        .select(*[col(c) for c in summary_group], col("date").alias("latest_date"), col("avg_temp").alias("latest_avg_temp"))
    )
    summary_df = (
        hottest.join(coldest, on=summary_group)
        .join(latest, on=summary_group)
        .join(
            heatwave_ranked.select(
                *[col(c) for c in summary_group],
                col("length_days").alias("longest_heatwave_days"),
                col("start_date").alias("heatwave_start"),
                col("end_date").alias("heatwave_end"),
                col("max_temp").alias("heatwave_max_temp"),
            ),
            on=summary_group,
            how="left",
        )
        .fillna({"longest_heatwave_days": 0, "heatwave_max_temp": 0.0})
    )
    summary_count = summary_df.count()
    logger.info("Batch summary rows: %d", summary_count)
    write_to_elasticsearch(summary_df, ES_INDEX_STATS)
    logger.info("Đã ghi summary/stats data vào ES index '%s'.", ES_INDEX_STATS)
    logger.info("========== Batch Job hoàn thành ==========")


if __name__ == "__main__":
    main()
