"""
test_config_bacth.py
--------------------
Chỉ kiểm tra 2 việc:
  1. Kết nối MinIO qua S3 API (dùng boto3)
  2. Kết nối MinIO qua Spark S3A connector (hadoop-aws JAR)
Chạy: python3 test_config_bacth.py
"""

import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("test_config")

# ── Cấu hình (lấy từ env hoặc dùng default) ──────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://35.240.139.79:9000")
MINIO_USER     = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_PASS     = os.getenv("MINIO_ROOT_PASSWORD", "password123")
BUCKET         = "raw-weather-data"

# Hadoop-AWS package cho Spark 3.4.x
S3A_PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"


# ── TEST 1: MinIO via boto3 ───────────────────────────────────────────────────
def test_minio_boto3() -> bool:
    """Kiểm tra kết nối S3 API của MinIO bằng boto3."""
    try:
        import boto3
        from botocore.client import Config
        from botocore.exceptions import EndpointConnectionError, ClientError
    except ImportError:
        logger.error("[TEST 1] boto3 chưa được cài. Chạy: pip install boto3")
        return False

    logger.info("[TEST 1] Đang kết nối MinIO qua boto3 ...")
    logger.info("         Endpoint : %s", MINIO_ENDPOINT)
    logger.info("         User     : %s", MINIO_USER)
    logger.info("         Bucket   : %s", BUCKET)

    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_USER,
            aws_secret_access_key=MINIO_PASS,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        # Liệt kê toàn bộ bucket để xác nhận kết nối
        buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
        logger.info("[TEST 1] ✓ Kết nối MinIO thành công. Buckets hiện có: %s", buckets)

        # Thử list object trong bucket target
        resp = s3.list_objects_v2(Bucket=BUCKET, MaxKeys=5)
        keys = [o["Key"] for o in resp.get("Contents", [])]
        logger.info("[TEST 1] ✓ Bucket '%s' có thể truy cập. 5 key đầu: %s", BUCKET, keys)
        return True

    except EndpointConnectionError as exc:
        logger.error("[TEST 1] ✗ Không thể kết nối endpoint '%s': %s", MINIO_ENDPOINT, exc)
        logger.error("         → Kiểm tra IP/port và firewall.")
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        msg  = exc.response["Error"]["Message"]
        logger.error("[TEST 1] ✗ MinIO trả lỗi [%s]: %s", code, msg)
        if code in ("InvalidArgument",):
            logger.error("         → Có thể sai port (9001 = Console UI, 9000 = S3 API).")
        elif code in ("NoSuchBucket",):
            logger.error("         → Bucket '%s' chưa tồn tại trên MinIO.", BUCKET)
        elif code in ("InvalidAccessKeyId", "SignatureDoesNotMatch"):
            logger.error("         → Sai credentials (user/password).")
    except Exception as exc:  # noqa: BLE001
        logger.error("[TEST 1] ✗ Lỗi không xác định: %s", exc)

    return False


# ── TEST 2: Spark + S3A connector ─────────────────────────────────────────────
def test_spark_s3a() -> bool:
    """Kiểm tra Spark có thể khởi tạo và list file trên MinIO qua s3a://."""
    logger.info("[TEST 2] Đang khởi tạo SparkSession với S3A connector ...")
    logger.info("         Packages : %s", S3A_PACKAGES)

    try:
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("test-minio-s3a")
            .config("spark.jars.packages", S3A_PACKAGES)
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            # Tắt log Spark để output gọn hơn
            .master("local[1]")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("[TEST 2] ✓ SparkSession khởi tạo thành công (app: %s)", spark.sparkContext.appName)

        # Thử list file trên MinIO qua s3a://
        test_path = f"s3a://{BUCKET}/"
        logger.info("[TEST 2] Đang list path '%s' qua S3A ...", test_path)

        sc = spark.sparkContext
        hadoop_fs  = sc._jvm.org.apache.hadoop.fs.FileSystem
        hadoop_path = sc._jvm.org.apache.hadoop.fs.Path(test_path)
        conf        = sc._jsc.hadoopConfiguration()
        fs          = hadoop_fs.get(hadoop_path.toUri(), conf)

        statuses = fs.listStatus(hadoop_path)
        paths    = [str(s.getPath()) for s in statuses[:5]]
        logger.info("[TEST 2] ✓ S3A kết nối thành công. 5 path đầu trong bucket: %s", paths)

        spark.stop()
        return True

    except Exception as exc:  # noqa: BLE001
        logger.error("[TEST 2] ✗ Spark S3A thất bại: %s", exc)
        logger.error("         → Kiểm tra: JAR hadoop-aws đã được tải chưa, endpoint đúng chưa.")
        return False


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("  Bắt đầu kiểm tra kết nối MinIO + Spark S3A")
    logger.info("  Endpoint : %s", MINIO_ENDPOINT)
    logger.info("=" * 60)

    result_boto3 = test_minio_boto3()
    logger.info("-" * 60)
    result_spark  = test_spark_s3a()

    logger.info("=" * 60)
    logger.info("  KẾT QUẢ TỔNG HỢP:")
    logger.info("  [TEST 1] MinIO boto3  : %s", "✓ PASS" if result_boto3 else "✗ FAIL")
    logger.info("  [TEST 2] Spark S3A   : %s", "✓ PASS" if result_spark  else "✗ FAIL")
    logger.info("=" * 60)

    if not result_boto3 or not result_spark:
        sys.exit(1)
