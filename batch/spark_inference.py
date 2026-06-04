import os
import math
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.ml import PipelineModel
from pyspark.sql.window import Window

# ==========================================
# Cấu hình
# ==========================================
ES_INDEX_ML = "weather_forecase_ml"
MINIO_ENDPOINT = "http://minio:9000"
MINIO_USER = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_PASS = os.getenv("MINIO_ROOT_PASSWORD", "password123")

spark = SparkSession.builder \
    .appName("Weather_ML_Inference_Final") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_USER) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASS) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# ==========================================
# Hàm chuẩn bị đặc trưng (Features)
# ==========================================
def prepare_inference_features(df_raw: DataFrame) -> DataFrame:
    # 1.
    if "datetime" in df_raw.columns:
        df_raw = df_raw.drop("datetime")
    df = df_raw.withColumnRenamed("Location", "location") \
               .withColumnRenamed("Local_Time", "datetime")

    # 2. Tạo grid_ts
    df = df.withColumn("grid_ts", F.to_timestamp(F.col("datetime")))
    
    # 3. Tạo các Time features (sin/cos)
    df = df.withColumn("hour", F.hour("grid_ts")).withColumn("month", F.month("grid_ts"))
    df = df.withColumn("hour_sin", F.sin(2 * math.pi * F.col("hour") / 24)) \
           .withColumn("hour_cos", F.cos(2 * math.pi * F.col("hour") / 24)) \
           .withColumn("month_sin", F.sin(2 * math.pi * F.col("month") / 12)) \
           .withColumn("month_cos", F.cos(2 * math.pi * F.col("month") / 12)) \
           .drop("hour", "month")
    
    # 4. Tạo Physic features
    df = df.withColumn("dew_spread", F.col("temp") - F.col("dew"))
    df = df.withColumn("winddir_sin", F.sin(F.col("winddir") * math.pi / 180)) \
           .withColumn("winddir_cos", F.cos(F.col("winddir") * math.pi / 180))

    # 5. Tạo Lag features 
    w_lag = Window.partitionBy("location").orderBy("grid_ts")
    base_cols = ["temp", "pressure", "humidity", "precip", "windspeed", "cloudcover","dew_spread","winddir_sin","winddir_cos"]
    lags = [1, 4, 8]
    
    for c in base_cols:
        for l in lags:
            df = df.withColumn(f"{c}_lag_{l}", F.lag(c, l).over(w_lag))
    
    df = df.dropna()

    # 6. 
    w_latest = Window.partitionBy("location").orderBy(F.col("grid_ts").desc())
    df_latest_only = df.withColumn("rn", F.row_number().over(w_latest)) \
                       .filter(F.col("rn") == 1) \
                       .drop("rn")
                       
    return df_latest_only

# ==========================================
# Hàm chạy dự báo (Inference)
# ==========================================
def run_production_inference(df_latest: DataFrame, total_steps: int = 2) -> DataFrame:
    df_infer = df_latest
    standard_vars = ["temp", "pressure", "humidity"]
    
    # Lưu giá trị hiện tại làm gốc
    for var in standard_vars + ["precip"]:
        df_infer = df_infer.withColumn(f"current_{var}", F.col(var))

    # Vòng lặp dự báo 
    for step in range(1, total_steps + 1):
        print(f"--- Đang dự báo cho Step {step} ---")
        base_path = f"s3a://raw-weather-data/models/step_{step}"
        
        # Load models
        model_temp = PipelineModel.load(f"{base_path}/temp_model")
        model_press = PipelineModel.load(f"{base_path}/pressure_model")
        model_hum = PipelineModel.load(f"{base_path}/humidity_model")
        model_clf = PipelineModel.load(f"{base_path}/precip_clf_model")
        model_reg = PipelineModel.load(f"{base_path}/precip_reg_model")

        # Chain transform
        df_infer = model_temp.transform(df_infer)
        df_infer = model_press.transform(df_infer)
        df_infer = model_hum.transform(df_infer)
        df_infer = model_clf.transform(df_infer)
        df_infer = model_reg.transform(df_infer)

        # Tính toán dự báo cuối cùng

        df_infer = df_infer.withColumn(
            f"pred_precip_step_{step}", 
            F.col(f"pred_is_rain_step_{step}") * F.col(f"pred_rain_amount_step_{step}")
        )
        
        # Tính forecast
        for var in standard_vars:
            df_infer = df_infer.withColumn(
                f"forecast_{var}_step_{step}", 
                F.round(F.col(f"current_{var}") + F.col(f"pred_{var}_step_{step}"), 2)
            )
        
        df_infer = df_infer.withColumn(
            f"forecast_precip_step_{step}", 
            F.round(F.col(f"pred_precip_step_{step}"), 2)
        )
        

        if "rawPrediction" in df_infer.columns:
            df_infer = df_infer.drop("rawPrediction")
        if "probability" in df_infer.columns:
            df_infer = df_infer.drop("probability")
        
        cols_to_drop = [
            "features",
            f"feat_temp_step_{step}", f"feat_press_step_{step}", 
            f"feat_hum_step_{step}", f"feat_precip_clf_step_{step}", 
            f"feat_precip_reg_step_{step}"
        ]
        
        for c in cols_to_drop:
            if c in df_infer.columns:
                df_infer = df_infer.drop(c)
            
    return df_infer

# ==========================================
# MAIN EXECUTION
# ==========================================
if __name__ == "__main__":
    print("1. Đọc dữ liệu từ MinIO...")
    df_raw = spark.read.parquet("s3a://raw-weather-data/processed/valid_weather")
    

    print("2. Đang tạo Lag Features cho tập Inference...")
    df_inference_ready = prepare_inference_features(df_raw)
    

    print("3. Bắt đầu chạy Model Predict...")
    df_result = run_production_inference(df_inference_ready, total_steps=4)
    
    print("4. Ghi kết quả vào Elasticsearch...")
    df_result.write.format("org.elasticsearch.spark.sql") \
        .mode("overwrite") \
        .option("es.nodes", "elasticsearch") \
        .option("es.port", "9200") \
        .save(ES_INDEX_ML)
        
    print("🎉 HOÀN TẤT INFERENCE!")