import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, col, from_json, from_unixtime, coalesce
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_data")
ES_HOST = os.getenv("ES_HOST", "elasticsearch")
ES_INDEX = "weather_realtime" 
CHECKPOINT_PATH = "/tmp/spark_checkpoints/weather_es"

def write_to_es(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    batch_df_with_id = batch_df.withColumn(
        "es_id", 
        concat(col("Location"), lit("_"), col("Local_Time"))
    )

    batch_df_with_id.write \
        .format("org.elasticsearch.spark.sql") \
        .mode("append") \
        .option("es.nodes", ES_HOST) \
        .option("es.port", "9200") \
        .option("es.resource", ES_INDEX) \
        .option("es.mapping.id", "es_id") \
        .option("es.nodes.wan.only", "true") \
        .save()

def main():
    time.sleep(40)
    spark = SparkSession.builder \
        .appName("Weather-Speed-Layer") \
        .config("spark.es.nodes", ES_HOST) \
        .config("spark.es.port", "9200") \
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # 1. Schema cho dự báo từng giờ
    hour_schema = StructType([
        StructField("datetime", StringType(), True),
        StructField("datetimeEpoch", LongType(), True),
        StructField("temp", DoubleType(), True),
        StructField("feelslike", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("dew", DoubleType(), True),
        StructField("precip", DoubleType(), True),
        StructField("precipprob", DoubleType(), True),
        StructField("snow", DoubleType(), True),
        StructField("snowdepth", DoubleType(), True),
        StructField("preciptype", ArrayType(StringType()), True), 
        StructField("windgust", DoubleType(), True),
        StructField("windspeed", DoubleType(), True),
        StructField("winddir", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("visibility", DoubleType(), True),
        StructField("cloudcover", DoubleType(), True),
        StructField("solarradiation", DoubleType(), True),
        StructField("solarenergy", DoubleType(), True),
        StructField("uvindex", DoubleType(), True),
        StructField("severerisk", DoubleType(), True),
        StructField("conditions", StringType(), True),
        StructField("icon", StringType(), True),
        StructField("stations", ArrayType(StringType()), True),
        StructField("source", StringType(), True)
    ])

    # 2. Schema cho dự báo từng ngày (chứa mảng các giờ)
    day_schema = StructType([
        StructField("datetime", StringType(), True),
        StructField("datetimeEpoch", LongType(), True),
        StructField("tempmax", DoubleType(), True),
        StructField("tempmin", DoubleType(), True),
        StructField("temp", DoubleType(), True),
        StructField("feelslikemax", DoubleType(), True),
        StructField("feelslikemin", DoubleType(), True),
        StructField("feelslike", DoubleType(), True),
        StructField("dew", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("precip", DoubleType(), True),
        StructField("precipprob", DoubleType(), True),
        StructField("precipcover", DoubleType(), True),
        StructField("preciptype", ArrayType(StringType()), True),
        StructField("snow", DoubleType(), True),
        StructField("snowdepth", DoubleType(), True),
        StructField("windgust", DoubleType(), True),
        StructField("windspeed", DoubleType(), True),
        StructField("winddir", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("cloudcover", DoubleType(), True),
        StructField("visibility", DoubleType(), True),
        StructField("solarradiation", DoubleType(), True),
        StructField("solarenergy", DoubleType(), True),
        StructField("uvindex", DoubleType(), True),
        StructField("severerisk", DoubleType(), True),
        StructField("sunrise", StringType(), True),
        StructField("sunriseEpoch", LongType(), True),
        StructField("sunset", StringType(), True),
        StructField("sunsetEpoch", LongType(), True),
        StructField("moonphase", DoubleType(), True),
        StructField("conditions", StringType(), True),
        StructField("description", StringType(), True),
        StructField("icon", StringType(), True),
        StructField("stations", ArrayType(StringType()), True),
        StructField("source", StringType(), True),
        StructField("hours", ArrayType(hour_schema), True) 
    ])

    # 3. Schema thời tiết hiện tại
    current_conditions_schema = StructType([
        StructField("datetime", StringType(), True),
        StructField("datetimeEpoch", LongType(), True),
        StructField("temp", DoubleType(), True),
        StructField("feelslike", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("dew", DoubleType(), True),
        StructField("precip", DoubleType(), True),
        StructField("precipprob", DoubleType(), True),
        StructField("snow", DoubleType(), True),
        StructField("snowdepth", DoubleType(), True),
        StructField("preciptype", ArrayType(StringType()), True), 
        StructField("windgust", DoubleType(), True),
        StructField("windspeed", DoubleType(), True),
        StructField("winddir", DoubleType(), True),
        StructField("pressure", DoubleType(), True),
        StructField("visibility", DoubleType(), True),
        StructField("cloudcover", DoubleType(), True),
        StructField("solarradiation", DoubleType(), True),
        StructField("solarenergy", DoubleType(), True),
        StructField("uvindex", DoubleType(), True),
        StructField("conditions", StringType(), True),
        StructField("icon", StringType(), True),
        StructField("stations", ArrayType(StringType()), True),   
        StructField("source", StringType(), True),
        StructField("sunrise", StringType(), True),
        StructField("sunriseEpoch", LongType(), True),
        StructField("sunset", StringType(), True),
        StructField("sunsetEpoch", LongType(), True),
        StructField("moonphase", DoubleType(), True)
    ])

    # 4. Schema gốc chứa tất cả 
    weather_schema = StructType([
        StructField("address", StringType(), True), 
        StructField("resolvedAddress", StringType(), True),
        StructField("timezone", StringType(), True),
        StructField("currentConditions", current_conditions_schema, True),
        StructField("days", ArrayType(day_schema), True) 
    ])

    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    parsed_stream = (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_string")
        .select(from_json(col("json_string"), weather_schema).alias("data"))
        .select(
            coalesce(col("data.address"), col("data.resolvedAddress")).alias("Location"), 
            col("data.timezone").alias("Timezone"),
            from_unixtime(col("data.currentConditions.datetimeEpoch"), "yyyy-MM-dd'T'HH:mm:ss").alias("Local_Time"),
            col("data.currentConditions.temp").alias("Temp_C"),
            col("data.currentConditions.feelslike").alias("Feels_Like"),
            col("data.currentConditions.humidity").alias("Humidity_%"),
            col("data.currentConditions.dew").alias("Dew_Point"),
            coalesce(col("data.currentConditions.precip"), lit(0.0)).alias("Precip_mm"),
            col("data.currentConditions.precipprob").alias("Precip_Prob_%"),
            col("data.currentConditions.preciptype").alias("Precip_Type"),
            col("data.currentConditions.snow").alias("Snow"),
            col("data.currentConditions.snowdepth").alias("Snow_Depth"),
            col("data.currentConditions.windspeed").alias("Wind_Speed"),
            col("data.currentConditions.windgust").alias("Wind_Gust"),
            col("data.currentConditions.winddir").alias("Wind_Dir"),
            col("data.currentConditions.pressure").alias("Pressure_hPa"),
            col("data.currentConditions.visibility").alias("Visibility_km"),
            col("data.currentConditions.cloudcover").alias("Cloud_Cover_%"),
            col("data.currentConditions.uvindex").alias("UV_Index"),
            col("data.currentConditions.solarradiation").alias("Solar_Rad"),
            col("data.currentConditions.solarenergy").alias("Solar_Energy"),
            col("data.currentConditions.conditions").alias("Conditions"),
            col("data.currentConditions.icon").alias("Icon"),
            col("data.currentConditions.sunrise").alias("Sunrise"),
            col("data.currentConditions.sunset").alias("Sunset"),
            col("data.currentConditions.moonphase").alias("Moon_Phase"),
            col("data.currentConditions.stations").alias("Stations"),
            col("data.currentConditions.source").alias("Source"),
            col("data.days").alias("Forecast_15_Days") 
        )
    )

    query = parsed_stream.writeStream \
        .foreachBatch(write_to_es) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()