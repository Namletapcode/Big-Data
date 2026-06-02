import os
import unittest

from pyspark.sql import SparkSession

from spark_batch import (
    DEFAULT_PROVINCE_MAPPING_PATH,
    apply_province_mapping,
    build_daily_aggregates,
    build_yoy_comparison,
    classify_daily_heat,
    load_province_mapping,
)


class BatchTransformationsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("batch-transformations-test")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.adaptive.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_dimension_and_broadcast_join_normalize_aliases(self):
        mapping_df = load_province_mapping(self.spark, DEFAULT_PROVINCE_MAPPING_PATH)
        raw_df = self.spark.createDataFrame(
            [
                ("Tỉnh Hà Giang, Việt Nam", "2025-06-30"),
                ("Tỉnh Hà Giang, Việt Nam", "2025-07-01"),
                ("Thành phố Hồ Chí Minh, VN", "2025-07-01"),
                ("Thừa Thiên-Huế, VN", "2025-07-01"),
            ],
            ["resolvedAddress", "date"],
        )

        mapped_df = apply_province_mapping(raw_df, mapping_df)
        rows = {
            (row.resolvedAddress, row.date): (row.Location, row.province_view)
            for row in mapped_df.select("resolvedAddress", "date", "Location", "province_view").collect()
        }
        plan = mapped_df._jdf.queryExecution().executedPlan().toString()

        self.assertEqual(rows[("Tỉnh Hà Giang, Việt Nam", "2025-06-30")], ("Hà Giang", "pre_merge_63"))
        self.assertEqual(rows[("Tỉnh Hà Giang, Việt Nam", "2025-07-01")], ("Tuyên Quang", "post_merge_34"))
        self.assertEqual(rows[("Thành phố Hồ Chí Minh, VN", "2025-07-01")], ("Hồ Chí Minh", "post_merge_34"))
        self.assertEqual(rows[("Thừa Thiên-Huế, VN", "2025-07-01")], ("Huế", "post_merge_34"))
        self.assertIn("BroadcastHashJoin", plan)

    def test_heat_classification_thresholds(self):
        self.assertEqual(classify_daily_heat(None, 30.0, 70.0)[1], "unknown")
        self.assertEqual(classify_daily_heat(26.0, 30.0, 70.0)[1], "normal")
        self.assertEqual(classify_daily_heat(27.0, 30.0, 70.0)[1], "caution")
        self.assertEqual(classify_daily_heat(30.0, 34.9, 50.0)[1], "high")
        self.assertEqual(classify_daily_heat(35.0, 39.9, 70.0)[1], "extreme")
        self.assertEqual(classify_daily_heat(20.0, 40.0, 20.0)[1], "extreme")

    def test_daily_alert_classification_and_yoy_sort_merge_join(self):
        source_df = self.spark.createDataFrame(
            [
                ("Hà Nội", "2023-06-01", 29.0, 60.0, 1.0),
                ("Hà Nội", "2024-06-01", 31.0, 72.0, 3.0),
                ("Hà Nội", "2024-02-29", 33.0, 75.0, 0.0),
                ("Hà Nội", "2025-02-28", 34.0, 74.0, 0.0),
                ("Huế", "2024-06-01", 24.0, 80.0, 2.0),
            ],
            ["Location", "date", "temp", "humidity", "precip"],
        )
        daily_df = build_daily_aggregates(source_df)
        daily_rows = {(row.Location, row.date): row for row in daily_df.collect()}
        yoy_df = build_yoy_comparison(daily_df)
        rows = yoy_df.collect()
        daily_plan = daily_df._jdf.queryExecution().executedPlan().toString()
        yoy_plan = yoy_df._jdf.queryExecution().executedPlan().toString()

        hanoi_row = daily_rows[("Hà Nội", "2024-06-01")]
        self.assertEqual(hanoi_row.heat_alert_level, "high")
        self.assertEqual(hanoi_row.alert_priority, 2)
        self.assertEqual(hanoi_row.alert_reason, "avg_temp_over_30")
        self.assertEqual(hanoi_row.alert_title, "Cảnh báo nắng nóng")
        self.assertTrue(hanoi_row.is_heat_alert)
        self.assertTrue(hanoi_row.is_rain_alert)
        self.assertEqual(hanoi_row.weather_alert_tags, ["Chú ý nắng nóng", "Có mưa"])
        hue_rain = daily_df.filter("Location = 'Huế'").collect()[0]
        self.assertEqual(hue_rain.heat_alert_level, "normal")
        self.assertEqual(hue_rain.rain_alert_level, "caution")
        self.assertEqual(hue_rain.alert_type, "rain")
        self.assertEqual(hue_rain.alert_title, "Chú ý có mưa")
        self.assertTrue(hue_rain.is_rain_alert)
        self.assertTrue(hue_rain.is_weather_alert)
        self.assertEqual(hue_rain.weather_alert_tags, ["Có mưa"])
        self.assertNotIn("PythonUDF", daily_plan)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].date, "2024-06-01")
        self.assertEqual(rows[0].heat_alert_level, "high")
        self.assertEqual(rows[0].delta_avg_temp, 2.0)
        self.assertIn("SortMergeJoin", yoy_plan)


if __name__ == "__main__":
    unittest.main()
