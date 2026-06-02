import unittest
from unittest.mock import patch

import api


class ElasticsearchStub:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def search(self, **kwargs):
        self.calls.append(kwargs)
        if isinstance(self.response, list):
            return self.response[len(self.calls) - 1]
        return self.response


class BatchApiTest(unittest.TestCase):
    def test_latest_enriches_forecast_heat_alerts(self):
        document = {
            "Location": "Hà Nội",
            "Local_Time": "2026-06-02T12:00:00",
            "Forecast_15_Days": [
                {"datetime": "2026-06-02", "temp": 31.0, "tempmax": 34.0, "humidity": 60.0, "precip": 2.0},
                {"datetime": "2026-06-03", "temp": 36.0, "tempmax": 39.0, "humidity": 72.0},
                {"datetime": "2026-06-04", "temp": 24.0, "tempmax": 26.0, "humidity": 80.0, "precip": 2.0},
            ],
        }
        es = ElasticsearchStub({"hits": {"hits": [{"_source": document}]}})
        with patch.object(api, "get_es_client", return_value=es):
            response = api.get_latest_weather(location="Hà Nội")

        self.assertEqual(response["Forecast_15_Days"][0]["heat_alert_level"], "high")
        self.assertEqual(response["Forecast_15_Days"][0]["alert_priority"], 2)
        self.assertEqual(response["Forecast_15_Days"][0]["alert_reason"], "avg_temp_over_30")
        self.assertTrue(response["Forecast_15_Days"][0]["is_heat_alert"])
        self.assertTrue(response["Forecast_15_Days"][0]["is_rain_alert"])
        self.assertEqual(
            [tag["type"] for tag in response["Forecast_15_Days"][0]["weather_alert_tags"]],
            ["heat", "rain"],
        )
        self.assertEqual(response["Forecast_15_Days"][1]["heat_alert_level"], "extreme")
        self.assertEqual(response["Forecast_15_Days"][2]["alert_type"], "rain")
        self.assertEqual(response["Forecast_15_Days"][2]["rain_alert_level"], "caution")
        self.assertEqual(response["Forecast_15_Days"][2]["alert_title"], "Chú ý có mưa")
        self.assertTrue(response["Forecast_15_Days"][2]["is_rain_alert"])
        self.assertEqual(response["Heat_Alerts"][0]["date"], "2026-06-03")
        self.assertEqual(response["Heat_Alerts"][0]["alert_priority"], 3)
        self.assertEqual(response["Weather_Alerts"][0]["date"], "2026-06-03")
        self.assertIn("2026-06-04", [alert["date"] for alert in response["Weather_Alerts"]])

    def test_latest_merges_longer_forecast_document(self):
        realtime_doc = {
            "Location": "Hà Nội",
            "Local_Time": "2026-06-02T12:00:00",
            "Forecast_15_Days": [{"datetime": "2026-06-02", "temp": 30.0}],
        }
        forecast_doc = {
            "Location": "Hà Nội",
            "Forecast_Updated_At": "2026-06-02T12:00:00",
            "Forecast_15_Days": [
                {"datetime": "2026-06-02", "temp": 30.0},
                {"datetime": "2026-06-03", "temp": 31.0},
                {"datetime": "2026-06-04", "temp": 32.0},
            ],
        }
        es = ElasticsearchStub(
            [
                {"hits": {"hits": [{"_source": realtime_doc}]}},
                {"hits": {"hits": [{"_source": forecast_doc}]}},
            ]
        )
        with patch.object(api, "get_es_client", return_value=es):
            response = api.get_latest_weather(location="Hà Nội")

        self.assertEqual(len(response["Forecast_15_Days"]), 3)
        self.assertEqual(response["Forecast_Source"], api.ES_INDEX_FORECAST)
        self.assertEqual(es.calls[1]["index"], api.ES_INDEX_FORECAST)

    def test_normalize_batch_location_aliases(self):
        self.assertEqual(api.normalize_batch_location("Thành phố Hồ Chí Minh, VN"), "Hồ Chí Minh")
        self.assertEqual(api.normalize_batch_location("Thừa Thiên-Huế, Việt Nam"), "Huế")
        self.assertEqual(api.normalize_batch_location("Tỉnh Hà Giang, Việt Nam"), "Tuyên Quang")
        self.assertEqual(api.normalize_batch_location("Tỉnh Hà Giang, Việt Nam", api.PRE_MERGE_VIEW), "Hà Giang")

    def test_yoy_uses_canonical_location_and_limit(self):
        es = ElasticsearchStub({"hits": {"hits": [{"_source": {"Location": "Hồ Chí Minh"}}]}})
        with patch.object(api, "get_es_client", return_value=es):
            response = api.get_batch_yoy(location="Bình Dương, VN", limit=12)

        self.assertEqual(response, [{"Location": "Hồ Chí Minh"}])
        self.assertEqual(es.calls[0]["index"], api.ES_INDEX_BATCH_YOY)
        self.assertEqual(es.calls[0]["body"]["size"], 12)
        self.assertEqual(
            es.calls[0]["body"]["query"],
            {"bool": {"filter": [{"term": {"Location.keyword": "Hồ Chí Minh"}}]}},
        )

    def test_daily_returns_heat_alert_level_without_shape_changes(self):
        document = {"Location": "Huế", "date": "2025-05-01", "heat_alert_level": "high"}
        es = ElasticsearchStub({"hits": {"hits": [{"_source": document}]}})
        with patch.object(api, "get_es_client", return_value=es):
            response = api.get_batch_daily(location="Thừa Thiên-Huế, VN", province_view=api.POST_MERGE_VIEW, limit=1)

        self.assertEqual(response, [document])
        self.assertEqual(
            es.calls[0]["body"]["query"],
            {
                "bool": {
                    "filter": [
                        {"term": {"province_view.keyword": api.POST_MERGE_VIEW}},
                        {"term": {"Location.keyword": "Huế"}},
                    ]
                }
            },
        )

    def test_daily_pre_merge_keeps_old_location(self):
        document = {"Location": "Hà Giang", "date": "2025-06-30", "province_view": api.PRE_MERGE_VIEW}
        es = ElasticsearchStub({"hits": {"hits": [{"_source": document}]}})
        with patch.object(api, "get_es_client", return_value=es):
            response = api.get_batch_daily(location="Tỉnh Hà Giang, Việt Nam", province_view=api.PRE_MERGE_VIEW, limit=1)

        self.assertEqual(response, [document])
        self.assertEqual(
            es.calls[0]["body"]["query"],
            {
                "bool": {
                    "filter": [
                        {"term": {"province_view.keyword": api.PRE_MERGE_VIEW}},
                        {"term": {"Location.keyword": "Hà Giang"}},
                    ]
                }
            },
        )


if __name__ == "__main__":
    unittest.main()
