import unittest
from unittest.mock import patch

import api


class ElasticsearchStub:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def search(self, **kwargs):
        self.calls.append(kwargs)
        return self.response


class BatchApiTest(unittest.TestCase):
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
