import os
import re
from datetime import date as date_cls
from typing import Optional, List, Any, Dict

from elasticsearch import Elasticsearch
from fastapi import APIRouter, HTTPException, Query


ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
ES_INDEX = os.getenv("ES_INDEX", "weather_realtime")
ES_INDEX_BATCH_DAILY = os.getenv("ES_INDEX_BATCH_DAILY", "weather_batch_daily")
ES_INDEX_BATCH_STATS = os.getenv("ES_INDEX_BATCH_STATS", "weather_batch_stats")
ES_INDEX_BATCH_YOY = os.getenv("ES_INDEX_BATCH_YOY", "weather_batch_yoy")
BATCH_PROVINCE_CUTOFF_DATE = date_cls.fromisoformat(os.getenv("BATCH_PROVINCE_CUTOFF_DATE", "2025-07-01"))
PRE_MERGE_VIEW = "pre_merge_63"
POST_MERGE_VIEW = "post_merge_34"

BATCH_LOCATION_MERGES = {
    "Hà Giang": "Tuyên Quang",
    "Yên Bái": "Lào Cai",
    "Bắc Kạn": "Thái Nguyên",
    "Vĩnh Phúc": "Phú Thọ",
    "Hòa Bình": "Phú Thọ",
    "Hoà Bình": "Phú Thọ",
    "Bắc Giang": "Bắc Ninh",
    "Thái Bình": "Hưng Yên",
    "Hải Dương": "Hải Phòng",
    "Hà Nam": "Ninh Bình",
    "Nam Định": "Ninh Bình",
    "Quảng Bình": "Quảng Trị",
    "Quảng Nam": "Đà Nẵng",
    "Kon Tum": "Quảng Ngãi",
    "Bình Định": "Gia Lai",
    "Ninh Thuận": "Khánh Hòa",
    "Đắk Nông": "Lâm Đồng",
    "Bình Thuận": "Lâm Đồng",
    "Phú Yên": "Đắk Lắk",
    "Đắk Lăk": "Đắk Lắk",
    "Bà Rịa - Vũng Tàu": "Hồ Chí Minh",
    "Bà Rịa Vũng Tàu": "Hồ Chí Minh",
    "Bình Dương": "Hồ Chí Minh",
    "TP Hồ Chí Minh": "Hồ Chí Minh",
    "TP. Hồ Chí Minh": "Hồ Chí Minh",
    "TP.HCM": "Hồ Chí Minh",
    "TP HCM": "Hồ Chí Minh",
    "Sài Gòn": "Hồ Chí Minh",
    "Bình Phước": "Đồng Nai",
    "Long An": "Tây Ninh",
    "Sóc Trăng": "Cần Thơ",
    "Hậu Giang": "Cần Thơ",
    "Bến Tre": "Vĩnh Long",
    "Trà Vinh": "Vĩnh Long",
    "Tiền Giang": "Đồng Tháp",
    "Bạc Liêu": "Cà Mau",
    "Kiên Giang": "An Giang",
    "Thừa Thiên Huế": "Huế",
    "Thừa Thiên-Huế": "Huế",
}

router = APIRouter()


def get_es_client() -> Elasticsearch:
    return Elasticsearch(
        ES_HOST,
        headers={
            "Accept": "application/vnd.elasticsearch+json; compatible-with=8",
            "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8",
        },
    )


def build_location_query(location: Optional[str]) -> Dict[str, Any]:
    if not location:
        return {"match_all": {}}
    return {"term": {"Location.keyword": location}}


def normalize_location_name(location: Optional[str]) -> Optional[str]:
    if not location:
        return location
    normalized = re.sub(r",\s*(VN|Việt Nam)\s*$", "", location).strip()
    normalized = re.sub(r"^(Tỉnh|Thành phố)\s+", "", normalized).strip()
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def normalize_batch_location(location: Optional[str], province_view: Optional[str] = POST_MERGE_VIEW) -> Optional[str]:
    normalized = normalize_location_name(location)
    if not normalized:
        return normalized
    if province_view == PRE_MERGE_VIEW:
        return normalized
    return BATCH_LOCATION_MERGES.get(normalized, normalized)


def normalize_province_view(province_view: Optional[str]) -> Optional[str]:
    if province_view in (PRE_MERGE_VIEW, POST_MERGE_VIEW):
        return province_view
    return None


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def classify_weather_alert(
    avg_temp: Any,
    max_temp: Any,
    avg_humidity: Any,
    precip: Any = None,
    precipprob: Any = None,
) -> Dict[str, Any]:
    avg_temp_f = _to_float(avg_temp)
    max_temp_f = _to_float(max_temp)
    avg_humidity_f = _to_float(avg_humidity)
    precip_f = _to_float(precip)
    precipprob_f = _to_float(precipprob)

    if avg_temp_f is None:
        temp_category = "unknown"
    elif avg_temp_f >= 40:
        temp_category = "cực kỳ nóng"
    elif avg_temp_f >= 35:
        temp_category = "rất nóng"
    elif avg_temp_f >= 30:
        temp_category = "nóng"
    elif avg_temp_f >= 25:
        temp_category = "ấm"
    else:
        temp_category = "mát"

    if avg_temp_f is None or max_temp_f is None:
        heat_alert_level = "unknown"
    elif max_temp_f >= 40:
        heat_alert_level = "extreme"
    elif avg_temp_f >= 35 and avg_humidity_f is not None and avg_humidity_f >= 70:
        heat_alert_level = "extreme"
    elif max_temp_f >= 35:
        heat_alert_level = "high"
    elif avg_temp_f >= 30:
        heat_alert_level = "high"
    elif avg_temp_f >= 27 and avg_humidity_f is not None and avg_humidity_f >= 70:
        heat_alert_level = "caution"
    else:
        heat_alert_level = "normal"

    if avg_temp_f is None or max_temp_f is None:
        heat_reason = "missing_temperature_data"
        heat_priority = -1
        heat_title = "Thiếu dữ liệu cảnh báo"
    elif max_temp_f >= 40:
        heat_reason = "max_temp_over_40"
        heat_priority = 3
        heat_title = "Cảnh báo nắng nóng nguy hiểm"
    elif avg_temp_f >= 35 and avg_humidity_f is not None and avg_humidity_f >= 70:
        heat_reason = "avg_temp_high_with_humidity"
        heat_priority = 3
        heat_title = "Cảnh báo nắng nóng nguy hiểm"
    elif max_temp_f >= 35:
        heat_reason = "max_temp_over_35"
        heat_priority = 2
        heat_title = "Cảnh báo nắng nóng"
    elif avg_temp_f >= 30:
        heat_reason = "avg_temp_over_30"
        heat_priority = 2
        heat_title = "Cảnh báo nắng nóng"
    elif avg_temp_f >= 27 and avg_humidity_f is not None and avg_humidity_f >= 70:
        heat_reason = "hot_and_humid"
        heat_priority = 1
        heat_title = "Chú ý thời tiết nóng ẩm"
    else:
        heat_reason = "normal"
        heat_priority = 0
        heat_title = "Thời tiết bình thường"

    has_rain = (precip_f is not None and precip_f > 0) or (precipprob_f is not None and precipprob_f >= 50)
    if not has_rain:
        rain_alert_level = "normal"
        rain_reason = "normal"
        rain_priority = 0
        rain_title = "Thời tiết bình thường"
    elif (precip_f is not None and precip_f >= 50) or (precipprob_f is not None and precipprob_f >= 95):
        rain_alert_level = "extreme"
        rain_reason = "heavy_rain_over_50mm"
        rain_priority = 3
        rain_title = "Cảnh báo mưa lớn"
    elif (precip_f is not None and precip_f >= 10) or (precipprob_f is not None and precipprob_f >= 80):
        rain_alert_level = "high"
        rain_reason = "rain_over_10mm"
        rain_priority = 2
        rain_title = "Chú ý có mưa"
    else:
        rain_alert_level = "caution"
        rain_reason = "rain_expected"
        rain_priority = 1
        rain_title = "Chú ý có mưa"

    if heat_priority >= rain_priority:
        alert_type = "heat"
        alert_level = heat_alert_level
        alert_priority = heat_priority
        alert_reason = heat_reason
        alert_title = heat_title
    else:
        alert_type = "rain"
        alert_level = rain_alert_level
        alert_priority = rain_priority
        alert_reason = rain_reason
        alert_title = rain_title

    weather_alert_tags = []
    if heat_alert_level in ("caution", "high", "extreme"):
        weather_alert_tags.append(
            {
                "type": "heat",
                "level": heat_alert_level,
                "label": "Chú ý nắng nóng" if heat_alert_level == "caution" else "Nắng nóng",
                "priority": heat_priority,
                "reason": heat_reason,
            }
        )
    if rain_alert_level in ("caution", "high", "extreme"):
        weather_alert_tags.append(
            {
                "type": "rain",
                "level": rain_alert_level,
                "label": "Có mưa",
                "priority": rain_priority,
                "reason": rain_reason,
            }
        )

    return {
        "temp_category": temp_category,
        "heat_alert_level": heat_alert_level,
        "rain_alert_level": rain_alert_level,
        "alert_type": alert_type,
        "alert_level": alert_level,
        "alert_priority": alert_priority,
        "is_heat_alert": heat_alert_level in ("caution", "high", "extreme"),
        "is_rain_alert": rain_alert_level in ("caution", "high", "extreme"),
        "is_weather_alert": alert_priority > 0,
        "weather_alert_tags": weather_alert_tags,
        "alert_reason": alert_reason,
        "alert_title": alert_title,
    }


def build_weather_alert_message(day: Dict[str, Any]) -> str:
    max_temp = _to_float(day.get("tempmax") if day.get("tempmax") is not None else day.get("max_temp"))
    avg_temp = _to_float(day.get("temp") if day.get("temp") is not None else day.get("avg_temp"))
    humidity = _to_float(day.get("humidity") if day.get("humidity") is not None else day.get("avg_humidity"))
    precip = _to_float(day.get("precip") if day.get("precip") is not None else day.get("total_precip"))
    precipprob = _to_float(day.get("precipprob"))

    if day.get("alert_type") == "rain":
        parts = []
        if precip is not None:
            parts.append(f"lượng mưa khoảng {precip:.1f} mm")
        if precipprob is not None:
            parts.append(f"xác suất mưa {precipprob:.0f}%")
        detail = ", ".join(parts) if parts else "có khả năng xuất hiện mưa"
        if day.get("alert_level") == "extreme":
            return f"{detail}. Cần chú ý mưa lớn và hạn chế di chuyển khi thời tiết xấu."
        return f"{detail}. Nên chuẩn bị áo mưa hoặc ô khi ra ngoài."

    parts = []
    if max_temp is not None:
        parts.append(f"nhiệt độ cao nhất khoảng {max_temp:.1f}°C")
    if avg_temp is not None:
        parts.append(f"trung bình {avg_temp:.1f}°C")
    if humidity is not None:
        parts.append(f"độ ẩm {humidity:.0f}%")
    detail = ", ".join(parts) if parts else "dữ liệu nhiệt độ chưa đầy đủ"

    level = day.get("alert_level") or day.get("heat_alert_level")
    if level == "extreme":
        return f"{detail}. Hạn chế hoạt động ngoài trời vào khung giờ nắng gắt."
    if level == "high":
        return f"{detail}. Nên chuẩn bị chống nắng và bổ sung nước."
    if level == "caution":
        return f"{detail}. Theo dõi cảm giác oi nóng khi hoạt động ngoài trời."
    return detail


def enrich_forecast_alerts(document: Dict[str, Any]) -> Dict[str, Any]:
    days = document.get("Forecast_15_Days")
    if not isinstance(days, list):
        document["Weather_Alerts"] = []
        document["Heat_Alerts"] = []
        return document

    weather_alerts = []
    heat_alerts = []
    for day in days:
        if not isinstance(day, dict):
            continue
        alert = classify_weather_alert(
            day.get("temp"),
            day.get("tempmax"),
            day.get("humidity"),
            day.get("precip"),
            day.get("precipprob"),
        )
        day.update(alert)
        day["alert_message"] = build_weather_alert_message(day)
        if day.get("is_weather_alert"):
            weather_alerts.append(
                {
                    "date": day.get("datetime"),
                    "alert_type": day.get("alert_type"),
                    "alert_level": day.get("alert_level"),
                    "heat_alert_level": day.get("heat_alert_level"),
                    "rain_alert_level": day.get("rain_alert_level"),
                    "alert_priority": day.get("alert_priority"),
                    "alert_title": day.get("alert_title"),
                    "alert_reason": day.get("alert_reason"),
                    "alert_message": day.get("alert_message"),
                    "weather_alert_tags": day.get("weather_alert_tags", []),
                    "max_temp": day.get("tempmax"),
                    "avg_temp": day.get("temp"),
                    "avg_humidity": day.get("humidity"),
                    "precip": day.get("precip"),
                    "precipprob": day.get("precipprob"),
                }
            )
        if day.get("is_heat_alert"):
            heat_alerts.append(
                {
                    "date": day.get("datetime"),
                    "heat_alert_level": day.get("heat_alert_level"),
                    "alert_priority": day.get("alert_priority"),
                    "alert_title": day.get("alert_title"),
                    "alert_reason": day.get("alert_reason"),
                    "alert_message": day.get("alert_message"),
                    "max_temp": day.get("tempmax"),
                    "avg_temp": day.get("temp"),
                    "avg_humidity": day.get("humidity"),
                }
            )

    document["Heat_Alerts"] = sorted(
        heat_alerts,
        key=lambda item: (-(item.get("alert_priority") or 0), item.get("date") or ""),
    )
    document["Weather_Alerts"] = sorted(
        weather_alerts,
        key=lambda item: (-(item.get("alert_priority") or 0), item.get("date") or ""),
    )
    return document


def infer_province_view(start_date: Optional[date_cls], end_date: Optional[date_cls]) -> Optional[str]:
    if end_date and end_date < BATCH_PROVINCE_CUTOFF_DATE:
        return PRE_MERGE_VIEW
    if start_date and start_date >= BATCH_PROVINCE_CUTOFF_DATE:
        return POST_MERGE_VIEW
    return None


def build_batch_filters(
    location: Optional[str] = None,
    province_view: Optional[str] = None,
    start_date: Optional[date_cls] = None,
    end_date: Optional[date_cls] = None,
) -> List[Dict[str, Any]]:
    filters: List[Dict[str, Any]] = []
    selected_view = normalize_province_view(province_view) or infer_province_view(start_date, end_date)
    if selected_view:
        filters.append({"term": {"province_view.keyword": selected_view}})

    if location:
        filters.append({"term": {"Location.keyword": normalize_batch_location(location, selected_view)}})

    if start_date or end_date:
        date_range: Dict[str, str] = {}
        if start_date:
            date_range["gte"] = start_date.isoformat()
        if end_date:
            date_range["lte"] = end_date.isoformat()
        filters.append({"range": {"date": date_range}})
    return filters


def build_batch_query(
    location: Optional[str] = None,
    province_view: Optional[str] = None,
    start_date: Optional[date_cls] = None,
    end_date: Optional[date_cls] = None,
) -> Dict[str, Any]:
    filters = build_batch_filters(location, province_view, start_date, end_date)
    if not filters:
        return {"match_all": {}}
    return {"bool": {"filter": filters}}


@router.get("/health")
def health_check() -> Dict[str, Any]:
    es = get_es_client()
    try:
        info = es.info()
        return {
            "status": "ok",
            "elasticsearch": {
                "cluster_name": info.get("cluster_name"),
                "version": info.get("version", {}).get("number"),
            },
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Elasticsearch not available: {e}")


@router.get("/weather/latest")
def get_latest_weather(
    location: Optional[str] = Query(
        None,
        description="Địa điểm, ví dụ: 'Hà Nội, Việt Nam'. Nếu bỏ trống sẽ lấy bản ghi mới nhất bất kỳ.",
    )
) -> Dict[str, Any]:
    es = get_es_client()
    query = build_location_query(location)

    try:
        resp = es.search(
            index=ES_INDEX,
            body={
                "size": 1,
                "query": query,
                "sort": [{"Local_Time": {"order": "desc"}}],
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Elasticsearch query error: {e}")

    hits = resp.get("hits", {}).get("hits", [])
    if not hits:
        raise HTTPException(status_code=404, detail="Không tìm thấy dữ liệu cho location này")

    return enrich_forecast_alerts(hits[0]["_source"])


@router.get("/weather/history")
def get_weather_history(
    location: str = Query(..., description="Địa điểm, ví dụ: 'Hà Nội, Việt Nam'"),
    limit: int = Query(50, ge=1, le=500, description="Số bản ghi lịch sử tối đa cần trả về"),
) -> List[Dict[str, Any]]:
    es = get_es_client()
    query = build_location_query(location)

    try:
        resp = es.search(
            index=ES_INDEX,
            body={
                "size": limit,
                "query": query,
                "sort": [{"Local_Time.keyword": {"order": "desc"}}],
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Elasticsearch query error: {e}")

    hits = resp.get("hits", {}).get("hits", [])
    return [h.get("_source", {}) for h in hits]


@router.get("/weather/batch/yoy")
def get_batch_yoy(
    location: Optional[str] = Query(
        None,
        description="Địa điểm; alias realtime hoặc tên tỉnh cũ sẽ được chuẩn hóa sang 34 tỉnh/thành mới.",
    ),
    province_view: Optional[str] = Query(None, description="pre_merge_63 hoặc post_merge_34"),
    limit: int = Query(50, ge=1, le=500, description="Số bản ghi so sánh cùng kỳ tối đa cần trả về"),
) -> List[Dict[str, Any]]:
    es = get_es_client()
    query = build_batch_query(location, province_view)

    try:
        resp = es.search(
            index=ES_INDEX_BATCH_YOY,
            body={
                "size": limit,
                "query": query,
                "sort": [{"date": {"order": "desc"}}],
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Elasticsearch query error: {e}")

    hits = resp.get("hits", {}).get("hits", [])
    return [h.get("_source", {}) for h in hits]


@router.get("/weather/locations")
def list_locations(limit: int = Query(20, ge=1, le=100)) -> List[str]:
    es = get_es_client()
    try:
        resp = es.search(
            index=ES_INDEX,
            body={
                "size": 0,
                "aggs": {
                    "locations": {
                        "terms": {
                            "field": "Location.keyword",
                            "size": limit,
                        }
                    }
                },
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Elasticsearch query error: {e}")

    buckets = resp.get("aggregations", {}).get("locations", {}).get("buckets", [])
    return [b["key"] for b in buckets]


@router.get("/weather/batch/locations")
def list_batch_locations(
    province_view: str = Query(POST_MERGE_VIEW, description="pre_merge_63 hoặc post_merge_34"),
    limit: int = Query(100, ge=1, le=200),
) -> List[str]:
    es = get_es_client()
    view = normalize_province_view(province_view) or POST_MERGE_VIEW
    try:
        resp = es.search(
            index=ES_INDEX_BATCH_DAILY,
            body={
                "size": 0,
                "query": {"term": {"province_view.keyword": view}},
                "aggs": {
                    "locations": {
                        "terms": {
                            "field": "Location.keyword",
                            "size": limit,
                            "order": {"_key": "asc"},
                        }
                    }
                },
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Elasticsearch query error: {e}")

    buckets = resp.get("aggregations", {}).get("locations", {}).get("buckets", [])
    return [b["key"] for b in buckets]


@router.get("/weather/batch/daily")
def get_batch_daily(
    location: Optional[str] = Query(
        None,
        description="Địa điểm, ví dụ: 'Hà Nội, Việt Nam'. Nếu bỏ trống sẽ lấy tất cả dữ liệu batch.",
    ),
    province_view: Optional[str] = Query(None, description="pre_merge_63 hoặc post_merge_34"),
    limit: int = Query(50, ge=1, le=500, description="Số bản ghi batch tối đa cần trả về"),
) -> List[Dict[str, Any]]:
    es = get_es_client()
    query = build_batch_query(location, province_view)

    try:
        resp = es.search(
            index=ES_INDEX_BATCH_DAILY,
            body={
                "size": limit,
                "query": query,
                "sort": [{"date": {"order": "desc"}}],
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Elasticsearch query error: {e}")

    hits = resp.get("hits", {}).get("hits", [])
    return [h.get("_source", {}) for h in hits]


@router.get("/weather/batch/humidity")
def get_batch_humidity(
    location: Optional[str] = Query(
        None,
        description="Địa điểm, ví dụ: 'Hà Nội, Việt Nam'. Nếu bỏ trống sẽ lấy dữ liệu độ ẩm batch cho tất cả địa điểm.",
    ),
    province_view: Optional[str] = Query(None, description="pre_merge_63 hoặc post_merge_34"),
    limit: int = Query(50, ge=1, le=500, description="Số bản ghi độ ẩm tối đa cần trả về"),
) -> List[Dict[str, Any]]:
    es = get_es_client()
    query = build_batch_query(location, province_view)

    try:
        resp = es.search(
            index=ES_INDEX_BATCH_DAILY,
            body={
                "size": limit,
                "query": query,
                "sort": [{"date": {"order": "desc"}}],
                "_source": ["Location", "date", "avg_humidity", "avg_temp", "total_precip"],
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Elasticsearch query error: {e}")

    hits = resp.get("hits", {}).get("hits", [])
    return [h.get("_source", {}) for h in hits]


@router.get("/weather/batch/humidity/summary")
def get_batch_humidity_summary(
    location: Optional[str] = Query(
        None,
        description="Địa điểm, ví dụ: 'Hà Nội, Việt Nam'. Nếu bỏ trống sẽ tính tổng hợp độ ẩm cho tất cả địa điểm.",
    ),
    province_view: Optional[str] = Query(None, description="pre_merge_63 hoặc post_merge_34"),
) -> Dict[str, Any]:
    es = get_es_client()
    normalized_location = normalize_batch_location(location, normalize_province_view(province_view))
    query = build_batch_query(location, province_view)

    try:
        resp = es.search(
            index=ES_INDEX_BATCH_DAILY,
            body={
                "size": 0,
                "query": query,
                "aggs": {
                    "avg_humidity": {"avg": {"field": "avg_humidity"}},
                    "min_humidity": {"min": {"field": "avg_humidity"}},
                    "max_humidity": {"max": {"field": "avg_humidity"}},
                },
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Elasticsearch query error: {e}")

    aggs = resp.get("aggregations", {})
    return {
        "location": normalized_location or "all",
        "avg_humidity": _r(aggs.get("avg_humidity", {}).get("value")),
        "min_humidity": _r(aggs.get("min_humidity", {}).get("value")),
        "max_humidity": _r(aggs.get("max_humidity", {}).get("value")),
    }


@router.get("/weather/batch/precipitation")
def get_batch_precipitation(
    location: Optional[str] = Query(
        None,
        description="Địa điểm, ví dụ: 'Hà Nội, Việt Nam'. Nếu bỏ trống sẽ lấy dữ liệu lượng mưa batch cho tất cả địa điểm.",
    ),
    province_view: Optional[str] = Query(None, description="pre_merge_63 hoặc post_merge_34"),
    limit: int = Query(50, ge=1, le=500, description="Số bản ghi lượng mưa tối đa cần trả về"),
) -> List[Dict[str, Any]]:
    es = get_es_client()
    query = build_batch_query(location, province_view)

    try:
        resp = es.search(
            index=ES_INDEX_BATCH_DAILY,
            body={
                "size": limit,
                "query": query,
                "sort": [{"date": {"order": "desc"}}],
                "_source": ["Location", "date", "total_precip", "avg_temp", "avg_humidity"],
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Elasticsearch query error: {e}")

    hits = resp.get("hits", {}).get("hits", [])
    return [h.get("_source", {}) for h in hits]


@router.get("/weather/batch/precipitation/summary")
def get_batch_precipitation_summary(
    location: Optional[str] = Query(
        None,
        description="Địa điểm, ví dụ: 'Hà Nội, Việt Nam'. Nếu bỏ trống sẽ tính tổng hợp lượng mưa cho tất cả địa điểm.",
    ),
    province_view: Optional[str] = Query(None, description="pre_merge_63 hoặc post_merge_34"),
) -> Dict[str, Any]:
    es = get_es_client()
    normalized_location = normalize_batch_location(location, normalize_province_view(province_view))
    query = build_batch_query(location, province_view)

    try:
        resp = es.search(
            index=ES_INDEX_BATCH_DAILY,
            body={
                "size": 0,
                "query": query,
                "aggs": {
                    "avg_precip": {"avg": {"field": "total_precip"}},
                    "min_precip": {"min": {"field": "total_precip"}},
                    "max_precip": {"max": {"field": "total_precip"}},
                },
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Elasticsearch query error: {e}")

    aggs = resp.get("aggregations", {})
    return {
        "location": normalized_location or "all",
        "avg_precip": _r(aggs.get("avg_precip", {}).get("value")),
        "min_precip": _r(aggs.get("min_precip", {}).get("value")),
        "max_precip": _r(aggs.get("max_precip", {}).get("value")),
    }


@router.get("/weather/batch/summary")
def get_batch_summary(
    location: Optional[str] = Query(
        None,
        description="Địa điểm, ví dụ: 'Hà Nội, Việt Nam'. Nếu bỏ trống sẽ lấy tổng hợp batch cho tất cả địa điểm.",
    ),
    province_view: Optional[str] = Query(None, description="pre_merge_63 hoặc post_merge_34"),
) -> Dict[str, Any]:
    es = get_es_client()
    view = normalize_province_view(province_view)
    normalized_location = normalize_batch_location(location, view)
    query = build_batch_query(location, province_view)

    try:
        resp = es.search(
            index=ES_INDEX_BATCH_STATS,
            body={
                "size": 1,
                "query": query,
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Elasticsearch query error: {e}")

    hits = resp.get("hits", {}).get("hits", [])
    if not hits:
        if normalized_location:
            fallback = build_batch_summary_from_daily(es, normalized_location, view)
            if fallback:
                return fallback
        raise HTTPException(status_code=404, detail="Không tìm thấy dữ liệu batch cho location này")

    return hits[0].get("_source", {})


def _r(v):
    """Round numeric value to 1 decimal."""
    if v is None:
        return None
    try:
        return round(float(v), 1)
    except (TypeError, ValueError):
        return v


def build_batch_summary_from_daily(
    es: Elasticsearch,
    location: str,
    province_view: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Fallback when weather_batch_stats is empty: derive summary from daily aggregates."""
    daily_query = build_batch_query(location, province_view)

    try:
        hottest_resp = es.search(
            index=ES_INDEX_BATCH_DAILY,
            body={
                "size": 1,
                "query": daily_query,
                "sort": [
                    {"max_temp": {"order": "desc"}},
                    {"avg_temp": {"order": "desc"}},
                ],
            },
        )
        coldest_resp = es.search(
            index=ES_INDEX_BATCH_DAILY,
            body={
                "size": 1,
                "query": daily_query,
                "sort": [
                    {"min_temp": {"order": "asc"}},
                    {"avg_temp": {"order": "asc"}},
                ],
            },
        )
        latest_resp = es.search(
            index=ES_INDEX_BATCH_DAILY,
            body={
                "size": 1,
                "query": daily_query,
                "sort": [{"date": {"order": "desc"}}],
            },
        )
        heat_resp = es.search(
            index=ES_INDEX_BATCH_DAILY,
            body={
                "size": 5000,
                "query": daily_query,
                "sort": [{"date": {"order": "asc"}}],
                "_source": ["Location", "date", "avg_temp", "max_temp"],
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Elasticsearch query error: {e}")

    hottest_hits = hottest_resp.get("hits", {}).get("hits", [])
    coldest_hits = coldest_resp.get("hits", {}).get("hits", [])
    latest_hits = latest_resp.get("hits", {}).get("hits", [])
    if not hottest_hits or not coldest_hits or not latest_hits:
        return None

    longest = {
        "longest_heatwave_days": 0,
        "heatwave_start": "",
        "heatwave_end": "",
        "heatwave_max_temp": 0.0,
    }
    current = None
    previous_date = None

    for hit in heat_resp.get("hits", {}).get("hits", []):
        src = hit.get("_source", {})
        date = src.get("date")
        avg_temp = src.get("avg_temp")
        max_temp = src.get("max_temp") or 0.0
        is_heat_day = avg_temp is not None and float(avg_temp) >= 30.0

        if not is_heat_day:
            current = None
            previous_date = date
            continue

        consecutive = False
        if current and previous_date and date:
            previous = date_cls.fromisoformat(previous_date)
            current_date = date_cls.fromisoformat(date)
            consecutive = (current_date - previous).days == 1

        if not current or not consecutive:
            current = {
                "longest_heatwave_days": 0,
                "heatwave_start": date,
                "heatwave_end": date,
                "heatwave_max_temp": 0.0,
            }

        current["longest_heatwave_days"] += 1
        current["heatwave_end"] = date
        current["heatwave_max_temp"] = max(float(current["heatwave_max_temp"]), float(max_temp))
        if current["longest_heatwave_days"] > longest["longest_heatwave_days"]:
            longest = current.copy()

        previous_date = date

    hottest = hottest_hits[0].get("_source", {})
    coldest = coldest_hits[0].get("_source", {})
    latest = latest_hits[0].get("_source", {})

    return {
        "Location": location,
        "hottest_date": hottest.get("date"),
        "hottest_temp": hottest.get("max_temp"),
        "coldest_date": coldest.get("date"),
        "coldest_temp": coldest.get("min_temp"),
        "latest_date": latest.get("date"),
        "latest_avg_temp": latest.get("avg_temp"),
        **longest,
    }


@router.get("/weather/chart")
def get_chart_data(
    location: str = Query(..., description="Địa điểm (realtime format, e.g. 'Hà Nội, VN')"),
    days: int = Query(30, ge=1, le=365, description="Số ngày dùng khi không truyền date range"),
    start_date: Optional[date_cls] = Query(None, description="Ngày bắt đầu, định dạng YYYY-MM-DD"),
    end_date: Optional[date_cls] = Query(None, description="Ngày kết thúc, định dạng YYYY-MM-DD"),
    province_view: Optional[str] = Query(None, description="pre_merge_63 hoặc post_merge_34"),
) -> Dict[str, Any]:
    """Lấy dữ liệu biểu đồ hoàn toàn từ batch daily (temp, humidity, precip)."""
    es = get_es_client()

    if start_date and end_date and start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date phải nhỏ hơn hoặc bằng end_date")

    filters = build_batch_filters(location, province_view, start_date, end_date)

    size = 1000 if start_date or end_date else days

    chart_map: Dict[str, Dict[str, Any]] = {}
    try:
        batch_resp = es.search(
            index=ES_INDEX_BATCH_DAILY,
            body={
                "size": size,
                "query": {"bool": {"filter": filters}} if filters else {"match_all": {}},
                "sort": [{"date": {"order": "desc"}}],
            },
        )
        for hit in batch_resp.get("hits", {}).get("hits", []):
            src = hit["_source"]
            d = src.get("date", "")
            if d:
                chart_map[d] = {
                    "date": d,
                    "avg_temp": _r(src.get("avg_temp")),
                    "min_temp": _r(src.get("min_temp")),
                    "max_temp": _r(src.get("max_temp")),
                    "avg_humidity": _r(src.get("avg_humidity")),
                    "total_precip": _r(src.get("total_precip")),
                }
    except Exception:
        pass

    sorted_data = sorted(chart_map.values(), key=lambda x: x["date"])
    if not (start_date or end_date):
        sorted_data = sorted_data[-days:]

    return {
        "location": location,
        "province_view": normalize_province_view(province_view) or infer_province_view(start_date, end_date),
        "days": days,
        "start_date": start_date.isoformat() if start_date else None,
        "end_date": end_date.isoformat() if end_date else None,
        "data": sorted_data,
    }


@router.get("/weather/batch/unpivoted")
def get_batch_unpivoted(
    location: Optional[str] = Query(
        None,
        description="Địa điểm cần lấy dữ liệu unpivoted.",
    ),
    limit: int = Query(100, ge=1, le=1000, description="Số bản ghi tối đa trả về"),
) -> List[Dict[str, Any]]:
    """Lấy dữ liệu unpivoted (Month, avg_temp) của các địa phương từ Elasticsearch."""
    es = get_es_client()
    filters = []
    if location:
        filters.append({"term": {"Location.keyword": normalize_location_name(location)}})
        
    query = {"bool": {"filter": filters}} if filters else {"match_all": {}}
    
    try:
        resp = es.search(
            index="weather_batch_unpivoted",
            body={
                "size": limit,
                "query": query,
                "sort": [
                    {"year_num": {"order": "desc"}}, 
                    {"Month.keyword": {"order": "asc"}}
                ],
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Elasticsearch query error: {e}")
        
    hits = resp.get("hits", {}).get("hits", [])
    return [h.get("_source", {}) for h in hits]
