import os
from typing import Optional, List, Any, Dict

from elasticsearch import Elasticsearch
from fastapi import APIRouter, HTTPException, Query


ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
ES_INDEX = os.getenv("ES_INDEX", "weather_realtime")
ES_INDEX_BATCH_DAILY = os.getenv("ES_INDEX_BATCH_DAILY", "weather_batch_daily")
ES_INDEX_BATCH_STATS = os.getenv("ES_INDEX_BATCH_STATS", "weather_batch_stats")

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

    return hits[0]["_source"]


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


@router.get("/weather/batch/daily")
def get_batch_daily(
    location: Optional[str] = Query(
        None,
        description="Địa điểm, ví dụ: 'Hà Nội, Việt Nam'. Nếu bỏ trống sẽ lấy tất cả dữ liệu batch.",
    ),
    limit: int = Query(50, ge=1, le=500, description="Số bản ghi batch tối đa cần trả về"),
) -> List[Dict[str, Any]]:
    es = get_es_client()
    query = build_location_query(location)

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


@router.get("/weather/batch/summary")
def get_batch_summary(
    location: Optional[str] = Query(
        None,
        description="Địa điểm, ví dụ: 'Hà Nội, Việt Nam'. Nếu bỏ trống sẽ lấy tổng hợp batch cho tất cả địa điểm.",
    )
) -> Dict[str, Any]:
    es = get_es_client()
    query = build_location_query(location)

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


@router.get("/weather/chart")
def get_chart_data(
    location: str = Query(..., description="Địa điểm (realtime format, e.g. 'Hà Nội, VN')"),
    days: int = Query(7, ge=1, le=365, description="Số ngày"),
) -> Dict[str, Any]:
    """Lấy dữ liệu biểu đồ: batch daily (temp, humidity, precip) + realtime (wind, pressure)."""
    from collections import defaultdict

    es = get_es_client()

    # Batch daily dùng tên ngắn ("Hà Nội"), realtime dùng "Hà Nội, VN"
    batch_loc = location.rsplit(", ", 1)[0] if ", " in location else location

    # 1. Query batch daily
    chart_map: Dict[str, Dict[str, Any]] = {}
    try:
        batch_resp = es.search(
            index=ES_INDEX_BATCH_DAILY,
            body={
                "size": days,
                "query": {"term": {"Location.keyword": batch_loc}},
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
                    "avg_wind": None,
                    "avg_pressure": None,
                }
    except Exception:
        pass

    # 2. Query realtime for wind, pressure (and fallback)
    rt_agg: Dict[str, Dict[str, list]] = defaultdict(
        lambda: {"wind": [], "pressure": [], "temp": [], "humidity": [], "precip": []}
    )
    try:
        rt_resp = es.search(
            index=ES_INDEX,
            body={
                "size": min(days * 50, 500),
                "query": build_location_query(location),
                "sort": [{"Local_Time": {"order": "desc"}}],
                "_source": ["Local_Time", "Wind_Speed", "Pressure_hPa",
                            "Temp_C", "Humidity_%", "Precip_mm"],
            },
        )
        for hit in rt_resp.get("hits", {}).get("hits", []):
            src = hit["_source"]
            lt = src.get("Local_Time", "")
            d = lt[:10] if len(lt) >= 10 else ""
            if not d:
                continue
            for field, key in [("Wind_Speed", "wind"), ("Pressure_hPa", "pressure"),
                               ("Temp_C", "temp"), ("Humidity_%", "humidity"),
                               ("Precip_mm", "precip")]:
                if src.get(field) is not None:
                    rt_agg[d][key].append(src[field])
    except Exception:
        pass

    # Merge wind/pressure into batch data
    for d, entry in chart_map.items():
        agg = rt_agg.get(d)
        if agg:
            if agg["wind"]:
                entry["avg_wind"] = _r(sum(agg["wind"]) / len(agg["wind"]))
            if agg["pressure"]:
                entry["avg_pressure"] = _r(sum(agg["pressure"]) / len(agg["pressure"]))

    # Fallback: no batch data → use realtime aggregation
    if not chart_map:
        for d, agg in rt_agg.items():
            entry = {
                "date": d, "avg_temp": None, "min_temp": None, "max_temp": None,
                "avg_humidity": None, "total_precip": None,
                "avg_wind": None, "avg_pressure": None,
            }
            if agg["temp"]:
                entry["avg_temp"] = _r(sum(agg["temp"]) / len(agg["temp"]))
                entry["min_temp"] = _r(min(agg["temp"]))
                entry["max_temp"] = _r(max(agg["temp"]))
            if agg["humidity"]:
                entry["avg_humidity"] = _r(sum(agg["humidity"]) / len(agg["humidity"]))
            if agg["precip"]:
                entry["total_precip"] = _r(sum(agg["precip"]))
            if agg["wind"]:
                entry["avg_wind"] = _r(sum(agg["wind"]) / len(agg["wind"]))
            if agg["pressure"]:
                entry["avg_pressure"] = _r(sum(agg["pressure"]) / len(agg["pressure"]))
            chart_map[d] = entry

    sorted_data = sorted(chart_map.values(), key=lambda x: x["date"])[-days:]
    return {"location": location, "days": days, "data": sorted_data}
