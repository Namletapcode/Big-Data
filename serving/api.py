import os
from typing import Optional, List, Any, Dict

from elasticsearch import Elasticsearch
from fastapi import APIRouter, HTTPException, Query


ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
ES_INDEX = os.getenv("ES_INDEX", "weather_realtime")

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
    # Location là text (analyzed). Dùng keyword để match chính xác theo string.
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
                "sort": [{"Local_Time.keyword": {"order": "desc"}}],
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

