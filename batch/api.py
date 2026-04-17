import os
from typing import Optional, List, Any, Dict

from elasticsearch import Elasticsearch
from fastapi import APIRouter, HTTPException, Query


ES_HOST = os.getenv("ES_HOST", "http://elasticsearch:9200")
ES_INDEX_BATCH = "weather_batch"

router = APIRouter()


def get_es_client() -> Elasticsearch:
    return Elasticsearch(
        ES_HOST,
        headers={
            "Accept": "application/vnd.elasticsearch+json; compatible-with=8",
            "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8",
        },
    )


@router.get("/weather/batch-averages")
def get_batch_averages(
    location: Optional[str] = Query(None, description="Địa điểm, ví dụ: 'Hà Nội'. Nếu bỏ trống sẽ lấy tất cả."),
    limit: int = Query(50, ge=1, le=500, description="Số bản ghi tối đa cần trả về"),
) -> List[Dict[str, Any]]:
    es = get_es_client()
    query = {"match_all": {}}
    if location:
        query = {"term": {"Location.keyword": location}}

    try:
        resp = es.search(
            index=ES_INDEX_BATCH,
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