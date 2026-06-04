# Serving Layer: Workflow, Elasticsearch Query và API

## 1. Vai trò của Serving Layer

`serving` là lớp phục vụ dữ liệu cho UI. Nó không trực tiếp crawl dữ liệu và cũng không xử lý batch nặng. Nhiệm vụ chính của nó là:

- Nhận request từ trình duyệt.
- Query dữ liệu đã được ghi sẵn trong Elasticsearch.
- Chuẩn hóa response thành JSON.
- Trả dữ liệu cho UI để visualize bằng card, table và chart.

Workflow tổng quát:

```text
Crawler / Spark Streaming / Spark Batch
  -> ghi dữ liệu vào Elasticsearch
  -> FastAPI Serving Layer query Elasticsearch
  -> UI gọi API qua fetch()
  -> render dữ liệu lên dashboard
```

Trong project này, serving layer dùng:

- `FastAPI`: tạo REST API và route UI.
- `Elasticsearch Python Client`: query index Elasticsearch.
- `Jinja2Templates`: render file HTML dashboard.
- `Chart.js`: visualize dữ liệu ở frontend.

Các file chính:

```text
serving/
  main.py                 # Khởi tạo FastAPI app, mount static, include router
  api.py                  # Toàn bộ REST API query Elasticsearch
  ui.py                   # Render dashboard index.html
  templates/index.html    # Frontend UI và JavaScript gọi API
  static/styles.css       # CSS giao diện
```

## 2. Entry Point của Serving

File `main.py` tạo FastAPI app:

```python
app = FastAPI(title="Weather Serving Layer", version="1.0.0")
```

Sau đó mount static files:

```python
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
```

Gắn API router với prefix `/api`:

```python
app.include_router(api_router, prefix="/api")
```

Gắn UI router:

```python
app.include_router(ui_router)
```

Nghĩa là:

```text
GET /              -> trả dashboard HTML
GET /api/...       -> trả JSON data cho UI
GET /static/...    -> trả CSS/static assets
```

Middleware trong `main.py` tắt cache cho dashboard/API/static:

```python
@app.middleware("http")
async def disable_dashboard_cache(request, call_next):
    response = await call_next(request)
    if request.url.path == "/" or request.url.path.startswith(("/api/", "/static/")):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return response
```

Mục đích: khi sửa UI hoặc API, browser không giữ bản cũ.

## 3. UI Route

File `ui.py` render dashboard:

```python
@router.get("/", response_class=HTMLResponse)
async def weather_dashboard(request: Request):
    try:
        locations = list_locations(limit=50)
    except Exception:
        locations = []

    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "locations": locations,
            "asset_version": int(os.path.getmtime(os.path.join(BASE_DIR, "static", "styles.css"))),
        },
    )
```

Ý nghĩa:

- Khi user mở `http://localhost:8000/`, FastAPI trả `templates/index.html`.
- `locations` được lấy từ Elasticsearch để đổ vào dropdown realtime.
- `asset_version` dùng để chống cache CSS.

## 4. Các Elasticsearch Index Đang Dùng

Trong `api.py`, các index được cấu hình bằng biến môi trường:

```python
ES_INDEX = os.getenv("ES_INDEX", "weather_realtime")
ES_INDEX_FORECAST = os.getenv("ES_INDEX_FORECAST", "weather_forecast")
ES_INDEX_BATCH_DAILY = os.getenv("ES_INDEX_BATCH_DAILY", "weather_batch_daily")
ES_INDEX_BATCH_STATS = os.getenv("ES_INDEX_BATCH_STATS", "weather_batch_stats")
ES_INDEX_BATCH_YOY = os.getenv("ES_INDEX_BATCH_YOY", "weather_batch_yoy")
ES_INDEX_BATCH_UNPIVOTED = os.getenv("ES_INDEX_BATCH_UNPIVOTED", "weather_batch_unpivoted")
ES_INDEX_BATCH_VALID_WEATHER = os.getenv("ES_INDEX_BATCH_VALID_WEATHER", "weather_batch_valid_weather")
```

Bảng vai trò:

| Index | Nguồn ghi | Vai trò |
|---|---|---|
| `weather_realtime` | Spark Streaming | Dữ liệu thời tiết realtime/latest |
| `weather_forecast` | Forecast pipeline | Dự báo 15 ngày |
| `weather_batch_daily` | Spark Batch | Dữ liệu aggregate theo ngày |
| `weather_batch_stats` | Spark Batch | Thống kê cực trị, ngày nóng nhất/lạnh nhất |
| `weather_batch_yoy` | Spark Batch | So sánh cùng kỳ năm trước |
| `weather_batch_unpivoted` | Spark Batch | Dữ liệu Month/avg_temp cho Pivot & Unpivot |
| `weather_batch_valid_weather` | Spark Batch | Dữ liệu raw đã clean/mapping |

Luồng đúng cho batch:

```text
MinIO historical raw
  -> Spark Batch clean + aggregate
  -> ghi processed Parquet ra MinIO
  -> ghi serving indexes vào Elasticsearch
  -> Backend API chỉ cần query Elasticsearch
```

## 5. Kết nối Elasticsearch

Hàm tạo client:

```python
def get_es_client() -> Elasticsearch:
    return Elasticsearch(
        ES_HOST,
        headers={
            "Accept": "application/vnd.elasticsearch+json; compatible-with=8",
            "Content-Type": "application/vnd.elasticsearch+json; compatible-with=8",
        },
    )
```

Điểm cần nhớ:

- `ES_HOST` thường là `http://elasticsearch:9200` khi chạy trong Docker.
- Khi chạy local có thể là `http://localhost:9200`.
- Header `compatible-with=8` dùng cho Elasticsearch 8.x.

Test health:

```python
@router.get("/health")
def health_check() -> Dict[str, Any]:
    es = get_es_client()
    info = es.info()
    return {
        "status": "ok",
        "elasticsearch": {
            "cluster_name": info.get("cluster_name"),
            "version": info.get("version", {}).get("number"),
        },
    }
```

Request:

```bash
curl http://localhost:8000/api/health
```

## 6. Query Realtime Data

### 6.1 Latest Weather

Endpoint:

```text
GET /api/weather/latest?location=Hà Nội, VN
```

Code chính:

```python
resp = es.search(
    index=ES_INDEX,
    body={
        "size": 1,
        "query": query,
        "sort": [{"Local_Time": {"order": "desc"}}],
    },
)
```

Ý nghĩa:

- Query index `weather_realtime`.
- Lấy 1 record mới nhất.
- Sort theo `Local_Time` giảm dần.

Query location:

```python
def build_location_query(location: Optional[str]) -> Dict[str, Any]:
    if not location:
        return {"match_all": {}}
    return {"term": {"Location.keyword": location}}
```

Syntax quan trọng:

```json
{
  "term": {
    "Location.keyword": "Hà Nội, VN"
  }
}
```

`.keyword` dùng để match chính xác chuỗi, không bị Elasticsearch analyzer tách từ.

### 6.2 Merge Forecast

Sau khi lấy realtime document, API lấy thêm forecast mới nhất:

```python
forecast = fetch_latest_forecast(es, document.get("Location"))
return enrich_forecast_alerts(merge_forecast_document(document, forecast))
```

Forecast query:

```python
resp = es.search(
    index=ES_INDEX_FORECAST,
    body={
        "size": 1,
        "query": build_location_query(location),
        "sort": [{"Forecast_Updated_At": {"order": "desc"}}],
    },
)
```

Luồng:

```text
weather_realtime
  -> lấy current weather mới nhất
weather_forecast
  -> lấy forecast mới nhất cùng Location
merge
  -> trả một JSON hoàn chỉnh cho UI
```

## 7. Query Batch Data

Batch data cần xử lý thêm vấn đề 63 tỉnh trước sáp nhập và 34 tỉnh sau sáp nhập.

Hai view chính:

```python
PRE_MERGE_VIEW = "pre_merge_63"
POST_MERGE_VIEW = "post_merge_34"
```

Quy tắc:

```text
pre_merge_63   -> dữ liệu theo 63 tỉnh cũ
post_merge_34  -> dữ liệu theo 34 tỉnh/thành mới
```

### 7.1 Normalize tên tỉnh

```python
def normalize_location_name(location: Optional[str]) -> Optional[str]:
    normalized = re.sub(r",\s*(VN|Việt Nam)\s*$", "", location).strip()
    normalized = re.sub(r"^(Tỉnh|Thành phố)\s+", "", normalized).strip()
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized
```

Ví dụ:

```text
"Tỉnh Bình Dương, Việt Nam" -> "Bình Dương"
"Thành phố Hồ Chí Minh"     -> "Hồ Chí Minh"
```

Sau đó nếu đang xem `post_merge_34`, tên tỉnh cũ được map sang tỉnh mới:

```python
def normalize_batch_location(location, province_view=POST_MERGE_VIEW):
    normalized = normalize_location_name(location)
    if province_view == PRE_MERGE_VIEW:
        return normalized
    return BATCH_LOCATION_MERGES.get(normalized, normalized)
```

Ví dụ:

```text
Bình Dương -> Hồ Chí Minh
Kiên Giang -> An Giang
Đắk Nông   -> Lâm Đồng
```

## 8. Build Batch Query

Đây là helper rất quan trọng:

```python
def build_batch_filters(location=None, province_view=None, start_date=None, end_date=None):
    filters = []
    selected_view = normalize_province_view(province_view) or infer_province_view(start_date, end_date)

    if selected_view:
        filters.append({"term": {"province_view.keyword": selected_view}})

    if location:
        filters.append({"term": {"Location.keyword": normalize_batch_location(location, selected_view)}})

    if start_date or end_date:
        date_range = {}
        if start_date:
            date_range["gte"] = start_date.isoformat()
        if end_date:
            date_range["lte"] = end_date.isoformat()
        filters.append({"range": {"date": date_range}})

    return filters
```

Sau đó đóng gói thành query:

```python
def build_batch_query(location=None, province_view=None, start_date=None, end_date=None):
    filters = build_batch_filters(location, province_view, start_date, end_date)
    if not filters:
        return {"match_all": {}}
    return {"bool": {"filter": filters}}
```

Ví dụ query Elasticsearch:

```json
{
  "bool": {
    "filter": [
      { "term": { "province_view.keyword": "post_merge_34" } },
      { "term": { "Location.keyword": "An Giang" } },
      { "range": { "date": { "gte": "2026-01-01", "lte": "2026-05-30" } } }
    ]
  }
}
```

Vì sao dùng `bool.filter`?

- `filter` không tính relevance score.
- Phù hợp cho dashboard vì ta chỉ cần lọc chính xác.
- Elasticsearch có thể cache filter tốt hơn query text.

## 9. Batch Summary API

Endpoint:

```text
GET /api/weather/batch/summary?location=An%20Giang&province_view=post_merge_34
```

Code:

```python
resp = es.search(
    index=ES_INDEX_BATCH_STATS,
    body={
        "size": 1,
        "query": query,
    },
)
```

Nguồn dữ liệu:

```text
weather_batch_stats
```

Trả về các thông tin như:

- `hottest_date`
- `hottest_temp`
- `coldest_date`
- `coldest_temp`
- `latest_date`
- `latest_avg_temp`
- `longest_heatwave_days`
- `heatwave_start`
- `heatwave_end`

Nếu index stats chưa có dữ liệu, API fallback sang daily:

```python
fallback = build_batch_summary_from_daily(es, normalized_location, view)
```

Fallback này query `weather_batch_daily` để tự tính:

- ngày nóng nhất
- ngày lạnh nhất
- ngày mới nhất
- chuỗi ngày nắng nóng dài nhất

## 10. Chart API

Endpoint:

```text
GET /api/weather/chart?location=An%20Giang&province_view=post_merge_34&start_date=2026-01-01&end_date=2026-05-30
```

Code:

```python
batch_resp = es.search(
    index=ES_INDEX_BATCH_DAILY,
    body={
        "size": size,
        "query": {"bool": {"filter": filters}} if filters else {"match_all": {}},
        "sort": [{"date": {"order": "desc"}}],
    },
)
```

Nguồn dữ liệu:

```text
weather_batch_daily
```

Response được gom thành:

```python
chart_map[d] = {
    "date": d,
    "avg_temp": _r(src.get("avg_temp")),
    "min_temp": _r(src.get("min_temp")),
    "max_temp": _r(src.get("max_temp")),
    "avg_humidity": _r(src.get("avg_humidity")),
    "total_precip": _r(src.get("total_precip")),
}
```

UI dùng dữ liệu này để vẽ:

- biểu đồ nhiệt độ
- biểu đồ độ ẩm
- biểu đồ lượng mưa

## 11. Pivot & Unpivot API

Đây là workflow đã được sửa về đúng kiến trúc.

Workflow:

```text
Spark Batch
  -> build daily_df
  -> pivot avg_temp theo month
  -> unpivot về dạng dòng
  -> ghi vào weather_batch_unpivoted
  -> Serving API query weather_batch_unpivoted
  -> UI vẽ bar chart + pivot table
```

Endpoint:

```text
GET /api/weather/batch/unpivoted?location=An%20Giang&province_view=post_merge_34&year=2026&limit=1000
```

Code query:

```python
filters = []
view = normalize_province_view(province_view) or POST_MERGE_VIEW
filters.append({"term": {"province_view.keyword": view}})

if location:
    filters.append({"term": {"Location.keyword": normalize_batch_location(location, view)}})

if year:
    filters.append({"term": {"year_num": year}})

query = {"bool": {"filter": filters}}
```

Elasticsearch search:

```python
resp = es.search(
    index=ES_INDEX_BATCH_UNPIVOTED,
    body={
        "size": limit,
        "query": query,
        "sort": [
            {"year_num": {"order": "desc", "unmapped_type": "integer"}},
            {"month_num": {"order": "asc", "missing": "_last", "unmapped_type": "integer"}},
            {"Month.keyword": {"order": "asc", "unmapped_type": "keyword"}},
        ],
    },
)
```

Document mẫu trong ES:

```json
{
  "Location": "An Giang",
  "year_num": 2026,
  "province_view": "post_merge_34",
  "Month": "Month_1",
  "avg_temp": 24.93548387096774,
  "month_num": 1
}
```

Tại sao cần `month_num`?

- Nếu sort string `Month.keyword`, thứ tự có thể thành `Month_1`, `Month_10`, `Month_11`, `Month_2`.
- `month_num` giúp sort đúng `1 -> 12`.

Syntax quan trọng:

```python
{"month_num": {"order": "asc", "missing": "_last", "unmapped_type": "integer"}}
```

`unmapped_type` giúp query không lỗi nếu index cũ chưa có field này.

## 12. Batch Locations API

Endpoint:

```text
GET /api/weather/batch/locations?province_view=post_merge_34&limit=200
```

Code:

```python
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
```

Điểm quan trọng:

- `size: 0`: không lấy document, chỉ lấy aggregation.
- `terms`: gom danh sách tỉnh duy nhất.
- `order: {"_key": "asc"}`: sort tên tỉnh A-Z.

Response:

```json
["An Giang", "Bắc Ninh", "Cao Bằng", "Cà Mau", "Cần Thơ"]
```

UI dùng API này để load dropdown tỉnh theo `pre_merge_63` hoặc `post_merge_34`.

Trong giao diện hiện tại, phần `Thống kê` và `Pivot & Unpivot` được gộp chung trong một section:

```html
<section id="batch-pivot-section" class="card section-panel hidden">
```

Hai control dùng chung cho cả thống kê và pivot:

```html
<select id="batch-period">
<select id="batch-location">
```

Điều này giúp người dùng chỉ cần chọn một lần `giai đoạn tỉnh` và `tỉnh`, sau đó dashboard đồng thời cập nhật cả thống kê tổng hợp và biểu đồ Pivot/Unpivot.

## 13. UI Gọi API Như Thế Nào

Frontend trong `templates/index.html` dùng `fetch()`.

Ví dụ load danh sách tỉnh:

```javascript
async function loadBatchLocations(period, selectId) {
  const params = new URLSearchParams({ province_view: period, limit: "200" });
  const res = await fetch(`/api/weather/batch/locations?${params}`);
  const locations = await res.json();
}
```

Ví dụ load chart:

```javascript
const params = new URLSearchParams({
  location: loc,
  province_view: $("chart-period").value,
  days: currentDays,
});

if (start) params.set("start_date", start);
if (end) params.set("end_date", end);

const res = await fetch(`/api/weather/chart?${params}`);
const json = await res.json();
chartData = json.data || [];
renderChart();
```

Ví dụ load pivot:

```javascript
const params = new URLSearchParams({
  location: loc,
  province_view: $("batch-period").value,
  limit: "1000",
});

if ($("pivot-year").value) {
  params.set("year", $("pivot-year").value);
}

const res = await fetch(`/api/weather/batch/unpivoted?${params}`);
const rows = await res.json();
```

Trong code hiện tại, pivot dùng chung `batch-location`:

```javascript
async function fetchPivotData() {
  const loc = $("batch-location").value;
  const params = new URLSearchParams({
    location: loc,
    province_view: $("batch-period").value,
    limit: "1000",
  });
  if ($("pivot-year").value) params.set("year", $("pivot-year").value);
  const res = await fetch(`/api/weather/batch/unpivoted?${params}`);
}
```

Khi mở tab `batch-pivot-section`, UI gọi một hàm gộp:

```javascript
async function fetchBatchAndPivot() {
  await fetchBatchSummary();
  await populatePivotYears($("batch-location").value);
  await fetchPivotData();
}
```

Router frontend ánh xạ tab này như sau:

```javascript
const FEATURE_FETCH = {
  "section-current": fetchLatest,
  "forecast-section": fetchLatest,
  "batch-pivot-section": fetchBatchAndPivot,
  "history-section": fetchChartData,
};
```

Ý nghĩa:

```text
User chọn tỉnh / giai đoạn / năm
  -> JavaScript build URL query params
  -> fetch API
  -> API query Elasticsearch
  -> API trả JSON
  -> JavaScript render chart/table
```

## 14. Render Pivot Trên UI

API trả dạng unpivoted:

```json
[
  {"Location": "An Giang", "year_num": 2026, "Month": "Month_1", "avg_temp": 24.9},
  {"Location": "An Giang", "year_num": 2026, "Month": "Month_2", "avg_temp": 26.6}
]
```

UI group lại thành pivot table:

```javascript
function groupPivotRows(rows) {
  const grouped = new Map();
  rows.forEach((row) => {
    const year = row.year_num || row.year || "N/A";
    const key = `${row.Location || "N/A"}|${year}`;
    if (!grouped.has(key)) {
      grouped.set(key, {
        Location: row.Location || "N/A",
        year_num: year,
        months: {},
      });
    }
    grouped.get(key).months[row.Month] = row.avg_temp;
  });
  return [...grouped.values()].sort((a, b) => Number(b.year_num) - Number(a.year_num));
}
```

Sau đó render table 12 tháng:

```javascript
const MONTH_LABELS = Array.from({ length: 12 }, (_, i) => `Month_${i + 1}`);
```

Và vẽ bar chart:

```javascript
pivotChartInstance = new Chart(ctx, {
  type: "bar",
  data: {
    labels: MONTH_LABELS.map((month) => `T${monthNumber(month)}`),
    datasets: [{
      label: "Nhiệt độ trung bình",
      data: pivotData.map((row) => row.avg_temp),
    }],
  },
});
```

## 15. Luồng Thuyết Trình Cho Pivot & Unpivot

Bạn có thể trình bày theo thứ tự:

```text
1. Spark Batch đọc historical data từ MinIO.
2. Batch clean dữ liệu và map tỉnh theo 63/34.
3. Batch aggregate daily: avg_temp, min_temp, max_temp, humidity, precip.
4. Từ daily_df, batch pivot avg_temp theo month.
5. Batch unpivot lại về dạng dòng để dễ query Elasticsearch.
6. Batch ghi kết quả vào weather_batch_unpivoted.
7. UI gọi /api/weather/batch/unpivoted.
8. FastAPI build bool filter theo Location, province_view, year.
9. Elasticsearch trả rows Month_1 -> Month_12.
10. UI render bar chart và pivot table.
```

Sơ đồ:

```text
MinIO historical
  -> Spark Batch
  -> daily_df
  -> pivot/unpivot
  -> Elasticsearch: weather_batch_unpivoted
  -> FastAPI: /api/weather/batch/unpivoted
  -> UI: Chart.js + table
```

## 16. Các Syntax Elasticsearch Cần Nhớ

### Match all

```python
{"match_all": {}}
```

Dùng khi không có filter.

### Term query exact match

```python
{"term": {"Location.keyword": "An Giang"}}
```

Dùng `.keyword` để match chính xác.

### Bool filter

```python
{
  "bool": {
    "filter": [
      {"term": {"province_view.keyword": "post_merge_34"}},
      {"term": {"Location.keyword": "An Giang"}},
      {"term": {"year_num": 2026}}
    ]
  }
}
```

Dùng cho dashboard vì nhanh, rõ ràng, không cần score.

### Range query

```python
{"range": {"date": {"gte": "2026-01-01", "lte": "2026-05-30"}}}
```

Dùng lọc theo khoảng ngày.

### Sort

```python
"sort": [{"date": {"order": "desc"}}]
```

Hoặc sort nhiều field:

```python
"sort": [
  {"year_num": {"order": "desc"}},
  {"month_num": {"order": "asc"}}
]
```

### Source filtering

```python
"_source": ["Location", "date", "avg_humidity", "avg_temp", "total_precip"]
```

Chỉ lấy field cần thiết để response nhẹ hơn.

### Aggregation terms

```python
"aggs": {
  "locations": {
    "terms": {
      "field": "Location.keyword",
      "size": 200,
      "order": {"_key": "asc"}
    }
  }
}
```

Dùng để lấy danh sách tỉnh duy nhất.

### Aggregation avg/min/max

```python
"aggs": {
  "avg_humidity": {"avg": {"field": "avg_humidity"}},
  "min_humidity": {"min": {"field": "avg_humidity"}},
  "max_humidity": {"max": {"field": "avg_humidity"}}
}
```

Dùng để tính summary nhanh ngay trên Elasticsearch.

## 17. Các API Quan Trọng Để Demo

Health:

```bash
curl http://localhost:8000/api/health
```

Latest realtime:

```bash
curl 'http://localhost:8000/api/weather/latest?location=Hà%20Nội,%20VN'
```

Batch locations 34 tỉnh:

```bash
curl 'http://localhost:8000/api/weather/batch/locations?province_view=post_merge_34&limit=200'
```

Batch summary:

```bash
curl 'http://localhost:8000/api/weather/batch/summary?location=An%20Giang&province_view=post_merge_34'
```

Chart data:

```bash
curl 'http://localhost:8000/api/weather/chart?location=An%20Giang&province_view=post_merge_34&start_date=2026-01-01&end_date=2026-05-30'
```

Pivot & Unpivot:

```bash
curl 'http://localhost:8000/api/weather/batch/unpivoted?location=An%20Giang&province_view=post_merge_34&year=2026&limit=1000'
```

Check ES trực tiếp:

```bash
curl 'http://localhost:9200/weather_batch_unpivoted/_count'
```

Search sample ES:

```bash
curl 'http://localhost:9200/weather_batch_unpivoted/_search?size=1&filter_path=hits.hits._source'
```

## 18. Điểm Cần Nhấn Mạnh Khi Thuyết Trình

- Serving layer không xử lý dữ liệu nặng, chỉ query dữ liệu đã được chuẩn bị.
- Elasticsearch là storage phục vụ truy vấn nhanh cho UI.
- MinIO là data lake, không phải nguồn query chính cho dashboard.
- Batch pipeline chịu trách nhiệm tạo index `daily`, `stats`, `yoy`, `unpivoted`.
- API dùng `bool.filter`, `term`, `range`, `sort`, `aggs` để lấy đúng dữ liệu.
- UI chỉ gọi REST API, không biết Elasticsearch nằm phía sau.
- `province_view` là field quan trọng để phân biệt dữ liệu 63 tỉnh và 34 tỉnh.
- `month_num` là field quan trọng để sort `Month_1 -> Month_12` đúng thứ tự.

## 19. Một Câu Giải Thích Ngắn Gọn

Serving layer của project này là lớp API trung gian giữa UI và Elasticsearch. Spark Batch/Streaming ghi dữ liệu đã xử lý vào các index Elasticsearch, sau đó FastAPI nhận request từ frontend, build Elasticsearch DSL query theo location, province view, ngày hoặc năm, lấy JSON từ Elasticsearch và trả về cho UI để visualize bằng chart và table.
