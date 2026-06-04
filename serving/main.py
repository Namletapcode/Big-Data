import os

from fastapi.staticfiles import StaticFiles
from fastapi import FastAPI

from api import router as api_router
from ui import router as ui_router

app = FastAPI(title="Weather Serving Layer", version="1.0.0")


@app.middleware("http")
async def disable_dashboard_cache(request, call_next):
    response = await call_next(request)
    if request.url.path == "/" or request.url.path.startswith(("/api/", "/static/")):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")

# API để test bằng Swagger
app.include_router(api_router, prefix="/api")

# UI backend
app.include_router(ui_router)
