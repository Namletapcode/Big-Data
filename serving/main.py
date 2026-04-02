import os

from fastapi.staticfiles import StaticFiles
from fastapi import FastAPI

from api import router as api_router
from ui import router as ui_router

app = FastAPI(title="Weather Serving Layer", version="1.0.0")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")

# API để test bằng Swagger
app.include_router(api_router, prefix="/api")

# UI backend
app.include_router(ui_router)