import os

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from api import list_locations


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

router = APIRouter()


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
        },
    )

