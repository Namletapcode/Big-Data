FROM python:3.14.3-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY src .

CMD ["python3", "-u", "weather_crawler.py"]
