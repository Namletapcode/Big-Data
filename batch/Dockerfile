FROM python:3.11-slim
RUN apt-get update && apt-get install -y default-jre && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY *.jsonl ./
COPY spark_batch.py ./
CMD ["spark-submit", "--packages", "org.elasticsearch:elasticsearch-spark-30_2.12:8.11.1", "spark_batch.py"]