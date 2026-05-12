FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends gcc librdkafka-dev && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY fraud_rules.py anomaly_detector.py spark_detector.py ./
CMD ["python", "-u", "anomaly_detector.py"]
