FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    iproute2 \
    iptables \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8001
ENTRYPOINT ["python", "server.py"]
