FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY src/ ./src/

# Default env (overridden by docker-compose)
ENV MESH_REGION_ID=region-a
ENV MESH_PORT=8000
ENV MESH_DB_PATH=/data/mesh.db
ENV MESH_CONFLICT_STRATEGY=lww
ENV MESH_PEER_URLS=""
ENV MESH_REPLICATION_INTERVAL_SECONDS=2.0

VOLUME ["/data"]
EXPOSE 8000

CMD ["sh", "-c", "uvicorn src.main:app --host 0.0.0.0 --port $MESH_PORT"]
