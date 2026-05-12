FROM python:3.12-slim

WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -e ".[redis,dev]"

CMD ["python", "-m", "examples.three_stage_pipeline"]
