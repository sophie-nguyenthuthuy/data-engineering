FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/opt/app/src

WORKDIR /opt/app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /opt/app/requirements.txt
RUN pip install -r /opt/app/requirements.txt

COPY src /opt/app/src
COPY dashboard /opt/app/dashboard

EXPOSE 3000 8501
