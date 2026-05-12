FROM python:3.12-slim

# PySpark 3.5 runs on Java 17. `default-jre-headless` installs the Debian-default
# JRE and sets the `/usr/lib/jvm/default-java` alternatives symlink, so JAVA_HOME
# stays stable across arm64/amd64 builds.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      default-jre-headless \
      procps \
      curl \
      ca-certificates \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

WORKDIR /app

COPY pyproject.toml /app/
COPY src /app/src
COPY workspace.yaml /app/
RUN pip install --no-cache-dir -e .

ENV DAGSTER_HOME=/opt/dagster
COPY dagster.yaml /opt/dagster/dagster.yaml

EXPOSE 3000

# `dagster dev` is a single-process supervisor that runs the webserver + daemon
# against the local SQLite instance. Not production-grade (that's Postgres +
# separate webserver/daemon containers) but matches the dev-stack ergonomics.
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-w", "workspace.yaml"]
