FROM python:3.10-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Flink 1.18 supports Java 11 and 17.
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PATH=/usr/lib/jvm/java-17-openjdk-amd64/bin:$PATH

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# PyFlink's bundled lib directory — where connector JARs must live.
RUN PYFLINK_LIB="$(python -c 'import pyflink, os; print(os.path.join(os.path.dirname(pyflink.__file__), "lib"))')" \
 && echo "PyFlink lib: $PYFLINK_LIB" \
 && curl -sSL -o "$PYFLINK_LIB/flink-sql-connector-kafka-3.1.0-1.18.jar" \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar \
 && curl -sSL -o "$PYFLINK_LIB/flink-connector-jdbc-3.1.2-1.18.jar" \
      https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.18/flink-connector-jdbc-3.1.2-1.18.jar \
 && curl -sSL -o "$PYFLINK_LIB/postgresql-42.7.3.jar" \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

COPY job.py .

CMD ["python", "-u", "job.py"]
