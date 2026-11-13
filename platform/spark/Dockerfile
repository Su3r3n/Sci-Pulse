FROM apache/spark:3.5.3-python3

USER root

ENV SPARK_HOME=/opt/spark
ENV IVY_DIR=/tmp/.ivy2

# Download Iceberg, Kafka, Avro, S3A, Postgres JDBC, and ClickHouse JDBC jars
RUN mkdir -p /opt/spark/jars/extra && \
    curl -fSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
      -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
      -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
      -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
      -o /opt/spark/jars/kafka-clients-3.4.1.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
      -o /opt/spark/jars/commons-pool2-2.11.1.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.5.3/spark-avro_2.12-3.5.3.jar \
      -o /opt/spark/jars/spark-avro_2.12-3.5.3.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -fSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
      -o /opt/spark/jars/postgresql-42.7.4.jar && \
    curl -fSL https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.7.1/clickhouse-jdbc-0.7.1-all.jar \
      -o /opt/spark/jars/clickhouse-jdbc-0.7.1-all.jar

RUN pip install --no-cache-dir requests==2.32.3 pyyaml==6.0.2

WORKDIR /opt/spark/work
COPY . /opt/spark/jobs/

USER 185
