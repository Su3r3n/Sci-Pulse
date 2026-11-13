# Kafka Connect image for the registry CDC connector (Р-2: AvroConfluent end to end).
#
#   docker build --build-arg BASE_IMAGE=debezium/connect:2.7.3.Final -t scipulse/connect:local platform/debezium
#  (docker-compose.yml passes BASE_IMAGE from DEBEZIUM_IMAGE)
#
# The stock Debezium image has no Confluent Avro converter, so the converter is copied from the
# Confluent Connect image (same Kafka line as Debezium 2.7). In cp-kafka-connect the converter and
# its dependencies are a flat jar directory, /usr/share/java/kafka-serde-tools (the
# confluent-hub-components directory there is empty). The directory is copied as one plugin, so
# its jars load in their own classloader next to the Debezium plugins. The version is pinned through
# CONFLUENT_VERSION; check it against the Schema Registry version in docker-compose.yml.
ARG BASE_IMAGE=debezium/connect:2.7.3.Final
ARG CONFLUENT_VERSION=7.7.1

FROM confluentinc/cp-kafka-connect:${CONFLUENT_VERSION} AS confluent

FROM ${BASE_IMAGE}
ARG CONFLUENT_VERSION=7.7.1

USER root
COPY --from=confluent /usr/share/java/kafka-serde-tools /kafka/connect/confluent-avro-converter
# Fails the build with a clear message if the pinned version does not ship the converter.
RUN test -f /kafka/connect/confluent-avro-converter/kafka-connect-avro-converter-${CONFLUENT_VERSION}.jar \
    || (echo "AvroConverter ${CONFLUENT_VERSION} not found in cp-kafka-connect:${CONFLUENT_VERSION}" >&2 && exit 1)

# Credentials for the FileConfigProvider (registry.json) are written here at start-up by the entrypoint.
# /secrets is a path inside the container filesystem, writable by the kafka user; it must not be a
# read-only bind mount.
RUN mkdir -p /secrets && chmod 700 /secrets && chown kafka:kafka /secrets

COPY scipulse-connect-entrypoint.sh /usr/local/bin/scipulse-connect-entrypoint.sh
RUN chmod 755 /usr/local/bin/scipulse-connect-entrypoint.sh

USER kafka

ENTRYPOINT ["/usr/local/bin/scipulse-connect-entrypoint.sh"]
CMD ["start"]
