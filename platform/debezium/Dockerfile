# Kafka Connect image for the registry CDC connector (Р-2: AvroConfluent end to end).
#
#   docker build --build-arg BASE_IMAGE=debezium/connect:2.7.3.Final -t scipulse/connect:local platform/debezium
#  (docker-compose.yml passes BASE_IMAGE from DEBEZIUM_IMAGE)
#
# The stock Debezium image has no Confluent Avro converter, so the converter plugin is copied
# from the Confluent Connect image (same Kafka line as Debezium 2.7). The plugin is version-pinned
# through CONFLUENT_VERSION; check it against the Schema Registry version in docker-compose.yml.
ARG BASE_IMAGE=debezium/connect:2.7.3.Final
ARG CONFLUENT_VERSION=7.7.1

FROM confluentinc/cp-kafka-connect:${CONFLUENT_VERSION} AS confluent
RUN test -d /usr/share/confluent-hub-components/confluentinc-kafka-connect-avro-converter \
    || (echo "avro converter plugin not found in cp-kafka-connect:${CONFLUENT_VERSION}" >&2 && exit 1)

FROM ${BASE_IMAGE}

USER root
COPY --from=confluent /usr/share/confluent-hub-components/confluentinc-kafka-connect-avro-converter /kafka/connect/confluentinc-kafka-connect-avro-converter

# Credentials for the FileConfigProvider (registry.json) are written here at start-up by the entrypoint.
RUN mkdir -p /secrets && chmod 700 /secrets && chown kafka:kafka /secrets

COPY scipulse-connect-entrypoint.sh /usr/local/bin/scipulse-connect-entrypoint.sh
RUN chmod 755 /usr/local/bin/scipulse-connect-entrypoint.sh

USER kafka

ENTRYPOINT ["/usr/local/bin/scipulse-connect-entrypoint.sh"]
CMD ["start"]
