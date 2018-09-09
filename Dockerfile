################################################################################
#
# BTC pipeline build definitions
#
# This Dockerfile contains the bulid definitions of all containers
# needed for the BTC processing pipeline. It is a multi-stage build so
# to build a specific image you need to specify a target
#
################################################################################



################################################################################
#
# Builder image
#
# This image builds the jar files for all components
#
################################################################################

FROM hseeberger/scala-sbt:8u181_2.12.6_1.2.1 AS builder

WORKDIR /app

COPY project /app/project
COPY *.sbt /app/

RUN sbt update

COPY ./ /app/

RUN sbt assembly



################################################################################
#
# Raw exporter image
#
################################################################################

FROM openjdk:8-jre-alpine AS rawexporter

WORKDIR /app

ENV JAVA_OPTS -XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap

COPY --from=builder /app/raw-exporter/target/scala-2.11/raw-exporter.jar /app/raw-exporter.jar

CMD /app/raw-exporter.jar



################################################################################
#
# Block processor flink cluster
#
################################################################################

FROM flink:1.6.0-scala_2.11 AS blockprocessor

COPY --chown=0 docker-flink-entrypoint.sh /docker-entrypoint.sh

# Install requirements
RUN set -ex; \
  apt-get update; \
  apt-get -y install bash; \
  rm -rf /var/lib/apt/lists/*; \
  cp $FLINK_HOME/opt/flink-s3-fs-hadoop-$FLINK_VERSION.jar \
     $FLINK_HOME/opt/flink-cep_2.11-$FLINK_VERSION.jar \
     $FLINK_HOME/opt/flink-cep-scala_2.11-$FLINK_VERSION.jar \
     $FLINK_HOME/opt/flink-queryable-state-runtime_2.11-$FLINK_VERSION.jar \
     $FLINK_HOME/opt/flink-metrics-prometheus-$FLINK_VERSION.jar \
     $FLINK_HOME/opt/flink-table_2.11-$FLINK_VERSION.jar \
     $FLINK_HOME/lib; \
  chmod a+x /docker-entrypoint.sh

COPY --chown=flink:flink block-processor/lib/* $FLINK_HOME/lib/

COPY --from=builder --chown=flink:flink /app/block-processor/target/scala-2.11/block-processor.jar $FLINK_HOME/lib/job.jar

USER flink
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["--help"]

