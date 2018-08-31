# Builder image

FROM hseeberger/scala-sbt:8u171_2.12.6_1.1.6 AS builder

WORKDIR /app

COPY project /app/project
COPY *.sbt /app/

RUN sbt update

COPY ./ /app/

RUN sbt assembly

# Raw exporter release

FROM openjdk:8u171-jre-alpine AS rawexporter

WORKDIR /app

ENV JAVA_OPTS -XX:+UnlockExperimentalVMOptions -XX:+UseCGroupMemoryLimitForHeap

COPY --from=builder /app/raw-exporter/target/scala-2.12/raw-exporter.jar /app/raw-exporter.jar

CMD /app/raw-exporter.jar

