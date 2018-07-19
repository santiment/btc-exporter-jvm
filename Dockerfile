FROM hseeberger/scala-sbt:8u171_2.12.6_1.1.6 AS builder

WORKDIR /app

COPY project /app/project
COPY *.sbt /app/

RUN sbt update

COPY ./src/ /app/src/

RUN sbt assembly

FROM openjdk:8u171-jre-alpine

WORKDIR /app

COPY --from=builder /app/target/scala-2.12/btc-exporter.jar /app/btc-exporter.jar

CMD /app/btc-exporter.jar
