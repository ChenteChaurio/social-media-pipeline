FROM hseeberger/scala-sbt:11.0.14_1.6.2_2.12.15 AS builder
WORKDIR /app
COPY build.sbt .
COPY src ./src
RUN sbt assembly

FROM bitnami/spark:3.4.0
USER root
WORKDIR /app
COPY --from=builder /app/target/scala-2.12/social-media-batch-assembly-1.0.jar app.jar

CMD ["spark-submit", \
     "--master", "${SPARK_MASTER}", \
     "--class", "BatchAggregations", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0", \
     "app.jar"]
