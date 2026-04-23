FROM sbtscala/scala-sbt:graalvm-ce-22.3.3-b1-java17_1.12.9_3.8.3 AS builder
WORKDIR /app
COPY build.sbt .
COPY project ./project
COPY src ./src
RUN sbt assembly

FROM apache/spark:3.5.8
USER root
WORKDIR /app
COPY --from=builder /app/target/scala-2.12/social-media-streaming-assembly-1.0.jar app.jar

CMD /opt/spark/bin/spark-submit \
    --master $SPARK_MASTER \
    --class StreamingMain \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
    app.jar
