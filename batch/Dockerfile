FROM sbtscala/scala-sbt:graalvm-ce-22.3.3-b1-java17_1.12.9_3.8.3 AS builder
WORKDIR /app
COPY build.sbt .
COPY project ./project
COPY src ./src
RUN sbt assembly

FROM apache/spark:3.5.8
USER root
WORKDIR /app
COPY --from=builder /app/target/scala-2.12/social-media-batch-assembly-1.0.jar app.jar
COPY run-batch.sh /app/run-batch.sh
RUN chmod +x /app/run-batch.sh

CMD ["/bin/bash", "/app/run-batch.sh"]