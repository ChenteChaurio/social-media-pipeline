FROM sbtscala/scala-sbt:graalvm-ce-22.3.3-b1-java17_1.12.9_3.8.3 AS builder
WORKDIR /app
COPY build.sbt .
COPY project ./project
COPY src ./src
RUN sbt assembly

FROM openjdk:27-ea-slim
WORKDIR /app
COPY --from=builder /app/target/scala-2.12/social-media-producer-assembly-1.0.jar app.jar
CMD ["java", "-jar", "app.jar"]
