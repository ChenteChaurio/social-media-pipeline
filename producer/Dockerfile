FROM hseeberger/scala-sbt:11.0.14_1.6.2_2.12.15 AS builder
WORKDIR /app
COPY build.sbt .
COPY src ./src
RUN sbt assembly

FROM openjdk:11-jre-slim
WORKDIR /app
COPY --from=builder /app/target/scala-2.12/social-media-producer-assembly-1.0.jar app.jar
CMD ["java", "-jar", "app.jar"]
