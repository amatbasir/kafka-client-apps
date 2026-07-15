# NOTE: pin these to whatever Maven/Java 25 image tags are available in your
# registry at build time - JDK 25 base images are still rolling out, so adjust
# if these exact tags aren't published yet.
FROM maven:3.9.11-eclipse-temurin-25 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn -B dependency:go-offline
COPY src ./src
RUN mvn -B clean package -DskipTests

FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /app/target/kafka-consumer-service-*.jar app.jar
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "app.jar"]
