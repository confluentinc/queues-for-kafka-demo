# Multi-stage build for the QfK KEDA External Scaler
# Produces a fat JAR via maven-shade-plugin, then runs on a minimal JRE image.

# --- Stage 1: Build ---
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
# Cache dependencies first
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn clean package -DskipTests -B

# --- Stage 2: Runtime ---
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/target/qfk-admin-keda-scaler-1.0-SNAPSHOT.jar app.jar
EXPOSE 9400
ENTRYPOINT ["java", "-jar", "app.jar"]
