# syntax=docker/dockerfile:1.7
#
# Dockerfile for scray-sync-api-rest (Spring Boot, Java 17, Maven).
#
# Build context MUST be the `scray-querying` directory of the repo,
# because this module depends on three sibling modules that aren't
# published to Maven Central and have to be built alongside it:
#   - scray-sync-api
#   - scray-sync-file
#   - scray-sync-events-mqtt
#
# Build from the repo root:
#   docker build \
#     -f scray-querying/modules/scray-sync-api-rest/Dockerfile \
#     -t scray/sync-api-rest:dev \
#     scray-querying
#
# Run:
#   docker run --rm -p 8082:8082 scray/sync-api-rest:dev
#
# Swagger UI: http://localhost:8082/swagger-ui/index.html

############################
# Stage 1 — Build
############################
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /workspace

# 1) Copy only POMs first so Docker can cache the dependency-download
#    layer when sources change but dependencies don't.
COPY pom.xml ./
COPY modules/scray-sync-api/pom.xml           modules/scray-sync-api/pom.xml
COPY modules/scray-sync-file/pom.xml          modules/scray-sync-file/pom.xml
COPY modules/scray-sync-events-mqtt/pom.xml   modules/scray-sync-events-mqtt/pom.xml
COPY modules/scray-sync-api-rest/pom.xml      modules/scray-sync-api-rest/pom.xml

# Pre-resolve dependencies into the BuildKit cache mount.
# The `|| true` swallows transient warnings from go-offline on multi-module builds.
RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -pl :scray-sync-api-rest -am dependency:go-offline -DskipTests || true

# 2) Now copy sources and build.
COPY modules/ modules/

RUN --mount=type=cache,target=/root/.m2 \
    mvn -B -pl :scray-sync-api-rest -am clean package -DskipTests \
 && cp modules/scray-sync-api-rest/target/scray-sync-api-rest-*.jar /workspace/app.jar

############################
# Stage 2 — Runtime
############################
FROM eclipse-temurin:17-jre-jammy AS runtime

# Run as a non-root user — best practice for any container.
RUN groupadd --system app \
 && useradd  --system --gid app --home /app --shell /sbin/nologin app \
 && mkdir -p /app \
 && chown app:app /app

WORKDIR /app
COPY --from=build --chown=app:app /workspace/app.jar /app/app.jar

USER app

# Spring Boot default port from application.properties
EXPOSE 8082

# JAVA_OPTS lets ops tune heap / GC / debug without rebuilding the image:
#   docker run -e JAVA_OPTS="-Xmx512m -XX:+UseG1GC" ...
ENV JAVA_OPTS=""

# Sensible defaults for containerised JVMs (uses cgroup memory limits properly).
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=75.0 -XX:+ExitOnOutOfMemoryError"

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
