# ==============================================================================
# cce-collector-service
# ==============================================================================
# IMPORTANT — the build context is the WORKSPACE directory, not this repository.
# This service depends on cce-common-util as a Gradle composite build
# (settings.gradle: includeBuild '../cce-common-util'), and Docker COPY cannot reach outside its
# build context. Build from the directory holding both repositories:
#
#     docker build -f cce-collector-service/Dockerfile -t cce-collector-service:2.0.0 .
#
# Building with this repository as the context fails in stage 1 with
# "Included build '/.../cce-common-util' does not exist".
# ==============================================================================

# ==============================================================================
# Stage 1: Build
# ==============================================================================
FROM eclipse-temurin:21-jdk-alpine AS builder

WORKDIR /workspace

# The shared library first — it changes less often than the service, so it layers better.
COPY cce-common-util/gradle/ cce-common-util/gradle/
COPY cce-common-util/gradlew cce-common-util/build.gradle cce-common-util/settings.gradle cce-common-util/
COPY cce-common-util/src/ cce-common-util/src/

# Service build files before source, for dependency-resolution layer caching
COPY cce-collector-service/gradle/ cce-collector-service/gradle/
COPY cce-collector-service/gradlew cce-collector-service/build.gradle cce-collector-service/settings.gradle cce-collector-service/

WORKDIR /workspace/cce-collector-service
RUN chmod +x gradlew ../cce-common-util/gradlew \
    && ./gradlew dependencies --no-daemon 2>/dev/null || true

COPY cce-collector-service/src/ src/

# Spring Boot's java plugin emits both the executable jar and a `-plain` jar, so a `*.jar` glob
# matches two files and a COPY into a single destination would fail. Resolve it here instead.
RUN ./gradlew build -x test --no-daemon \
    && find build/libs -name '*.jar' ! -name '*-plain.jar' -exec cp {} /workspace/app.jar \;

# ==============================================================================
# Stage 2: Runtime
# ==============================================================================
FROM eclipse-temurin:21-jre-alpine

# wget: container healthcheck; bash: Portainer/shell exec (Alpine default is /bin/sh only)
RUN apk add --no-cache wget bash \
    && command -v wget >/dev/null

# Create non-root user
RUN addgroup -g 1001 cce && adduser -u 1001 -G cce -s /bin/sh -D cce

WORKDIR /app

COPY --from=builder /workspace/app.jar app.jar

RUN chown -R cce:cce /app

USER cce

# Pin the listen port to the one this image EXPOSEs and health-checks so the two cannot drift.
# Override both together if you need a different port.
ENV SERVER_PORT=8080

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD ["/usr/bin/wget", "--no-verbose", "--tries=1", "--spider", "http://127.0.0.1:8080/actuator/health"]

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
