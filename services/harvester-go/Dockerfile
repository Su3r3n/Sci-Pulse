# Build from repository root:
#   docker build -f services/harvester-go/Dockerfile -t scipulse/harvester:latest .
FROM golang:1.26-alpine AS builder

WORKDIR /src

# Copy local shared library and harvester module manifests first for layer caching.
COPY services/common-go/ ./services/common-go/
COPY services/harvester-go/go.mod services/harvester-go/go.sum* ./services/harvester-go/

WORKDIR /src/services/harvester-go
RUN go mod download

WORKDIR /src
COPY services/harvester-go/ ./services/harvester-go/

WORKDIR /src/services/harvester-go
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/harvester ./cmd/harvester

FROM alpine:3.21

RUN adduser -D -H -u 10001 -s /sbin/nologin appuser \
    && apk add --no-cache ca-certificates tzdata \
    && mkdir -p /app/data/cache \
    && chown -R 10001:10001 /app

WORKDIR /app
COPY --from=builder /out/harvester /usr/local/bin/harvester

USER 10001:10001
EXPOSE 8082

ENTRYPOINT ["/usr/local/bin/harvester"]
CMD ["run"]
