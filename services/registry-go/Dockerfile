# Build from repository root:
#   docker build -f services/registry-go/Dockerfile -t scipulse/registry:latest .
FROM golang:1.26-alpine AS builder

WORKDIR /src

# Copy local shared library and registry module manifests first for layer caching.
COPY services/common-go/ ./services/common-go/
COPY services/registry-go/go.mod services/registry-go/go.sum* ./services/registry-go/

WORKDIR /src/services/registry-go
RUN go mod download

WORKDIR /src
COPY services/registry-go/ ./services/registry-go/

WORKDIR /src/services/registry-go
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/registry ./cmd/registry

FROM alpine:3.21

RUN adduser -D -H -u 10001 -s /sbin/nologin appuser \
    && apk add --no-cache ca-certificates tzdata

COPY --from=builder /out/registry /usr/local/bin/registry

USER 10001:10001
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/registry"]
