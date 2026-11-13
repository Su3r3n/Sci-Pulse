# Build from repository root:
#   docker build -f services/loadgen-go/Dockerfile -t scipulse/loadgen:latest .
FROM golang:1.26-alpine AS builder

WORKDIR /src

# Shared library and module manifests first for layer caching.
COPY services/common-go/ ./services/common-go/
COPY services/loadgen-go/go.mod services/loadgen-go/go.sum* ./services/loadgen-go/

WORKDIR /src/services/loadgen-go
RUN go mod download

WORKDIR /src
COPY services/loadgen-go/ ./services/loadgen-go/

WORKDIR /src/services/loadgen-go
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/loadgen ./cmd/loadgen

FROM alpine:3.21

RUN adduser -D -H -u 10001 -s /sbin/nologin appuser \
    && apk add --no-cache ca-certificates tzdata wget

COPY --from=builder /out/loadgen /usr/local/bin/loadgen

USER 10001:10001
EXPOSE 8084

ENTRYPOINT ["/usr/local/bin/loadgen"]
