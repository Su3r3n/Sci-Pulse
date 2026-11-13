#!/usr/bin/env sh
set -eu

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_PASS="${MINIO_ROOT_PASSWORD:-minioadmin_dev_pass}"
BUCKET="${S3_WAREHOUSE_BUCKET:-warehouse}"

until mc alias set local "$MINIO_ENDPOINT" "$MINIO_USER" "$MINIO_PASS" >/dev/null 2>&1; do
  echo "Waiting for MinIO at $MINIO_ENDPOINT..."
  sleep 2
done

mc mb --ignore-existing "local/${BUCKET}"
echo "MinIO bucket '${BUCKET}' ready."
