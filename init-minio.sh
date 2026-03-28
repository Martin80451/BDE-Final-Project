#!/bin/bash
set -e

echo "Waiting for MinIO to start..."

until mc alias set minio http://minio:9000 user user12345 >/dev/null 2>&1 && mc admin info minio >/dev/null 2>&1; do
  echo "MinIO not ready yet; waiting..."
  sleep 2
done

echo "MinIO is ready! Configuring mc client..."

echo "Creating bucket..."

mc mb --ignore-existing minio/nyc-tlc-taxi-data

echo "Uploading data..."

mc cp /data/fhvhv_tripdata_2025-11.parquet minio/nyc-tlc-taxi-data/raw

echo "MinIO setup complete!"

mc cp /data/fhvhv_tripdata_2025-11.parquet minio/nyc-tlc-taxi-data

echo "MinIO setup complete!"

