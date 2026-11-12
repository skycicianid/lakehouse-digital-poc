#!/usr/bin/env bash
# Carica 01_bronze_from_gcs_to_iceberg.py su GCS e lo esegue come batch Dataproc Serverless.
# Legge BUCKET_NAME, CATALOG e NAMESPACE dal file .env nella root del progetto.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${HERE}/.env"

PROJECT="skyita-da-architects-sandbox"
REGION="europe-west1"
SUBNET="skyita-da-architects-sandbox-test"
SERVICE_ACCOUNT="dataproc@skyita-da-architects-sandbox.iam.gserviceaccount.com"
DATAPROC_VERSION="2.3"
SPARK_AVRO_PKG="org.apache.spark:spark-avro_2.13:3.5.3"
SCRIPT_LOCAL="${HERE}/medallion/01_bronze_from_gcs_to_iceberg.py"
SCRIPT_GCS="gs://dciciani-lakehouse/digital-poc/script/01_bronze_from_gcs_to_iceberg.py"
MAPPING_LOCAL="${HERE}/mapping/field_mapping.json"
MAPPING_GCS="gs://dciciani-lakehouse/digital-poc/script/field_mapping.json"

# read_env KEY: restituisce il valore di KEY nel .env (tollera spazi attorno a '=')
read_env() {
  grep -E "^$1[[:space:]]*=" "$ENV_FILE" | head -n1 | sed -E 's/^[^=]*=[[:space:]]*//' | tr -d '\r'
}

TARGET_BASE="$(read_env BUCKET_NAME)"
CATALOG="$(read_env CATALOG)"
NAMESPACE="$(read_env NAMESPACE)"

[ -n "$TARGET_BASE" ] || { echo "BUCKET_NAME mancante in ${ENV_FILE}" >&2; exit 1; }
[ -n "$CATALOG" ]     || { echo "CATALOG mancante in ${ENV_FILE}" >&2; exit 1; }
[ -n "$NAMESPACE" ]   || { echo "NAMESPACE mancante in ${ENV_FILE}" >&2; exit 1; }

echo "Upload script -> ${SCRIPT_GCS}"
gcloud storage cp "$SCRIPT_LOCAL" "$SCRIPT_GCS" --project="$PROJECT"

echo "Upload mapping -> ${MAPPING_GCS}"
gcloud storage cp "$MAPPING_LOCAL" "$MAPPING_GCS" --project="$PROJECT"

BATCH_ID="hitdata-bronze-$(date +%Y%m%d-%H%M%S)"

echo "Submit batch ${BATCH_ID} -> ${CATALOG}.${NAMESPACE}.hit_data_stg"
gcloud dataproc batches submit pyspark "$SCRIPT_GCS" \
  --project="$PROJECT" \
  --region="$REGION" \
  --batch="$BATCH_ID" \
  --service-account="$SERVICE_ACCOUNT" \
  --subnet="$SUBNET" \
  --version="$DATAPROC_VERSION" \
  --properties="spark.jars.packages=${SPARK_AVRO_PKG},dataproc.lakehouse.catalog.${CATALOG}=projects/${PROJECT}/catalogs/${CATALOG}" \
  -- "$TARGET_BASE" "$CATALOG" "$NAMESPACE" "$MAPPING_GCS"
