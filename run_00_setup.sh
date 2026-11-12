#!/usr/bin/env bash
# Carica 00_setup_from_gcs_to_gcs.py su GCS e lo esegue come batch Dataproc Serverless.
# Legge SOURCE_GCS_HIT_DATA e BUCKET_NAME dal file .env nella root del progetto.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${HERE}/.env"

PROJECT="skyita-da-architects-sandbox"
REGION="europe-west1"
SUBNET="skyita-da-architects-sandbox-test"
SERVICE_ACCOUNT="dataproc@skyita-da-architects-sandbox.iam.gserviceaccount.com"
DATAPROC_VERSION="2.3"
SPARK_AVRO_PKG="org.apache.spark:spark-avro_2.13:3.5.3"
SCRIPT_LOCAL="${HERE}/medallion/00_setup_from_gcs_to_gcs.py"
SCRIPT_GCS="gs://dciciani-lakehouse/digital-poc/script/00_setup_from_gcs_to_gcs.py"

# read_env KEY: restituisce il valore di KEY nel .env (tollera spazi attorno a '=')
read_env() {
  grep -E "^$1[[:space:]]*=" "$ENV_FILE" | head -n1 | sed -E 's/^[^=]*=[[:space:]]*//' | tr -d '\r'
}

SOURCE_BASE="$(read_env SOURCE_GCS_HIT_DATA)"
TARGET_BASE="$(read_env BUCKET_NAME)"

[ -n "$SOURCE_BASE" ] || { echo "SOURCE_GCS_HIT_DATA mancante in ${ENV_FILE}" >&2; exit 1; }
[ -n "$TARGET_BASE" ] || { echo "BUCKET_NAME mancante in ${ENV_FILE}" >&2; exit 1; }

echo "Upload script -> ${SCRIPT_GCS}"
gcloud storage cp "$SCRIPT_LOCAL" "$SCRIPT_GCS" --project="$PROJECT"

BATCH_ID="hitdata-avro-$(date +%Y%m%d-%H%M%S)"

echo "Submit batch ${BATCH_ID}"
gcloud dataproc batches submit pyspark "$SCRIPT_GCS" \
  --project="$PROJECT" \
  --region="$REGION" \
  --batch="$BATCH_ID" \
  --service-account="$SERVICE_ACCOUNT" \
  --subnet="$SUBNET" \
  --version="$DATAPROC_VERSION" \
  --properties="spark.jars.packages=${SPARK_AVRO_PKG}" \
  -- "$SOURCE_BASE" "$TARGET_BASE"
