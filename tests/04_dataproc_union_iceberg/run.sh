#!/usr/bin/env bash
# Test 04: carica job.py su GCS e lo lancia come batch Dataproc Serverless.
#
# Di default NON lancia nulla: stampa il comando che verrebbe eseguito.
# Il lancio reale richiede --yes, e carica lo script su GCS solo in quel caso.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "${HERE}/../.." && pwd)"
ENV_FILE="${ROOT}/.env"

PROJECT="skyita-da-architects-sandbox"
REGION="europe-west1"
SUBNET="skyita-da-architects-sandbox-test"
SERVICE_ACCOUNT="dataproc@skyita-da-architects-sandbox.iam.gserviceaccount.com"
DATAPROC_VERSION="2.3"
# Nessun jar BigQuery passato a mano: il job usa il connettore già presente nell'immagine Dataproc.
# Il jar spark-bigquery-with-dependencies_2.13-0.46.1 in spark.jars causava un conflitto di classpath
# (Property 'scala.binary.version' not found), vedi tests/04_dataproc_union_iceberg/README.md.
SPARK_AVRO_PKG="org.apache.spark:spark-avro_2.13:3.5.3"
TARGET_TABLE="hit_data_union"

SCRIPT_LOCAL="${HERE}/job.py"
SCRIPT_GCS="gs://dciciani-lakehouse/digital-poc/script/04_union_job.py"

read_env() {
  grep -E "^$1[[:space:]]*=" "$ENV_FILE" | head -n1 | sed -E 's/^[^=]*=[[:space:]]*//' | tr -d '\r'
}

CATALOG="$(read_env CATALOG)"
NAMESPACE="$(read_env NAMESPACE)"

[ -n "$CATALOG" ]   || { echo "CATALOG mancante in ${ENV_FILE}" >&2; exit 1; }
[ -n "$NAMESPACE" ] || { echo "NAMESPACE mancante in ${ENV_FILE}" >&2; exit 1; }

BATCH_ID="union-iceberg-bq-$(date +%Y%m%d-%H%M%S)"

CMD=(gcloud dataproc batches submit pyspark "$SCRIPT_GCS"
  --project="$PROJECT"
  --region="$REGION"
  --batch="$BATCH_ID"
  --service-account="$SERVICE_ACCOUNT"
  --subnet="$SUBNET"
  --version="$DATAPROC_VERSION"
  --properties="spark.jars.packages=${SPARK_AVRO_PKG},dataproc.lakehouse.catalog.${CATALOG}=projects/${PROJECT}/catalogs/${CATALOG}"
  -- "$CATALOG" "$NAMESPACE" "$TARGET_TABLE")

echo "Upload script -> ${SCRIPT_GCS}"
echo "Batch         -> ${BATCH_ID}"
echo "Output        -> ${CATALOG}.${NAMESPACE}.${TARGET_TABLE}"
echo
printf 'Comando:\n  gcloud storage cp %q %q --project=%q\n  ' "$SCRIPT_LOCAL" "$SCRIPT_GCS" "$PROJECT"
printf '%q ' "${CMD[@]}"
echo
echo

if [ "${1:-}" != "--yes" ]; then
  echo "Nessun job lanciato. Rilancia con --yes per eseguire."
  exit 0
fi

gcloud storage cp "$SCRIPT_LOCAL" "$SCRIPT_GCS" --project="$PROJECT"
"${CMD[@]}"
