#!/usr/bin/env bash
# Carica la pipe 02_silver_user_profiling_consent.py (+ utils, sqlglot, query SQL) su GCS
# e la esegue come batch Dataproc Serverless.
# Legge CATALOG e NAMESPACE dal file .env nella root del progetto.
#
# Uso:
#   ./run_02_silver.sh [YYYY-MM-DD]          # solo piano, non esegue nulla
#   ./run_02_silver.sh [YYYY-MM-DD] --run    # upload + submit del batch
# Default data: ieri in UTC.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="${HERE}/.env"

PROJECT="skyita-da-architects-sandbox"
REGION="europe-west1"
SUBNET="skyita-da-architects-sandbox-test"
SERVICE_ACCOUNT="dataproc@skyita-da-architects-sandbox.iam.gserviceaccount.com"
DATAPROC_VERSION="2.3"
GCS_PREFIX="gs://dciciani-lakehouse/digital-poc/script"

SCRIPT_LOCAL="${HERE}/medallion/02_silver_user_profiling_consent.py"
SCRIPT_GCS="${GCS_PREFIX}/02_silver_user_profiling_consent.py"
SQL_LOCAL="${HERE}/config/query_myskyapp_user_prof_consent.sql"
SQL_GCS="${GCS_PREFIX}/query_myskyapp_user_prof_consent.sql"
SQL_BASENAME="$(basename "$SQL_LOCAL")"
UTILS_DIR="${HERE}/utils"
UTILS_ZIP_GCS="${GCS_PREFIX}/utils.zip"
SQLGLOT_ZIP_GCS="${GCS_PREFIX}/sqlglot.zip"

# Argomenti: [YYYY-MM-DD] [--run]
PARTITION_DATE=""
RUN=0
for arg in "$@"; do
  case "$arg" in
    --run) RUN=1 ;;
    *)     PARTITION_DATE="$arg" ;;
  esac
done
PARTITION_DATE="${PARTITION_DATE:-$(date -u -v-1d +%Y-%m-%d)}"

read_env() {
  grep -E "^$1[[:space:]]*=" "$ENV_FILE" | head -n1 | sed -E 's/^[^=]*=[[:space:]]*//' | tr -d '\r'
}

CATALOG="$(read_env CATALOG)"
NAMESPACE="$(read_env NAMESPACE)"

[ -n "$CATALOG" ]   || { echo "CATALOG mancante in ${ENV_FILE}" >&2; exit 1; }
[ -n "$NAMESPACE" ] || { echo "NAMESPACE mancante in ${ENV_FILE}" >&2; exit 1; }

BATCH_ID="silver-profiling-consent-$(date -u +%Y%m%d-%H%M%S)"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT
UTILS_ZIP_LOCAL="${WORK_DIR}/utils.zip"

echo "=== PIANO run_02_silver ==="
echo "Data partizione : ${PARTITION_DATE}"
echo "Input           : ${CATALOG}.${NAMESPACE}.hit_data_stg"
echo "Output          : ${CATALOG}.${NAMESPACE}.user_profiling_consent_tracking"
echo "Batch           : ${BATCH_ID}"
echo "Upload          : ${SCRIPT_GCS}"
echo "                  ${SQL_GCS}"
echo "                  ${UTILS_ZIP_GCS} (ricreato da ${UTILS_DIR})"
echo "Dipendenze      : ${SQLGLOT_ZIP_GCS} (riusato, non ricaricato)"
echo ""

if [ "$RUN" -ne 1 ]; then
  echo "Nessuna azione eseguita. Rilancia con --run per caricare e sottomettere il batch."
  exit 0
fi

# 1. Verifica che sqlglot.zip sia già su GCS
gcloud storage ls "$SQLGLOT_ZIP_GCS" --project="$PROJECT" >/dev/null

# 2. Upload pipe, query SQL e utils.zip
(cd "$HERE" && zip -qr "$UTILS_ZIP_LOCAL" utils -x "*/__pycache__/*" "*.pyc")

echo "Upload script -> ${SCRIPT_GCS}"
gcloud storage cp "$SCRIPT_LOCAL" "$SCRIPT_GCS" --project="$PROJECT"

echo "Upload query  -> ${SQL_GCS}"
gcloud storage cp "$SQL_LOCAL" "$SQL_GCS" --project="$PROJECT"

echo "Upload utils  -> ${UTILS_ZIP_GCS}"
gcloud storage cp "$UTILS_ZIP_LOCAL" "$UTILS_ZIP_GCS" --project="$PROJECT"

# 3. Submit batch
echo "Submit batch ${BATCH_ID}"
gcloud dataproc batches submit pyspark "$SCRIPT_GCS" \
  --project="$PROJECT" \
  --region="$REGION" \
  --batch="$BATCH_ID" \
  --service-account="$SERVICE_ACCOUNT" \
  --subnet="$SUBNET" \
  --version="$DATAPROC_VERSION" \
  --files="$SQL_GCS" \
  --py-files="${UTILS_ZIP_GCS},${SQLGLOT_ZIP_GCS}" \
  --properties="dataproc.lakehouse.catalog.${CATALOG}=projects/${PROJECT}/catalogs/${CATALOG}" \
  -- "$CATALOG" "$NAMESPACE" "$PARTITION_DATE" "$SQL_BASENAME"
