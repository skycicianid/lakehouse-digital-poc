#!/usr/bin/env bash
# Verifica READ-ONLY che l'ambiente reale corrisponda a quanto dichiarato in infra/.
# Non modifica nulla. Esce con codice != 0 se un controllo fallisce.
# Un controllo negato (403) è riportato come NON VERIFICABILE, non come OK.
set -uo pipefail

PROJECT="${PROJECT:-skyita-da-architects-sandbox}"
LAKEHOUSE_BUCKET="${LAKEHOUSE_BUCKET:-dciciani-lakehouse}"
CATALOG_BUCKET="${CATALOG_BUCKET:-dciciani-lakehouse-catalog}"
STATE_BUCKET="${STATE_BUCKET:-dciciani-lakehouse-tfstate}"
DATAPROC_SA="${DATAPROC_SA:-dataproc@skyita-da-architects-sandbox.iam.gserviceaccount.com}"

FAILS=0
UNVERIFIED=0

pass() { echo "OK    $1"; }
fail() { echo "FAIL  $1"; FAILS=$((FAILS + 1)); }
unverified() { echo "N/V   $1"; UNVERIFIED=$((UNVERIFIED + 1)); }

TOKEN="$(gcloud auth print-access-token 2>/dev/null)" || { echo "Nessun token gcloud: esegui gcloud auth login" >&2; exit 2; }

# storage_json BUCKET -> JSON completo del bucket (projection=full)
storage_json() {
  curl -s -H "Authorization: Bearer ${TOKEN}" \
    "https://storage.googleapis.com/storage/v1/b/$1?projection=full"
}

# field JSON PYEXPR -> valore estratto, oppure stringa vuota
field() {
  python3 -c "import json,sys; d=json.load(sys.stdin); print($2)" <<<"$1" 2>/dev/null
}

check_bucket() {
  local name="$1" expect_versioning="$2"
  local json
  json="$(storage_json "$name")"

  if [[ "$(field "$json" "'error' in d")" == "True" ]]; then
    unverified "bucket $name: lettura negata o inesistente"
    return
  fi

  [[ "$(field "$json" "d.get('location')")" == "EU" ]] \
    && pass "bucket $name: location EU" || fail "bucket $name: location diversa da EU"

  [[ "$(field "$json" "d.get('iamConfiguration',{}).get('uniformBucketLevelAccess',{}).get('enabled')")" == "True" ]] \
    && pass "bucket $name: uniform bucket-level access attivo" || fail "bucket $name: uniform access non attivo"

  [[ "$(field "$json" "d.get('iamConfiguration',{}).get('publicAccessPrevention')")" == "enforced" ]] \
    && pass "bucket $name: public access prevention enforced" || fail "bucket $name: public access prevention non enforced"

  local ver
  ver="$(field "$json" "d.get('versioning',{}).get('enabled', False)")"
  [[ "$ver" == "$expect_versioning" ]] \
    && pass "bucket $name: versioning=$ver come atteso" || fail "bucket $name: versioning=$ver, atteso $expect_versioning"
}

check_dataproc_role() {
  local bucket="$1"
  local json
  json="$(storage_json "$bucket")"
  if [[ "$(field "$json" "'error' in d")" == "True" ]]; then
    unverified "IAM $bucket: lettura negata"
    return
  fi
  local found
  found="$(curl -s -H "Authorization: Bearer ${TOKEN}" \
    "https://storage.googleapis.com/storage/v1/b/${bucket}/iam" \
    | python3 -c "import json,sys; d=json.load(sys.stdin); print(any(b['role']=='roles/storage.objectAdmin' and 'serviceAccount:${DATAPROC_SA}' in b.get('members',[]) for b in d.get('bindings',[])))" 2>/dev/null)"
  [[ "$found" == "True" ]] \
    && pass "IAM $bucket: dataproc SA ha objectAdmin" || fail "IAM $bucket: dataproc SA senza objectAdmin"
}

check_catalog() {
  local out
  out="$(curl -s -H "Authorization: Bearer ${TOKEN}" \
    "https://biglake.googleapis.com/v1/projects/${PROJECT}/locations/eu/catalogs/${CATALOG_BUCKET}")"
  if [[ "$(field "$out" "'error' in d")" == "True" ]]; then
    unverified "catalog ${CATALOG_BUCKET}: biglake.catalogs.get negato. Non verificabile con questo account"
  else
    pass "catalog ${CATALOG_BUCKET}: leggibile"
  fi
}

echo "== Verifica ambiente ${PROJECT} (read-only)"
check_bucket "$LAKEHOUSE_BUCKET" "False"
check_bucket "$CATALOG_BUCKET" "False"
check_bucket "$STATE_BUCKET" "True"
check_dataproc_role "$LAKEHOUSE_BUCKET"
check_dataproc_role "$CATALOG_BUCKET"
check_catalog

echo
echo "Esito: ${FAILS} FAIL, ${UNVERIFIED} non verificabili"
[[ "$FAILS" -eq 0 ]]
