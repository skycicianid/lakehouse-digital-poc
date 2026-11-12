import json
import sys
import uuid

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("hitdata_bronze_stg").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

# ============================================================================
# CONFIGURAZIONE
# Passati dalla CLI come argomenti (il job su Dataproc non legge il .env):
#   argv[1] = bucket di destinazione del job 00 (BUCKET_NAME, es. gs://bucket/)
#   argv[2] = catalogo Iceberg (CATALOG del .env)
#   argv[3] = namespace (NAMESPACE del .env)
#   argv[4] = path GCS del mapping (field_mapping.json)
# ============================================================================
BUCKET_BASE = sys.argv[1]
CATALOG = sys.argv[2]
NAMESPACE = sys.argv[3]
MAPPING_PATH = sys.argv[4]

# Stesso percorso scritto da 00_setup_from_gcs_to_gcs.py
SOURCE_PATH = BUCKET_BASE + "digital_poc/myskyapp/hit_data_cmp/year=2026/month=11/day=11/"

TABLE = "hit_data_stg"

SISTEMA_SORGENTE = "digital_skyita_myskyapp"
ENTITA_SORGENTE = "HIT_DATA_CMP"
MAPPING_VERSION = "4-1"
RUN_ID = str(uuid.uuid4())

# Campi tecnici già presenti nel raw (nome raw -> nome bronze)
RAW_TECHNICAL = {
    "ingestion_worker_ts": "_ingestion_worker_ts",
    "ingestion_ingester_read_ts": "_ingestion_ingester_read_ts",
    "ingestion_ingester_write_ts": "_ingestion_ingester_write_ts",
    "ingest_row_id": "_ingest_row_id",
    "ingest_schema_version": "_ingest_schema_version",
    "ingestion_file_name": "_ingestion_file_name",
}

print("SPARK VERSION =", spark.version)
print("SOURCE =", SOURCE_PATH)
print("TARGET =", f"{CATALOG}.{NAMESPACE}.{TABLE}")

# ============================================================================
# 1. NAMESPACE
# ============================================================================
spark.sql(f"""
CREATE NAMESPACE IF NOT EXISTS `{CATALOG}`.`{NAMESPACE}`
""")

# ============================================================================
# 2. LETTURA AVRO DAL BUCKET TARGET
# ============================================================================
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW avro_src
    USING avro
    OPTIONS (
        path '{SOURCE_PATH}'
    )
""")

raw_cols = {c.upper(): c for c in spark.table("avro_src").columns}

# ============================================================================
# 3. MAPPING: solo HIT_DATA_CMP di digital_skyita_myskyapp
#    Attributo Sorgente = colonna raw, Dato Elementare = colonna bronze
# ============================================================================
mapping_json = "\n".join(spark.sparkContext.textFile(MAPPING_PATH).collect())
mapping = [
    r for r in json.loads(mapping_json)
    if r["Sistema Sorgente"] == SISTEMA_SORGENTE and r["Entità Sorgente"] == ENTITA_SORGENTE
]

def raw_ref(name):
    """Restituisce il riferimento SQL alla colonna raw (case-insensitive) o None."""
    real = raw_cols.get(name.upper())
    return f"`{real}`" if real else None

select_exprs = []
missing = []

for r in mapping:
    src, dst = r["Attributo Sorgente"], r["Dato Elementare"]
    ref = raw_ref(src)

    if ref is None:
        missing.append(src)
        select_exprs.append(f"CAST(NULL AS STRING) AS `{dst}`")
        continue

    if r.get("Normalizzazione") == "datetime":
        # Due output: stringa ISO (dst) e timestamp UTC (dst_ts), come da "fields"
        ts = f"COALESCE(try_to_timestamp({ref}), timestamp_seconds(try_cast({ref} AS BIGINT)))"
        select_exprs.append(f"date_format({ts}, \"yyyy-MM-dd'T'HH:mm:ss\") AS `{dst}`")
        select_exprs.append(f"{ts} AS `{dst}_ts`")
    else:
        select_exprs.append(f"{ref} AS `{dst}`")

# Campi tecnici raw
for raw_name, bronze_name in RAW_TECHNICAL.items():
    ref = raw_ref(raw_name)
    select_exprs.append(f"{ref} AS `{bronze_name}`" if ref else f"CAST(NULL AS STRING) AS `{bronze_name}`")

# Campi tecnici generati
feed_ref = raw_ref("FEED_DATE")
select_exprs += [
    f"CAST(to_timestamp({feed_ref}) AS TIMESTAMP) AS partition_date",
    "_run_ts AS _process_mapping_start_ts",
    f"'{RUN_ID}' AS _process_transaction_id",
    f"'{MAPPING_VERSION}' AS _process_mapping_version",
    "hour(_run_ts) AS _clustering_hour",
    "minute(_run_ts) AS _clustering_minute",
]

if missing:
    print(f"ATTENZIONE: {len(missing)} campi del mapping assenti nel raw, impostati a NULL")

select_sql = ",\n        ".join(select_exprs)

print("SELECT SQL:\n", select_sql)
# ============================================================================
# 4. SCRITTURA SU ICEBERG
#    DROP necessario: la tabella esistente ha la partizione su feed_date_dt
#    e CREATE OR REPLACE ... AS SELECT mantiene quella partition spec.
# ============================================================================
spark.sql(f"""
    DROP TABLE IF EXISTS `{CATALOG}`.`{NAMESPACE}`.{TABLE}
""")

spark.sql(f"""
    CREATE OR REPLACE TABLE `{CATALOG}`.`{NAMESPACE}`.{TABLE}
    USING iceberg
    PARTITIONED BY (days(partition_date))
    AS
    WITH src AS (
        SELECT *, current_timestamp() AS _run_ts FROM avro_src
    )
    SELECT
        {select_sql}
    FROM src
""")

# ============================================================================
# 5. VERIFICA
# ============================================================================
spark.sql(f"""
SELECT COUNT(*) AS num_rows
FROM `{CATALOG}`.`{NAMESPACE}`.{TABLE}
""").show()
