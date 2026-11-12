"""
Test 04: UNION tra la tabella Iceberg hit_data_stg (bronze) e la copia BigQuery
skyita-da-architects-sandbox.dciciani.hit_data_stg_test (partizione 2026-11-10 della nativa),
scritta in una nuova tabella Iceberg.

Perimetro: solo il giorno 2026-11-10 della tabella nativa (come nel test 01).
Tipi: tutte le colonne castate a STRING, tranne partition_date (TIMESTAMP su entrambi i lati,
necessario per la partizione days(partition_date)).

Argomenti (CLI):
  argv[1] = catalogo Iceberg (CATALOG del .env)
  argv[2] = namespace (NAMESPACE del .env)
  argv[3] = tabella Iceberg di output (hit_data_union)
"""
import sys

from pyspark.sql import SparkSession, functions as F

CATALOG = sys.argv[1]
NAMESPACE = sys.argv[2]
TARGET_TABLE = sys.argv[3]

ICEBERG_TABLE = f"`{CATALOG}`.`{NAMESPACE}`.hit_data_stg"
TARGET = f"`{CATALOG}`.`{NAMESPACE}`.{TARGET_TABLE}"

# Copia della sola partizione 2026-11-10 della nativa, creata con bq nel progetto sandbox
# (lettura cross-project dalla nativa bloccata da IAM, vedi README).
NATIVE_TABLE = "skyita-da-architects-sandbox.dciciani.hit_data_stg_test"
NATIVE_DAY = "2026-11-10"
BQ_PARENT_PROJECT = "skyita-da-architects-sandbox"

PARTITION_COL = "partition_date"

spark = SparkSession.builder.appName("hitdata_union_iceberg_bq").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")


def stringify(df):
    """Cast a STRING di tutte le colonne tranne la colonna di partizione."""
    return df.select(
        [
            F.col(c).cast("string").alias(c) if c != PARTITION_COL else F.col(c)
            for c in df.columns
        ]
    )


# ============================================================================
# 1. LETTURA ICEBERG (bronze)
# ============================================================================
iceberg_df = spark.table(ICEBERG_TABLE)
iceberg_df = stringify(iceberg_df.withColumn(PARTITION_COL, F.col(PARTITION_COL).cast("timestamp")))
iceberg_count = iceberg_df.count()
print(f"[ICEBERG] {ICEBERG_TABLE}: righe = {iceberg_count}")

# ============================================================================
# 2. LETTURA BIGQUERY NATIVA (solo il giorno richiesto, filtro pushdown)
# ============================================================================
native_df = (
    spark.read.format("bigquery")
    .option("table", NATIVE_TABLE)
    .option("parentProject", BQ_PARENT_PROJECT)
    .load()
)
native_df = native_df.filter(
    F.date_trunc("day", F.col(PARTITION_COL)) == F.lit(f"{NATIVE_DAY} 00:00:00").cast("timestamp")
)
native_df = stringify(native_df)
native_count = native_df.count()
print(f"[NATIVE] {NATIVE_TABLE} giorno {NATIVE_DAY}: righe = {native_count}")

# ============================================================================
# 3. UNION ALL per nome colonna
#    allowMissingColumns=False: se le colonne non coincidono il job fallisce invece di riempire NULL.
# ============================================================================
union_df = iceberg_df.unionByName(native_df, allowMissingColumns=False)
expected = iceberg_count + native_count
print(f"[UNION] righe attese = {expected}")

# ============================================================================
# 4. SCRITTURA NUOVA TABELLA ICEBERG (non tocca hit_data_stg)
# ============================================================================
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{CATALOG}`.`{NAMESPACE}`")
spark.sql(f"DROP TABLE IF EXISTS {TARGET}")
union_df.createOrReplaceTempView("union_view")
spark.sql(
    f"""
    CREATE TABLE {TARGET}
    USING iceberg
    PARTITIONED BY (days({PARTITION_COL}))
    AS SELECT * FROM union_view
    """
)

# ============================================================================
# 5. VERIFICA: righe della tabella di output = righe attese
# ============================================================================
written = spark.table(TARGET).count()
print(f"[OUTPUT] {TARGET}: righe = {written}")

if written == expected:
    print("STATUS: OK")
else:
    print(f"STATUS: KO - righe output ({written}) diverse da attese ({expected})")
    raise RuntimeError("Verifica fallita: conteggio output diverso dall'UNION")
