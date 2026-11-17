import datetime as dt
import os
import sys

from pyspark import StorageLevel
from pyspark.sql import SparkSession

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from utils.sql_to_pyspark import run_bq_sql  # noqa: E402

# ============================================================================
# CONFIGURAZIONE
# Passata dalla CLI come argomenti:
#   argv[1] = catalogo Iceberg (CATALOG del .env)
#   argv[2] = namespace (NAMESPACE del .env)
#   argv[3] = data di partizione YYYY-MM-DD (@partition_date della query)
#   argv[4] = path della query .sql (opzionale, default config/)
# ============================================================================
CATALOG = sys.argv[1]
NAMESPACE = sys.argv[2]
PARTITION_DATE = dt.date.fromisoformat(sys.argv[3]).isoformat()
SQL_PATH = (
    sys.argv[4]
    if len(sys.argv) > 4
    else os.path.join(PROJECT_ROOT, "config", "query_myskyapp_user_prof_consent.sql")
)

INPUT_TABLE = "hit_data_stg"
OUTPUT_TABLE = "user_profiling_consent_tracking"

# Riferimenti BigQuery usati nella query -> tabelle Iceberg (catalog.namespace.tabella)
TABLE_MAP = {
    "$source_project_cmp.digital_skyita_myskyapp.hit_data_stg": f"{CATALOG}.{NAMESPACE}.{INPUT_TABLE}",
    # La query legge anche la propria storico: punta alla tabella di output.
    "$source_project_daita.digital_skyita_myskyapp.user_profiling_consent_tracking": f"{CATALOG}.{NAMESPACE}.{OUTPUT_TABLE}",
}


def fq(table):
    return f"`{CATALOG}`.`{NAMESPACE}`.`{table}`"


spark = SparkSession.builder.appName("silver_user_profiling_consent").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

print("INPUT  =", fq(INPUT_TABLE))
print("OUTPUT =", fq(OUTPUT_TABLE))
print("PARTITION_DATE =", PARTITION_DATE)
print("SQL    =", SQL_PATH)

# ============================================================================
# 1. NAMESPACE + TABELLA DI OUTPUT
#    Creata vuota se manca: la query ne legge lo storico, quindi deve esistere
#    già alla prima esecuzione. I tipi delle colonne ereditate da hit_data_stg
#    vengono letti dallo schema reale.
# ============================================================================
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{CATALOG}`.`{NAMESPACE}`")

hit_types = {f.name: f.dataType.simpleString() for f in spark.table(fq(INPUT_TABLE)).schema.fields}

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {fq(OUTPUT_TABLE)} (
        consent_source                      STRING,
        id_hit                              STRING,
        date_feed                           {hit_types["date_feed"]},
        date_hit_time_ts                    {hit_types["date_hit_time_ts"]},
        id_mcvis                            {hit_types["id_mcvis"]},
        id_external_post_evar               {hit_types["id_external_post_evar"]},
        profiling_denied_consent_event      STRING,
        profiling_granted_consent_event     STRING,
        flg_event_tracking_consent          BOOLEAN,
        flg_privacy_consent_cookie_profiling BOOLEAN,
        partition_date                      TIMESTAMP,
        _clustering_hour                    INT,
        _clustering_minute                  INT,
        snapshot_time                       TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (days(partition_date))
""")

# ============================================================================
# 2. TRADUZIONE + ESECUZIONE DELLA QUERY (utils/sql_to_pyspark.py)
# ============================================================================
spark_sql, df = run_bq_sql(
    spark,
    SQL_PATH,
    params={"partition_date": PARTITION_DATE},
    table_map=TABLE_MAP,
)
print("SPARK SQL:\n", spark_sql)

# Materializza il risultato: la query legge la stessa tabella su cui scriviamo.
df = df.persist(StorageLevel.DISK_ONLY)
print("ROWS =", df.count())

# ============================================================================
# 3. SCRITTURA ICEBERG
#    overwritePartitions sovrascrive solo la partizione del giorno elaborato,
#    così la pipe è rieseguibile senza duplicare dati.
# ============================================================================
df.writeTo(fq(OUTPUT_TABLE)).overwritePartitions()
df.unpersist()

# ============================================================================
# 4. VERIFICA
# ============================================================================
spark.sql(f"""
    SELECT consent_source, COUNT(*) AS num_rows, COUNT(DISTINCT id_hit) AS num_hit
    FROM {fq(OUTPUT_TABLE)}
    WHERE CAST(partition_date AS DATE) = DATE('{PARTITION_DATE}')
    GROUP BY consent_source
""").show()
