"""
Test 06 (probe): verifica se il catalog Iceberg accetta viste e se Spark risolve una tabella BigQuery in una vista.
Crea due viste di prova in un namespace dedicato, registra l'esito, poi le cancella.

Argomenti (CLI):
  argv[1] = catalogo Iceberg (CATALOG del .env)
  argv[2] = namespace di test (test_probe)
"""
import sys

from pyspark.sql import SparkSession

CATALOG = sys.argv[1]
NAMESPACE = sys.argv[2]
ICEBERG_TABLE = f"`{CATALOG}`.`digital-analytics-myskyapp`.hit_data_stg"
PROBE_ICEBERG_VIEW = f"`{CATALOG}`.`{NAMESPACE}`.v_probe_iceberg"
PROBE_BQ_VIEW = f"`{CATALOG}`.`{NAMESPACE}`.v_probe_bq"
BQ_TABLE = "`dciciani`.`hit_data_stg_test`"

spark = SparkSession.builder.appName("test06_probe_view").getOrCreate()
results = {}
spark.conf.set("parentProject", "skyita-da-architects-sandbox")

def attempt(label, sql):
    try:
        spark.sql(sql)
        results[label] = "OK"
    except Exception as e:  # noqa: BLE001 - l'esito dell'errore è il dato del probe
        results[label] = f"ERRORE: {type(e).__name__}: {str(e)[:500]}"
    print(f"[{label}] {results[label]}")


attempt("CREATE NAMESPACE", f"CREATE NAMESPACE IF NOT EXISTS `{CATALOG}`.`{NAMESPACE}`")

# 1. Il catalog accetta viste Iceberg? (vista su sola tabella Iceberg)
attempt(
    "1_VIEW_ICEBERG",
    f"CREATE OR REPLACE VIEW {PROBE_ICEBERG_VIEW} AS SELECT * FROM {ICEBERG_TABLE}",
)
if results["1_VIEW_ICEBERG"] == "OK":
    attempt("1_QUERY_VIEW_ICEBERG", f"SELECT COUNT(*) FROM {PROBE_ICEBERG_VIEW}")
else:
    print('Spark cannot resolve Iceberg table in Iceberg view')

# 2. Spark risolve una tabella BigQuery dentro una vista Iceberg?
attempt(
    "2_VIEW_BQ_REF",
    f"CREATE OR REPLACE VIEW {PROBE_BQ_VIEW} AS SELECT * FROM {BQ_TABLE}",
)
if results["2_VIEW_BQ_REF"] == "OK":
    attempt("2_QUERY_VIEW_BQ", f"SELECT COUNT(*) FROM {PROBE_BQ_VIEW}")
else:
    print('Spark cannot resolve BigQuery table in Iceberg view')


# Pulizia
attempt("CLEAN_VIEW_ICEBERG", f"DROP VIEW IF EXISTS {PROBE_ICEBERG_VIEW}")
attempt("CLEAN_VIEW_BQ", f"DROP VIEW IF EXISTS {PROBE_BQ_VIEW}")
attempt("CLEAN_NAMESPACE", f"DROP NAMESPACE IF EXISTS `{CATALOG}`.`{NAMESPACE}`")

print("PROBE RESULTS:")
for k, v in results.items():
    print(f"  {k}: {v}")
