"""
Fase 1 (spec §9): Spark su Dataproc legge le due sorgenti.
- Nativa BigQuery: lettura diretta con il connettore BigQuery (nessuna copia).
- Iceberg: lettura dal catalog Lakehouse configurato in spark.sql.catalog.

Argomenti:
  argv[1] = catalogo Iceberg (es. dciciani-lakehouse-catalog)
  argv[2] = namespace Iceberg (es. digital-analytics-myskyapp)
"""
import sys

from pyspark.sql import SparkSession

CATALOG = sys.argv[1]
NAMESPACE = sys.argv[2]

NATIVE = "skyita-da-cmp-test.digital_skyita_myskyapp.hit_data_stg"
ICEBERG = f"`{CATALOG}`.`{NAMESPACE}`.hit_data_stg"
BILLING_PROJECT = "skyita-da-architects-sandbox"

spark = SparkSession.builder.appName("poc_fase1_read_sources").getOrCreate()
results = {}


def step(label, fn):
    try:
        results[label] = fn()
        print(f"[{label}] OK: {results[label]}")
    except Exception as e:  # noqa: BLE001 - l'errore e' il dato del test
        results[label] = f"ERRORE {type(e).__name__}: {str(e)[:400]}"
        print(f"[{label}] {results[label]}")


def read_native():
    df = (
        spark.read.format("bigquery")
        .option("table", NATIVE)
        .option("parentProject", BILLING_PROJECT)
        .load()
    )
    return {"righe": df.count(), "colonne": len(df.columns)}


def read_iceberg():
    df = spark.table(ICEBERG)
    return {"righe": df.count(), "colonne": len(df.columns)}


step("F1_NATIVA_BQ_DIRETTA", read_native)
step("F1_ICEBERG_CATALOG", read_iceberg)

print("RISULTATI FASE 1:")
for k, v in results.items():
    print(f"  {k}: {v}")
