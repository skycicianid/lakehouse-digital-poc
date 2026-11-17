"""
Fase 3 (spec §9, passi 4-5): interroga da una nuova sessione Spark, su un cluster
Dataproc diverso (batch nuovo = sessione e cluster nuovi), una vista persistente
registrata nel catalog. Usato sulla vista di controllo solo-Iceberg.

Argomenti:
  argv[1] = catalogo Iceberg
  argv[2] = namespace di test (es. poc_fase3)
  argv[3] = nome della vista (es. v_ctrl_iceberg)
"""
import sys

from pyspark.sql import SparkSession

CATALOG, NS, VIEW = sys.argv[1], sys.argv[2], sys.argv[3]
FQN = f"`{CATALOG}`.`{NS}`.`{VIEW}`"

spark = SparkSession.builder.appName("poc_fase3_query_persisted_view").getOrCreate()
try:
    n = spark.sql(f"SELECT COUNT(*) AS n FROM {FQN}").collect()[0]["n"]
    print(f"[PERSIST_QUERY] OK {FQN}: righe = {n}")
except Exception as e:  # noqa: BLE001
    print(f"[PERSIST_QUERY] ERRORE {type(e).__name__}: {str(e)[:400]}")
