import logging
import sys

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("hitdata_test").getOrCreate()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("hitdata_avro_to_avro")

# ============================================================================
# CONFIGURAZIONE
# Passata dalla CLI run_00_setup.sh come argomenti:
#   argv[1] = base sorgente (es. gs://bucket-sorgente/)
#   argv[2] = bucket di destinazione (es. gs://bucket-destinazione/)
# ============================================================================
SOURCE_BASE = sys.argv[1]
TARGET_BASE = sys.argv[2]

SOURCE_GCS_HIT_DATA_FULNAME = SOURCE_BASE
TARGET_BUCKET_NAME = TARGET_BASE + "digital_poc/myskyapp/hit_data_cmp/year=2026/month=11/day=11"

# ============================================================================
# 1. LETTURA AVRO SORGENTE (Spark SQL)
# ============================================================================
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW avro_src
    USING avro
    OPTIONS (
        path '{SOURCE_GCS_HIT_DATA_FULNAME}'
    )
""")

# ============================================================================
# 2. SCRITTURA AVRO SU BUCKET DI DESTINAZIONE (Spark SQL)
#    ATTENZIONE: INSERT OVERWRITE DIRECTORY sovrascrive il contenuto
#    della cartella TARGET_BUCKET_NAME.
# ============================================================================
spark.sql(f"""
    INSERT OVERWRITE DIRECTORY '{TARGET_BUCKET_NAME}'
    USING avro
    SELECT * FROM avro_src
""")

# ============================================================================
# 3. VERIFICA
# ============================================================================
spark.sql(f"""
    CREATE OR REPLACE TEMPORARY VIEW avro_tgt
    USING avro
    OPTIONS (
        path '{TARGET_BUCKET_NAME}'
    )
""")

counts = spark.sql("""
    SELECT
        (SELECT COUNT(*) FROM avro_src) AS source_rows,
        (SELECT COUNT(*) FROM avro_tgt) AS target_rows
""").collect()[0]

source_rows = counts["source_rows"]
target_rows = counts["target_rows"]
log.info("Righe sorgente: %s, righe destinazione: %s", source_rows, target_rows)

if source_rows == target_rows:
    log.info("STATUS: OK")
else:
    log.error("STATUS: KO - righe sorgente (%s) diverse da destinazione (%s)", source_rows, target_rows)
    raise RuntimeError("Verifica fallita: conteggi sorgente e destinazione diversi")
