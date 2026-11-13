"""Isolate REST view creation with the registered BigQuery catalog."""
import json
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test06_catalog_view_shapes").getOrCreate()
catalog, namespace = sys.argv[1:3]
base = f"`{catalog}`.`{namespace}`"
native = "bq.`skyita-da-architects-sandbox`.dciciani.hit_data_stg_test"
iceberg = f"`{catalog}`.`digital-analytics-myskyapp`.hit_data_stg"
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {base}")
results = {}
for context in ("default", "explicit"):
    if context == "explicit":
        spark.sql(f"USE {base}")
    for shape, sql in (
        ("iceberg", f"SELECT partition_date FROM {iceberg}"),
        ("native", f"SELECT partition_date FROM {native}"),
        ("union", f"SELECT partition_date FROM {native} UNION ALL SELECT partition_date FROM {iceberg}"),
        ("full_native", f"SELECT * FROM {native}"),
        ("full_iceberg", f"SELECT * FROM {iceberg}"),
        ("long_sql", f"SELECT partition_date /*{'padding ' * 1200}*/ FROM {native}"),
    ):
        view = f"{base}.v_{context}_{shape}"
        try:
            spark.sql(f"CREATE VIEW {view} AS {sql}")
            n = spark.table(view).count()
            results[f"{context}_{shape}"] = {"status": "PASS", "rows": n}
        except Exception as exc:
            results[f"{context}_{shape}"] = {"status": "FAIL", "error": str(exc)[:800]}
        print("VIEW_PROBE=" + json.dumps({f"{context}_{shape}": results[f"{context}_{shape}"]}), flush=True)
print("PROBE_RESULTS=" + json.dumps(results), flush=True)
