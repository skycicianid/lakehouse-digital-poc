"""Probe SQL data-source resolution and persistent cross-source Iceberg views.

Uses only new objects in a dedicated namespace. No source data is modified.
Run again with --verify to read the saved view from a separate Dataproc batch.
"""
import argparse
import json

from pyspark.sql import SparkSession


def ident(value):
    return "`" + value.replace("`", "``") + "`"


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("catalog")
parser.add_argument("namespace")
parser.add_argument("--native", default="skyita-da-architects-sandbox.dciciani.hit_data_stg_test")
parser.add_argument("--iceberg-namespace", default="digital-analytics-myskyapp")
parser.add_argument("--verify", action="store_true")
args = parser.parse_args()
spark = SparkSession.builder.appName("test06_datasource_view").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("parentProject", "skyita-da-architects-sandbox")
base = f"{ident(args.catalog)}.{ident(args.namespace)}"
view = f"{base}.v_union_direct"
native = f"bigquery.{ident(args.native)}"
iceberg = f"{ident(args.catalog)}.{ident(args.iceberg_namespace)}.hit_data_stg"
results = {}


def check(label, action):
    try:
        result = action()
        results[label] = {"status": "PASS", "result": result}
        print(label, json.dumps(results[label], default=str), flush=True)
        return True
    except Exception as exc:
        results[label] = {"status": "FAIL", "error": str(exc)}
        print(label, json.dumps(results[label]), flush=True)
        return False


def rows(sql):
    return [row.asDict() for row in spark.sql(sql).collect()]


if not args.verify:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {base}")
    # Spark SQL supports provider.`path` resolution without a custom TableCatalog.
    direct_ok = check("DIRECT_BIGQUERY_SQL", lambda: rows(f"SELECT COUNT(*) AS n FROM {native}"))
    # Independent alternative: a metadata-only data-source table in spark_catalog.
    alias = "spark_catalog.default." + ident("bq_probe_" + args.namespace)
    alias_ok = check("REGISTER_DATASOURCE", lambda: rows(
        f"CREATE TABLE {alias} USING bigquery OPTIONS ("
        f"table '{args.native}', parentProject 'skyita-da-architects-sandbox')"))
    if alias_ok:
        check("QUERY_DATASOURCE", lambda: rows(f"SELECT COUNT(*) AS n FROM {alias}"))
    if direct_ok:
        # Minimal typed contract; separate source labels make branch counts verifiable.
        sql = (
            f"SELECT CAST(partition_date AS TIMESTAMP) AS partition_date, 'bigquery' AS source FROM {native} "
            f"UNION ALL SELECT CAST(partition_date AS TIMESTAMP), 'iceberg' FROM {iceberg}"
        )
        check("CREATE_PERSISTENT_VIEW", lambda: rows(f"CREATE VIEW {view} AS {sql}"))
    if alias_ok:
        registered_view = f"{base}.v_union_registered"
        check("CREATE_REGISTERED_VIEW", lambda: rows(
            f"CREATE VIEW {registered_view} AS "
            f"SELECT CAST(partition_date AS TIMESTAMP) AS partition_date, 'bigquery' AS source FROM {alias} "
            f"UNION ALL SELECT CAST(partition_date AS TIMESTAMP), 'iceberg' FROM {iceberg}"))
        check("READ_REGISTERED_VIEW", lambda: rows(
            f"SELECT source, COUNT(*) AS n FROM {registered_view} GROUP BY source ORDER BY source"))
        check("READ_NEW_SESSION", lambda: [r.asDict() for r in spark.newSession().sql(
            f"SELECT source, COUNT(*) AS n FROM {registered_view} GROUP BY source ORDER BY source").collect()])
        print("CATALOG_IMPLEMENTATION=" + spark.conf.get("spark.sql.catalogImplementation"), flush=True)
        print("HIVE_METASTORE_URIS=" + str(spark.sparkContext._jsc.hadoopConfiguration().get("hive.metastore.uris")), flush=True)
        # Keep metadata for a separate-batch verification. No source data was copied.
        view = registered_view

ok = check("READ_PERSISTENT_VIEW", lambda: rows(
    f"SELECT source, COUNT(*) AS n FROM {view} GROUP BY source ORDER BY source"))
if ok:
    spark.table(view).explain(extended=True)
print("PROBE_RESULTS=" + json.dumps(results, default=str), flush=True)
if not ok:
    raise RuntimeError("Persistent cross-source view did not pass; inspect PROBE_RESULTS")
