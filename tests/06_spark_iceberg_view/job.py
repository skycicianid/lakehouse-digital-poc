"""Create or independently consume persistent Iceberg views of BigQuery + Iceberg.

The bq catalog is supplied by BigQueryReadOnlyCatalog.java. Consumer mode never
registers relations, creates temporary views, or reconstructs federation SQL.
The default test is interoperability UNION ALL (same data as test 04), not cutover.
"""
import argparse
import json
from datetime import date

from pyspark.sql import SparkSession, functions as F


def ident(value):
    return "`" + value.replace("`", "``") + "`"


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("catalog")
parser.add_argument("namespace")
parser.add_argument("--mode", choices=("create", "verify"), default="create")
parser.add_argument("--native", default="skyita-da-architects-sandbox.dciciani.hit_data_stg_test")
parser.add_argument("--iceberg-namespace", default="digital-analytics-myskyapp")
parser.add_argument("--expected", type=int)
parser.add_argument("--materialize", action="store_true")
parser.add_argument("--cutover", type=date.fromisoformat)
args = parser.parse_args()
spark = SparkSession.builder.appName("test06_persistent_federation").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
base = f"{ident(args.catalog)}.{ident(args.namespace)}"
view = f"{base}.hit_data_union_view"
join_view = f"{base}.hit_data_join_view"
native = "bq." + ".".join(ident(p) for p in args.native.split("."))
iceberg = f"{ident(args.catalog)}.{ident(args.iceberg_namespace)}.hit_data_stg"
expected = args.expected

if args.mode == "create":
    native_df = spark.table(native)
    iceberg_df = spark.table(iceberg)
    if set(native_df.columns) != set(iceberg_df.columns):
        raise ValueError("Source columns differ; define an explicit canonical contract before proceeding")
    # Same explicit test contract as test 04; no implicit positional UNION casts.
    columns = iceberg_df.columns
    def projection_for(df):
        types = {field.name: field.dataType.simpleString() for field in df.schema.fields}
        return ", ".join(
            ident(c) if types[c] == ("timestamp" if c == "partition_date" else "string")
            else f"CAST({ident(c)} AS {'TIMESTAMP' if c == 'partition_date' else 'STRING'}) AS {ident(c)}"
            for c in columns
        )
    native_filter = iceberg_filter = ""
    if args.cutover:
        native_filter = f" WHERE partition_date < TIMESTAMP '{args.cutover} 00:00:00'"
        iceberg_filter = f" WHERE partition_date >= TIMESTAMP '{args.cutover} 00:00:00'"
    native_count = spark.sql(f"SELECT COUNT(*) FROM {native}{native_filter}").first()[0]
    iceberg_count = spark.sql(f"SELECT COUNT(*) FROM {iceberg}{iceberg_filter}").first()[0]
    if not native_count or not iceberg_count:
        raise ValueError("Both branches must contain test rows; check source dates and cutover")
    expected = native_count + iceberg_count
    print("SOURCE_COUNTS=" + json.dumps({"bigquery": native_count, "iceberg": iceberg_count}), flush=True)
    spark.sql(f"USE {ident(args.catalog)}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {base}")
    spark.sql(f"USE {base}")
    native_contract = f"{base}.hit_data_native_contract"
    iceberg_contract = f"{base}.hit_data_iceberg_contract"
    spark.sql(f"CREATE VIEW {native_contract} AS SELECT {projection_for(native_df)} FROM {native}{native_filter}")
    spark.sql(f"CREATE VIEW {iceberg_contract} AS SELECT {projection_for(iceberg_df)} FROM {iceberg}{iceberg_filter}")
    definition = f"SELECT * FROM {native_contract} UNION ALL SELECT * FROM {iceberg_contract}"
    print("VIEW_SQL_LENGTH=" + str(len(definition)), flush=True)
    spark.sql(f"CREATE VIEW {view} AS {definition}")
    spark.sql(f"CREATE VIEW {join_view} AS "
              f"SELECT CAST(n.id_hit_high AS STRING) AS id_hit_high, "
              f"CAST(n.id_hit_low AS STRING) AS id_hit_low FROM {native} n JOIN {iceberg} i "
              "ON CAST(n.id_hit_high AS STRING) = CAST(i.id_hit_high AS STRING) "
              "AND CAST(n.id_hit_low AS STRING) = CAST(i.id_hit_low AS STRING)")

# Consumer path: read a single persisted object. No federation code executes here.
df = spark.table(view)
actual = df.count()
if expected is not None and actual != expected:
    raise AssertionError(f"View has {actual} rows; expected {expected}")
df.explain(extended=True)
joined = spark.table(join_view).count()
if args.materialize:
    target = f"{base}.hit_data_union_materialized"
    # create() deliberately refuses to replace any existing table.
    df.writeTo(target).using("iceberg").partitionedBy(F.days("partition_date")).create()
    if spark.table(target).count() != actual:
        raise AssertionError("Materialized row count differs from persisted view")
print("RESULT=" + json.dumps({"mode": args.mode, "view": view, "rows": actual,
                             "join_rows": joined, "materialized": args.materialize}), flush=True)
print("STATUS: OK", flush=True)
