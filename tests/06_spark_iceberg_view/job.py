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
parser.add_argument("--mode", choices=("create", "verify", "cutover-check"), default="create")
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
    # Contracts only normalize sources; the cutover lives in the short final view, so the
    # REST catalog definition-size limit is not hit and only one view changes at sunset.
    native_sql = f"SELECT {projection_for(native_df)} FROM {native}"
    iceberg_sql = f"SELECT {projection_for(iceberg_df)} FROM {iceberg}"
    print("CONTRACT_SQL_LENGTH=" + json.dumps({"bigquery": len(native_sql), "iceberg": len(iceberg_sql)}), flush=True)
    spark.sql(f"CREATE VIEW {native_contract} AS {native_sql}")
    spark.sql(f"CREATE VIEW {iceberg_contract} AS {iceberg_sql}")
    definition = (f"SELECT * FROM {native_contract}{native_filter} "
                  f"UNION ALL SELECT * FROM {iceberg_contract}{iceberg_filter}")
    print("VIEW_SQL_LENGTH=" + str(len(definition)), flush=True)
    spark.sql(f"CREATE VIEW {view} AS {definition}")
    spark.sql(f"CREATE VIEW {join_view} AS "
              f"SELECT CAST(n.id_hit_high AS STRING) AS id_hit_high, "
              f"CAST(n.id_hit_low AS STRING) AS id_hit_low FROM {native} n JOIN {iceberg} i "
              "ON CAST(n.id_hit_high AS STRING) = CAST(i.id_hit_high AS STRING) "
              "AND CAST(n.id_hit_low AS STRING) = CAST(i.id_hit_low AS STRING)")

if args.mode == "cutover-check":
    # Independent consumer: validates per-day continuity at the cutover and that each
    # branch only reads the days it owns. Read-only: creates nothing.
    if not args.cutover:
        raise ValueError("--cutover is required in cutover-check mode")
    # Without AQE the executed plan is final, so scan metrics can be read after collect().
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    cut = f"TIMESTAMP '{args.cutover} 00:00:00'"
    failures = []

    def per_day(table, where=""):
        rows = spark.sql(f"SELECT CAST(TO_DATE(partition_date) AS STRING) d, COUNT(*) n "
                         f"FROM {table}{where} GROUP BY 1").collect()
        return {r["d"]: r["n"] for r in rows}

    native_days = per_day(native)
    iceberg_days = per_day(iceberg)
    print("NATIVE_DAYS=" + json.dumps(native_days, sort_keys=True), flush=True)
    print("ICEBERG_DAYS=" + json.dumps(iceberg_days, sort_keys=True), flush=True)
    cut_s = str(args.cutover)
    expected_days = {d: n for d, n in native_days.items() if d < cut_s}
    for d, n in iceberg_days.items():
        if d >= cut_s:
            expected_days[d] = n
    view_days = per_day(view)
    print("VIEW_DAYS=" + json.dumps(view_days, sort_keys=True), flush=True)
    if not any(d < cut_s for d in native_days):
        failures.append("BigQuery has no day before the cutover")
    if not any(d >= cut_s for d in iceberg_days):
        failures.append("Iceberg has no day on/after the cutover")
    if not any(d >= cut_s for d in native_days):
        print("WARNING: no overlapping day in BigQuery; duplicate exclusion not exercised", flush=True)
    if view_days != expected_days:
        failures.append(f"Per-day mismatch: view={view_days} expected={expected_days}")

    def branch_reads(where):
        """Run a filtered aggregate on the view and return rows output by each source scan."""
        agg = spark.table(view).where(where).groupBy().count()
        total = agg.collect()[0][0]
        plan = agg._jdf.queryExecution().executedPlan()
        leaves = plan.collectLeaves()
        reads = {"bigquery": None, "iceberg": None}
        scans = []
        for i in range(leaves.size()):
            leaf = leaves.apply(i)
            text = leaf.simpleString(200)
            metric = leaf.metrics().get("numOutputRows")
            rows = metric.get().value() if metric.isDefined() else None
            branch = "iceberg" if args.catalog in text else "bigquery"
            scans.append({"branch": branch, "node": leaf.nodeName(), "rows": rows, "plan": text})
            if rows is not None:
                reads[branch] = (reads[branch] or 0) + rows
        print("PLAN[" + where + "]\n" + agg._jdf.queryExecution().executedPlan().toString(), flush=True)
        print("SCANS[" + where + "]=" + json.dumps(scans), flush=True)
        return total, reads

    hist_day = min(d for d in native_days if d < cut_s) if native_days else cut_s
    checks = {
        "recent_only": (f"partition_date >= {cut}",
                        lambda r: not r["bigquery"],
                        sum(n for d, n in expected_days.items() if d >= cut_s)),
        "single_history_day": (f"partition_date >= TIMESTAMP '{hist_day} 00:00:00' AND "
                               f"partition_date < TIMESTAMP '{hist_day} 00:00:00' + INTERVAL 1 DAY",
                               lambda r: not r["iceberg"] and r["bigquery"] == native_days.get(hist_day),
                               native_days.get(hist_day)),
        "across_cutover": ("partition_date IS NOT NULL",
                           lambda r: bool(r["bigquery"]) and bool(r["iceberg"]),
                           sum(expected_days.values())),
    }
    pruning = {}
    for name, (where, ok, want) in checks.items():
        total, reads = branch_reads(where)
        passed = ok(reads) and total == want
        pruning[name] = {"rows": total, "expected": want, "scan_rows": reads, "pass": passed}
        if not passed:
            failures.append(f"{name}: rows={total} expected={want} scan_rows={reads}")
    print("PRUNING=" + json.dumps(pruning), flush=True)
    print("RESULT=" + json.dumps({"mode": args.mode, "view": view, "cutover": cut_s,
                                 "failures": failures}), flush=True)
    if failures:
        raise AssertionError("; ".join(failures))
    print("STATUS: OK", flush=True)
    raise SystemExit(0)

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
