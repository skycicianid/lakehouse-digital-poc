"""Utility: esegue una query BigQuery (file .sql) su PySpark.

Passi:
  1. lega i parametri @nome a letterali stringa (es. @partition_date)
  2. risolve i riferimenti `progetto.dataset.tabella` (anche con placeholder
     $source_project_*) verso catalog.namespace.tabella Iceberg
  3. rimuove le virgole finali prima di FROM (BigQuery le accetta, Spark no)
  4. transpila BigQuery -> Spark SQL con sqlglot (QUALIFY incluso)
  5. esegue con spark.sql e restituisce il DataFrame

Dipendenza: sqlglot (vedi utils/requirements.txt).
"""

import re
from pathlib import Path

import sqlglot
from sqlglot import expressions as exp

_PARAM_RE = re.compile(r"@([A-Za-z_][A-Za-z0-9_]*)")
_BACKTICK_RE = re.compile(r"`([^`]+)`")
_TRAILING_COMMA_RE = re.compile(r",(\s*)(?=FROM\b)", re.IGNORECASE)
_UNRESOLVED_RE = re.compile(r"\$source_project_\w+")


def load_sql(path):
    return Path(path).read_text(encoding="utf-8")


def bind_params(sql, params):
    """Sostituisce @nome con il letterale stringa params['nome']."""

    def repl(match):
        name = match.group(1)
        if name not in params:
            raise KeyError(f"Parametro @{name} non fornito")
        value = str(params[name]).replace("'", "''")
        return f"'{value}'"

    return _PARAM_RE.sub(repl, sql)


def resolve_tables(sql, table_map):
    """Riscrive `chiave` in `catalog`.`namespace`.`tabella` se la chiave è in table_map.

    table_map: {"$source_project_x.dataset.tabella": "catalog.namespace.tabella"}
    """

    def repl(match):
        inner = match.group(1)
        if inner in table_map:
            return ".".join(f"`{part}`" for part in table_map[inner].split("."))
        return match.group(0)

    return _BACKTICK_RE.sub(repl, sql)


def remove_trailing_commas(sql):
    """'SELECT a, b,\n FROM t' -> 'SELECT a, b\n FROM t'."""
    return _TRAILING_COMMA_RE.sub(r"\1", sql)


def _order_unordered_row_number(tree):
    """Spark richiede ORDER BY nelle ROW_NUMBER; BigQuery no.

    Senza ORDER BY la scelta della riga è arbitraria in entrambi i motori:
    si aggiunge ORDER BY 1 (costante) per mantenere lo stesso comportamento.
    """
    for window in tree.find_all(exp.Window):
        if isinstance(window.this, exp.RowNumber) and not window.args.get("order"):
            window.set("order", exp.Order(expressions=[exp.Ordered(this=exp.Literal.number(1))]))
    return tree


def to_spark_sql(sql):
    """Transpila BigQuery -> Spark SQL."""
    statements = []
    for tree in sqlglot.parse(sql, read="bigquery"):
        if tree is None:
            continue
        _order_unordered_row_number(tree)
        statements.append(tree.sql(dialect="spark", pretty=True))
    return ";\n".join(statements)


def bq_to_spark_sql(sql, params, table_map):
    """Pipeline completa di traduzione: testo BigQuery -> testo Spark SQL."""
    sql = bind_params(sql, params)
    sql = resolve_tables(sql, table_map)

    unresolved = sorted(set(_UNRESOLVED_RE.findall(sql)))
    if unresolved:
        raise ValueError(
            "Riferimenti a tabelle non mappate (aggiungerli a table_map): "
            + ", ".join(unresolved)
        )

    sql = remove_trailing_commas(sql)
    return to_spark_sql(sql)


def run_bq_sql(spark, sql_path, params, table_map):
    """Legge il .sql, lo traduce e lo esegue. Ritorna (spark_sql, DataFrame)."""
    spark_sql = bq_to_spark_sql(load_sql(sql_path), params, table_map)
    return spark_sql, spark.sql(spark_sql)
