# Test di interoperabilità Iceberg + BigQuery

| ID | Test | Esito |
|---|---|---|
| 01 | [UNION ALL nativa BigQuery + Iceberg](01_union_all/README.md) | PASS |
| 02 | [JOIN nativa BigQuery + Iceberg su chiave hit](02_join/README.md) | PASS |
| 03 | [Confronto valori riga per riga, nativa vs Iceberg](03_values_compare/README.md) | PASS (162 colonne di business; 5 colonne di lineage escluse) |
| 04 | [UNION Dataproc Iceberg + BigQuery nativa, output in `hit_data_union`](04_dataproc_union_iceberg/README.md) | PASS |
| 05 | [View BigQuery: UNION ALL e JOIN tra Iceberg e nativa](05_bq_union_view/README.md) | PASS |
| 06 | [Vista Iceberg su BQ e Iceberg letta da Spark, con cut-over](06_spark_iceberg_view/README.md) | PASS (18/11) con il catalogo BigQuery nativo del connettore, senza codice custom: 38.554 righe, cut-over e scelta della sorgente per data verificati, rilettura da batch indipendente e materializzazione OK |
