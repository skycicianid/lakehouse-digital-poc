# Risultati POC – Unified Time Series Federation

Riferimento: [poc.md](../../poc.md). Esecuzione ripartita da zero sulla specifica, il 2026-11-13. Written for: team del progetto e reviewer della POC.

## Esito complessivo

**L'ipotesi architetturale non è validata.**

- **BigQuery (Fase 2)**: una Logical View con cutover è creabile e corretta sui conteggi, ma non è continua: 201.452 righe della nativa non compaiono nella vista, perché i dati non attraversano il cutover nel modo previsto dalla specifica.
- **Spark (Fase 3)**: una vista persistente solo-Iceberg funziona e si legge da un batch separato. Una vista persistente che riferisce la tabella BigQuery Native **non è creabile**: è il rischio R01 della specifica, verificato negativamente.
- **Spark (Fase 1)**: il service account Dataproc non legge la nativa (`bigquery.tables.get` negato).

## Matrice per fase

| Fase | Esito | Pagina |
|---|---|---|
| 0 – Inventario (preliminare) | Dati di partenza documentati | [00_inventario.md](00_inventario.md) |
| 1 – Interoperabilità | **Parziale**: BQ legge entrambe; Spark legge solo la Iceberg | [fase-1-interoperabilita.md](fase-1-interoperabilita.md) |
| 2 – Logical View BigQuery | **Corretta ma non continua**: −201.452 righe | [fase-2-bigquery-logical-view.md](fase-2-bigquery-logical-view.md) |
| 3 – Vista Spark cross-source | **Non superata** (vista solo-Iceberg persistente: PASS) | [fase-3-spark-view.md](fase-3-spark-view.md) |
| 4 – Benchmark | Non eseguita: soglie non concordate, dipende da 2 e 3 | [fase-4-5-non-eseguite.md](fase-4-5-non-eseguite.md) |
| 5 – Governance e resilienza | Non eseguita; evidenza IAM raccolta | [fase-4-5-non-eseguite.md](fase-4-5-non-eseguite.md) |

## Oggetti creati

| Oggetto | Tipo | Note |
|---|---|---|
| `skyita-da-architects-sandbox.poc_unified_ts` | Dataset BigQuery, EU | Creato per la Fase 2 |
| `skyita-da-architects-sandbox.poc_unified_ts.sales_unified` | Logical View | Cutover 2026-10-13 |
| `dciciani-lakehouse-catalog.poc_fase3.v_ctrl_iceberg` | Vista Spark (Iceberg) | Persistente, letta da un altro batch |
| `dciciani-lakehouse-catalog.poc_fase3` | Namespace Iceberg | Creato per la Fase 3 |

Nessuna copia di tabelle è stata creata. La nativa e la Iceberg sono state lette in sola lettura.

## Decisioni e deviazioni dalla specifica

1. **Cutover su `partition_date`** (event time, proxy), data `2026-10-13`: primo giorno presente nella Iceberg. La specifica lascia la scelta aperta.
2. **Dati non nel modello della specifica**: la nativa riceve dati fino al 2026-11-13, la Iceberg contiene solo il 2026-10-13, e quel giorno è copiato dalla nativa (5.724 righe su 7.104). Il cutover non è un passaggio reale.
3. **Dataproc Metastore / Hive Metastore non testato, per scelta**: il candidato della specifica (§5.1, §7) è escluso per ragionamento sulla risoluzione dei nomi BigQuery, non con una prova. Nessun servizio metastore è stato creato.

## Decisioni necessarie

1. **Permesso alla nativa** per `dataproc@skyita-da-architects-sandbox.iam.gserviceaccount.com` su `skyita-da-cmp-test:digital_skyita_myskyapp` (`bigquery.dataViewer`). Lo concede un owner di quel progetto. Senza, la lettura nativa da Spark resta bloccata.
2. **Contratto del cutover**: la nativa smette di ricevere dati a una data, o resta sorgente per il suo intervallo completo? Da questo dipende la definizione della vista.
3. **Soglie del benchmark** (Fase 4).
4. **Chiave logica**: `(id_hit_high, id_hit_low)` non è unica nella nativa (124.094 duplicati). Va definita nel contratto (§5.3).

## Script

- [spark/fase1_read_sources.py](spark/fase1_read_sources.py)
- [spark/fase3_persistent_view.py](spark/fase3_persistent_view.py)
- [spark/fase3_query_persisted_view.py](spark/fase3_query_persisted_view.py)

## Batch Dataproc eseguiti

| Batch | Scopo | Esito |
|---|---|---|
| `poc-f1-read-20261113-141236` | Fase 1: lettura nativa e Iceberg | SUCCEEDED; nativa negata |
| `poc-f3-view-20261113-141534` | Fase 3: vista solo-Iceberg e tentativo cross-source | SUCCEEDED; cross-source fallisce |
| `poc-f3-persist-20261113-141825` | Fase 3: lettura della vista da un altro batch | SUCCEEDED; 5.724 righe |
