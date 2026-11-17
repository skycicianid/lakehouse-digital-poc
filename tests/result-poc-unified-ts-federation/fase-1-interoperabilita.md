# Fase 1 – Setup e interoperabilità

Data: 2026-11-13. Written for: team del progetto e reviewer della POC.

**Obiettivo (poc.md §9):** dimostrare che entrambi gli engine possano leggere le sorgenti.

**Esito: parziale.** BigQuery legge entrambe le sorgenti. Spark legge la Iceberg dal catalog, ma **non legge la nativa BigQuery**: il service account Dataproc non ha il permesso `bigquery.tables.get`.

## Matrice sorgenti × engine

| Sorgente | BigQuery Engine | Spark su Dataproc |
|---|---|---|
| BQ Native `skyita-da-cmp-test.digital_skyita_myskyapp.hit_data_stg` | Sì (query di conteggio su 2.976.430 righe) | **No**: `Access Denied ... Permission bigquery.tables.get denied` |
| Iceberg `dciciani-lakehouse-catalog.digital-analytics-myskyapp.hit_data_stg` | Sì (5.724 righe) | Sì: 5.724 righe, 167 colonne |

## Evidenze

- Batch Spark `poc-f1-read-20261113-141236`, **SUCCEEDED** (il job ha completato, ma la lettura nativa è registrata come errore nel suo output).
- Lettura diretta nativa dal connettore BigQuery (`spark.read.format("bigquery")`, `parentProject=skyita-da-architects-sandbox`): errore `BigQueryException: Access Denied: Table skyita-da-cmp-test:digital_skyita_myskyapp.hit_data_stg: Permission bigquery.tables.get denied`. Log del driver: `gs://dataproc-staging-europe-west1-899928023529-ls9dyxdd/.../driveroutput`.
- Lettura Iceberg dal catalog `spark.table(...)`: 5.724 righe, 167 colonne.
- Lo stesso account utente legge la nativa da BigQuery: quindi il blocco riguarda l'identità del service account Dataproc, non il progetto in sé.

## Esito per criterio

| Criterio | Esito |
|---|---|
| BigQuery legge nativa e Iceberg | PASS |
| Spark legge la Iceberg | PASS |
| Spark legge la nativa BQ | **FAIL**: permesso mancante per il service account Dataproc |
| Matrice di compatibilità prodotta | PASS (questa tabella) |

## Cosa serve per chiudere

`bigquery.dataViewer` (o `bigquery.tables.get` + `bigquery.tables.getData`) per `dataproc@skyita-da-architects-sandbox.iam.gserviceaccount.com` sul dataset `skyita-da-cmp-test:digital_skyita_myskyapp`. Lo può concedere un owner del progetto `skyita-da-cmp-test`. Senza, la Fase 3 può solo dire che la vista non è creabile per il motivo di risoluzione dei nomi, non per la lettura effettiva della nativa.

## Script

- [spark/fase1_read_sources.py](spark/fase1_read_sources.py)
