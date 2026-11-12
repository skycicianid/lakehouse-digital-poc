# Test 01: UNION ALL tabella nativa BigQuery + tabella Iceberg

**Obiettivo.** Verificare che una tabella Iceberg esposta dalla catalog Lakehouse e una tabella nativa BigQuery possano essere combinate in una stessa query BigQuery.

## Oggetti coinvolti

| Ruolo | Oggetto | Tipo | Location |
|---|---|---|---|
| Nativa | `skyita-da-cmp-test.digital_skyita_myskyapp.hit_data_stg` | Tabella BigQuery nativa | EU |
| Iceberg | `skyita-da-architects-sandbox.dciciani-lakehouse-catalog.digital-analytics-myskyapp.hit_data_stg` | Tabella Iceberg (catalog `gcs-bucket`) | EU |

La catalog `dciciani-lakehouse-catalog` è di tipo `CATALOG_TYPE_GCS_BUCKET`, primary `EU`, warehouse `gs://dciciani-lakehouse-catalog`.

## Query

File: [query.sql](query.sql)

Union su 167 colonne con nome identico nelle due tabelle, tutte castate a `STRING`. Il filtro `TIMESTAMP_TRUNC(partition_date, DAY) = 2026-11-10` si applica solo alla tabella nativa, come nella query originale.

## Esito

| Verifica | Righe |
|---|---|
| Nativa, giorno 2026-11-10 | 9.955 |
| Iceberg, tutte le partizioni | 5.724 |
| UNION ALL | 15.679 |

Controllo: 9.955 + 5.724 = 15.679. La union restituisce tutte le righe di entrambi i lati, senza perdite né duplicazioni.

**Esito: PASS.** Le due tabelle sono leggibili e combinabili in una sola query BigQuery, in location EU.

## Limiti

- Il test conta le righe e non confronta i valori: non verifica che le colonne siano popolate allo stesso modo.
- Il filtro sulla nativa è sul giorno 2026-11-10, mentre la Iceberg contiene solo il giorno 2026-10-13. Il test dimostra la compatibilità, non la coerenza dei dati tra i due giorni.
- Le colonne sono castate a `STRING`: il confronto dei tipi nativi resta da fare.
- Per la sola union non c'è deduplicazione.

## Data esecuzione

2026-11-12, query lanciata con `bq query --location=EU` in sola lettura.
