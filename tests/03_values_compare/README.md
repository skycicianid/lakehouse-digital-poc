# Test 03: confronto valori riga per riga, nativa vs Iceberg

**Obiettivo.** Verificare che le righe presenti sia nella tabella nativa BigQuery sia nella tabella Iceberg abbiano gli stessi valori nelle colonne di business.

## Oggetti coinvolti

| Ruolo | Oggetto | Tipo | Location |
|---|---|---|---|
| Nativa | `skyita-da-cmp-test.digital_skyita_myskyapp.hit_data_stg` | Tabella BigQuery nativa | EU |
| Iceberg | `skyita-da-architects-sandbox.dciciani-lakehouse-catalog.digital-analytics-myskyapp.hit_data_stg` | Tabella Iceberg (catalog `gcs-bucket`) | EU |

Giorno: `2026-10-13`, l'unico presente nella Iceberg.

## Query

File: [query.sql](query.sql)

Confronto con `EXCEPT DISTINCT` su 162 colonne castate a `STRING`, cioè le 167 del test 01 meno 5 colonne di lineage (vedi sotto).

## Esito

| Verifica | Valore |
|---|---|
| Righe Iceberg | 5.724 |
| Righe Iceberg non identiche a una riga nativa | 0 |
| Di cui con chiave corrispondente nella nativa | 0 |

**Esito: PASS.** Su tutte le colonne di business, le 5.724 righe Iceberg coincidono con la nativa.

## Colonne escluse: lineage

Un primo tentativo sulle 167 colonne dava 5.724 righe "non identiche". La diagnosi per colonna (file di supporto [diagnose_mismatch_per_column.sql](diagnose_mismatch_per_column.sql)) ha mostrato che la differenza è solo su 5 colonne, presenti in ogni riga:

- `_clustering_hour`
- `_clustering_minute`
- `_process_mapping_start_ts`
- `_process_mapping_version`
- `_process_transaction_id`

Sono metadati di processo che la pipeline bronze valorizza al momento della scrittura. Nella tabella nativa hanno valori diversi per costruzione, quindi non sono un problema di dati. Le escludo dal confronto, ma **non ho verificato il loro significato né i valori**.

Le altre 162 colonne, comprese quelle di ingestione `_ingest*` e `_ingestion*`, risultano identiche (0 mismatch).

## Limiti

- Il confronto è limitato al giorno 2026-10-13. Non verifica i giorni presenti solo nella nativa (1.380 righe senza corrispondenza, vedi test 02).
- Le 5 colonne di lineage non sono confrontate.
- Il confronto è su stringhe: due valori numericamente uguali ma formattati in modo diverso risulterebbero diversi. Non è il caso qui, perché tutte le colonne combaciano.

## Data esecuzione

2026-11-12, query lanciate con `bq query --location=EU` in sola lettura.
