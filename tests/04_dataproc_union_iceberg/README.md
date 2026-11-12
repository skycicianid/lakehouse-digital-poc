# Test 04: UNION Dataproc tra Iceberg e BigQuery, output in una nuova tabella Iceberg

**Obiettivo.** Verificare che un job Spark su Dataproc Serverless possa leggere la tabella Iceberg del bronze e una tabella BigQuery, fare `UNION ALL` per nome colonna e scrivere il risultato in una nuova tabella Iceberg.

**Stato: PASS.** Batch `union-iceberg-bq-20261113-000517`, SUCCEEDED. La tabella `hit_data_union` contiene 15.679 righe, come atteso.

## Oggetti coinvolti

| Ruolo | Oggetto | Tipo |
|---|---|---|
| Sorgente 1 | `dciciani-lakehouse-catalog.digital-analytics-myskyapp.hit_data_stg` | Tabella Iceberg del bronze, letta per intero |
| Sorgente 2 | `skyita-da-architects-sandbox.dciciani.hit_data_stg_test` | Tabella BigQuery, copia della sola partizione `2026-11-10` della nativa |
| Output | `dciciani-lakehouse-catalog.digital-analytics-myskyapp.hit_data_union` | Nuova tabella Iceberg, partizionata per `days(partition_date)` |

La sorgente 2 è la copia della partizione `2026-11-10` di `skyita-da-cmp-test.digital_skyita_myskyapp.hit_data_stg`, creata con `bq query` (CTAS) nel progetto sandbox. Il job non legge direttamente la nativa.

L'output non tocca `hit_data_stg`: la pipeline bronze la ricrea a ogni esecuzione, quindi il nome è diverso di proposito.

## Perché la copia

Il primo tentativo leggeva la nativa direttamente da `skyita-da-cmp-test`. Il job è fallito con 403 su `bigquery.tables.get`: il service account Dataproc non ha accesso a quel dataset, e il mio account non può modificarne l'IAM. Il service account ha già `bigquery.dataViewer` sul progetto sandbox, quindi la copia nel sandbox elimina il problema di permessi cross-project.

## Storia dei tentativi

| Batch | Esito | Causa |
|---|---|---|
| `union-iceberg-bq-20261112-231615` | FAILED | Connettore `spark-bigquery-with-dependencies_2.13-0.46.1.jar` passato con `spark.jars`: conflitto di classpath, `Property 'scala.binary.version' not found` |
| `union-iceberg-bq-20261112-232934` | FAILED | Connettore dell'immagine caricato correttamente, ma lettura della nativa negata (403) |
| Nuovo lancio | Da eseguire | Sorgente 2 spostata sulla copia nel sandbox |

## Scelte di progetto

- **Perimetro**: solo il giorno `2026-11-10`, come nel test 01. Il filtro sul giorno resta nel job, anche se la copia contiene solo quel giorno.
- **Tipi**: tutte le colonne castate a `STRING`, tranne `partition_date` che resta `TIMESTAMP` su entrambi i lati (serve per la partizione).
- **Unione per nome**: `unionByName(allowMissingColumns=False)`. Se le colonne non coincidono, il job fallisce invece di riempire valori `NULL`.
- **Connettore BigQuery**: quello presente nell'immagine Dataproc 2.3. Nessun jar passato a mano.
- **Progetto di fatturazione**: `parentProject = skyita-da-architects-sandbox`.

## File

- [job.py](job.py): il job Spark (lettura, union, scrittura, verifica).
- [run.sh](run.sh): carica il job su GCS e lancia il batch Dataproc. Senza `--yes` stampa solo il comando.

## Come eseguire

```bash
# 1. Mostra il comando senza lanciare nulla
./tests/04_dataproc_union_iceberg/run.sh

# 2. Lancio reale, dopo approvazione del comando stampato
./tests/04_dataproc_union_iceberg/run.sh --yes
```

## Numeri attesi

| Verifica | Valore atteso |
|---|---|
| Righe Iceberg `hit_data_stg` | 5.724 |
| Righe copia sandbox (2026-11-10) | 9.955 |
| Righe `hit_data_union` | 15.679 |

I totali coincidono con il test 01 (UNION ALL), che usava la stessa partizione.

## Criterio di esito

Il job stampa tre conteggi e termina con `STATUS: OK` se:

```
righe(hit_data_union) = righe(hit_data_stg Iceberg) + righe(copia sandbox)
```

Se il conteggio non coincide, il job termina con errore.

## Verifiche da fare dopo il lancio

1. Lo stato del batch: `gcloud dataproc batches describe <BATCH_ID> --region=europe-west1`.
2. I log del job, per i tre conteggi e lo `STATUS`.
3. Il conteggio di `hit_data_union` da BigQuery, con la stessa location del test 01 (EU).

## Limiti

- Il test verifica i conteggi, non i valori: come nel test 03, le colonne potrebbero differire a parità di conteggio.
- Le 5 colonne di lineage (`_process_*`, `_clustering_*`) sono incluse nell'unione. Nella Iceberg hanno valori diversi per costruzione.
- La copia è una sola partizione, non l'intera nativa. Il test non dice nulla sui giorni non copiati.
- Il connettore è quello dell'immagine Dataproc 2.3, non una versione fissata da noi.

## Esito

Dai log del driver, batch `union-iceberg-bq-20261113-000517`:

| Verifica | Valore |
|---|---|
| Batch ID | `union-iceberg-bq-20261113-000517` |
| Righe Iceberg `hit_data_stg` | 5.724 |
| Righe copia sandbox (2026-11-10) | 9.955 |
| Righe attese (`UNION`) | 15.679 |
| Righe output `hit_data_union` | 15.679 |
| Snapshot Iceberg committato | `1747370555152384936` (append, 2 file) |
| Esito | **PASS**: `STATUS: OK` |

**Nota sui log.** Il driver riporta un `RESTException` ("Table does not exist ... stage-create") nella sezione di scrittura. Non è un errore del commit: è un `WARN` di `RESTMetricsReporter` (`Failed to report metrics to REST endpoint`), e il commit dello snapshot è avvenuto subito dopo, con `addedRecords=15679`. Il batch è terminato con SUCCEEDED.
