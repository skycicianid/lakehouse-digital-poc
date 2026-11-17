# Correzione del test 06

**PASS sul campione sandbox.** Le viste sono state create e rilette da due batch
Dataproc indipendenti; la materializzazione contiene 15.679 righe e 167 colonne.
Questo sblocca il test 06. I limiti della PoC complessiva sono indicati sotto.

**PASS anche il test di cut-over (16 novembre 2026).** Con lo storico su BigQuery
prima del cut-over e i dati correnti su Iceberg dal cut-over in poi, la vista
restituisce ogni giorno da un solo ramo. Il ramo BigQuery viene escluso dal piano
nelle query sui soli dati recenti. Vedi [Verifica del cut-over](#verifica-del-cut-over-del-16-novembre-2026).

## Implementazione

[BigQueryReadOnlyCatalog.java](BigQueryReadOnlyCatalog.java) implementa un
`TableCatalog` Spark che risolve `bq.project.dataset.table` e delega schema e scan
al provider ufficiale `Spark35BigQueryTableProvider`, incluso in Dataproc 2.3.
Mantiene le opzioni della tabella in ogni scan e pubblica solo `BATCH_READ`.
Non aggiunge storage o un motore di federazione.

Le viste UNION e JOIN vengono salvate nel catalogo Iceberg esistente. Due viste
di contratto normalizzano separatamente le sorgenti: la UNION finale le referenzia
con una definizione compatta. Non serve
un Hive Metastore: i riferimenti BigQuery si risolvono dal catalogo configurato
in ogni nuovo batch. Il consumer carica il JAR e configura i due cataloghi:

```python
df = spark.table(
    "`dciciani-lakehouse-catalog`.federation_layered_20261113.hit_data_union_view"
)
```

Lo scan mantiene almeno una colonna anche per `COUNT(*)`: il provider V2 incluso
nel runtime tenta altrimenti un query job BigQuery, negato al service account
(`bigquery.jobs.create`), anche impostando `optimizedEmptyProjection=false`.
Il conteggio usa così la Storage API e l'aggregazione Spark. Filtri e pruning delle
altre colonne vengono delegati al provider, senza ampliare i permessi IAM.

## File

- [build_catalog.py](build_catalog.py): compila il JAR contro le librerie del
  runtime Dataproc e lo pubblica su GCS, senza sovrascrivere un JAR esistente.
- [job.py](job.py): crea le viste oppure le consuma da un batch indipendente.
  In modalità `verify` non registra relazioni e non ricrea viste o SQL di unione.
- [run.ps1](run.ps1): upload e lancio da PowerShell, senza dipendenza da `.env`.
- [probe_datasource.py](probe_datasource.py): verifica SQL diretto e registrazione
  `USING bigquery` con il connettore incluso nell'immagine.

## Oggetti

| Ruolo | Oggetto |
|---|---|
| BigQuery nativa | `skyita-da-architects-sandbox.dciciani.hit_data_stg_test` |
| Iceberg | `dciciani-lakehouse-catalog.digital-analytics-myskyapp.hit_data_stg` |
| Namespace dedicato | `dciciani-lakehouse-catalog.federation_layered_20261113` |
| Contratti | `hit_data_native_contract` e `hit_data_iceberg_contract` |
| Vista UNION | `hit_data_union_view` nel namespace dedicato |
| Vista JOIN | `hit_data_join_view` nel namespace dedicato |
| Output opzionale | `hit_data_union_materialized` nel namespace dedicato |

Le sorgenti non vengono modificate. Il setup rifiuta viste e output già esistenti:
per ripeterlo usare un nuovo namespace. La materializzazione è un output di test
opzionale e non serve alla federazione.

## Esecuzione

Per verificare direttamente i metadati persistiti delle viste, senza avviare Spark:

```powershell
./tests/06_spark_iceberg_view/inspect_views.ps1
```

Lo script legge i file metadata dal warehouse, controlla `view-uuid`, versione
corrente e rappresentazione SQL con dialect `spark`, poi stampa l'SQL delle tre
viste e verifica i riferimenti ai due rami. La vista finale contiene la UNION
delle viste di contratto; queste puntano rispettivamente al catalogo `bq` e alla
tabella Iceberg. Non legge la tabella materializzata. Se trova più file metadata
per una vista si ferma: in quel caso serve il puntatore corrente del catalogo REST.

```powershell
# Il default usa il JAR della correzione già compilato.
./tests/06_spark_iceberg_view/run.ps1 -Mode create -Namespace test06_nuovo

# Batch indipendente: legge le viste salvate e materializza l'UNION.
./tests/06_spark_iceberg_view/run.ps1 -Mode verify -Namespace test06_nuovo -Expected 15679 -Materialize

# Per ricompilare usare un percorso artefatti nuovo, e passarlo anche agli altri comandi.
./tests/06_spark_iceberg_view/run.ps1 -Mode build -ArtifactPrefix gs://BUCKET/poc06/BUILD_UNIVOCA
```

Adeguare `Expected` ai conteggi prodotti dal setup se cambiano le sorgenti.

## Perimetro

- Il campione è quello del test 04: 9.955 righe nella nativa sandbox del
  10 novembre e 5.724 righe Iceberg del 13 ottobre; totale di riferimento 15.679.
  Il setup controlla i conteggi effettivi dei due rami.
- Il contratto riprende il test 04: colonne esplicite per nome, tutte `STRING`
  tranne `partition_date TIMESTAMP`. Nomi di colonna differenti fanno fallire il
  setup. Non è ancora il contratto enterprise con tutti i tipi di business.
- La JOIN usa `(id_hit_high, id_hit_low)`, come il test 02. Su giorni diversi è
  atteso un risultato vuoto: non dimostra corrispondenze positive fra righe.
- La UNION del 13 novembre verifica interoperabilità e persistenza. La continuità
  al cut-over è verificata a parte, su un campione ordinato (sezione dedicata sotto).
  Il setup rifiuta rami senza righe.
- Il test usa la copia sandbox già esistente, senza creare nuove copie delle
  sorgenti. L'accesso del service account alla tabella originale
  `skyita-da-cmp-test.digital_skyita_myskyapp.hit_data_stg`, negato nel test 04,
  resta un requisito IAM da verificare separatamente.
- L'adattatore dipende dal provider V2 per Spark 3.5 e va riverificato cambiando
  runtime/connettore. Benchmark, pruning, governance e resilienza restano da provare.

## Diagnosi

I probe originali dimostravano che il catalogo accetta viste ma non risolve nomi
BigQuery senza catalogo. `parentProject` non modifica la risoluzione SQL.
I nuovi probe `probe-datasource-20261113-1320` e
`probe-datasource-v2-20261113` hanno verificato:

| Tentativo | Esito nel runtime 2.3.43 |
|---|---|
| Query diretta `bigquery.` con nome BigQuery quotato | `UNSUPPORTED_DATASOURCE_FOR_DIRECT_QUERY` |
| `CREATE TABLE ... USING bigquery OPTIONS (...)` | Registrazione riuscita |
| Lettura della relazione nel session catalog | `bigquery is not a valid Spark SQL Data Source` |

Il probe originale catturava gli errori e poteva risultare `SUCCEEDED` anche con
una verifica fallita. I nuovi job falliscono se la verifica finale non passa.

Il catalogo REST rifiuta con `BadRequestException: invalid argument` la UNION
monolitica di 8.095 caratteri. Il probe [probe_catalog_views.py](probe_catalog_views.py)
conferma che le viste con lo schema completo sono accettate, mentre anche una
query con una sola colonna e circa 9.600 caratteri di commento viene rifiutata,
sia con catalogo corrente predefinito sia esplicito. È quindi un vincolo sulla
definizione, non sull'accesso cross-source o sul numero di colonne. La soglia
esatta non è stata misurata; non assumere che sia quella delle viste BigQuery.
Le due viste di contratto e la UNION finale di 206 caratteri sono accettate.

## Verifica del 13 novembre 2026

| Verifica | Esito |
|---|---|
| Compilazione sul runtime Dataproc | PASS, `build-bq-catalog-v3-20261113` |
| Creazione contratti, UNION e JOIN | PASS, `poc06-create-20261113-144149` |
| Schema UNION | 167 colonne |
| Righe BigQuery / Iceberg | 9.955 / 5.724 |
| Righe UNION | 15.679, uguali alla somma delle sorgenti |
| Righe JOIN | 0, attese per giorni diversi |
| Lettura da batch indipendente e materializzazione | PASS, `poc06-verify-20261113-144455`: 15.679 righe |

Esecuzioni verificabili nella console:
[setup](https://console.cloud.google.com/dataproc/batches/europe-west1/poc06-create-20261113-144149?project=skyita-da-architects-sandbox),
[consumer indipendente](https://console.cloud.google.com/dataproc/batches/europe-west1/poc06-verify-20261113-144455?project=skyita-da-architects-sandbox).
Entrambi i driver terminano con `STATUS: OK`. Il consumer ha creato
`dciciani-lakehouse-catalog.federation_layered_20261113.hit_data_union_materialized`
leggendo la vista salvata, senza ricostruire la federazione.

L'assenza di un `TableCatalog` nel connettore standard non esclude un adattatore
sul suo `TableProvider`. Riferimenti:
[cataloghi Spark/Iceberg](https://iceberg.apache.org/docs/latest/spark-configuration/),
[connettore ufficiale BigQuery](https://github.com/GoogleCloudDataproc/spark-bigquery-connector).

## Verifica del cut-over del 16 novembre 2026

Obiettivo: lo storico resta su BigQuery fino alla scadenza della retention, i
dati nuovi stanno su Iceberg, e i job Spark leggono un'unica vista.

### Campione

| Ramo | Oggetto | Giorni (righe) |
|---|---|---|
| BigQuery, storico | `skyita-da-architects-sandbox.dciciani.hit_data_stg_hist` | 2026-10-08 (2.171), 2026-10-12 (30.659), 2026-10-13 (7.104) |
| Iceberg, corrente | `dciciani-lakehouse-catalog.digital-analytics-myskyapp.hit_data_stg` | 2026-10-13 (5.724) |

La copia è una CTAS dall'originale `skyita-da-cmp-test.digital_skyita_myskyapp.hit_data_stg`,
partizionata per giorno, creata con utenza personale (il service account Dataproc
non legge l'originale). Nell'originale non ci sono i giorni dal 9 all'11 ottobre.
Il 13 ottobre è presente su entrambi i lati, con conteggi diversi, per verificare
che il giorno del cut-over venga letto da un solo ramo. Cut-over: `2026-10-13`.

### Struttura delle viste

Namespace `federation_cutover_20261116b`. Le viste di contratto normalizzano le
sorgenti senza filtri. Il cut-over sta solo nella vista finale (319 caratteri):

```sql
SELECT * FROM hit_data_native_contract  WHERE partition_date <  TIMESTAMP '2026-10-13 00:00:00'
UNION ALL
SELECT * FROM hit_data_iceberg_contract WHERE partition_date >= TIMESTAMP '2026-10-13 00:00:00'
```

Quando lo storico è scaduto basta ridefinire questa vista con il solo ramo Iceberg.
Il nome resta lo stesso e i consumer non cambiano.

**Limite del catalogo REST.** Il primo tentativo metteva il filtro nei contratti.
Il contratto BigQuery con il filtro (4.094 caratteri) è stato accettato, quello
Iceberg (4.101 caratteri) è stato rifiutato con `BadRequestException: invalid argument`.
I contratti senza filtro (4.039 e 4.045 caratteri) sono accettati. Il dato è
compatibile con un limite di 4.096 caratteri sul testo della vista. Il namespace
`federation_cutover_20261116` del primo tentativo contiene solo
`hit_data_native_contract` e può essere eliminato.

### Esito

Batch `poc06-create-20261116-163521` (setup) e `poc06-cutover-check-20261116-163832`
(consumer indipendente, modalità `cutover-check`), entrambi `STATUS: OK`.

| Verifica | Atteso | Ottenuto |
|---|---|---|
| Righe totali della vista | 32.830 + 5.724 = 38.554 | 38.554 |
| 2026-10-08 / 2026-10-12 | da BigQuery: 2.171 / 30.659 | 2.171 / 30.659 |
| 2026-10-13 | solo da Iceberg: 5.724 (non 7.104 né 12.828) | 5.724 |
| JOIN sulla chiave hit (senza cut-over) | corrispondenze sul 13 ottobre | 5.724 righe |

Righe lette da ogni ramo, misurate con la metrica `numOutputRows` degli scan:

| Query sulla vista | Scan BigQuery | Scan Iceberg |
|---|---|---|
| `partition_date >= '2026-10-13'` (solo recenti) | **assente dal piano**, nessuna read session | 5.724 |
| solo `2026-10-08` (un giorno di storico) | 2.171 su 32.830: filtro applicato da BigQuery | 0, filtri contraddittori applicati in planning |
| tutta la vista | 32.830 | 5.724 |

Conseguenze per la transizione:

- le query sui dati recenti non dipendono da BigQuery: niente costi Storage API,
  permessi o disponibilità del connettore;
- le query sullo storico leggono da BigQuery solo i giorni richiesti, perché
  `partition_date` resta `TIMESTAMP` su entrambi i lati e il contratto non lo converte;
- il giorno del cut-over non viene duplicato.

### Limiti

- L'eliminazione del ramo BigQuery è un comportamento osservato su Spark 3.5.3
  (Dataproc 2.3). Va ricontrollata a ogni aggiornamento del runtime, insieme
  all'adattatore: `run.ps1 -Mode cutover-check` serve come test di regressione.
- Campione di 38.554 righe: prestazioni e costi su volumi reali sono ancora da misurare.
- Il pruning è garantito solo per i filtri su `partition_date`. Filtri su colonne
  convertite dal contratto (`CAST ... AS STRING`) potrebbero non arrivare a BigQuery.

### Riesecuzione

```powershell
./tests/06_spark_iceberg_view/run.ps1 -Mode create -Namespace <nuovo_namespace> `
  -Native skyita-da-architects-sandbox.dciciani.hit_data_stg_hist -Cutover 2026-10-13
./tests/06_spark_iceberg_view/run.ps1 -Mode cutover-check -Namespace <nuovo_namespace> `
  -Native skyita-da-architects-sandbox.dciciani.hit_data_stg_hist -Cutover 2026-10-13
```
