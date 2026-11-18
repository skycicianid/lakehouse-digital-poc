# Test 06: soluzione con il catalogo BigQuery nativo

Questo documento spiega come funziona la soluzione del test 06, come è stata verificata
e come si è arrivati a questa versione. La panoramica è nel [README](README.md).

## 1. Il problema

Una vista Iceberg salva solo il testo SQL nel catalog. Quando un job la legge, Spark
deve capire a quale sistema appartiene ogni nome di tabella, e lo fa dal primo
elemento del nome, che deve corrispondere a un **catalogo registrato** nella sessione:

```sql
SELECT ... FROM bq.`skyita-da-architects-sandbox`.dciciani.hit_data_stg_hist   -- catalogo "bq"
UNION ALL
SELECT ... FROM `dciciani-lakehouse-catalog`.`digital-analytics-myskyapp`.hit_data_stg  -- catalogo Lakehouse
```

Il catalogo Lakehouse per Iceberg era già configurato su Dataproc. Per BigQuery
serviva un catalogo Spark, cioè una classe che implementa `TableCatalog`. Senza,
il nome BigQuery non viene risolto e la vista non si può creare (vedi §9).

Leggere BigQuery come DataFrame (`spark.read.format("bigquery")`, test 04) non basta:
il DataFrame esiste solo dentro il job e non può essere referenziato da una vista
persistente.

## 2. La soluzione: `BigQueryCatalog` del connettore ufficiale

Il connettore Spark-BigQuery di Google include, dalla versione 0.42.0, il catalogo
`com.google.cloud.spark.bigquery.BigQueryCatalog`. Si trova nell'artefatto
DataSource V2 `spark-3.5-bigquery`, già presente nel runtime Dataproc 2.3. Google lo
documenta per Spark gestito:
[Query BigQuery tables](https://docs.cloud.google.com/managed-spark/docs/guides/query-bigquery-tables).
Si attiva con due properties del batch:

```text
spark.sql.catalog.bq=com.google.cloud.spark.bigquery.BigQueryCatalog
spark.sql.catalog.bq.parentProject=skyita-da-architects-sandbox
```

Con queste properties il nome `bq.progetto.dataset.tabella` viene risolto come
tabella BigQuery. Il catalogo accetta nomi a due parti (`bq.dataset.tabella`) e a tre
(`bq.progetto.dataset.tabella`). Le letture passano dal provider DataSource V2 del
connettore, cioè dalla BigQuery Storage Read API, con filtri e selezione delle
colonne applicati da BigQuery.

Non servono JAR, Hive Metastore o forzature della versione del connettore
(`dataproc.sparkBqConnector.version`): il connettore preinstallato in Dataproc 2.3
include già la classe.

`run.ps1` imposta queste properties per default e non carica `--jars`.
`job.py` è lo stesso della soluzione custom, perché il formato dei nomi non cambia.

## 3. Struttura delle viste

Il setup (`--mode create`) crea quattro viste Iceberg in un namespace dedicato del
catalog Lakehouse. Le sorgenti non vengono modificate.

| Vista | Contenuto |
|---|---|
| `hit_data_native_contract` | `SELECT` delle 167 colonne della tabella BigQuery, convertite al contratto comune |
| `hit_data_iceberg_contract` | Stessa cosa per la tabella Iceberg |
| `hit_data_union_view` | La vista per gli utenti: `UNION ALL` dei due contratti con il filtro di cut-over |
| `hit_data_join_view` | `JOIN` tra BigQuery e Iceberg sulla chiave hit `(id_hit_high, id_hit_low)` |

**Contratto.** Le due sorgenti devono avere le stesse colonne, altrimenti il setup si
ferma. Ogni colonna viene portata a `STRING`, tranne `partition_date`, che resta
`TIMESTAMP` su entrambi i lati. È il contratto di test già usato nel test 04, non
quello con i tipi di business.

**Vista finale con cut-over** (331 caratteri):

```sql
SELECT * FROM hit_data_native_contract  WHERE partition_date <  TIMESTAMP '2026-10-13 00:00:00'
UNION ALL
SELECT * FROM hit_data_iceberg_contract WHERE partition_date >= TIMESTAMP '2026-10-13 00:00:00'
```

Ogni giorno arriva da un solo ramo, anche quando è presente in entrambe le sorgenti.
Quando lo storico BigQuery scade basta ridefinire questa vista con il solo ramo
Iceberg: il nome resta lo stesso e le query degli utenti non cambiano.

**Perché due livelli di viste.** Il catalog REST rifiuta con
`BadRequestException: invalid argument` le viste il cui testo SQL supera circa
4.096 caratteri (valore dedotto dalle prove, non documentato). La `UNION` in una
sola vista, con 167 colonne per ramo, superava gli 8.000 caratteri. I due contratti
(4.039 e 4.045 caratteri) restano sotto il limite, e la vista finale li richiama per
nome. Il filtro di cut-over va nella vista finale perché, messo nei contratti,
fa superare il limite a quello Iceberg (4.101 caratteri).

## 4. Cosa succede quando un utente interroga la vista

```python
spark.table("`dciciani-lakehouse-catalog`.federation_native_catalog_20261118.hit_data_union_view") \
     .where("partition_date >= '2026-10-13'")
```

1. Spark legge dal catalog Lakehouse il testo SQL della vista e quello dei contratti.
2. Risolve i nomi: `bq.…` con `BigQueryCatalog`, il resto con il catalog Lakehouse.
3. Combina il filtro dell'utente con quelli del cut-over. Se un ramo non può avere
   righe (qui: `< 2026-10-13` e `>= 2026-10-13` insieme), lo toglie dal piano e quella
   sorgente non viene letta.
4. Per il ramo BigQuery apre una read session della Storage Read API, passando
   colonne e filtri. BigQuery restituisce solo le righe e le colonne richieste.
5. Spark unisce le righe dei due rami e calcola il risultato.

I dati BigQuery non vengono copiati in Iceberg né salvati da nessuna parte: vengono
letti al momento della query. BigQuery non esegue SQL e non avvia query job: tutto
il calcolo lo fa Spark. L'utente vede un'unica tabella e, con la vista attuale, non
può sapere da quale sorgente arriva una riga. Se serve, si può aggiungere ai contratti
una colonna `source`.

Ogni job che legge la vista deve registrare entrambi i cataloghi. Se `bq` manca, la
vista viene trovata ma la lettura fallisce.

## 5. Modalità del job e controlli

| Modalità | Cosa fa | Controlli che fanno fallire il batch |
|---|---|---|
| `create` | Conta le due sorgenti, crea namespace, contratti, vista finale e JOIN | Colonne diverse tra le sorgenti; un ramo vuoto; `count(vista) ≠ count(BQ) + count(Iceberg)` |
| `cutover-check` | Da un batch indipendente: conteggi per giorno e righe lette da ciascuno scan (metrica `numOutputRows`), con AQE disattivato perché il piano letto sia quello eseguito | Conteggi per giorno diversi dagli attesi; ramo BigQuery presente nelle query sui soli dati recenti; ramo Iceberg con righe nella query su un giorno di storico; un ramo assente nella query su tutta la vista |
| `verify` | Da un batch indipendente: conta le righe della vista, conta la JOIN, opzionalmente materializza la vista in `hit_data_union_materialized` | Righe della vista ≠ `--expected`; righe della tabella materializzata ≠ righe della vista |

I batch consumer (`cutover-check` e `verify`) non registrano relazioni e non
ricostruiscono SQL di unione: leggono soltanto la vista salvata.

## 6. Verifica del 18 novembre 2026

Runtime Dataproc Serverless 2.3 (Spark 3.5.3), service account
`dataproc@skyita-da-architects-sandbox.iam.gserviceaccount.com`, namespace
`federation_native_catalog_20261118`, cut-over `2026-10-13`.

### Campione

| Ramo | Oggetto | Giorni (righe) |
|---|---|---|
| BigQuery, storico | `skyita-da-architects-sandbox.dciciani.hit_data_stg_hist` | 2026-10-08 (2.171), 2026-10-12 (30.659), 2026-10-13 (7.104) |
| Iceberg, corrente | `dciciani-lakehouse-catalog.digital-analytics-myskyapp.hit_data_stg` | 2026-10-13 (5.724) |

`hit_data_stg_hist` è una copia CTAS della tabella originale
`skyita-da-cmp-test.digital_skyita_myskyapp.hit_data_stg`, fatta con utenza personale
perché il service account Dataproc non può leggere l'originale (403). Il 13 ottobre è
presente su entrambe le sorgenti con conteggi diversi, per verificare che il giorno di
cut-over venga letto da un solo ramo.

### Batch

| Batch | Modalità | Esito |
|---|---|---|
| `poc06-create-20261118-093051` | create | `STATUS: OK`: sorgenti 32.830 (BigQuery, prima del cut-over) e 5.724 (Iceberg) |
| `poc06-cutover-check-20261118-093420` | cutover-check | `STATUS: OK`, nessun errore |
| `poc06-verify-20261118-093733` | verify `-Expected 38554 -Materialize` | `STATUS: OK`: 38.554 righe, JOIN 5.724, materializzazione riuscita |

### Risultati

| Verifica | Atteso | Ottenuto |
|---|---|---|
| Righe totali della vista | 32.830 + 5.724 = 38.554 | 38.554 |
| 2026-10-08 / 2026-10-12 | da BigQuery: 2.171 / 30.659 | 2.171 / 30.659 |
| 2026-10-13 | solo da Iceberg: 5.724 (non 7.104 né 12.828) | 5.724 |

| Query sulla vista | Righe | Scan BigQuery | Scan Iceberg |
|---|---|---|---|
| `partition_date >= '2026-10-13'` (solo recenti) | 5.724 | **assente dal piano**, nessuna read session | 5.724 |
| solo `2026-10-08` (un giorno di storico) | 2.171 | 2.171: filtro applicato da BigQuery | 0: nessun file letto |
| `partition_date IS NOT NULL` (tutta la vista) | 38.554 | 32.830 | 5.724 |

Sono gli stessi numeri della soluzione custom del 16 novembre (§8).

**Nessun query job BigQuery.** Il service account non ha `bigquery.jobs.create`:
un query job sarebbe fallito con 403 e avrebbe fatto fallire il batch. Nei log ci
sono solo read session della Storage Read API (`ReadSessionCreator`).

## 7. Come mostrarlo nella Spark UI

Console GCP → Dataproc → Serverless → Batches → `poc06-cutover-check-20261118-093420`
→ **View Spark UI** → scheda **SQL / DataFrame**. Le query 0–2 (`job.py:95`) sono i
conteggi di supporto per giorno. Le query 3–5 (`job.py:121`) sono le tre query sulla
vista da mostrare:

| ID | Query | Cosa mostra il grafo |
|---|---|---|
| 3 | solo dati recenti | Un solo `BatchScan`, sulla tabella Iceberg: 5.724 righe. Nessun nodo BigQuery |
| 4 | solo 2026-10-08 | `Union` con `BatchScan hit_data_stg_hist` (BigQuery) a 2.171 righe e scan Iceberg a 0 righe |
| 5 | tutta la vista | `Union` con BigQuery a 32.830 righe e Iceberg a 5.724 |

Nella query 4 lo scan Iceberg compare nel piano, ma i filtri sui metadati Iceberg
escludono tutti i file, quindi non legge dati. Gli altri nodi (`ColumnarToRow`,
`Filter`, `Project`, `HashAggregate`) sono passaggi interni di Spark.

## 8. Limiti e punti aperti

- **`COUNT(*)` diretto su `bq.`.** Con una proiezione vuota il connettore può lanciare
  un query job BigQuery, che con i permessi attuali fallirebbe. Nei batch del 18/11
  il caso non si è presentato, ma un `SELECT COUNT(*)` diretto sulla tabella `bq.`
  senza colonne non è stato provato. Se servisse, la soluzione è concedere
  `bigquery.jobs.create`, non tornare al catalogo custom.
- **Scrittura.** Il catalogo nativo può anche creare e cancellare tabelle BigQuery
  (`createTable`, `dropTable`). La sola lettura va imposta con i permessi IAM.
- **Configurazione dei consumer.** Ogni job deve avere entrambi i cataloghi:
  conviene un template di batch standard.
- **Accesso alla tabella originale.** Il service account Dataproc riceve 403 su
  `skyita-da-cmp-test.digital_skyita_myskyapp.hit_data_stg`: è un requisito IAM aperto.
- **Selezione della sorgente solo su `partition_date`.** È verificata solo per filtri
  su `partition_date`. Filtri su colonne convertite a `STRING` dal contratto potrebbero
  non arrivare a BigQuery: il risultato resta corretto, ma BigQuery legge più dati.
- **Solo Spark.** La vista è SQL Spark e altri motori non la leggono. BigQuery usa la
  propria vista: dal 18/11 `dciciani.hit_data_cutover_view` (test 05), con lo stesso
  cut-over e gli stessi conteggi.
- **Limite di dimensione delle viste.** Circa 4.096 caratteri nel catalog REST:
  impone i due livelli di viste.
- **Volumi.** Campione di 38.554 righe: prestazioni e costi della Storage Read API su
  dati reali sono da misurare.
- **Regressione.** L'esclusione del ramo BigQuery è stata osservata su Spark 3.5.3:
  `run.ps1 -Mode cutover-check` va rieseguito a ogni cambio di runtime.

## 9. Storia: diagnosi e soluzione custom

### Diagnosi del 13 novembre (mattina)

| Probe | Prova | Esito |
|---|---|---|
| `probe-view-20261113-010212` | `CREATE VIEW` Iceberg su tabella Iceberg | OK: il catalog accetta viste da Spark |
| `probe-view-20261113-010212` | `CREATE VIEW` con nome BigQuery a tre parti | `REQUIRES_SINGLE_PART_NAMESPACE` |
| `probe-view-20261113-123408` | Nome a due parti con `parentProject` | `TABLE_OR_VIEW_NOT_FOUND` |
| `probe-datasource-20261113-1320` | Query diretta `bigquery.`<nome> | `UNSUPPORTED_DATASOURCE_FOR_DIRECT_QUERY` |
| `probe-datasource-v2-20261113` | `CREATE TABLE ... USING bigquery` nel session catalog e lettura | Registrazione OK, lettura: `bigquery is not a valid Spark SQL Data Source` |

Conclusione corretta: serve un catalogo Spark per BigQuery. Conclusione sbagliata:
"il connettore non lo offre". Era stato controllato solo
`spark-bigquery-with-dependencies_2.13-0.46.1`, che non contiene il catalogo, e non
`spark-3.5-bigquery`, che lo contiene.

### Soluzione custom del 13 e 16 novembre

[BigQueryReadOnlyCatalog.java](BigQueryReadOnlyCatalog.java) è un `TableCatalog` in
sola lettura che risolve `bq.progetto.dataset.tabella` e passa schema e letture a
`Spark35BigQueryTableProvider`, la classe interna del connettore. Per evitare query job
mantiene sempre almeno una colonna nello scan, anche per `COUNT(*)`. Va compilato con
[build_catalog.py](build_catalog.py) e caricato con `--jars`; si usa lanciando `run.ps1`
con lo switch `-CustomCatalog`.

| Data | Batch | Risultato |
|---|---|---|
| 13/11 | `build-bq-catalog-v3-20261113` | Compilazione del JAR |
| 13/11 | `poc06-create-20261113-144149`, `poc06-verify-20261113-144455` | UNION 15.679 righe (9.955 BQ del 10/11 + 5.724 Iceberg), 167 colonne, JOIN 0 righe (giorni diversi), materializzazione OK |
| 16/11 | `poc06-create-20261116-163521`, `poc06-cutover-check-20261116-163832` | Cut-over: stessi risultati del §6 |

### Confronto

| | Catalogo custom | Catalogo nativo |
|---|---|---|
| Classe | `poc.BigQueryReadOnlyCatalog` | `com.google.cloud.spark.bigquery.BigQueryCatalog` |
| JAR da compilare e caricare | Sì | No |
| Manutenzione | Dipende da una classe interna del connettore, da ricontrollare a ogni runtime | A carico di Google |
| Lettura/scrittura | Solo lettura | Lettura e scrittura (limitare via IAM) |
| `COUNT(*)` senza colonne | Gestito: niente query job | Non provato (§8) |
| Risultati del test | PASS | PASS, stessi numeri |

Il catalogo custom resta nel repository come riferimento, ma non serve per il test 06.
