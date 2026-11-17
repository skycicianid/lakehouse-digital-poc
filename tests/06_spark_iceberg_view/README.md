# Test 06: vista Iceberg creata con Spark su tabella nativa BigQuery e tabella Iceberg

> Aggiornamento del 13 novembre 2026: creazione e lettura delle viste UNION e JOIN
> riuscite con un adattatore di catalogo Spark per BigQuery: 167 colonne e 15.679
> righe nell'UNION. PASS anche rilettura da batch indipendente e materializzazione.
> Vedi [correzione e verifica](SOLUTION.md).
> Le conclusioni sotto sono la diagnosi storica prima dell'adattatore.

**Obiettivo.** Creare con Spark una vista Iceberg che unisca (union e join) una tabella nativa BigQuery e una tabella Iceberg, rileggerla con Spark e riscriverla come tabella Iceberg materializzata, senza logica aggiuntiva.

**Stato attuale: PASS sul campione sandbox**, con rilettura da batch indipendente e materializzazione; vedi [SOLUTION.md](SOLUTION.md).

**Diagnosi storica, superata dalla correzione.** Al momento delle verifiche descritte sotto il job del test non era ancora stato scritto e la soluzione era stata considerata infattibile con il solo connettore standard.

## Oggetti previsti

| Ruolo | Oggetto | Tipo |
|---|---|---|
| Nativa | `skyita-da-architects-sandbox.dciciani.hit_data_stg_test` | Tabella BigQuery, copia della partizione 2026-11-10 (come test 04) |
| Iceberg | `skyita-da-architects-sandbox.dciciani-lakehouse-catalog.digital-analytics-myskyapp.hit_data_stg` | Tabella Iceberg, giorno 2026-10-13 (come test 01, 02, 03) |
| Vista | nome da definire, nel catalog Iceberg `dciciani-lakehouse-catalog` | Vista Iceberg che unisce e joina le due sorgenti |
| Output | tabella Iceberg materializzata | Scritta da Spark leggendo la vista, senza logica |

## Perché non è fattibile così

### 1. La vista Iceberg non può referenziare la tabella BigQuery

Una vista Iceberg salva il testo SQL nel catalog, e Spark lo risolve quando la vista viene letta. Spark risolve i nomi di tabella tramite i cataloghi registrati. Il connettore BigQuery per Spark (`spark-bigquery`, presente nell'immagine Dataproc 2.3) legge una tabella BigQuery come DataFrame, non come tabella con nome risolvibile in SQL. Quindi una vista del tipo `SELECT ... FROM <tabella BigQuery> UNION ALL SELECT ... FROM <Iceberg>` non può essere creata come vista Iceberg persistente.

**Verificato con il probe (batch `probe-view-20261113-010212`, SUCCEEDED).** `CREATE VIEW` con riferimento a `skyita-da-architects-sandbox.dciciani.hit_data_stg_test` fallisce con `[REQUIRES_SINGLE_PART_NAMESPACE] spark_catalog requires a single-part namespace`. È un errore di risoluzione del nome: Spark non riconosce il nome a tre parti come tabella BigQuery. Non è un errore di permessi. Il probe non ha provato un catalogo BigQuery registrato a parte, quindi resta aperta solo questa strada, non testata.

### 2. Il service account Dataproc non legge la nativa

Come documentato nel test 04, il service account Dataproc riceve 403 su `bigquery.tables.get` per `skyita-da-cmp-test.digital_skyita_myskyapp.hit_data_stg`. Per questo il test 04 ha usato una copia nel sandbox. Anche una soluzione che legga la nativa direttamente da Spark è bloccata dagli stessi permessi.

### 3. Il catalog Iceberg non accetta viste da BigQuery

Verificato nel test 05: BigQuery rifiuta `CREATE VIEW` nel dataset `dciciani-lakehouse-catalog.digital-analytics-myskyapp` con l'errore `Biglake resources cannot be modified in BigQuery`. Quindi la vista non può nemmeno essere creata da BigQuery nel catalog.

**Smentito dal probe:** il catalog accetta viste Iceberg lato Spark. `CREATE VIEW` su `hit_data_stg` riesce e la vista si può leggere con `SELECT COUNT(*)`. Il rifiuto di BigQuery (punto 3 sopra) resta valido: le viste si creano da Spark, non da BigQuery.

### 4. Le opzioni per la parte nativa

| Opzione | Esito |
|---|---|
| Vista che referenzia direttamente la nativa | Non fattibile per il punto 1 |
| Copiare la nativa in una tabella Iceberg di staging, poi vista Iceberg su due tabelle Iceberg | Fattibile: il probe conferma le viste Iceberg. La vista non referenzia più la nativa, quindi il requisito "BQ + Iceberg" non è rispettato alla lettera |
| Vista BigQuery sulle due sorgenti (test 05) | Già fatta, ma non è una vista Iceberg e non è letta da Spark come richiesto |

## Problema della join

La join non è stata decisa perché ogni scelta dà un risultato non significativo con i dati attuali:

- **Chiave hit (test 02):** la copia nativa contiene solo il 2026-11-10 e l'Iceberg solo il 2026-10-13. Le chiavi non si sovrappongono: 0 righe.
- **Chiave `id_mcvis` (test 05):** 164.074 coppie per 6 visitatori. La vista esploderebbe e la tabella materializzata conterrebbe righe duplicate per costruzione.

Per un test significativo serve una coppia di sorgenti che si sovrapponga sulla chiave scelta, cioè la stessa partizione su entrambi i lati.

## Cosa serve per sbloccare il test

1. Un account o un service account che legga `skyita-da-cmp-test.digital_skyita_myskyapp.hit_data_stg` da Dataproc, oppure una copia della stessa partizione che copra anche il giorno 2026-10-13.
2. Una decisione sulla join: chiave e giorni delle due sorgenti.
3. Per il riferimento diretto alla nativa serve un catalogo BigQuery registrato in Spark. Il connettore non lo offre: vedi la sezione "Verifica del catalogo BigQuery" sotto. Senza, la nativa va copiata in Iceberg prima della vista.

Il punto 2 del vecchio elenco (verifica delle viste) è fatto: il catalog accetta viste.

## Probe di verifica

Job: [probe_view.py](probe_view.py). Batch `probe-view-20261113-010212`, SUCCEEDED. Namespace di test `test_probe`, creato e cancellato dal job.

| Prova | Esito |
|---|---|
| `CREATE NAMESPACE` nel catalog | OK |
| `CREATE VIEW` su tabella Iceberg | OK |
| `SELECT` dalla vista Iceberg | OK |
| `CREATE VIEW` con riferimento alla BQ a tre parti | ERRORE `REQUIRES_SINGLE_PART_NAMESPACE` |
| Pulizia (viste e namespace) | OK |

Secondo probe, batch `probe-view-20261113-123408`, SUCCEEDED. Ipotesi suggerita da Gemini e verificata:

| Prova | Esito |
|---|---|
| `CREATE VIEW` Iceberg con catalog `RESTSessionCatalog` (non fallback su `spark_catalog`) | OK, quindi l'ipotesi "catalogo non registrato come SparkCatalog" è smentita |
| `CREATE VIEW` con riferimento BQ a due parti (`dciciani.hit_data_stg_test`) con `parentProject` impostato | ERRORE `TABLE_OR_VIEW_NOT_FOUND`: Spark cerca `dciciani` come namespace nel session catalog |

Conclusione: rimuovere il progetto dal nome non basta. Il riferimento diretto alla nativa resta non risolvibile da SQL, come già scritto sopra. `parentProject` agisce sul connettore (DataFrame), non sulla risoluzione SQL dei nomi.

## Verifica del catalogo BigQuery

Opzione scelta: registrare un catalogo BigQuery in Spark, così la vista potrebbe riferire la nativa direttamente.

Controllo sul jar `spark-bigquery-with-dependencies_2.13-0.46.1` (il connettore usato nel test 04), scaricato da Maven Central e solo elencato, non eseguito:

- Nessuna classe che implementi `CatalogPlugin` o `TableCatalog` per BigQuery. Le classi con "Catalog" riguardano solo OpenLineage e librerie interne.
- Nessuna voce in `META-INF/services` che registri un catalogo.
- Versione più recente su Maven: 0.46.1. Non c'è una versione più nuova con supporto a catalogo.

Conclusione: con questo connettore non è possibile registrare un catalogo BigQuery in Spark, quindi il nome della tabella nativa non si può risolvere in SQL. Una vista Iceberg persistente non può quindi contenere un riferimento diretto alla nativa.

Distinzione importante: Spark legge la nativa come DataFrame (`spark.read.format("bigquery")`), ed è quello che fa il test 04. Il DataFrame però vive solo durante il job: non è salvato nel catalog e non può essere referenziato da una vista persistente. Un `createOrReplaceTempView` sul DataFrame esiste solo nella sessione Spark, quindi non è una vista Iceberg.

## Esito

| Verifica | Valore |
|---|---|
| Job Spark del test 06 | Non scritto |
| Vista Iceberg del test 06 | Non creata |
| Tabella materializzata | Non creata |
| Probe viste Iceberg | Riuscito |
| Probe riferimento BQ da Spark | Fallito (nessun catalogo BQ registrato) |

## Data

2026-11-13.
