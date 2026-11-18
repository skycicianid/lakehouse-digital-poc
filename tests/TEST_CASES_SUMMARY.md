# Test Cases Summary – Lakehouse Digital POC

Analisi statica della directory `tests/` (commit `180a376`, 17/11/2026), aggiornata il 18/11/2026 per il test 06. Per l'analisi del 17/11 non è stato eseguito nessun job: gli esiti sono quelli documentati nei README del repository. L'aggiornamento del 18/11 riporta tre batch del test 06 rieseguiti con il catalogo BigQuery nativo del connettore.

## 1. Overview

La POC verifica che due motori di consumo, BigQuery e Apache Spark su Dataproc Serverless (runtime 2.3, Spark 3.5), possano leggere dati distribuiti tra:

- **BigQuery Native**: storico (`hit_data_stg` nel progetto `skyita-da-cmp-test`, o la sua copia sandbox `dciciani.hit_data_stg_test` / `dciciani.hit_data_stg_hist`).
- **Apache Iceberg**: dati nuovi, nella tabella `digital-analytics-myskyapp.hit_data_stg` del BigLake/Lakehouse REST catalog `dciciani-lakehouse-catalog` (warehouse `gs://dciciani-lakehouse-catalog`).

BigQuery legge la tabella Iceberg tramite il catalog federato (nome a quattro parti `project.catalog.namespace.table`). Spark legge Iceberg dal catalog Lakehouse (`dataproc.lakehouse.catalog.*`) e BigQuery con il connettore `spark-bigquery` dell'immagine Dataproc, come DataFrame (test 04) o tramite il catalogo Spark `com.google.cloud.spark.bigquery.BigQueryCatalog` incluso nel connettore (test 06; fino al 16/11 al suo posto c'era il catalogo custom `BigQueryReadOnlyCatalog`).

I sei test del repository (`01`–`06`) sono mappati sui quattro scenari richiesti:

| Cartella | Motore | Scenario |
|---|---|---|
| `01_union_all`, `02_join`, `03_values_compare` | BigQuery | TC-01 |
| `04_dataproc_union_iceberg` | Spark | TC-02 |
| `05_bq_union_view` | BigQuery | TC-03 |
| `06_spark_iceberg_view` | Spark | TC-04 (in variante: vista Iceberg, non vista BigQuery) e, in parte, TC-02 |

**Natura delle evidenze.** Nel repository non ci sono log, output o file di risultato. Le evidenze sono i conteggi riportati nei README, gli ID dei batch Dataproc e i link alla console GCP. Per i test SQL (01, 02, 03, 05) non ci sono assertion nel codice: il confronto con il valore atteso è stato fatto a mano e riportato nel README. I job Spark (04, 06) contengono invece controlli che fanno fallire il batch.

## 2. Matrice riassuntiva dei test

| ID | Scenario | Descrizione | File di riferimento | Stato | Note |
|---|---|---|---|---|---|
| TC-01 | BigQuery → BQ + Iceberg | Lettura diretta delle due sorgenti | `01_union_all/query.sql`, `02_join/query.sql`, `03_values_compare/query.sql`, `03_values_compare/diagnose_mismatch_per_column.sql` | **Implementato e verificato** | Legge la nativa **originale**. UNION (15.679 righe), JOIN 1:1 (5.724) e confronto valori (0 differenze su 162 colonne) documentati il 12/11. Nessuna assertion nell'SQL. |
| TC-02 | Spark → BQ + Iceberg | Lettura diretta delle due sorgenti | `04_dataproc_union_iceberg/job.py`, `04_dataproc_union_iceberg/run.sh` | **Implementato e verificato** | Batch `union-iceberg-bq-20261113-000517` SUCCEEDED, `STATUS: OK`, 15.679 righe. La nativa letta è una **copia sandbox**: il SA Dataproc riceve 403 sull'originale. Verifica solo sui conteggi. |
| TC-03 | BigQuery → Vista unificata | Lettura della vista che combina BQ e Iceberg | `05_bq_union_view/query.sql`, `05_bq_union_view/join_view.sql` | **Implementato e verificato** | Viste `dciciani.hit_data_union_view` (15.679 righe) e `dciciani.hit_data_join_view` create. Nel repository ci sono solo le DDL: le query di lettura usate per la verifica non sono versionate. La vista non applica il cut-over. |
| TC-04 | Spark → Vista unificata | Lettura della vista che combina BQ e Iceberg | `06_spark_iceberg_view/job.py`, `06_spark_iceberg_view/run.ps1`, `06_spark_iceberg_view/SOLUTION.md` | **Implementato e verificato (variante vista Iceberg)** | Spark legge una vista unificata **Iceberg** nel catalog Lakehouse che referenzia direttamente la tabella BigQuery tramite il catalogo nativo `BigQueryCatalog`, senza codice custom. Batch del 18/11 `poc06-create-20261118-093051`, `poc06-cutover-check-20261118-093420`, `poc06-verify-20261118-093733`, tutti `STATUS: OK`: 38.554 righe, cut-over e scelta della sorgente per data verificati. Non è una **vista BigQuery**: nessun codice legge da Spark le viste del test 05. |

## 3. Dettaglio dei casi di test

### TC-01 – BigQuery Job: lettura diretta da BigQuery e Iceberg

- **Obiettivo:** una query BigQuery legge e combina la tabella nativa e la tabella Iceberg.
- **Implementazione individuata:**
  - `01`: `UNION ALL` su 167 colonne castate a `STRING`. Nativa filtrata sul 2026-11-10, Iceberg intera (2026-10-13).
  - `02`: `COUNT(*)` della `JOIN` sulla chiave `(id_hit_high, id_hit_low)` per il 2026-10-13.
  - `03`: `EXCEPT DISTINCT` Iceberg − nativa su 162 colonne. Esclude 5 colonne di lineage (`_process_*`, `_clustering_*`), individuate con la query di diagnosi per colonna (`COUNTIF ... IS DISTINCT FROM`).
- **File coinvolti:** `01_union_all/query.sql`, `02_join/query.sql`, `03_values_compare/query.sql`, `03_values_compare/diagnose_mismatch_per_column.sql`, relativi `README.md`.
- **Modalità di accesso ai dati:** diretta. Nativa `skyita-da-cmp-test.digital_skyita_myskyapp.hit_data_stg`, Iceberg `skyita-da-architects-sandbox.dciciani-lakehouse-catalog.digital-analytics-myskyapp.hit_data_stg`, nella stessa query (location EU).
- **Validazioni effettuate:** nel codice nessuna assertion; le query restituiscono righe o conteggi. Nei README i conteggi sono confrontati con i valori attesi (9.955 + 5.724 = 15.679; chiavi univoche e join 1:1 = 5.724; 0 righe non identiche).
- **Risultato:** documentato PASS il 12/11/2026 con `bq query`, in sola lettura. È lo scenario con l'evidenza più solida, perché il test 03 confronta anche i valori e non solo i conteggi.
- **Eventuali criticità:**
  - Il test 01 unisce giorni diversi (nativa 10/11, Iceberg 13/10): dimostra la compatibilità, non la continuità dei dati.
  - Il cast a `STRING` non verifica la compatibilità dei tipi nativi.
  - 1.380 righe della nativa del 13/10 senza corrispondenza in Iceberg: causa non indagata (README 02).
  - Le 5 colonne di lineage sono escluse dal confronto senza verificarne il significato.

### TC-02 – Spark Job: lettura diretta da BigQuery e Iceberg

- **Obiettivo:** un job Spark legge entrambe le sorgenti e le combina.
- **Implementazione individuata:** `job.py` legge la Iceberg con `spark.table(...)` e la tabella BigQuery con `spark.read.format("bigquery")`, con filtro sul giorno 2026-11-10. Esegue il cast a `STRING` (eccetto `partition_date`) e fa `unionByName(allowMissingColumns=False)`. Scrive poi una nuova tabella Iceberg `hit_data_union`, partizionata per `days(partition_date)`. `run.sh` senza `--yes` stampa solo il comando.
- **File coinvolti:** `04_dataproc_union_iceberg/job.py`, `04_dataproc_union_iceberg/run.sh`, `04_dataproc_union_iceberg/README.md`. Altra modalità di lettura diretta: `06_spark_iceberg_view/job.py` (`spark.table("bq.<project>.<dataset>.<table>")` tramite il catalogo `bq`, cioè `BigQueryCatalog` nativo o il precedente `BigQueryReadOnlyCatalog`).
- **Modalità di accesso ai dati:** diretta. BigQuery come DataFrame del connettore (Storage Read API), Iceberg dal catalog REST Lakehouse.
- **Validazioni effettuate:** il job fallisce (`RuntimeError`) se le righe scritte sono diverse da `count(iceberg) + count(nativa)`. `unionByName` fallisce se gli schemi non coincidono per nome.
- **Risultato:** batch `union-iceberg-bq-20261113-000517` SUCCEEDED, `STATUS: OK`. 5.724 + 9.955 = 15.679 righe, snapshot `1747370555152384936` (dati dal README). La lettura Spark di entrambe le sorgenti è quindi dimostrata.
- **Eventuali criticità:**
  - La sorgente BigQuery è `skyita-da-architects-sandbox.dciciani.hit_data_stg_test`, una CTAS della partizione 10/11, e non la tabella nativa originale. Il SA Dataproc riceve 403 (`bigquery.tables.get`) sull'originale: è un requisito IAM aperto.
  - Il valore atteso è calcolato nello stesso job dai conteggi delle sorgenti, non è un valore indipendente. Nessun confronto dei valori, solo conteggi.
  - Il README è incoerente: la tabella "Storia dei tentativi" riporta ancora "Nuovo lancio – Da eseguire", mentre la sezione "Esito" documenta il PASS.
  - Il test scrive una tabella Iceberg: va oltre la sola lettura richiesta, ma non altera le sorgenti.

### TC-03 – BigQuery Job: lettura tramite vista unificata BigQuery

- **Obiettivo:** un job BigQuery interroga un'unica vista che espone dati nativi e Iceberg.
- **Implementazione individuata:**
  - `query.sql` crea `skyita-da-architects-sandbox.dciciani.hit_data_union_view`: `UNION ALL` sulle 167 colonne in `STRING` tra la copia nativa (filtro 2026-11-10) e la Iceberg.
  - `join_view.sql` crea `dciciani.hit_data_join_view`: `JOIN` su `id_mcvis` con 5 colonne per lato.
- **File coinvolti:** `05_bq_union_view/query.sql`, `05_bq_union_view/join_view.sql`, `05_bq_union_view/README.md`.
- **Modalità di accesso ai dati:** tramite vista logica BigQuery nel dataset `dciciani`. Non si può creare una vista nel dataset del catalog: `Biglake resources cannot be modified in BigQuery`.
- **Validazioni effettuate:** nessuna nel codice, che contiene solo le DDL. Il README riporta i conteggi letti dalla vista: union 15.679 (atteso 15.679); join 164.074 righe per 6 visitatori.
- **Risultato:** documentato PASS il 13/11/2026. Lettura tramite vista unificata dimostrata per la `UNION ALL`.
- **Eventuali criticità:**
  - Le query di consumo (`SELECT COUNT(*) FROM ...view`) non sono nel repository: l'esito non è riproducibile dal solo codice.
  - Anche qui la sorgente nativa è la copia sandbox, non l'originale.
  - La vista JOIN produce un prodotto cartesiano per visitatore su giorni diversi (fino a 68.080 righe per un singolo `id_mcvis`). Prova che la JOIN funziona tecnicamente, ma il risultato non ha significato di business.
  - Il cut-over (storico BQ < data, Iceberg ≥ data) richiesto da `poc.md` non è implementato nella vista BigQuery. Esiste solo nella variante Spark del test 06.

### TC-04 – Spark Job: lettura tramite vista unificata

- **Obiettivo:** un job Spark interroga un'unica vista che unifica la tabella nativa e quella Iceberg. Lo scenario come formulato parla di **vista BigQuery**; il test 06 la realizza come **vista Iceberg** persistente nel catalog Lakehouse (vedi criticità).
- **Implementazione individuata (aggiornata al 18/11):**
  - La vista Iceberg referenzia direttamente la tabella BigQuery con il nome `bq.<progetto>.<dataset>.<tabella>`. Il nome viene risolto dal catalogo Spark **nativo** del connettore, `com.google.cloud.spark.bigquery.BigQueryCatalog` (dalla versione 0.42.0, artefatto `spark-3.5-bigquery`, già presente in Dataproc 2.3). Si attiva con la property `spark.sql.catalog.bq=com.google.cloud.spark.bigquery.BigQueryCatalog` (default di `run.ps1`), senza JAR né codice custom.
  - `job.py --mode create` crea due viste di "contratto" (`hit_data_native_contract`, `hit_data_iceberg_contract`), la vista `hit_data_union_view` (`UNION ALL` dei contratti con il filtro di cut-over) e `hit_data_join_view`.
  - `--mode verify` legge la vista da un batch indipendente, senza codice di federazione, e opzionalmente la materializza.
  - `--mode cutover-check` verifica i conteggi per giorno e quale sorgente viene letta, dal piano fisico (metrica `numOutputRows`).
  - Prima soluzione (13–16/11): `BigQueryReadOnlyCatalog.java`, un `TableCatalog` custom costruito sul provider del connettore, nato dalla conclusione (errata) che il connettore non offrisse un catalogo. Era stato controllato il jar `spark-bigquery-with-dependencies_2.13`, che non lo contiene. Resta nel repository come storico.
- **File coinvolti:** `06_spark_iceberg_view/job.py`, `run.ps1`, `inspect_views.ps1`, `SOLUTION.md`, `README.md`; storico: `BigQueryReadOnlyCatalog.java`, `build_catalog.py`, probe `probe_view.py`, `probe_datasource.py`, `probe_catalog_views.py`.
- **Modalità di accesso ai dati:** tramite vista Iceberg (``spark.table("`dciciani-lakehouse-catalog`.<ns>.hit_data_union_view")``). Spark risolve la vista: il ramo BigQuery viene letto con la Storage Read API tramite il catalogo `bq`, il ramo Iceberg dal catalog REST. I dati BigQuery non vengono copiati e BigQuery non esegue query job: unione e calcolo li fa Spark. Il consumer deve configurare entrambi i cataloghi, ma non deve caricare JAR.
- **Validazioni effettuate (nel codice):**
  - `create`: insiemi di colonne uguali, entrambi i rami non vuoti, `count(view) == count(bq) + count(iceberg)`.
  - `verify`: `count(view) == --expected` (valore passato da CLI, quindi indipendente); conteggio della materializzazione uguale a quello della vista.
  - `cutover-check`: conteggi per giorno uguali a quelli attesi; ramo BQ assente dal piano per le query sui dati recenti; ramo Iceberg a 0 righe per un giorno di storico.
  - `inspect_views.ps1`: controlla nei metadata della vista su GCS che l'SQL `spark` referenzi entrambi i rami.
- **Risultato:**
  - 18/11, catalogo nativo, namespace `federation_native_catalog_20261118`, cut-over 2026-10-13: `poc06-create-20261118-093051`, `poc06-cutover-check-20261118-093420`, `poc06-verify-20261118-093733`, tutti `STATUS: OK`. 38.554 righe (32.830 storico BQ + 5.724 Iceberg); il 13/10 è letto solo da Iceberg; per le query sui dati recenti il ramo BQ non compare nel piano; per un giorno di storico BQ restituisce solo quel giorno (2.171 righe) e Iceberg 0; materializzazione riuscita. Risultati identici a quelli del catalogo custom del 16/11.
  - 13/11 e 16/11, catalogo custom: `poc06-verify-20261113-144455` (15.679 righe) e `poc06-cutover-check-20261116-163832`, `STATUS: OK`.
  - È quindi dimostrato, con soli componenti nativi del runtime, che Spark legge una vista unificata persistente BigQuery + Iceberg da una sessione indipendente. **Non** è dimostrato che Spark legga una vista BigQuery.
- **Eventuali criticità:**
  - **Scostamento dal requisito:** la vista è Iceberg (catalog Lakehouse), non BigQuery. `poc.md` ammette viste diverse per motore ("Non è richiesto che i due motori utilizzino lo stesso catalogo, la stessa vista"), ma lo scenario TC-04 come formulato resta scoperto.
  - Leggere una vista BigQuery dal connettore Spark richiede di materializzarla con un query job BigQuery. Il SA Dataproc non ha `bigquery.jobs.create` (`SOLUTION.md`), quindi questa strada è probabilmente bloccata anche lato IAM. Non è stata provata: **da verificare**.
  - Con il catalogo nativo un `SELECT COUNT(*)` diretto sulla tabella `bq.` senza colonne potrebbe richiedere un query job (e quindi `bigquery.jobs.create`). Il caso non si è presentato nei batch del 18/11 e non è stato provato direttamente.
  - Il catalogo nativo consente anche scritture (`createTable`, `dropTable`): la sola lettura va garantita via IAM.
  - La scelta automatica della sorgente è verificata solo per filtri su `partition_date`; filtri su colonne convertite a `STRING` dal contratto potrebbero non arrivare a BigQuery.
  - Il catalog REST rifiuta le viste con testo SQL oltre circa 4.096 caratteri (limite osservato, non documentato). Per questo servono le viste di contratto.
  - La JOIN (5.724 righe il 18/11) non è controllata da nessuna assertion: `verify` ne stampa il conteggio senza confrontarlo.
  - `probe_view.py` cattura le eccezioni e termina SUCCEEDED anche con prove fallite: il suo esito "SUCCEEDED" non significa PASS.

## 4. Gap Analysis

| Area | Requisito | Realizzato | Gap |
|---|---|---|---|
| Copertura scenari | 4 scenari | TC-01, TC-02, TC-03 coperti; TC-04 in variante | Manca la lettura Spark di una **vista BigQuery** |
| Sorgente BigQuery | Tabella nativa | TC-01 sull'originale; TC-02/03/04 su copie CTAS sandbox (`hit_data_stg_test`, `hit_data_stg_hist`) | Il SA Dataproc non legge l'originale (403): requisito IAM aperto |
| Sorgente Iceberg | Tabella Iceberg | Stessa tabella in tutti i test (5.724 righe, solo 13/10) | Campione minimo, una sola partizione |
| Vista unificata | Vista usata come unico punto di accesso | BigQuery: vista logica (test 05). Spark: vista Iceberg persistente (test 06) con il catalogo BigQuery nativo del connettore, senza codice custom | Due viste distinte, nessuna condivisa; la vista BQ non ha il cut-over |
| Componenti custom | Soluzione con componenti nativi | Dal 18/11 il test 06 usa `BigQueryCatalog` del connettore al posto di `BigQueryReadOnlyCatalog` | `COUNT(*)` senza colonne su `bq.` non provato; sola lettura da garantire via IAM |
| Diretto vs vista | Distinzione netta | Rispettata: 01–04 diretti, 05–06 tramite vista | — |
| Spark → vista BQ | Lettura della vista BigQuery | Non tentata | Servono `viewsEnabled`, un `materializationDataset` e `bigquery.jobs.create` (non verificato) |
| Assertion | Verifiche sui dati nel codice | Solo nei job Spark (04, 06), e solo sui conteggi | SQL (01, 02, 03, 05) senza assertion; confronto dei valori solo in 03 |
| Evidenze | Esecuzioni tracciate | Batch ID e conteggi nei README | Nessun log o output salvato nel repository; query di consumo del test 05 non versionate |
| Tipi | Contratto dati | Tutto `STRING`, eccetto `partition_date` | Fedeltà dei tipi non verificata |
| Coerenza dati | Stessi giorni sui due lati | Overlap solo sul 13/10 (test 02, 03, cut-over del 06) | Le UNION di 01/04/05/06 combinano giorni diversi |
| Duplicazioni | — | 01 ≈ 05 (stessa UNION, prima diretta e poi in vista); 02 ≈ JOIN del 06 | Non è un problema: varia solo la modalità di accesso |

## 5. Conclusioni

- **Scenari implementati:** TC-01, TC-02 e TC-03 sono implementati. TC-04 è implementato in variante: vista Iceberg letta da Spark invece di una vista BigQuery. Dal 18/11 la variante usa solo componenti nativi del runtime Dataproc 2.3: la tabella BigQuery è risolta dal catalogo `BigQueryCatalog` del connettore, e il catalogo custom `BigQueryReadOnlyCatalog` non serve più.
- **Scenari verificati (evidenza documentata):** TC-01 (con confronto dei valori), TC-02 e TC-03 (sui conteggi). Per TC-04 è verificata la variante Iceberg, inclusi lettura da batch indipendente, materializzazione e pruning al cut-over, prima con il catalogo custom (13 e 16/11) e poi con quello nativo (18/11, rieseguito). Le altre evidenze sono dichiarative (README e batch ID) e non sono state rieseguite in questa analisi.
- **Da completare o correggere:**
  1. TC-04 come formulato: provare la lettura da Spark di `dciciani.hit_data_union_view` (connettore con `viewsEnabled=true` e `materializationDataset`), oppure formalizzare che la vista Iceberg del test 06 sostituisce il requisito, coerentemente con `poc.md`.
  2. Versionare le query di consumo del test 05 e aggiungere il cut-over alla vista BigQuery.
  3. Aggiornare `04_dataproc_union_iceberg/README.md` ("Da eseguire" superato). I README del test 06 sono stati aggiornati il 18/11.
  4. Aggiungere un'assertion sul conteggio di `hit_data_join_view` nel test 06.
  5. Test 06: provare un `COUNT(*)` diretto su `bq.` con il catalogo nativo, per capire se serve `bigquery.jobs.create`. Dal 18/11 il catalogo nativo è il default di `run.ps1` (`-CustomCatalog` per quello precedente).
- **Verifiche ulteriori per chiudere la POC:**
  - Accesso del SA Dataproc alla nativa originale (`skyita-da-cmp-test`), senza copie.
  - Confronto dei valori (non solo conteggi) per i percorsi Spark e vista, come nel test 03.
  - Contratto dati con i tipi di business reali al posto del cast a `STRING`.
  - Volumi realistici: prestazioni, costi della Storage API, pruning su filtri diversi da `partition_date`.
  - Conservazione di log e output delle esecuzioni (o un test di regressione automatizzato, es. `run.ps1 -Mode cutover-check`) per non dipendere solo dai README.
