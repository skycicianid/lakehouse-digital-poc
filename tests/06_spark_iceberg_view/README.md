# Test 06: Spark legge una vista Iceberg che unisce BigQuery e Iceberg

**Obiettivo.** Un job Spark interroga un'unica vista persistente che espone insieme
una tabella nativa BigQuery (storico) e una tabella Iceberg (dati correnti).
L'utente non sceglie la sorgente: in base alla data richiesta Spark legge BigQuery,
Iceberg o entrambe.

**Stato: PASS (18 novembre 2026), con soli componenti nativi del runtime Dataproc 2.3.**
La tabella BigQuery è referenziata nella vista tramite il catalogo ufficiale del
connettore, `com.google.cloud.spark.bigquery.BigQueryCatalog`. Non serve codice custom.
Il dettaglio è in [SOLUTION.md](SOLUTION.md).

## In breve

| Domanda | Risposta |
|---|---|
| Dove sta la vista? | Nel catalog Iceberg Lakehouse `dciciani-lakehouse-catalog`, come vista Iceberg (SQL dialect `spark`) |
| I dati BigQuery vengono copiati? | No. Spark li legge al momento della query con la BigQuery Storage Read API |
| Chi esegue la query? | Spark. BigQuery fa solo da storage: nessun query job BigQuery |
| Cosa vede l'utente? | Una sola tabella (167 colonne) con le righe di entrambe le sorgenti |
| Come si sceglie la sorgente? | Con il filtro di cut-over nella vista: storico `< 2026-10-13` da BigQuery, dal `2026-10-13` in poi da Iceberg |
| Cosa serve nel job consumer? | Due cataloghi configurati con le properties del batch: Lakehouse (Iceberg) e `bq` (BigQuery). Nessun JAR |

## Esito

| Batch (18/11/2026) | Cosa verifica | Esito |
|---|---|---|
| `poc06-create-20261118-093051` | Creazione delle viste | `STATUS: OK` |
| `poc06-cutover-check-20261118-093420` | Continuità al cut-over e scelta del ramo per data | `STATUS: OK` |
| `poc06-verify-20261118-093733` | Lettura da batch indipendente e materializzazione | `STATUS: OK`, 38.554 righe |

| Query sulla vista | Righe | Lette da BigQuery | Lette da Iceberg |
|---|---|---|---|
| solo dati recenti (`>= 2026-10-13`) | 5.724 | nessuna: ramo assente dal piano | 5.724 |
| un giorno di storico (`2026-10-08`) | 2.171 | 2.171 | 0 |
| tutta la vista | 38.554 | 32.830 | 5.724 |

## Esecuzione

```powershell
# Setup: crea contratti, vista UNION con cut-over e vista JOIN in un namespace nuovo
./tests/06_spark_iceberg_view/run.ps1 -Mode create -Namespace <nuovo_namespace> `
  -Native skyita-da-architects-sandbox.dciciani.hit_data_stg_hist -Cutover 2026-10-13

# Consumer indipendente: controlla il cut-over e quale ramo viene letto
./tests/06_spark_iceberg_view/run.ps1 -Mode cutover-check -Namespace <nuovo_namespace> `
  -Native skyita-da-architects-sandbox.dciciani.hit_data_stg_hist -Cutover 2026-10-13

# Consumer indipendente: conta le righe e (facoltativo) materializza la vista
./tests/06_spark_iceberg_view/run.ps1 -Mode verify -Namespace <nuovo_namespace> -Expected 38554 -Materialize
```

Per default `run.ps1` usa il catalogo nativo. Con `-CustomCatalog` usa il catalogo custom del 13 novembre
(vedi sotto).

## File

| File | Ruolo |
|---|---|
| [job.py](job.py) | Job Spark: `create`, `verify`, `cutover-check` |
| [run.ps1](run.ps1) | Upload e lancio del batch Dataproc Serverless; usa il catalogo nativo per default, `-CustomCatalog` quello custom |
| [inspect_views.ps1](inspect_views.ps1) | Legge da GCS i metadata delle viste e ne stampa l'SQL, senza Spark |
| [SOLUTION.md](SOLUTION.md) | Spiegazione dettagliata, risultati, limiti |
| [BigQueryReadOnlyCatalog.java](BigQueryReadOnlyCatalog.java), [build_catalog.py](build_catalog.py) | Catalogo custom della prima soluzione (13/11). Superato, mantenuto come storico |
| [probe_view.py](probe_view.py), [probe_datasource.py](probe_datasource.py), [probe_catalog_views.py](probe_catalog_views.py) | Probe della fase di diagnosi |

## Storia del test

1. **13/11, mattina: "infattibile".** I probe hanno mostrato che il catalog Iceberg
   accetta viste, ma che Spark non risolve un nome di tabella BigQuery dentro una
   vista (`REQUIRES_SINGLE_PART_NAMESPACE`, `TABLE_OR_VIEW_NOT_FOUND`). Serviva un
   catalogo Spark per BigQuery. Il controllo del jar
   `spark-bigquery-with-dependencies_2.13-0.46.1` non ne ha trovato uno, e il test è
   stato dichiarato infattibile.
2. **13/11, pomeriggio: catalogo custom.** È stato scritto `BigQueryReadOnlyCatalog.java`,
   un catalogo Spark che passa le letture al provider ufficiale del connettore. Con
   questo la vista è stata creata e riletta (15.679 righe).
3. **16/11: cut-over.** Stessa soluzione su un campione con storico BigQuery e dati
   correnti Iceberg sovrapposti il 13/10: ogni giorno arriva da un solo ramo, e per i
   dati recenti il ramo BigQuery non viene letto.
4. **18/11: catalogo nativo.** Il catalogo ufficiale `BigQueryCatalog` esiste dalla
   versione 0.42.0 del connettore, nell'artefatto `spark-3.5-bigquery` già presente in
   Dataproc 2.3. Il controllo del 13/11 aveva guardato un altro jar. Il test è stato
   rieseguito con il catalogo nativo al posto di quello custom, con gli stessi
   risultati del 16/11.

La conclusione "infattibile" del punto 1 è quindi superata, e il catalogo custom del
punto 2 non è più necessario.
