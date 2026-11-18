# Test 05: VIEW BigQuery su tabella Iceberg e tabella nativa

**Obiettivo.** Verificare che una view BigQuery possa combinare la tabella Iceberg `hit_data_stg` e la copia nativa BigQuery, sia con UNION ALL sia con una JOIN.

**Stato: PASS.** Entrambe le view sono create e restituiscono i valori attesi.

**Aggiornamento 18/11/2026: vista con cut-over, PASS.** La vista `hit_data_cutover_view` applica lo stesso cut-over del test 06 (storico da BigQuery prima del 2026-10-13, Iceberg dal 2026-10-13), sullo stesso campione e con gli stessi risultati: 38.554 righe. BigQuery legge solo la sorgente necessaria per la data richiesta. Dettagli nella sezione [Vista con cut-over](#vista-con-cut-over-18112026).

Le view `hit_data_union_view` e `hit_data_join_view` descritte sotto restano invariate: documentano il primo test, senza cut-over.

## Oggetti coinvolti

| Ruolo | Oggetto | Tipo |
|---|---|---|
| Sorgente 1 | `skyita-da-architects-sandbox.dciciani-lakehouse-catalog.digital-analytics-myskyapp.hit_data_stg` | Tabella Iceberg, tutte le partizioni (giorno 2026-10-13) |
| Sorgente 2 | `skyita-da-architects-sandbox.dciciani.hit_data_stg_test` | Tabella BigQuery nativa, copia della partizione 2026-11-10 |
| View 1 | `skyita-da-architects-sandbox.dciciani.hit_data_union_view` | View, UNION ALL delle due sorgenti |
| View 2 | `skyita-da-architects-sandbox.dciciani.hit_data_join_view` | View, JOIN delle due sorgenti per visitatore |

Le view sono nel dataset `dciciani` del progetto sandbox e non nel dataset Iceberg: BigQuery non crea viste dentro `dciciani-lakehouse-catalog.digital-analytics-myskyapp`, che è una risorsa BigLake.

## View 1: UNION ALL

File: [query.sql](query.sql)

Union su 167 colonne con nome identico, tutte castate a `STRING`. Il filtro `partition_date = 2026-11-10` si applica solo alla copia nativa.

| Verifica | Righe |
|---|---|
| Copia nativa (2026-11-10) | 9.955 |
| Iceberg (tutte le partizioni) | 5.724 |
| `hit_data_union_view` | 15.679 |

Controllo: 9.955 + 5.724 = 15.679.

## View 2: JOIN semplice

File: [join_view.sql](join_view.sql).

Join interna tra le due tabelle sulla chiave `id_mcvis` (identificativo del visitatore). Una riga della view per ogni coppia di righe con lo stesso `id_mcvis`, senza aggregazioni. Nella view compaiono `id_mcvis`, `id_country`, `post_pagename`, `date_hit_time` e `partition_date` di entrambe le tabelle, con suffisso `_iceberg` o `_nativa`.

Ogni riga Iceberg viene abbinata a ogni riga nativa dello stesso visitatore: il numero di righe della view è quindi `righe_iceberg × righe_nativa`, per ciascun visitatore.

| id_mcvis (troncato) | Righe Iceberg | Righe nativa | Righe nella view |
|---|---|---|---|
| No6PJvma… | 592 | 115 | 68.080 |
| PcF+k4LB… | 99 | 515 | 50.985 |
| kOVPYzmi… | 130 | 286 | 37.180 |
| vtZKN4Uk… | 67 | 70 | 4.690 |
| uoBOvjkY… | 43 | 47 | 2.021 |
| EYBylxJF… | 86 | 13 | 1.118 |

Totale: 164.074 righe per 6 visitatori.

I due lati hanno giorni diversi (2026-10-13 nella Iceberg, 2026-11-10 nella nativa), quindi ogni riga della view mette in relazione un evento di un giorno con un evento dello stesso visitatore in un altro giorno.

### Perché la chiave hit non si può usare

`id_hit_high`/`id_hit_low` e `_ingest_row_id` non si sovrappongono tra le due tabelle (0 abbinamenti): i giorni diversi rendono impossibile un match a livello di singolo hit.

## Esito

| Verifica | Valore |
|---|---|
| View union: righe | 15.679 (atteso 15.679) |
| View join: visitatori distinti | 6 |
| View join: righe | 164.074 |

## Limiti

- La join moltiplica le righe: una riga Iceberg con N corrispondenze nella nativa compare N volte. Per contare eventi distinti serve un'altra query.
- La join si basa su un solo giorno per lato: il risultato non dice nulla sul comportamento dello stesso visitatore nello stesso giorno.
- La union non deduplica.
- Le colonne sono castate a `STRING`: il confronto dei tipi nativi resta da fare.

## Data esecuzione

2026-11-13, in location EU, con l'account `daniele.ciciani@skytv.it`.

## Vista con cut-over (18/11/2026)

**Obiettivo.** Portare in BigQuery la logica di cut-over del [test 06](../06_spark_iceberg_view/README.md): l'utente interroga una sola vista e, in base alla data richiesta, BigQuery legge lo storico nativo, la tabella Iceberg o entrambe. Ogni giorno arriva da una sola sorgente, anche quando è presente in tutte e due.

### Oggetti

| Ruolo | Oggetto |
|---|---|
| Storico (BigQuery nativa) | `skyita-da-architects-sandbox.dciciani.hit_data_stg_hist`, partizionata per giorno su `partition_date` |
| Corrente (Iceberg) | `skyita-da-architects-sandbox.dciciani-lakehouse-catalog.digital-analytics-myskyapp.hit_data_stg` |
| Contratto nativa | `dciciani.hit_data_native_contract` |
| Contratto Iceberg | `dciciani.hit_data_iceberg_contract` |
| Vista per gli utenti | `dciciani.hit_data_cutover_view` |

Lo storico è lo stesso campione del test 06: copia CTAS della nativa originale con i giorni 2026-10-08 (2.171), 2026-10-12 (30.659) e 2026-10-13 (7.104). Il 13/10 è presente anche in Iceberg (5.724 righe), quindi la sovrapposizione al cut-over viene messa alla prova.

### Struttura

Uguale al test 06. File: [cutover_view.sql](cutover_view.sql), template con segnaposto `{{PROJECT}}`, `{{DATASET}}`, `{{NATIVE}}`, `{{ICEBERG}}`, `{{CUTOVER}}`.

- **Contratti.** Le 167 colonne di ciascuna sorgente in `STRING`, tranne `partition_date`, che resta `TIMESTAMP` per permettere il partition pruning. A differenza della view `hit_data_union_view`, dove `partition_date` è castata a `STRING`.
- **Vista finale.** Contiene solo il cut-over:

```sql
SELECT * FROM hit_data_native_contract  WHERE partition_date <  TIMESTAMP '2026-10-13 00:00:00'
UNION ALL
SELECT * FROM hit_data_iceberg_contract WHERE partition_date >= TIMESTAMP '2026-10-13 00:00:00'
```

In BigQuery non c'è il limite di circa 4.096 caratteri del catalog REST. I due livelli restano per simmetria con il test 06 e perché, quando lo storico viene dismesso, basta ridefinire la vista finale.

### Esecuzione

[run_cutover.ps1](run_cutover.ps1) usa `bq` con l'utenza locale. Ogni esecuzione salva l'esito in `results/<mode>-<run_id>.json`.

```powershell
# Crea contratti e vista, poi controlla count(vista) = count(BQ < cut-over) + count(Iceberg >= cut-over)
./tests/05_bq_union_view/run_cutover.ps1 -Mode create -Cutover 2026-10-13

# Sola lettura: conteggi per giorno e quale sorgente legge ogni query
./tests/05_bq_union_view/run_cutover.ps1 -Mode cutover-check -Cutover 2026-10-13
```

Le query di controllo girano con `--nouse_cache`. Una risposta dalla cache non avrebbe statistiche di lettura, e lo script in quel caso si ferma.

### Come si misura quale sorgente viene letta

BigQuery legge le due tabelle nello **stesso stage** di input: il plan mostra `FROM ...hit_data_stg_hist | (1 similar table omitted)`, con un solo `recordsRead` per lo stage. Il plan quindi non dice quante righe arrivano da ciascuna sorgente, a differenza della metrica `numOutputRows` per scan in Spark. Si usano le statistiche del job, che sono separate per sorgente:

| Sorgente | Statistica | Significato |
|---|---|---|
| BigQuery nativa | `totalPartitionsProcessed` | Partizioni della nativa lette |
| Iceberg | `objectStorageStats.objectStorageBytesRead` | Byte letti dai file Iceberg su GCS; il campo manca se Iceberg non viene letta |

### Esito

| Run (18/11/2026) | Job | Esito |
|---|---|---|
| `create` | `poc05_*_20261118_113512` | `STATUS: OK`: sorgenti 32.830 (BigQuery, prima del cut-over) + 5.724 (Iceberg) = 38.554 righe della vista |
| `cutover-check` | `poc05_*_20261118_113834` | `STATUS: OK` |

| Giorno | Nativa | Iceberg | Vista | Atteso |
|---|---|---|---|---|
| 2026-10-08 | 2.171 | – | 2.171 | da BigQuery |
| 2026-10-12 | 30.659 | – | 30.659 | da BigQuery |
| 2026-10-13 | 7.104 | 5.724 | 5.724 | solo da Iceberg |

| Query sulla vista | Righe | Partizioni nativa lette | Byte Iceberg da GCS | Byte elaborati |
|---|---|---|---|---|
| solo dati recenti (`>= 2026-10-13`) | 5.724 | **0** | 131.157 | 45.792 |
| un giorno di storico (`2026-10-08`) | 2.171 | 1 | **0** (Iceberg non letta) | 17.368 |
| tutta la vista | 38.554 | 2 (08/10 e 12/10; il 13/10 nativo no) | 131.157 | 308.432 |

Sono gli stessi risultati del test 06 su Spark: il giorno di cut-over arriva solo da Iceberg, e per ogni query viene letta solo la sorgente che serve. In BigQuery il pruning della nativa avviene a livello di partizione: anche nella query su tutta la vista la partizione nativa del 13/10 non viene letta.

### Limiti

- Come nel test 06, la scelta della sorgente è verificata solo per filtri su `partition_date`. Filtri su colonne convertite a `STRING` dal contratto non riducono le partizioni lette.
- Le query sono `COUNT(*)`: i byte elaborati riguardano solo `partition_date` (8 byte per riga). Con più colonne crescono, ma le sorgenti lette non cambiano.
- Eseguito con utenza personale, non con un service account applicativo.
- Il confronto con il test 06 è sui conteggi, non sui valori. Un confronto dei valori tra le due viste (FR-08) resta da fare.
