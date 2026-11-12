# Test 05: VIEW BigQuery su tabella Iceberg e tabella nativa

**Obiettivo.** Verificare che una view BigQuery possa combinare la tabella Iceberg `hit_data_stg` e la copia nativa BigQuery, sia con UNION ALL sia con una JOIN.

**Stato: PASS.** Entrambe le view sono create e restituiscono i valori attesi.

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
