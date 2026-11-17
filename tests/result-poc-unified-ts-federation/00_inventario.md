# 00 – Inventario delle sorgenti

Data: 2026-11-13. Sola lettura. Location EU.

Written for: team del progetto e reviewer della POC.

## Sorgenti

| Ruolo | Tabella | Righe | Date (`partition_date`) | Giorni | Visitatori distinti | Date nulle |
|---|---|---|---|---|---|---|
| BQ Native (storico) | `skyita-da-cmp-test.digital_skyita_myskyapp.hit_data_stg` | 2.976.430 | 2025-09-26 → 2026-11-13 | 379 | 37.181 | 0 |
| Iceberg (nuovi dati) | `skyita-da-architects-sandbox.dciciani-lakehouse-catalog.digital-analytics-myskyapp.hit_data_stg` | 5.724 | 2026-10-13 | 1 | 142 | 0 |

## Schemi

- Stesse 167 colonne, stessi nomi e stessi tipi (`STRING`, `INTEGER`, `TIMESTAMP`) nelle due tabelle.
- **Ordine diverso**: la `UNION ALL` in BigQuery abbina per posizione, quindi le colonne vanno elencate esplicitamente.
- Partizione nativa: `DAY` su `partition_date`, cluster su `_clustering_hour`.

## Sovrapposizione sul giorno 2026-10-13

| Misura | Valore |
|---|---|
| Righe native del giorno | 7.104 |
| Righe Iceberg del giorno | 5.724 |
| Righe Iceberg con chiave `(id_hit_high, id_hit_low)` presente nella nativa | 5.724 (100%) |
| Righe Iceberg identiche nella nativa per chiave, `id_mcvis` e `partition_date` | 5.724 (100%) |

La Iceberg è una copia del 2026-10-13 già presente nella nativa, non dati nuovi.

## Distribuzione rispetto a un cutover al 2026-10-13

| Parte | Righe nativa |
|---|---|
| Prima del 2026-10-13 | 2.769.254 |
| Il 2026-10-13 | 7.104 |
| Dopo il 2026-10-13 | 200.072 |

Il lato nativo continua a ricevere dati fino al 2026-11-13. Con il modello della specifica (nativa prima del cutover, Iceberg dopo) le 200.072 righe successive al cutover non avrebbero una sorgente che le contenga. Questo è un fatto dei dati, non un errore della vista: va discusso prima della Fase 2.

## Implicazioni per il piano

- Il cutover candidato è `2026-10-13`, il primo giorno presente nella Iceberg: è l'unico punto in cui la Iceberg contribuisce.
- Con quel cutover una `UNION ALL` semplice duplicherebbe 5.724 righe; la vista con filtri `<` / `>=` le evita.
- La continuità non è garantita: 200.072 righe nella finestra successiva al cutover sarebbero escluse. La Fase 2 misura questa perdita invece di nasconderla.
