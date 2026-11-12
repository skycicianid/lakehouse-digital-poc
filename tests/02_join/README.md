# Test 02: JOIN nativa BigQuery + Iceberg

**Obiettivo.** Verificare che una join tra tabella nativa BigQuery e tabella Iceberg, sulla stessa chiave, restituisca le corrispondenze attese.

## Oggetti coinvolti

| Ruolo | Oggetto | Tipo | Location |
|---|---|---|---|
| Nativa | `skyita-da-cmp-test.digital_skyita_myskyapp.hit_data_stg` | Tabella BigQuery nativa | EU |
| Iceberg | `skyita-da-architects-sandbox.dciciani-lakehouse-catalog.digital-analytics-myskyapp.hit_data_stg` | Tabella Iceberg (catalog `gcs-bucket`) | EU |

Chiave di join: `(id_hit_high, id_hit_low)`, confrontata come `STRING`.
Giorno: `2026-10-13`, l'unico presente nella Iceberg.

## Query

File: [query.sql](query.sql)

## Verifiche preliminari (sola lettura)

| Verifica | Nativa (2026-10-13) | Iceberg |
|---|---|---|
| Righe | 7.104 | 5.724 |
| Chiavi distinte | 7.104 | 5.724 |
| Chiavi Iceberg con corrispondenza nella nativa | | 5.724 |

Le chiavi sono univoche su entrambi i lati, quindi la join non può moltiplicare le righe.

## Esito

| Risultato | Valore |
|---|---|
| Righe della join | 5.724 |
| Righe Iceberg | 5.724 |

Ogni riga Iceberg trova esattamente una riga nativa: join 1:1.

**Esito: PASS.** La join tra le due tabelle funziona in una sola query BigQuery, in location EU.

## Limiti

- Il test verifica che le chiavi corrispondano, non che i valori delle colonne siano uguali: è il test 03.
- La nativa contiene 7.104 righe per quel giorno e la Iceberg 5.724: 1.380 righe della nativa non hanno corrispondenza nel bronze. Non ho verificato il motivo.
- La chiave è composta da due colonne: non ho verificato che sia unica nella tabella intera, solo nel giorno.

## Data esecuzione

2026-11-12, query lanciate con `bq query --location=EU` in sola lettura.
