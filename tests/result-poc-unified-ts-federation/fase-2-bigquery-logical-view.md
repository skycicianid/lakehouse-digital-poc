# Fase 2 – Federazione BigQuery (Logical View con cutover)

Data: 2026-11-13. Written for: team del progetto e reviewer della POC.

**Obiettivo (poc.md §9):** realizzare un oggetto logico persistente nel BigQuery Engine, con cutover temporale.

**Esito: funziona e misura la continuità, ma la continuità non è garantita con i dati attuali.** La vista è corretta rispetto alla sua definizione, non perde né duplica righe *della Iceberg*, ma non contiene le 201.452 righe della nativa che la definizione esclude.

## Oggetto creato

- Vista: `skyita-da-architects-sandbox.poc_unified_ts.sales_unified` (dataset nuovo, EU).
- Sorgenti: nativa `skyita-da-cmp-test.digital_skyita_myskyapp.hit_data_stg` (prima del cutover) e Iceberg `skyita-da-architects-sandbox.dciciani-lakehouse-catalog.digital-analytics-myskyapp.hit_data_stg` (dal cutover).
- Cutover: `partition_date` (event time, scelto come proxy: vedi limiti), data `2026-10-13 00:00:00`, primo giorno presente nella Iceberg.
- Colonne: 167, lista esplicita nello stesso ordine nei due rami (le tabelle hanno stessi nomi e tipi ma ordine diverso).
- SQL: riportato sotto (generato da una lista di colonne ricavata dallo schema della nativa).

```sql
CREATE OR REPLACE VIEW `skyita-da-architects-sandbox.poc_unified_ts.sales_unified` AS
SELECT <167 colonne> FROM `skyita-da-cmp-test.digital_skyita_myskyapp.hit_data_stg`
WHERE partition_date < TIMESTAMP('2026-10-13 00:00:00')
UNION ALL
SELECT <167 colonne> FROM `skyita-da-architects-sandbox.dciciani-lakehouse-catalog.digital-analytics-myskyapp.hit_data_stg`
WHERE partition_date >= TIMESTAMP('2026-10-13 00:00:00');
```

## Risultati

| Verifica | Atteso | Ottenuto | Esito |
|---|---|---|---|
| Righe vista = righe nativa prima del cutover + righe Iceberg | 2.769.254 + 5.724 = 2.774.978 | 2.774.978 | PASS |
| Righe dal cutover in poi nella vista = righe Iceberg | 5.724 | 5.724 | PASS |
| Duplicati aggiunti dalla vista rispetto alla nativa (righe − chiavi distinte) | 124.094 (quelli già nella nativa) | 124.094 (2.774.978 − 2.650.884) | PASS: nessun duplicato introdotto |
| Schema della vista | 167 colonne | 167 colonne | PASS |
| Continuità: nessuna riga della nativa esclusa | 2.976.430 | 2.774.978 | **FAIL**: 201.452 righe mancanti |

### Dettaglio della perdita

| Componente | Righe |
|---|---|
| Nativa completa | 2.976.430 |
| Nativa prima del cutover (inclusa) | 2.769.254 |
| Nativa il 2026-10-13 (esclusa dal filtro `<`) | 7.104 |
| Nativa dal 2026-10-14 in poi (esclusa dal filtro, nessuna sorgente Iceberg) | 200.072 |
| Iceberg il 2026-10-13 (inclusa) | 5.724 |

Righe nativa mancanti dalla vista: 7.104 + 200.072 − 5.724 = **201.452**.

Le 5.724 righe Iceberg del 2026-10-13 sono identiche a righe della nativa per chiave `(id_hit_high, id_hit_low)`, `id_mcvis` e `partition_date`: la Iceberg è una copia parziale di quel giorno (5.724 su 7.104 righe native). Il lato nativo continua a ricevere dati fino al 2026-11-13, quindi il cutover non è ancora un passaggio reale.

### Pruning (dry-run, byte stimati)

| Filtro | Vista | Sola nativa | Nota |
|---|---|---|---|
| `partition_date >= 2026-10-14` | 0 B | 1.600.576 B | La vista elimina il ramo nativo grazie al cutover |
| `partition_date < 2026-10-01` | 21.458.336 B | 21.458.336 B | Stessa lettura: la vista non aggiunge scansioni |

La Iceberg non riporta byte nel dry-run (metadati), quindi non è confrontabile.

## Limiti

- La chiave `(id_hit_high, id_hit_low)` non è unica nella nativa: 124.094 duplicati già presenti nella sorgente. Non è un difetto della vista, ma va risolto nello shared data contract (§5.3) prima di usare la vista per conteggi.
- `partition_date` è usato come event time, ma non è stato verificato che rappresenti l'event time reale. La specifica (§6) chiede di distinguere event time, ingestion time e partition time.
- Il cutover è stato scelto sul primo giorno della Iceberg. Con dati reali il cutover sarà definito dal contratto.
- Le colonne non sono castate né confrontate sui valori: il confronto di valori è fuori da questa fase.
- Non eseguite le analisi di query plan e costo oltre al dry-run.

## Cosa serve per una continuità valida

Una delle due:
1. La nativa smette di ricevere dati a una data di cutover e la Iceberg contiene tutti i giorni successivi (è il modello della specifica, oggi non vero).
2. Il contratto dichiara che la nativa resta sorgente per il suo intervallo completo e la Iceberg solo per i giorni che non ha; questo cambierebbe la definizione della vista.
