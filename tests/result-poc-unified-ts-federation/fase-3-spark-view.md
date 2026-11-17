# Fase 3 – Federazione Spark (vista persistente cross-source)

Data: 2026-11-13. Written for: team del progetto e reviewer della POC.

**Obiettivo (poc.md §9):** esporre da Spark un oggetto logico persistente che combini BigQuery Native e Iceberg, senza codice di federazione nel consumer.

**Esito: non superata.** Spark crea e legge viste persistenti nel catalog Iceberg, ma non può riferire la tabella BigQuery Native in una vista. Il criterio della specifica («se il meccanismo non funziona, la soluzione target Spark non è soddisfatta», OBS-01) è verificato negativamente.

## Passi eseguiti

Batch `poc-f3-view-20261113-141534`, **SUCCEEDED**. Script: [spark/fase3_persistent_view.py](spark/fase3_persistent_view.py). Catalog `dciciani-lakehouse-catalog`, namespace di test `poc_fase3`.

| Passo spec | Prova | Esito |
|---|---|---|
| 2, registrazione persistente | `CREATE NAMESPACE` nel catalog | OK |
| 2, registrazione persistente | `CREATE VIEW` su sola Iceberg (`v_ctrl_iceberg`) | OK |
| 2, registrazione persistente | `SELECT COUNT(*)` sulla vista Iceberg | OK: 5.724 righe |
| 3, vista cross-source | `CREATE VIEW` con UNION ALL nativa (nome a tre parti) + Iceberg, cutover 2026-10-13 | **ERRORE** `[REQUIRES_SINGLE_PART_NAMESPACE] spark_catalog requires a single-part namespace, but got skyita-da-cmp-test.digital_skyita_myskyapp` |
| 4, interrogazione da nuova sessione | `SELECT` sulla vista cross-source | Non eseguibile: la vista non esiste (`TABLE_OR_VIEW_NOT_FOUND`) |
| 4–5, nuova sessione e cluster | Batch `poc-f3-persist-20261113-141825`, **SUCCEEDED**: `SELECT COUNT(*)` su `v_ctrl_iceberg` | OK: 5.724 righe (vista solo-Iceberg) |

Nota: la lista colonne della vista cross-source è esplicita nell'ordine della nativa. Il fallimento avviene in fase di analisi SQL, prima della lettura, quindi non dipende dai permessi della nativa.

## Perché il riferimento alla nativa non funziona

- Spark SQL risolve i nomi tramite i cataloghi registrati. Il nome a tre parti della nativa viene interpretato come namespace di `spark_catalog`, che accetta solo nomi a una parte.
- Il connettore BigQuery per Spark (immagine Dataproc 2.3) è usato come lettura di DataFrame nel job di Fase 1. In questa fase non ho verificato in modo separato se esista un catalogo BigQuery registrabile: l'unica prova è il fallimento della risoluzione SQL sopra.
- Il Lakehouse runtime catalog accetta viste Iceberg (passo S1–S2). Non ho verificato la sua configurazione per fonti BigQuery.

## Non eseguito

- **Passo 4 completo** (vista cross-source da nuova sessione): la vista non è stata creata.
- **Passo 5** (altro cluster Dataproc): da fare con la vista solo-Iceberg, vedi sotto.
- **Passo 6–7** (esecuzione nativa e piani): non eseguiti, senza una vista cross-source non hanno oggetto.
- **Dataproc Metastore / Hive Metastore** (candidato della specifica §5.1, §7): non testato, su richiesta. Escluso per ragionamento: il metastore registra nomi, ma la risoluzione dei riferimenti BigQuery non è disponibile in Spark SQL (vedi sopra). Nessun servizio è stato creato. La specifica chiede "validazione o esclusione tecnica": questa è un'esclusione basata sul ragionamento, non su una prova.

## Persistenza della vista solo-Iceberg

Confermata. La vista `v_ctrl_iceberg` creata nel batch `poc-f3-view-20261113-141534` è stata letta da un batch separato (`poc-f3-persist-20261113-141825`): sessione Spark nuova, cluster Dataproc Serverless nuovo, nessuno stato in memoria. Risultato: 5.724 righe.

Limite: il batch è serverless, quindi è un cluster diverso ma non un riavvio di un cluster persistente. Il test di riavvio della Fase 5 non è stato eseguito.

## Esito per criterio

| Criterio | Esito |
|---|---|
| Vista Spark persistente sola Iceberg | PASS |
| Vista Spark persistente cross-source (BQ nativa + Iceberg) | **FAIL** |
| Lettura da nuova sessione e altro cluster (vista sola Iceberg) | PASS |
| Vista cross-source letta da nuova sessione | Non eseguibile (vista non creata) |
| Esecuzione nativa e piani di esecuzione | Non eseguiti |

## Script

- [spark/fase3_persistent_view.py](spark/fase3_persistent_view.py)
- [spark/fase3_query_persisted_view.py](spark/fase3_query_persisted_view.py)
