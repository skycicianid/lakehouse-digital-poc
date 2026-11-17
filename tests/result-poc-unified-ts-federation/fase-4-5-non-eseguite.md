# Fase 4 (Benchmark) e Fase 5 (Governance e resilienza)

Data: 2026-11-13. Written for: team del progetto e reviewer della POC.

## Fase 4 – Benchmark

**Esito: non eseguita.**

Motivi:
- La specifica (§10) chiede di concordare le soglie prima dei benchmark. Non sono state concordate.
- Gli scenari della specifica (storico, corrente, completo, aggregazione) dipendono da una vista Spark cross-source, che non esiste (Fase 3). Per la sola vista BigQuery (Fase 2) lo scenario "completo" è misurabile, ma il risultato di continuità è negativo: un benchmark su un risultato sbagliato non è significativo.
- Dato già disponibile come dry-run della Fase 2: pruning corretto per il filtro post-cutover (0 B contro 1,6 MB della sola nativa). Non è un benchmark completo: manca il tempo di esecuzione e il costo reale.

Precondizioni: soglie concordate; continuità della vista risolta (Fase 2); decisione sulla Fase 3.

## Fase 5 – Governance e resilienza

**Esito: non eseguita, con evidenze parziali.**

Evidenze raccolte:
- **IAM (R05)**: il service account Dataproc `dataproc@skyita-da-architects-sandbox.iam.gserviceaccount.com` riceve `Permission bigquery.tables.get denied` sulla nativa `skyita-da-cmp-test.digital_skyita_myskyapp.hit_data_stg` (batch `poc-f1-read-20261113-141236`, log del driver). Le stesse tabelle sono leggibili dal mio account utente. Quindi l'identità determina l'accesso alla stessa sorgente.
- **Permesso sulla vista BigQuery**: la vista `poc_unified_ts.sales_unified` non è stata letta con un'identità diversa dalla mia, quindi non si sa se il lettore della vista debba avere anche accesso alle sorgenti. Da verificare (OBS-04).
- **Persistenza tra sessioni**: la vista Spark solo-Iceberg è leggibile da un batch separato (vedi Fase 3). Non è un test di riavvio di un cluster persistente.

Non eseguiti:
- Test IAM con identità differenti (serve un secondo account o service account).
- Test di riavvio di cluster persistenti (i batch Serverless non lo prevedono).
- Test di indisponibilità di una sorgente.
- Verifica delle modifiche di schema.
- Test di dati tardivi e sovrapposizioni temporali (la sovrapposizione del 2026-10-13 è documentata in Fase 2).

Precondizioni: un secondo service account di test; accesso alla nativa per il service account Dataproc (decisione 1 del piano); un cluster persistente se si vuole il test di riavvio.
