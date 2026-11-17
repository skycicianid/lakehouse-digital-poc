# POC – Unified Time Series Federation
## Accesso trasparente a BigQuery Native e Apache Iceberg

**Ambito:** Google Cloud Data Platform / Lakehouse Architecture  
**Stato:** POC Proposal  
**Motori di consumo:** Apache Spark su Dataproc, BigQuery Engine  
**Principio architetturale:** Engine-Native Federation with Shared Data Contract

---

## 1. Executive Summary

La POC ha l'obiettivo di verificare la possibilità di esporre serie temporali distribuite tra BigQuery Native e Apache Iceberg attraverso oggetti logici persistenti, senza migrare o duplicare permanentemente i dati.

Lo scenario prevede che lo storico delle serie temporali rimanga fisicamente in BigQuery Native, mentre i nuovi dati vengano prodotti e conservati in Apache Iceberg a partire da una data di cutover.

Sono previsti due motori di consumo:

- Apache Spark su Google Cloud Dataproc.
- Google BigQuery Engine.

Ogni motore deve poter interrogare un oggetto logico persistente che nasconda la distribuzione fisica dei dati.

Non è richiesto che i due motori utilizzino lo stesso catalogo, la stessa vista o lo stesso percorso di esecuzione.

Al contrario, l'architettura deve privilegiare la soluzione più efficiente e nativa per ciascun motore.

**L'ipotesi architetturale da verificare è che sia possibile ottenere la trasparenza logica senza introdurre un motore di federazione aggiuntivo e senza spostare lo storico BigQuery.**

La principale incertezza tecnica riguarda il supporto di Spark e del catalogo scelto alla registrazione persistente di una vista cross-source che combini BigQuery Native e Apache Iceberg.

## 2. Business Context

L'evoluzione della Data Platform prevede una transizione progressiva verso un'architettura Lakehouse basata su Apache Iceberg.

Le serie temporali esistenti sono attualmente conservate in tabelle BigQuery Native.

Per evitare una migrazione massiva dello storico, si intende adottare una strategia di separazione temporale:

- Dati precedenti al cutover: BigQuery Native.
- Dati dal cutover in avanti: Apache Iceberg.

La transizione non deve comportare modifiche alla logica di elaborazione dei consumer.

I consumer devono continuare a interrogare un singolo oggetto logico, indipendentemente dallo storage sottostante.

## 3. Requisiti funzionali

**FR-01 – Accesso unificato**

Ogni consumer deve poter interrogare l'intera serie temporale mediante un unico oggetto logico.

**FR-02 – Trasparenza dello storage**

Il consumer non deve conoscere la collocazione fisica dei dati né implementare logiche di selezione delle sorgenti.

**FR-03 – Nessuna migrazione dello storico**

I dati storici devono rimanere nelle tabelle BigQuery Native.

**FR-04 – Supporto Apache Iceberg**

I nuovi dati devono essere letti direttamente dalle tabelle Iceberg residenti su Cloud Storage.

**FR-05 – Federazione indipendente per engine**

Spark e BigQuery devono utilizzare implementazioni autonome, ottimizzate per le rispettive capacità.

**FR-06 – Oggetti persistenti**

La logica di federazione deve essere registrata e amministrata centralmente, senza essere ricreata dal codice di ogni consumer.

**FR-07 – Accesso SQL**

Entrambi i motori devono supportare interrogazioni SQL sull'oggetto logico.

**FR-08 – Coerenza dei risultati**

Le interrogazioni equivalenti devono restituire risultati semanticamente coerenti nei due engine.

## 4. Requisiti non funzionali

| Area | Requisito |
|---|---|
| Performance | Evitare letture non necessarie delle sorgenti |
| Scalabilità | Supportare elaborazioni distribuite Spark e query analitiche BigQuery |
| Costi | Evitare duplicazioni permanenti e minimizzare trasferimenti |
| Governance | Preservare autorizzazioni, policy e audit |
| Manutenibilità | Centralizzare le definizioni logiche |
| Affidabilità | Garantire continuità delle serie temporali |
| Evoluzione | Supportare modifiche controllate degli schemi |
| Portabilità | Non vincolare Spark all'esecuzione tramite BigQuery Engine |

## 5. Architettura target

L'architettura prevede due livelli di serving indipendenti.

### 5.1 Spark Serving Layer

Il consumer Spark deve poter interrogare un oggetto persistente, ad esempio:

```python
df = spark.table("enterprise.sales_unified")
```

La federazione viene eseguita da Spark attraverso:

- Spark BigQuery Connector per lo storico BigQuery Native.
- Apache Iceberg Connector per i nuovi dati.
- Unione logica dei dataset con filtri temporali.
- Catalogo Spark con supporto a oggetti persistenti.

Non deve essere necessario implementare `UNION ALL`, creare DataFrame intermedi o registrare viste temporanee nel codice del consumer.

**Il candidato iniziale è Dataproc Metastore con una vista Spark persistente.**

La fattibilità deve essere verificata, poiché la persistenza di una vista cross-catalog e la risoluzione SQL delle sorgenti BigQuery non sono capacità da assumere automaticamente.

### 5.2 BigQuery Serving Layer

Il consumer BigQuery deve poter interrogare una vista persistente, ad esempio:

```sql
SELECT *
FROM `project.serving.sales_unified`;
```

La vista deve combinare:

- Tabella BigQuery Native contenente lo storico.
- Tabella Apache Iceberg accessibile al BigQuery Engine.

La federazione viene eseguita interamente da BigQuery attraverso una Logical View.

Questo percorso non deve dipendere da Spark, Dataproc o da un motore SQL esterno.

### 5.3 Shared Data Contract

Le due implementazioni devono condividere esclusivamente il contratto semantico e le regole di governo dei dati.

Il contratto include:

- Schema canonico.
- Identificativo logico del dataset.
- Data e semantica del cutover.
- Tipi e precisione dei campi.
- Regole di gestione dei valori null.
- Chiavi logiche e granularità.
- Policy per dati tardivi e correzioni retroattive.
- Versionamento e compatibilità.
- Regole di data quality.

Le definizioni SQL possono essere differenti e ottimizzate separatamente.

## 6. Strategia di federazione temporale

La federazione si basa su un cutover deterministico.

Esempio:

| Intervallo temporale | Storage |
|---|---|
| Prima del 12 novembre 2026 | BigQuery Native |
| Dal 12 novembre 2026 | Apache Iceberg |

La logica concettuale è:

```sql
SELECT
  customer_id,
  event_date,
  amount
FROM historical_data
WHERE event_date < DATE '2026-11-12'

UNION ALL

SELECT
  customer_id,
  event_date,
  amount
FROM current_data
WHERE event_date >= DATE '2026-11-12';
```

La data è puramente illustrativa.

La POC dovrà verificare che la federazione garantisca continuità temporale e assenza di duplicazioni.

Il cutover dovrà essere definito su un attributo temporale preciso, distinguendo event time, ingestion time e partition time.

## 7. Soluzioni tecnologiche candidate

### Spark / Dataproc

| Componente | Tecnologia candidata | Ruolo |
|---|---|---|
| Compute | Apache Spark su Dataproc | Esecuzione distribuita |
| Storico | Spark BigQuery Connector | Accesso BigQuery Native |
| Nuovi dati | Apache Iceberg Spark Connector | Lettura Iceberg |
| Catalogo Iceberg | Lakehouse runtime catalog / REST Catalog compatibile | Discovery delle tabelle |
| Vista persistente | Dataproc Metastore / Hive Metastore | Registrazione logica cross-source |
| Alternativa | Catalogo con supporto Iceberg View | Vista persistente, se compatibile |

Il Dataproc Metastore rappresenta un candidato da verificare, non una soluzione già approvata.

La POC deve accertare che il catalogo possa persistere e risolvere correttamente riferimenti a sorgenti eterogenee.

### BigQuery Engine

| Componente | Tecnologia candidata | Ruolo |
|---|---|---|
| Compute | BigQuery Engine | Esecuzione SQL |
| Storico | BigQuery Native Table | Lettura dello storico |
| Nuovi dati | Iceberg Table accessibile a BigQuery | Lettura dei nuovi dati |
| Catalogo | Lakehouse runtime catalog, se compatibile | Accesso alle tabelle Iceberg |
| Vista persistente | BigQuery Logical View | Federazione SQL |

Per il percorso BigQuery, la POC dovrà verificare la specifica integrazione Iceberg disponibile nell'ambiente, inclusi catalogo, formato delle tabelle e funzionalità SQL supportate.

## 8. Principali osservazioni architetturali

### OBS-01 – Persistenza della vista Spark

**Priorità: critica**

Spark supporta le viste SQL persistenti, ma questo non garantisce automaticamente che una vista possa referenziare sorgenti appartenenti a cataloghi e connettori differenti.

È necessario verificare:

- Supporto del catalogo alle viste persistenti.
- Registrazione delle tabelle BigQuery come relazioni SQL persistenti.
- Risoluzione delle tabelle Iceberg.
- Persistenza dopo riavvio dei cluster.
- Accessibilità da sessioni Spark indipendenti.

Se il meccanismo non funziona, la soluzione target Spark non è soddisfatta.

### OBS-02 – Limiti del Lakehouse runtime catalog

**Priorità: alta**

Il Lakehouse runtime catalog non deve essere considerato automaticamente un catalogo universale di viste SQL.

Il supporto alle tabelle e il supporto alle viste sono funzionalità distinte.

La POC deve verificare esplicitamente quali tipi di oggetti possono essere registrati e interrogati.

### OBS-03 – Ottimizzazione delle query

**Priorità: alta**

Le query devono evitare di leggere entrambe le sorgenti quando l'intervallo temporale richiesto interessa esclusivamente una di esse.

Devono essere analizzati:

- Predicate pushdown.
- Partition pruning.
- Column pruning.
- Query planning.
- Parallelismo delle letture.
- Costi di accesso alle sorgenti.

### OBS-04 – Sicurezza e governance

**Priorità: alta**

La presenza di un oggetto logico non deve comportare un bypass delle autorizzazioni sulle sorgenti.

Occorre verificare il modello di identità, i permessi sui cataloghi, l'accesso allo storage, le policy di sicurezza e i meccanismi di auditing.

## 9. Piano della POC

### Fase 1 – Setup e interoperabilità

**Obiettivo:** dimostrare che entrambi gli engine possano leggere le sorgenti.

Attività:

1. Identificare una tabella BigQuery Native di test.
2. Creare o individuare una tabella Iceberg compatibile.
3. Configurare Spark su Dataproc.
4. Configurare i connettori BigQuery e Iceberg.
5. Verificare la lettura delle due sorgenti da Spark.
6. Verificare la lettura delle due sorgenti da BigQuery Engine.

**Output:** matrice di compatibilità delle sorgenti e dei cataloghi.

### Fase 2 – Federazione BigQuery

**Obiettivo:** realizzare un oggetto logico persistente nel BigQuery Engine.

Attività:

1. Creare una BigQuery Logical View.
2. Implementare il cutover temporale.
3. Validare lo schema canonico.
4. Verificare correttezza dei risultati.
5. Analizzare query plan, pruning e costi.

**Output:** vista BigQuery funzionante e risultati dei test.

### Fase 3 – Federazione Spark

**Obiettivo:** esporre un oggetto logico persistente interrogabile da Spark senza codice applicativo di federazione.

Attività:

1. Configurare un catalogo Spark condiviso.
2. Verificare la registrazione persistente delle sorgenti.
3. Tentare la creazione di una vista cross-source.
4. Interrogare la vista da una nuova sessione Spark.
5. Ripetere il test da un cluster Dataproc differente.
6. Verificare l'esecuzione nativa in Spark.
7. Analizzare i piani di esecuzione.

**Output:** validazione o esclusione tecnica della soluzione basata su Dataproc Metastore.

### Fase 4 – Benchmark

**Obiettivo:** misurare prestazioni e costi.

Scenari:

| Scenario | Intervallo | Comportamento atteso |
|---|---|---|
| Storico | Prima del cutover | Accesso prioritario a BigQuery |
| Corrente | Dopo il cutover | Accesso prioritario a Iceberg |
| Completo | Attraversa il cutover | Lettura di entrambe le sorgenti |
| Aggregazione | Intera serie temporale | Risultati coerenti tra engine |
| Schema evolution | Modifica compatibile | Continuità del servizio |

Metriche:

- Tempo totale di esecuzione.
- Byte letti per sorgente.
- Dati trasferiti.
- Numero di partizioni lette.
- Costi BigQuery.
- Risorse Spark utilizzate.
- Eventuali materializzazioni intermedie.
- Correttezza e completezza dei risultati.

### Fase 5 – Governance e resilienza

**Obiettivo:** verificare il comportamento enterprise.

Attività:

1. Test IAM con identità differenti.
2. Verifica dei permessi sulle sorgenti.
3. Test di riavvio dei cluster.
4. Test di indisponibilità di una sorgente.
5. Verifica delle modifiche allo schema.
6. Test di dati tardivi e sovrapposizioni temporali.

**Output:** valutazione dei rischi residui e requisiti di hardening.

## 10. Criteri di successo

La POC sarà considerata riuscita quando:

**Spark**
- Interroga un singolo oggetto persistente.
- Non contiene logica applicativa di federazione.
- Non utilizza BigQuery come motore intermedio di federazione.
- Accede alle sorgenti attraverso i rispettivi connettori.
- Mantiene il comportamento dopo il riavvio dei cluster.
- Dimostra pruning e performance compatibili con il workload.

**BigQuery**
- Interroga una singola Logical View.
- Accede sia alle tabelle native sia alle tabelle Iceberg.
- Non richiede Spark o altri motori di federazione.
- Mantiene correttezza e compatibilità dello schema.
- Dimostra performance e costi accettabili.

**Trasversali**
- Nessuna migrazione permanente dello storico.
- Nessuna duplicazione permanente dei dati per la federazione.
- Risultati equivalenti tra engine.
- Cutover coerente.
- Sicurezza e governance verificabili.

Le soglie quantitative di performance e costo dovranno essere concordate prima dell'esecuzione dei benchmark.

## 11. Rischi e decisioni aperte

| ID | Rischio / decisione | Priorità |
|---|---|---|
| R01 | Persistenza della vista Spark cross-source | Critica |
| R02 | Compatibilità delle relazioni BigQuery con Spark SQL e Metastore | Critica |
| R03 | Supporto effettivo delle tabelle Iceberg da BigQuery | Alta |
| R04 | Mancato partition pruning | Alta |
| R05 | Coerenza delle autorizzazioni | Alta |
| R06 | Costi di lettura e trasferimento | Media |

In caso di esito negativo sulla persistenza delle viste Spark, dovrà essere valutato un catalogo alternativo che supporti effettivamente le viste cross-source.

Un modulo applicativo di federazione non è considerato equivalente al requisito target, poiché reintrodurrebbe dipendenze nel consumer.

## 12. Decisione architetturale proposta

Adottare il principio:

**Engine-Native Federation with Shared Data Contract**

L'architettura deve prevedere:

- Due serving layer indipendenti.
- Un oggetto logico persistente per ciascun engine.
- Accesso diretto alle sorgenti fisiche.
- Nessuna migrazione dello storico BigQuery.
- Nessun motore SQL intermedio obbligatorio.
- Un contratto semantico condiviso.
- Ottimizzazioni specifiche per Spark e BigQuery.

**La POC dovrà stabilire se l'ecosistema Dataproc/Spark e i cataloghi disponibili consentano di soddisfare il requisito di persistenza della federazione senza compromessi sul modello di consumo.**

L'approvazione dell'architettura definitiva sarà subordinata ai risultati della POC, con particolare attenzione alla fattibilità delle viste Spark cross-source, alla correttezza temporale e alle prestazioni dei due engine