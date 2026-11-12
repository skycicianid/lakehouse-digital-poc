# infra: bucket e IAM del lakehouse (Terraform)

Questo modulo descrive in Terraform i bucket del lakehouse e i binding IAM del service account Dataproc, così l'ambiente è riproducibile e versionato.

## Stato attuale

| Risorsa | Stato | Note |
|---|---|---|
| Bucket `dciciani-lakehouse` | Da importare | Attributi allineati a quelli reali: nessuna modifica prevista |
| Bucket `dciciani-lakehouse-catalog` | Da importare | Warehouse del catalog Iceberg |
| Binding `roles/storage.objectAdmin` per Dataproc SA su entrambi i bucket | **Da applicare** | Additivo, non ancora applicato (vedi sotto) |
| Bucket `dciciani-lakehouse-tfstate` | Creato a mano (bootstrap) | Contiene lo state, non è gestito da questo modulo |
| Catalog Iceberg `dciciani-lakehouse-catalog` | **Non gestito** | Vedi [catalog.tf](catalog.tf) |

## Struttura

- [versions.tf](versions.tf): provider Google `~> 6.0` e backend GCS.
- [providers.tf](providers.tf): configurazione del provider.
- [variables.tf](variables.tf): nomi, location e service account. I default corrispondono all'ambiente attuale.
- [main.tf](main.tf): blocchi `import` e bucket dati e catalog.
- [iam.tf](iam.tf): binding IAM additivi per il service account Dataproc.
- [catalog.tf](catalog.tf): motivi per cui il catalog non è ancora gestito.
- [outputs.tf](outputs.tf).
- [tests/buckets.tftest.hcl](tests/buckets.tftest.hcl): test hermetici con provider mockato.
- [scripts/verify.sh](scripts/verify.sh): verifica read-only dell'ambiente reale.

## Come eseguire

```bash
cd infra

# Test di configurazione (nessuna chiamata a GCP)
terraform init -backend=false
terraform test

# Credenziali: il backend GCS e il provider usano il token dell'account gcloud attivo
export GOOGLE_OAUTH_ACCESS_TOKEN="$(gcloud auth print-access-token)"
terraform init -reconfigure
terraform plan
terraform apply   # da eseguire con approvazione esplicita

# Verifica read-only dell'ambiente reale
./scripts/verify.sh
```

## Permessi: cosa si può e non si può fare

Verificato con l'account attivo `daniele.ciciani@skytv.it`:

- **Può** modificare l'IAM dei singoli bucket (`storage.buckets.setIamPolicy` presente). Per questo i binding Dataproc sono fatti a livello bucket.
- **Non può** modificare l'IAM di progetto (`resourcemanager.projects.setIamPolicy` assente). Non posso quindi concedere ruoli a livello progetto.
- **Non può** leggere il catalog Iceberg (`biglake.catalogs.get` negato in location `eu`). Non posso importarlo né confrontarlo.
- Le credenziali ADC di Terraform appartengono all'account personale `daniele.ciciani96@gmail.com`, che non ha accesso al bucket di state. Per questo il backend usa `GOOGLE_OAUTH_ACCESS_TOKEN` con l'account di lavoro.

## Scelte di configurazione

- **Nessun blocco `versioning` sui bucket dati**: il versioning è disattivato, come oggi. Il bucket di state invece ha versioning attivo.
- **`prevent_destroy = true`** su entrambi i bucket dati. Un `destroy` non deve cancellare lo storico.
- **`ignore_changes = [encryption]`**: il provider riporta un blocco `encryption` senza CMEK. Non è configurabile con `null` e non è un drift reale. Il bucket usa la chiave Google-managed.
- **IAM additivo**: si usa `google_storage_bucket_iam_member`, che non sovrascrive i binding legacy esistenti (`projectOwner`, `projectEditor`, `projectViewer`).

## Stato dei test

- `terraform fmt -check` e `terraform validate`: OK.
- `terraform test`: 3 test superati (attributi dei bucket, bucket catalog, IAM Dataproc).
- `scripts/verify.sh`: 12 controlli OK, 2 FAIL (binding Dataproc non ancora applicati), 1 non verificabile (catalog).

## Prossimi passi

1. Approvare e applicare `terraform apply`: importa i due bucket e aggiunge i due binding `objectAdmin`. Il plan previsto è `2 to import, 2 to add, 0 to change, 0 to destroy`.
2. Dopo l'apply, `scripts/verify.sh` deve dare 0 FAIL e `terraform plan -detailed-exitcode` deve uscire con 0.
3. Per il catalog: ottenere `biglake.catalogs.get` (o leggerlo con un account che lo ha), verificare lo schema di `google_biglake_catalog` nel registry e solo poi aggiungerlo con import.
