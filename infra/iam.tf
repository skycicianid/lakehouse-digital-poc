# IAM a livello bucket per il service account Dataproc.
#
# Usiamo google_storage_bucket_iam_member (additivo): NON gestiamo il binding completo
# del bucket, quindi i binding legacy già presenti (projectOwner/projectEditor/projectViewer)
# non vengono toccati.
#
# Motivo: l'account che esegue Terraform non ha permessi per modificare l'IAM a livello
# di progetto (resourcemanager.projects.setIamPolicy assente), ma può modificare l'IAM dei
# bucket (storage.buckets.setIamPolicy presente). Vedi infra/README.md.

locals {
  dataproc_bucket_names = {
    lakehouse = google_storage_bucket.lakehouse.name
    catalog   = google_storage_bucket.catalog.name
  }
}

resource "google_storage_bucket_iam_member" "dataproc_object_admin" {
  for_each = local.dataproc_bucket_names

  bucket = each.value
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.dataproc_service_account_email}"
}
