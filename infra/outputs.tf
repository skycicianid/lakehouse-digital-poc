output "lakehouse_bucket" {
  description = "Nome del bucket dati."
  value       = google_storage_bucket.lakehouse.name
}

output "catalog_bucket" {
  description = "Nome del bucket warehouse del catalog."
  value       = google_storage_bucket.catalog.name
}

output "dataproc_bucket_iam_members" {
  description = "Bucket su cui il service account Dataproc ha objectAdmin."
  value       = keys(local.dataproc_bucket_names)
}
