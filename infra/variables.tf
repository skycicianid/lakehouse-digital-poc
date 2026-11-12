variable "project_id" {
  description = "Progetto GCP."
  type        = string
  default     = "skyita-da-architects-sandbox"
}

variable "region" {
  description = "Regione di default del provider."
  type        = string
  default     = "europe-west1"
}

variable "location" {
  description = "Location multi-region dei bucket (EU)."
  type        = string
  default     = "EU"
}

variable "lakehouse_bucket_name" {
  description = "Bucket dei dati (sorgente e Iceberg). Esiste già: viene importato."
  type        = string
  default     = "dciciani-lakehouse"
}

variable "catalog_bucket_name" {
  description = "Bucket warehouse del catalog Iceberg (GCS bucket catalog). Esiste già: viene importato. Per i bucket di tipo catalog il nome del catalog coincide con quello del bucket."
  type        = string
  default     = "dciciani-lakehouse-catalog"
}

variable "dataproc_service_account_email" {
  description = "Service account che esegue i batch Dataproc."
  type        = string
  default     = "dataproc@skyita-da-architects-sandbox.iam.gserviceaccount.com"
}
