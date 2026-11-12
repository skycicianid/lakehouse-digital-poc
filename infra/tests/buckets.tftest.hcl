# Test hermetico: provider mockato, nessuna chiamata a GCP.
# Verifica che la configurazione dichiari gli attributi attesi.
# Esecuzione: cd infra && terraform init -backend=false && terraform test

mock_provider "google" {}

# I bucket sono importati (blocchi import in main.tf): con il mock vanno sovrascritti,
# altrimenti terraform rifiuta l'import da provider mockato.
override_resource {
  target = google_storage_bucket.lakehouse
}

override_resource {
  target = google_storage_bucket.catalog
}

run "bucket_dati_attributi" {
  command = plan

  assert {
    condition     = google_storage_bucket.lakehouse.location == "EU"
    error_message = "Il bucket dati deve stare in location EU."
  }

  assert {
    condition     = google_storage_bucket.lakehouse.uniform_bucket_level_access == true
    error_message = "Uniform bucket-level access deve essere attivo."
  }

  assert {
    condition     = google_storage_bucket.lakehouse.public_access_prevention == "enforced"
    error_message = "La prevenzione dell'accesso pubblico deve essere enforced."
  }

  assert {
    condition     = google_storage_bucket.lakehouse.force_destroy == false
    error_message = "force_destroy deve essere false: un destroy non deve svuotare il bucket."
  }
}

run "bucket_catalog_attributi" {
  command = plan

  assert {
    condition     = google_storage_bucket.catalog.location == "EU"
    error_message = "Il bucket catalog deve stare in location EU."
  }

  assert {
    condition     = google_storage_bucket.catalog.public_access_prevention == "enforced"
    error_message = "La prevenzione dell'accesso pubblico deve essere enforced sul bucket catalog."
  }

  assert {
    condition     = google_storage_bucket.catalog.force_destroy == false
    error_message = "force_destroy deve essere false sul bucket catalog."
  }
}

run "iam_dataproc_additivo" {
  command = plan

  assert {
    condition     = google_storage_bucket_iam_member.dataproc_object_admin["lakehouse"].role == "roles/storage.objectAdmin"
    error_message = "Il service account Dataproc deve avere objectAdmin sul bucket dati."
  }

  assert {
    condition     = google_storage_bucket_iam_member.dataproc_object_admin["catalog"].role == "roles/storage.objectAdmin"
    error_message = "Il service account Dataproc deve avere objectAdmin sul bucket catalog."
  }

  assert {
    condition     = google_storage_bucket_iam_member.dataproc_object_admin["lakehouse"].member == "serviceAccount:dataproc@skyita-da-architects-sandbox.iam.gserviceaccount.com"
    error_message = "Il member IAM deve essere il service account Dataproc del progetto."
  }
}
