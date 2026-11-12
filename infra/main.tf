# Bucket dati e warehouse del catalog. Esistono già in GCP: sono adottati con import
# (blocchi import sotto), non ricreati. Gli attributi sono quelli reali verificati
# con l'API Storage, così il primo plan non propone modifiche.

import {
  to = google_storage_bucket.lakehouse
  id = var.lakehouse_bucket_name
}

import {
  to = google_storage_bucket.catalog
  id = var.catalog_bucket_name
}

resource "google_storage_bucket" "lakehouse" {
  name          = var.lakehouse_bucket_name
  location      = var.location
  storage_class = "STANDARD"
  force_destroy = false

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  lifecycle {
    prevent_destroy = true
    # Blocco encryption senza CMEK (default Google): il provider lo riporta nello stato
    # ma non è configurabile con null. Non è un drift reale.
    ignore_changes = [encryption]
  }
}

resource "google_storage_bucket" "catalog" {
  name          = var.catalog_bucket_name
  location      = var.location
  storage_class = "STANDARD"
  force_destroy = false

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  lifecycle {
    prevent_destroy = true
    # Blocco encryption senza CMEK (default Google): il provider lo riporta nello stato
    # ma non è configurabile con null. Non è un drift reale.
    ignore_changes = [encryption]
  }
}
