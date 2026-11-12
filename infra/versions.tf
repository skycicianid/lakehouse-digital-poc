terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.0"
    }
  }

  # State su GCS. Il bucket di state è creato a mano (bootstrap) e NON è gestito da questo modulo.
  backend "gcs" {
    bucket = "dciciani-lakehouse-tfstate"
    prefix = "lakehouse-poc"
  }
}
