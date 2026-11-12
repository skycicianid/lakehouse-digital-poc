# Catalog Iceberg (CATALOG_TYPE_GCS_BUCKET) su bucket dciciani-lakehouse-catalog.
#
# STATO: NON gestito da Terraform in questa versione. Motivi verificati:
#   1. Il service account/utente che esegue Terraform riceve 403 su biglake.catalogs.get
#      per il catalog dciciani-lakehouse-catalog (location "eu"): non posso leggerne
#      la configurazione, quindi non posso importarlo né confrontarlo.
#   2. La documentazione della risorsa google_biglake_catalog (provider google-beta
#      v6.2.0) espone solo name, location e project. Non espone il tipo di catalog
#      (CATALOG_TYPE_GCS_BUCKET) né il bucket collegato, quindi non è verificabile che
#      basti a descrivere il catalog esistente.
#
# Prossimo passo: ottenere permesso biglake.catalogs.get (o leggere il catalog con un
# account che lo ha) e verificare nel registry lo schema di google_biglake_catalog.
# Solo allora aggiungere la risorsa con import.
#
# Il catalog è verificato a valle da infra/scripts/verify.sh, che segnala esplicitamente
# se la lettura è negata invece di darla per buona.
