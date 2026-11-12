# comandi

gcloud dataproc batches submit pyspark gs://dciciani-lakehouse/script/load_hitdata_iceberg.py \
    --project=skyita-da-architects-sandbox \
    --service-account=dataproc@skyita-da-architects-sandbox.iam.gserviceaccount.com \
    --region=europe-west1 \
    --subnet=skyita-da-architects-sandbox-test \
    --version=2.3 \
    --properties="dataproc.lakehouse.catalog.dciciani-lakehouse=projects/skyita-da-architects-sandbox/catalogs/dciciani-lakehouse"
    

gcloud dataproc batches submit pyspark \
  gs://dciciani-lakehouse/script/load_hitdata_iceberg.py \
  --project=skyita-da-architects-sandbox \
  --service-account=dataproc@skyita-da-architects-sandbox.iam.gserviceaccount.com \
  --region=europe-west1 \
  --subnet=skyita-da-architects-sandbox-test \
  --version=2.3 \
  --properties="dataproc.lakehouse.catalog.dciciani-lakehouse=projects/skyita-da-architects-sandbox/catalogs/dciciani-lakehouse,spark.jars.packages=org.apache.spark:spark-avro_2.12:3.5.3"

###COMANDO FUNZIONANE => legge da Avro e scrive su Iceberg
gcloud dataproc batches submit pyspark gs://dciciani-lakehouse/script/load_hitdata_iceberg.py \
    --project=skyita-da-architects-sandbox \
    --service-account=dataproc@skyita-da-architects-sandbox.iam.gserviceaccount.com \
    --region=europe-west1 \
    --subnet=skyita-da-architects-sandbox-test \
    --version=2.3 \
    --properties="spark.jars.packages=org.apache.spark:spark-avro_2.13:3.5.3,dataproc.lakehouse.catalog.dciciani-lakehouse=projects/skyita-da-architects-sandbox/catalogs/dciciani-lakehouse"



gcloud storage cp ./Downloads/lakehouse/load_hitdata_iceberg.py gs://dciciani-lakehouse/script/load_hitdata_iceberg.py

