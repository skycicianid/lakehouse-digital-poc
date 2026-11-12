# Infra Setup commands

## Create an Artifact Registry
gcloud artifacts repositories create lakehouse-ar  --repository-format=docker   --location
=europe-west1


## Create a new Pub/Sub topic
gcloud pubsub topics create topicTest


## Build Container
gcloud builds submit --tag europe-west1-docker.pkg.dev/skyita-da-architects-sandbox/lakehouse-ar/pubsub

## Deploy the App
gcloud run deploy pubsub-tutorial --image REGION-docker.pkg.dev/PROJECT_ID/REPOSITORY/pubsub  --no-allow-unauthenticated

# Integrate with Pub Sub
## Create a SA to rappresent Pub/Sub sub identity
gcloud iam service-accounts create cloud-run-pubsub-invoker \
    --display-name "Cloud Run Pub/Sub Invoker"

## Create a Pub/Sub subscription
gcloud run services add-iam-policy-binding pubsub-tutorial \
--member=serviceAccount:cloud-run-pubsub-invoker@PROJECT_ID.iam.gserviceaccount.com \
--role=roles/run.invoker

# Useful Links
- https://docs.cloud.google.com/run/docs/tutorials/pubsub#run_pubsub_server-python

