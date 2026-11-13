param(
    [ValidateSet('build', 'create', 'verify')][string]$Mode = 'create',
    [string]$Namespace = 'federation_layered_20261113',
    [string]$Project = 'skyita-da-architects-sandbox',
    [string]$Region = 'europe-west1',
    [string]$Catalog = 'dciciani-lakehouse-catalog',
    [string]$ArtifactPrefix = 'gs://dciciani-lakehouse/digital-poc/script/catalog-fix-v3-20261113',
    [int]$Expected = -1,
    [switch]$Materialize,
    [string]$Native = 'skyita-da-architects-sandbox.dciciani.hit_data_stg_test',
    [string]$Cutover
)
$ErrorActionPreference = 'Stop'

function Invoke-Gcloud {
    & gcloud.cmd @args
    if ($LASTEXITCODE -ne 0) { throw "gcloud failed with exit code $LASTEXITCODE" }
}

$batch = "poc06-$Mode-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
$common = @(
    "--project=$Project", "--region=$Region", "--batch=$batch",
    "--service-account=dataproc@$Project.iam.gserviceaccount.com",
    '--subnet=skyita-da-architects-sandbox-test', '--version=2.3', '--ttl=15m'
)
if ($Mode -eq 'build') {
    Invoke-Gcloud storage cp "$PSScriptRoot/BigQueryReadOnlyCatalog.java" "$PSScriptRoot/build_catalog.py" "$ArtifactPrefix/" "--project=$Project"
    Invoke-Gcloud dataproc batches submit pyspark "$ArtifactPrefix/build_catalog.py" @common "--files=$ArtifactPrefix/BigQueryReadOnlyCatalog.java" '--' "$ArtifactPrefix/bigquery-readonly-catalog.jar"
} else {
    Invoke-Gcloud storage cp "$PSScriptRoot/job.py" "$ArtifactPrefix/job.py" "--project=$Project"
    $properties = @(
        "dataproc.lakehouse.catalog.$Catalog=projects/$Project/catalogs/$Catalog",
        'spark.sql.catalog.bq=poc.BigQueryReadOnlyCatalog',
        "spark.sql.catalog.bq.parentProject=$Project"
    ) -join ','
    $jobArgs = @($Catalog, $Namespace, '--mode', $Mode, '--native', $Native)
    if ($Expected -ge 0) { $jobArgs += @('--expected', "$Expected") }
    if ($Materialize) { $jobArgs += '--materialize' }
    if ($Cutover) { $jobArgs += @('--cutover', $Cutover) }
    Invoke-Gcloud dataproc batches submit pyspark "$ArtifactPrefix/job.py" @common "--jars=$ArtifactPrefix/bigquery-readonly-catalog.jar" "--properties=$properties" '--' @jobArgs
}
