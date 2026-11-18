param(
    [ValidateSet('build', 'create', 'verify', 'cutover-check')][string]$Mode = 'create',
    [string]$Namespace = 'federation_layered_20261113',
    [string]$Project = 'skyita-da-architects-sandbox',
    [string]$Region = 'europe-west1',
    [string]$Catalog = 'dciciani-lakehouse-catalog',
    [string]$ArtifactPrefix = 'gs://dciciani-lakehouse/digital-poc/script/catalog-fix-v3-20261113',
    [int]$Expected = -1,
    [switch]$Materialize,
    [string]$Native = 'skyita-da-architects-sandbox.dciciani.hit_data_stg_test',
    [string]$Cutover,
    # Default: the connector's built-in com.google.cloud.spark.bigquery.BigQueryCatalog (>= 0.42.0),
    # no JAR needed. -CustomCatalog uses the legacy BigQueryReadOnlyCatalog JAR (built with -Mode build).
    [switch]$CustomCatalog,
    # Optional: force the Dataproc-provided connector version (dataproc.sparkBqConnector.version).
    [string]$ConnectorVersion
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
    $catalogClass = if ($CustomCatalog) { 'poc.BigQueryReadOnlyCatalog' } else { 'com.google.cloud.spark.bigquery.BigQueryCatalog' }
    $propertyList = @(
        "dataproc.lakehouse.catalog.$Catalog=projects/$Project/catalogs/$Catalog",
        "spark.sql.catalog.bq=$catalogClass",
        "spark.sql.catalog.bq.parentProject=$Project"
    )
    if ($ConnectorVersion) { $propertyList += "dataproc.sparkBqConnector.version=$ConnectorVersion" }
    $properties = $propertyList -join ','
    $jarArgs = if ($CustomCatalog) { @("--jars=$ArtifactPrefix/bigquery-readonly-catalog.jar") } else { @() }
    $jobArgs = @($Catalog, $Namespace, '--mode', $Mode, '--native', $Native)
    if ($Expected -ge 0) { $jobArgs += @('--expected', "$Expected") }
    if ($Materialize) { $jobArgs += '--materialize' }
    if ($Cutover) { $jobArgs += @('--cutover', $Cutover) }
    Invoke-Gcloud dataproc batches submit pyspark "$ArtifactPrefix/job.py" @common @jarArgs "--properties=$properties" '--' @jobArgs
}
