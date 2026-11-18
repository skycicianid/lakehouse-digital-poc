<#
Test 05 / cut-over: vista BigQuery con cut-over tra storico nativo e Iceberg (stessa logica del test 06).

  -Mode create         crea i due contratti e la vista hit_data_cutover_view (cutover_view.sql)
                       e controlla count(vista) = count(BQ < cut-over) + count(Iceberg >= cut-over)
  -Mode cutover-check  sola lettura: conteggi per giorno e quale sorgente legge ogni query,
                       dal query plan del job BigQuery (recordsRead degli stage di lettura)

Ogni controllo fallito fa terminare lo script con errore. L'esito viene salvato in results/.
#>
param(
    [ValidateSet('create', 'cutover-check')][string]$Mode = 'create',
    [string]$Project = 'skyita-da-architects-sandbox',
    [string]$Dataset = 'dciciani',
    [string]$Native = 'skyita-da-architects-sandbox.dciciani.hit_data_stg_hist',
    [string]$Iceberg = 'skyita-da-architects-sandbox.dciciani-lakehouse-catalog.digital-analytics-myskyapp.hit_data_stg',
    [Parameter(Mandatory = $true)][string]$Cutover,
    [string]$Location = 'EU'
)
$ErrorActionPreference = 'Stop'
$null = [datetime]::ParseExact($Cutover, 'yyyy-MM-dd', $null)

$runId = Get-Date -Format 'yyyyMMdd_HHmmss'
$view = "$Project.$Dataset.hit_data_cutover_view"
$cut = "TIMESTAMP '$Cutover 00:00:00'"
$failures = New-Object System.Collections.Generic.List[string]
$result = [ordered]@{ mode = $Mode; run_id = $runId; view = $view; native = $Native; iceberg = $Iceberg; cutover = $Cutover }

function Invoke-Bq([string]$Sql, [string]$Name, [switch]$NoRows) {
    $jobId = "poc05_$($Name)_$runId"
    # The SQL goes through a file and cmd redirection: PowerShell 5.1 pipes add a UTF-8 BOM that bq
    # rejects, and the contracts are longer than the cmd.exe command-line limit.
    $sqlFile = [System.IO.Path]::GetTempFileName()
    try {
        [System.IO.File]::WriteAllText($sqlFile, $Sql, [System.Text.Encoding]::ASCII)
        $out = & cmd.exe /c "bq.cmd --quiet --project_id=$Project --location=$Location query --use_legacy_sql=false --nouse_cache --format=json --max_rows=10000 --job_id=$jobId < `"$sqlFile`""
    } finally { Remove-Item -Force $sqlFile }
    if ($LASTEXITCODE -ne 0) { throw "bq query $jobId failed: $($out | Out-String)" }
    $rows = @()
    if (-not $NoRows) {
        $text = ($out | Out-String).Trim()
        if ($text) { $rows = @($text | ConvertFrom-Json) }
    }
    [pscustomobject]@{ JobId = $jobId; Rows = $rows }
}

function Get-Scalar([string]$Sql, [string]$Name) {
    [long](Invoke-Bq $Sql $Name).Rows[0].n
}

function Get-PerDay([string]$Table, [string]$Name, [string]$Where = '') {
    $q = "SELECT CAST(DATE(partition_date) AS STRING) AS d, COUNT(*) AS n FROM ``$Table`` $Where GROUP BY 1"
    $days = [ordered]@{}
    foreach ($r in ((Invoke-Bq $q $Name).Rows | Sort-Object d)) { $days[$r.d] = [long]$r.n }
    $days
}

function Format-Days($Days) { ($Days.Keys | Sort-Object | ForEach-Object { "$_=$($Days[$_])" }) -join ', ' }

function Get-SourceReads([string]$JobId) {
    # BigQuery legge le due tabelle nello stesso stage di input ("1 similar table omitted"), quindi il
    # query plan non dà le righe per sorgente. Si usano le statistiche del job, che sono per sorgente:
    #   nativa  -> totalPartitionsProcessed (partizioni BigQuery lette) ed estimatedBytesProcessed
    #   Iceberg -> objectStorageStats.objectStorageBytesRead (byte letti dai file su GCS; assente se 0)
    $job = (& bq.cmd --quiet "--project_id=$Project" "--location=$Location" --format=json show -j $JobId | Out-String) | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) { throw "bq show -j $JobId failed" }
    $q = $job.statistics.query
    if ($q.cacheHit) { throw "Job $JobId was served from cache: no read statistics" }
    $inputStages = @($q.queryPlan | Where-Object { $_.steps | Where-Object { $_.kind -eq 'READ' -and ($_.substeps -join ' ') -match 'FROM (?!__stage)' } })
    [ordered]@{
        native_partitions      = [long]$q.totalPartitionsProcessed
        native_bytes_estimated = [long]$q.estimatedBytesProcessed
        iceberg_storage_bytes  = [long]$q.objectStorageStats.objectStorageBytesRead
        input_records_total    = [long](($inputStages | ForEach-Object { [long]$_.recordsRead } | Measure-Object -Sum).Sum)
        bytes_processed        = [long]$q.totalBytesProcessed
        bytes_billed           = [long]$q.totalBytesBilled
        slot_ms                = [long]$q.totalSlotMs
    }
}

if ($Mode -eq 'create') {
    $nativeCount = Get-Scalar "SELECT COUNT(*) AS n FROM ``$Native`` WHERE partition_date < $cut" 'src_native'
    $icebergCount = Get-Scalar "SELECT COUNT(*) AS n FROM ``$Iceberg`` WHERE partition_date >= $cut" 'src_iceberg'
    Write-Host "SOURCE_COUNTS: bigquery=$nativeCount iceberg=$icebergCount"
    if (-not $nativeCount -or -not $icebergCount) { throw 'Both branches must contain test rows; check source dates and cutover' }

    $sql = (Get-Content -Raw -Encoding UTF8 "$PSScriptRoot/cutover_view.sql").
        Replace('{{PROJECT}}', $Project).Replace('{{DATASET}}', $Dataset).
        Replace('{{NATIVE}}', $Native).Replace('{{ICEBERG}}', $Iceberg).Replace('{{CUTOVER}}', $Cutover)
    $null = Invoke-Bq $sql 'create_views' -NoRows

    $viewCount = Get-Scalar "SELECT COUNT(*) AS n FROM ``$view``" 'view_count'
    $expected = $nativeCount + $icebergCount
    Write-Host "VIEW_COUNT: $viewCount (expected $expected)"
    if ($viewCount -ne $expected) { $failures.Add("View has $viewCount rows; expected $expected") }
    $result.source_counts = [ordered]@{ bigquery = $nativeCount; iceberg = $icebergCount }
    $result.view_rows = $viewCount
}

if ($Mode -eq 'cutover-check') {
    $nativeDays = Get-PerDay $Native 'days_native'
    $icebergDays = Get-PerDay $Iceberg 'days_iceberg'
    $viewDays = Get-PerDay $view 'days_view'
    $expectedDays = [ordered]@{}
    foreach ($d in $nativeDays.Keys) { if ($d -lt $Cutover) { $expectedDays[$d] = $nativeDays[$d] } }
    foreach ($d in $icebergDays.Keys) { if ($d -ge $Cutover) { $expectedDays[$d] = $icebergDays[$d] } }
    Write-Host "NATIVE_DAYS:  $(Format-Days $nativeDays)"
    Write-Host "ICEBERG_DAYS: $(Format-Days $icebergDays)"
    Write-Host "VIEW_DAYS:    $(Format-Days $viewDays)"
    Write-Host "EXPECTED:     $(Format-Days $expectedDays)"

    if (-not @($nativeDays.Keys | Where-Object { $_ -lt $Cutover }).Count) { $failures.Add('BigQuery has no day before the cutover') }
    if (-not @($icebergDays.Keys | Where-Object { $_ -ge $Cutover }).Count) { $failures.Add('Iceberg has no day on/after the cutover') }
    if (-not @($nativeDays.Keys | Where-Object { $_ -ge $Cutover }).Count) {
        Write-Host 'WARNING: no overlapping day in BigQuery; duplicate exclusion not exercised'
    }
    if ((Format-Days $viewDays) -ne (Format-Days $expectedDays)) {
        $failures.Add("Per-day mismatch: view={$(Format-Days $viewDays)} expected={$(Format-Days $expectedDays)}")
    }

    $histDays = @($nativeDays.Keys | Where-Object { $_ -lt $Cutover } | Sort-Object)
    $histDay = $histDays[0]
    $recentRows = [long](($expectedDays.Keys | Where-Object { $_ -ge $Cutover } | ForEach-Object { $expectedDays[$_] } | Measure-Object -Sum).Sum)
    $allRows = [long](($expectedDays.Values | Measure-Object -Sum).Sum)
    # want_partitions: partizioni della nativa che devono essere lette; iceberg: se i file Iceberg vanno letti.
    $checks = [ordered]@{
        recent_only        = @{ where = "partition_date >= $cut"; want = $recentRows; want_partitions = 0; iceberg = $true }
        single_history_day = @{ where = "partition_date >= TIMESTAMP '$histDay 00:00:00' AND partition_date < TIMESTAMP_ADD(TIMESTAMP '$histDay 00:00:00', INTERVAL 1 DAY)"
                                want = $nativeDays[$histDay]; want_partitions = 1; iceberg = $false }
        across_cutover     = @{ where = 'partition_date IS NOT NULL'; want = $allRows; want_partitions = $histDays.Count; iceberg = $true }
    }
    $pruning = [ordered]@{}
    foreach ($name in $checks.Keys) {
        $c = $checks[$name]
        $q = Invoke-Bq "SELECT COUNT(*) AS n FROM ``$view`` WHERE $($c.where)" "check_$name"
        $rows = [long]$q.Rows[0].n
        $s = Get-SourceReads $q.JobId
        $passed = $rows -eq $c.want -and $s.native_partitions -eq $c.want_partitions -and
            ([bool]$s.iceberg_storage_bytes -eq $c.iceberg)
        $pruning[$name] = [ordered]@{
            job_id = $q.JobId; where = $c.where; rows = $rows; expected = $c.want
            expected_native_partitions = $c.want_partitions; expected_iceberg_read = $c.iceberg
            stats = $s; pass = [bool]$passed
        }
        Write-Host ("CHECK {0}: rows={1}/{2} native_partitions={3}/{4} iceberg_gcs_bytes={5} (expected read={6}) bytes_processed={7} pass={8}" -f `
            $name, $rows, $c.want, $s.native_partitions, $c.want_partitions, $s.iceberg_storage_bytes, $c.iceberg, $s.bytes_processed, [bool]$passed)
        if (-not $passed) { $failures.Add("${name}: rows=$rows/$($c.want) native_partitions=$($s.native_partitions)/$($c.want_partitions) iceberg_gcs_bytes=$($s.iceberg_storage_bytes) expected_iceberg_read=$($c.iceberg)") }
    }
    $result.native_days = $nativeDays
    $result.iceberg_days = $icebergDays
    $result.view_days = $viewDays
    $result.expected_days = $expectedDays
    $result.pruning = $pruning
}

$result.failures = @($failures)
$result.status = if ($failures.Count) { 'FAILED' } else { 'OK' }
$resultsDir = Join-Path $PSScriptRoot 'results'
if (-not (Test-Path $resultsDir)) { $null = New-Item -ItemType Directory $resultsDir }
$resultFile = Join-Path $resultsDir "$Mode-$runId.json"
$result | ConvertTo-Json -Depth 8 | Out-File -Encoding utf8 $resultFile
Write-Host "RESULT: $resultFile"
if ($failures.Count) { throw ($failures -join '; ') }
Write-Host 'STATUS: OK'
