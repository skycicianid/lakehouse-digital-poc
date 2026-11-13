param(
    [string]$Prefix = 'gs://dciciani-lakehouse-catalog/federation_layered_20261113'
)
$ErrorActionPreference = 'Stop'
$Prefix = $Prefix.TrimEnd('/')
$names = @('hit_data_union_view', 'hit_data_native_contract', 'hit_data_iceberg_contract')

foreach ($name in $names) {
    $paths = @(& gcloud.cmd storage ls "$Prefix/$name/**")
    if ($LASTEXITCODE -ne 0) { throw "Cannot list metadata for $name" }
    $metadataPaths = @($paths | Where-Object { $_ -match '\.metadata\.json$' })
    if ($metadataPaths.Count -ne 1) {
        throw "$name has $($metadataPaths.Count) metadata files: use the REST catalog to identify current metadata before inspecting."
    }
    $tempFile = [System.IO.Path]::GetTempFileName()
    try {
        & gcloud.cmd storage cp $metadataPaths[0] $tempFile
        if ($LASTEXITCODE -ne 0) { throw "Cannot download metadata for $name" }
        $stream = [System.IO.File]::OpenRead($tempFile)
        try {
            # gcloud may already decompress objects with Content-Encoding: gzip.
            $magic1 = $stream.ReadByte()
            $magic2 = $stream.ReadByte()
            $stream.Position = 0
            if ($magic1 -eq 0x1f -and $magic2 -eq 0x8b) {
                $stream = [System.IO.Compression.GZipStream]::new($stream, [System.IO.Compression.CompressionMode]::Decompress)
            }
            $reader = [System.IO.StreamReader]::new($stream)
            try { $metadata = $reader.ReadToEnd() | ConvertFrom-Json }
            finally { $reader.Dispose() }
        } finally { $stream.Dispose() }
        if (-not $metadata.'view-uuid') { throw "$name is not Iceberg view metadata" }
        $version = @($metadata.versions | Where-Object { $_.'version-id' -eq $metadata.'current-version-id' })
        if ($version.Count -ne 1) { throw "Cannot identify current view version for $name" }
        $representation = @($version[0].representations | Where-Object { $_.type -eq 'sql' -and $_.dialect -eq 'spark' })
        if ($representation.Count -ne 1) { throw "Cannot identify Spark SQL for $name" }
        $sql = $representation[0].sql
        $required = switch ($name) {
            'hit_data_union_view' { @('UNION ALL', 'hit_data_native_contract', 'hit_data_iceberg_contract') }
            'hit_data_native_contract' { @('bq.', 'skyita-da-architects-sandbox', 'dciciani', 'hit_data_stg_test') }
            'hit_data_iceberg_contract' { @('dciciani-lakehouse-catalog', 'digital-analytics-myskyapp', 'hit_data_stg') }
        }
        foreach ($term in $required) {
            if ($sql.IndexOf($term, [System.StringComparison]::OrdinalIgnoreCase) -lt 0) {
                throw "$name does not contain expected source reference: $term"
            }
        }
        [PSCustomObject]@{
            View = $name
            Metadata = $metadataPaths[0]
            ViewUUID = $metadata.'view-uuid'
            FormatVersion = $metadata.'format-version'
            CurrentVersion = $metadata.'current-version-id'
            Dialect = $representation[0].dialect
            SQL = $sql
        } | Format-List
    } finally {
        Remove-Item -LiteralPath $tempFile -ErrorAction SilentlyContinue
    }
}
Write-Output 'STATUS: OK - persisted Iceberg view metadata contains both source branches'
