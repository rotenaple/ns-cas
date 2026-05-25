param(
    [string]$Version = "latest"
)

$ErrorActionPreference = "Stop"
$ImageName = "ns-cas"

# 1. Get Unix Timestamp
$BuildDate = [int][double]::Parse((Get-Date -UFormat %s))

Write-Host "Building ${ImageName}:${Version} (Date: $BuildDate)..." -ForegroundColor Cyan

# 2. Build
docker build --build-arg "BUILD_DATE=$BuildDate" -t "${ImageName}:${Version}" .

# 3. Save
$TarFile = "${ImageName}_${Version}.tar"
Write-Host "Saving to $TarFile..." -ForegroundColor Cyan

docker save -o $TarFile "${ImageName}:${Version}"

Write-Host "Done! Created $TarFile" -ForegroundColor Green
