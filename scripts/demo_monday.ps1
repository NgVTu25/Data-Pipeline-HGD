param(
    [switch]$Auto,
    [string]$ApiUrl = "http://127.0.0.1:8000",
    [string]$ApiKey = "dev-secret-key",
    [int]$CdcWaitSeconds = 8
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$RepoRoot = Split-Path -Parent $PSScriptRoot
$PythonExe = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $PythonExe)) {
    $PythonExe = "python"
}

function Write-Banner {
    param([string]$Title)
    Write-Host ""
    Write-Host "========================================================================" -ForegroundColor Cyan
    Write-Host "  $Title" -ForegroundColor Yellow
    Write-Host "========================================================================" -ForegroundColor Cyan
}

function Pause-Step {
    param([string]$Message = "Nhan Enter de tiep tuc buoc tiep theo (Ctrl+C de dung)...")
    if (-not $Auto) {
        Write-Host ""
        [void](Read-Host $Message)
    }
}

Push-Location $RepoRoot
try {
    # Buoc 1: Kiem tra Docker & Debezium Connectors
    Write-Banner "BUOC 1: Kiem tra Docker Containers & Debezium Connectors"
    docker ps --filter "name=hgd-" --format "table {{.Names}}`t{{.Status}}`t{{.Ports}}"
    Write-Host "`nTrang thai Debezium Connectors (http://localhost:8083/connectors):" -ForegroundColor Green
    try {
        $connectors = Invoke-RestMethod -Uri "http://localhost:8083/connectors?expand=status" -TimeoutSec 5
        $connectors.PSObject.Properties | ForEach-Object {
            $name = $_.Name
            $state = $_.Value.status.connector.state
            $tasks = ($_.Value.status.tasks | ForEach-Object { "$($_.id):$($_.state)" }) -join ", "
            Write-Host " - $name -> connector=$state | tasks=[$tasks]"
        }
    } catch {
        Write-Warning "Khong ket noi duoc Debezium Connect tai http://localhost:8083 ($($_.Exception.Message))"
    }
    Pause-Step

    # Buoc 2: Doi soat TRUOC bien dong
    Write-Banner "BUOC 2: Doi soat 3 tang TRUOC bien dong (Nguon <-> Mirror <-> C360)"
    & $PythonExe -m src.reconciliation.reconcile_pipeline
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Doi soat truoc bien dong tra ve ma loi $LASTEXITCODE."
    }
    Pause-Step

    # Buoc 3: Day bien dong CDC vao nguon
    Write-Banner "BUOC 3: Day bien dong CDC vao bang nguon VSS (INSERT / UPDATE / doi MA_CD / DELETE)"
    & $PythonExe -m src.data_generator.simulate_cdc_changes --target local
    if ($LASTEXITCODE -ne 0) {
        throw "simulate_cdc_changes that bai voi ma loi $LASTEXITCODE"
    }
    Pause-Step

    # Buoc 4: Cho CDC vao Mirror & chay Near Real-time C360
    Write-Banner "BUOC 4: Cho $CdcWaitSeconds giay cho CDC dong bo sang Mirror & chay Near Real-time C360"
    Start-Sleep -Seconds $CdcWaitSeconds
    & $PythonExe -m airflow.dags.dag_hgd_c360_pipeline
    if ($LASTEXITCODE -ne 0) {
        throw "dag_hgd_c360_pipeline that bai voi ma loi $LASTEXITCODE"
    }
    Pause-Step

    # Buoc 5: Doi soat SAU bien dong
    Write-Banner "BUOC 5: Doi soat 3 tang SAU bien dong (Chung minh 100% khop)"
    & $PythonExe -m src.reconciliation.reconcile_pipeline
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Doi soat sau bien dong tra ve ma loi $LASTEXITCODE."
    }
    Pause-Step

    # Buoc 6: Kiem thu nhanh REST API & nhac Refresh Power BI
    Write-Banner "BUOC 6: Kiem thu REST API C360 & Cap nhat Power BI Desktop"
    $apiAvailable = $false
    try {
        $null = Invoke-WebRequest -Uri "$ApiUrl/docs" -TimeoutSec 3 -UseBasicParsing
        $apiAvailable = $true
    } catch {
        $apiAvailable = $false
    }

    if ($apiAvailable) {
        $headers = @{ "X-API-Key" = $ApiKey }
        Write-Host "1) GET /api/v1/ca-nhan/tim-kiem?ho_ten=Nguyen&ngay_sinh=1990&limit=3" -ForegroundColor Green
        try {
            $searchRes = Invoke-RestMethod -Uri "$ApiUrl/api/v1/ca-nhan/tim-kiem?ho_ten=Nguyen&ngay_sinh=1990&limit=3" -Headers $headers -TimeoutSec 5
            $searchRes | ConvertTo-Json -Depth 4
        } catch {
            Write-Warning "Loi goi /tim-kiem: $($_.Exception.Message)"
        }

        Write-Host "`n2) GET /api/v1/ca-nhan/thay-doi?limit=5&safe_lag_seconds=0" -ForegroundColor Green
        try {
            $deltaRes = Invoke-RestMethod -Uri "$ApiUrl/api/v1/ca-nhan/thay-doi?limit=5&safe_lag_seconds=0" -Headers $headers -TimeoutSec 5
            $deltaRes | ConvertTo-Json -Depth 4
        } catch {
            Write-Warning "Loi goi /thay-doi: $($_.Exception.Message)"
        }
    } else {
        Write-Warning "FastAPI chua chay tai $ApiUrl. Mo terminal khac va chay lenh sau de test API:"
        Write-Host "  .\.venv\Scripts\uvicorn.exe src.api.main:app --reload" -ForegroundColor Cyan
    }

    Write-Host ""
    Write-Host ">>> NHAC NHO DEMO: Hay mo Power BI Desktop va bam nut 'Refresh' de cap nhat so lieu moi nhat tu 3 View BI!" -ForegroundColor Magenta
}
finally {
    Pop-Location
}
