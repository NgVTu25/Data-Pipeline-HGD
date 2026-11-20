<#
.SYNOPSIS
    Reset Debezium JDBC Sink connector về cuối topic Kafka (latest) và dọn sạch C360_SYNC_QUEUE.
    Dùng sau khi chạy Full Load để tránh Sink replay hàng triệu message backlog cũ thành ACTION_TYPE = 'U'.
#>

param(
    [string]$ConnectUrl          = "http://localhost:8083",
    [string]$ConnectorName       = "c360-mirror-jdbc-sink-connector",
    [string]$SourceConnectorName = "vss-hgd-oracle-source-connector",
    [string]$TopicName           = "vss_cdc.VSS.VSS_HO_GD_NHANKHAU",
    [string]$ConfigPath          = "src/ingestion/debezium/c360-mirror-jdbc-sink-connector.json",
    [string]$KafkaContainer      = "hgd-kafka",
    [string]$OracleContainer     = "hgd-oracle",
    [string]$OracleConnStr       = "C360/c360_pwd@//localhost:1521/FREEPDB1",
    [switch]$SkipSourceOffsetReset
)

$ErrorActionPreference = "Stop"
$ConsumerGroup = "connect-$ConnectorName"

Write-Host "=== [1/5] Xoa connector '$ConnectorName' (neu dang ton tai) ===" -ForegroundColor Cyan
try {
    Invoke-RestMethod -Method Delete -Uri "$ConnectUrl/connectors/$ConnectorName" | Out-Null
    Write-Host "Da xoa connector '$ConnectorName'." -ForegroundColor Green
} catch {
    if ($_.Exception.Response.StatusCode.value__ -eq 404) {
        Write-Host "Connector '$ConnectorName' chua ton tai, bo qua buoc xoa." -ForegroundColor Yellow
    } else {
        throw
    }
}

Write-Host "Cho 3 giay de consumer group ngat ket noi..." -ForegroundColor DarkGray
Start-Sleep -Seconds 3

Write-Host "=== [2/5] Reset Kafka consumer group '$ConsumerGroup' len --to-latest ===" -ForegroundColor Cyan
docker exec $KafkaContainer kafka-consumer-groups `
    --bootstrap-server kafka:29092 `
    --group $ConsumerGroup `
    --topic $TopicName `
    --reset-offsets --to-latest --execute

if ($LASTEXITCODE -ne 0) {
    Write-Warning "Khong the reset offset (co the group chua co offset). Tiep tuc..."
}

Write-Host "=== [3/5] Don sach bang C360.C360_SYNC_QUEUE ===" -ForegroundColor Cyan
$sqlTruncate = @"
TRUNCATE TABLE C360.C360_SYNC_QUEUE;
SELECT COUNT(*) AS REMAINING_QUEUE_ROWS FROM C360.C360_SYNC_QUEUE;
EXIT;
"@
$sqlTruncate | docker exec -i $OracleContainer sqlplus -s $OracleConnStr

Write-Host "=== [4/5] Dang ky lai connector tu '$ConfigPath' ===" -ForegroundColor Cyan
if (-not (Test-Path $ConfigPath)) {
    throw "Khong tim thay file cau hinh: $ConfigPath"
}
Invoke-RestMethod -Method Post `
    -Uri "$ConnectUrl/connectors" `
    -ContentType "application/json" `
    -InFile $ConfigPath | Out-Null

Start-Sleep -Seconds 3

if (-not $SkipSourceOffsetReset) {
    Write-Host "=== [5/6] Reset offset Source connector '$SourceConnectorName' ve SCN hien tai ===" -ForegroundColor Cyan
    try {
        Invoke-RestMethod -Method Get -Uri "$ConnectUrl/connectors/$SourceConnectorName" | Out-Null
        Invoke-RestMethod -Method Put -Uri "$ConnectUrl/connectors/$SourceConnectorName/stop" | Out-Null
        Start-Sleep -Seconds 2
        Invoke-RestMethod -Method Delete -Uri "$ConnectUrl/connectors/$SourceConnectorName/offsets" | Out-Null
        Invoke-RestMethod -Method Put -Uri "$ConnectUrl/connectors/$SourceConnectorName/resume" | Out-Null
        Start-Sleep -Seconds 3
        Write-Host "Da reset offset Source connector '$SourceConnectorName' thanh cong." -ForegroundColor Green
    } catch {
        if ($_.Exception.Response.StatusCode.value__ -eq 404) {
            Write-Host "Source connector '$SourceConnectorName' chua ton tai, bo qua reset offset." -ForegroundColor Yellow
        } else {
            Write-Warning "Khong the reset offset Source connector: $_"
        }
    }
}

Write-Host "=== [6/6] Kiem tra trang thai connector ===" -ForegroundColor Cyan
$status = Invoke-RestMethod -Uri "$ConnectUrl/connectors/$ConnectorName/status"
$status | ConvertTo-Json -Depth 5

Write-Host "`nHoan tat! Tu gio Source & Sink chi nhan cac bien dong CDC moi (I / U / D)." -ForegroundColor Green
