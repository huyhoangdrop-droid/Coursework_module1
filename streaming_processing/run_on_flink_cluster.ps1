<#
.SYNOPSIS
Submit a PyFlink job to the Flink cluster running in docker compose.
.EXAMPLE
.\run_on_flink_cluster.ps1
.\run_on_flink_cluster.ps1 -JobFile "optional/3_process_clean_stream_sql.py"
#>

param (
    [string]$JobFile = "process_clean_stream_datastream.py"
)

$ErrorActionPreference = "Stop"

# Đảm bảo script luôn chạy ở đúng thư mục chứa nó
Set-Location $PSScriptRoot

# Cách check an toàn trong PowerShell: Không báo lỗi nếu không thấy image
$imageID = docker images -q pyflink:2.0
if ([string]::IsNullOrWhiteSpace($imageID)) {
    Write-Host "PyFlink image not found. Building jobmanager..." -ForegroundColor Yellow
    # Tạm tắt ErrorAction để docker compose tự do in log
    $ErrorActionPreference = "Continue" 
    docker compose build jobmanager
    $ErrorActionPreference = "Stop"
}

Write-Host "Starting Flink cluster..." -ForegroundColor Cyan
# Tạm tắt ErrorAction để docker compose tự do in log
$ErrorActionPreference = "Continue"
docker compose up -d jobmanager taskmanager
$ErrorActionPreference = "Stop"

Write-Host "Waiting for JobManager REST API..." -ForegroundColor Cyan

# Vòng lặp chờ Flink khởi động
while ($true) {
    try {
        $null = Invoke-RestMethod -Uri "http://localhost:8881/overview" -Method Get -ErrorAction Stop
        break
    } catch {
        Write-Host "Still waiting for Flink JobManager..."
        Start-Sleep -Seconds 2
    }
}

Write-Host "Submitting job: $JobFile" -ForegroundColor Green
$ErrorActionPreference = "Continue"
docker compose exec -w /opt/job jobmanager `
  flink run -d `
  -pyexec python3 `
  -py "/opt/job/$JobFile"

Write-Host "`nSubmitted. Check http://localhost:8881 (Running Jobs) and TaskManager stdout for output." -ForegroundColor Cyan