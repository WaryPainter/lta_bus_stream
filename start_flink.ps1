# Apache Flink Environment Setup
$ROOT = $PSScriptRoot
$env:JAVA_HOME = "$ROOT\flink_setup\jdk11\jdk-11.0.30+7"
$env:PATH = "$env:JAVA_HOME\bin;" + $env:PATH
$FLINK_HOME = "$ROOT\flink_setup\flink-1.20.3"
$SH = "C:\Program Files\Git\bin\sh.exe"

Write-Host "Starting Apache Flink Cluster..." -ForegroundColor Green
& $SH "$FLINK_HOME/bin/start-cluster.sh"
Write-Host "Flink Web UI: http://localhost:8081" -ForegroundColor Cyan
