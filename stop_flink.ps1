# Apache Flink Environment Teardown
$ROOT = $PSScriptRoot
$FLINK_HOME = "$ROOT\flink_setup\flink-1.20.3"
$SH = "C:\Program Files\Git\bin\sh.exe"

Write-Host "Stopping Apache Flink Cluster..." -ForegroundColor Yellow
& $SH "$FLINK_HOME/bin/stop-cluster.sh"
