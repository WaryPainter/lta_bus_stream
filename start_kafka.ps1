# Apache Kafka KRaft Mode Startup
$ROOT = $PSScriptRoot
$JAVA_EXE = "$ROOT\flink_setup\jdk11\jdk-11.0.30+7\bin\java.exe"
$KAFKA_HOME = "$ROOT\k"
$CONF = "$KAFKA_HOME\config\kraft\server.properties"
$LOG4J = "-Dlog4j.configuration=file:$KAFKA_HOME\config\log4j.properties"
$HEAP = "-Xmx512M", "-Xms512M"
$CP = "$KAFKA_HOME\libs\*"

Write-Host "Starting Kafka in KRaft mode..."
& $JAVA_EXE $HEAP $LOG4J -cp $CP kafka.Kafka $CONF
