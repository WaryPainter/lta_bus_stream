# View Kafka Messages
$ROOT = $PSScriptRoot
$JAVA_EXE = "$ROOT\flink_setup\jdk11\jdk-11.0.30+7\bin\java.exe"
$CP = "$ROOT\flink_setup\flink-1.20.3\lib\*"

# Default to 5 messages, but allow user to pass a number
$count = if ($args[0]) { $args[0] } else { 5 }

Write-Host "Reading $count messages from 'lta-bus-arrivals'..." -ForegroundColor Cyan

& $JAVA_EXE -cp $CP org.apache.kafka.tools.consumer.ConsoleConsumer --bootstrap-server localhost:9092 --topic lta-bus-arrivals --from-beginning --max-messages $count
