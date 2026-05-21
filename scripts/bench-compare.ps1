param(
    [int]$DurationSeconds = 30,
    [int]$Records = 200000,
    [int]$PayloadBytes = 1024,
    [int]$Partitions = 4,
    [int]$Producers = 4,
    [int]$Consumers = 4,
    [int]$BatchSize = 16,
    [int]$ProducerInflight = 4,
    [string]$ResultsDir = "bench-results",
    [switch]$SkipDocker
)

$ErrorActionPreference = "Stop"

New-Item -ItemType Directory -Force $ResultsDir | Out-Null

function Run-And-Capture {
    param(
        [string]$Name,
        [scriptblock]$Command
    )
    $path = Join-Path $ResultsDir "$Name.txt"
    & $Command 2>&1 | Tee-Object -FilePath $path
}

Run-And-Capture "go-benchmarks" {
    go test ./protocol ./transport ./store ./broker -run '^$' -bench . -benchmem -benchtime=5s
}

Run-And-Capture "ech0-embedded" {
    go run ./cmd/ech0bench --duration "$($DurationSeconds)s" --producers $Producers --consumers $Consumers --partitions $Partitions --payload-bytes $PayloadBytes --batch-size $BatchSize --producer-inflight $ProducerInflight --fetch-batch 256 --commit-every 8
}

if (-not $SkipDocker) {
    Push-Location deploy/docker
    try {
        docker compose -f docker-compose.kafka-bench.yml up -d
        docker exec ech0-kafka-bench-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --create --if-not-exists --topic ech0-bench --partitions $Partitions --replication-factor 3 --config min.insync.replicas=2 | Out-Host
        Pop-Location
        Run-And-Capture "kafka-producer" {
            docker exec ech0-kafka-bench-1 /opt/kafka/bin/kafka-producer-perf-test.sh --topic ech0-bench --num-records $Records --record-size $PayloadBytes --throughput -1 --producer-props bootstrap.servers=kafka1:9092 acks=all linger.ms=5 batch.size=32768 compression.type=none
        }
        Push-Location deploy/docker
        docker compose -f docker-compose.nats-bench.yml up -d
        Pop-Location
        Run-And-Capture "nats-bench" {
            docker exec ech0-nats-bench-box nats bench ech0.bench --server nats://nats:4222 --pub $Producers --sub $Consumers --size $PayloadBytes --msgs $Records
        }
    }
    finally {
        if ((Get-Location).Path.EndsWith("deploy\docker")) {
            Pop-Location
        }
    }
}
