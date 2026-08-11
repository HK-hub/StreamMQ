# StreamMQ JMH Benchmark Runner v4 - No Fork Mode
$ErrorActionPreference = "Stop"
$projectDir = "D:\learn\project\StreamMQ"
$benchDir = "$projectDir\streammq-benchmark"
$resultsDir = "$projectDir\benchmark-results"
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"

New-Item -ItemType Directory -Path $resultsDir -Force | Out-Null

Write-Output "========================================"
Write-Output "StreamMQ JMH Benchmarks (No Fork Mode)"
Write-Output "Time: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Output "Redis: 127.0.0.1:6379 (local)"
Write-Output "========================================"

# Build classpath
Push-Location $benchDir
mvn dependency:build-classpath "-Dmdep.outputFile=cp.txt" "-Dmdep.includeScope=test" -q 2>&1 | Out-Null
$classpath = (Get-Content "cp.txt" -Raw).Trim()
Pop-Location

$testClasses = "$benchDir\target\test-classes"
$mainClasses = "$benchDir\target\classes"
$jvmArgs = "-Dstreammq.redis.mode=local"

function Run-Benchmark {
    param($name, $outputFile)
    Write-Output "`nRunning: $name -> $outputFile"
    & java $jvmArgs -cp "$testClasses;$mainClasses;$classpath" org.openjdk.jmh.Main $name -wi 2 -i 3 -w 2s -r 3s -f 0 -o $outputFile
    Write-Output "Completed: $name"
}

# Run all benchmarks
$serFile = "$resultsDir\serialization-$timestamp.txt"
$sendFile = "$resultsDir\send-$timestamp.txt"
$consFile = "$resultsDir\consume-$timestamp.txt"

Run-Benchmark "io.github.streammq.benchmark.SerializationBenchmark" $serFile
Run-Benchmark "io.github.streammq.benchmark.StreamMessageTemplateBenchmark" $sendFile
Run-Benchmark "io.github.streammq.benchmark.StreamConsumerBenchmark" $consFile

# Generate summary
Write-Output "`n========================================"
Write-Output "ALL BENCHMARKS COMPLETE"
Write-Output "========================================"

# Extract key results
Write-Output "`n========== PERFORMANCE SUMMARY =========="

foreach ($file in @($serFile, $sendFile, $consFile)) {
    if (Test-Path $file) {
        Write-Output "`n--- $(Split-Path $file -Leaf) ---"
        Select-String -Path $file -Pattern "^Result " | ForEach-Object {
            $line = $_.Line
            # Extract benchmark name and score
            if ($line -match 'Result ".*\.(.*?)":\s+([\d.]+)\s+([^\s]+)') {
                $bmName = $Matches[1]
                $score = $Matches[2]
                $unit = $Matches[3]
                Write-Output "  $bmName : $score $unit"
            }
        }
    }
}

Write-Output "`nResults: $resultsDir"