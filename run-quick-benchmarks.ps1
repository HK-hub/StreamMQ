# StreamMQ Quick JMH Benchmark Runner
$ErrorActionPreference = "Stop"
$projectDir = "D:\learn\project\StreamMQ"
$benchDir = "$projectDir\streammq-benchmark"
$resultsDir = "$projectDir\benchmark-results"
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"

New-Item -ItemType Directory -Path $resultsDir -Force | Out-Null

Write-Output "========================================"
Write-Output "StreamMQ Quick JMH Benchmarks"
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
    Write-Output "`nRunning: $name"
    & java $jvmArgs -cp "$testClasses;$mainClasses;$classpath" org.openjdk.jmh.Main $name -wi 2 -i 3 -w 2s -r 3s -f 0 -o $outputFile
    Write-Output "Completed: $name"
}

# Run send benchmark
$sendFile = "$resultsDir\send-$timestamp.txt"
Run-Benchmark "io.github.streammq.benchmark.StreamMessageTemplateBenchmark" $sendFile

# Run consume benchmark
$consFile = "$resultsDir\consume-$timestamp.txt"
Run-Benchmark "io.github.streammq.benchmark.StreamConsumerBenchmark" $consFile

Write-Output "`n========================================"
Write-Output "BENCHMARKS COMPLETE"
Write-Output "========================================"

# Extract results
foreach ($file in @($sendFile, $consFile)) {
    if (Test-Path $file) {
        Write-Output "`n--- $(Split-Path $file -Leaf) ---"
        Select-String -Path $file -Pattern "^Result " | ForEach-Object {
            $line = $_.Line
            if ($line -match 'Result ".*\.(.*?)":\s+([\d.]+)\s+([^\s]+)') {
                Write-Output "  $($Matches[1]) : $($Matches[2]) $($Matches[3])"
            }
        }
    }
}

Write-Output "`nResults saved to: $resultsDir"