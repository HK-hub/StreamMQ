# StreamMQ JMH Benchmark Runner
# Uses local Redis at 127.0.0.1:6379

$ErrorActionPreference = "Stop"
$projectDir = "D:\learn\project\StreamMQ"
$benchDir = "$projectDir\streammq-benchmark"
$resultsDir = "$projectDir\benchmark-results"
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"

New-Item -ItemType Directory -Path $resultsDir -Force | Out-Null

Write-Output "========================================"
Write-Output "StreamMQ JMH Benchmarks (No-Fork Mode)"
Write-Output "Time: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Output "Redis: 127.0.0.1:6379 (local)"
Write-Output "========================================"

# Step 1: Compile
Write-Output "`n[1/4] Compiling benchmark module..."
Push-Location $projectDir
mvn test-compile -pl streammq-benchmark -q 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Error "Compilation failed"
    Pop-Location
    exit 1
}
Write-Output "Compilation OK"

# Step 2: Build classpath
Write-Output "`n[2/4] Building classpath..."
$cpFile = "$benchDir\target\test-classpath.txt"
Push-Location $benchDir
mvn dependency:build-classpath "-Dmdep.outputFile=$cpFile" "-Dmdep.includeScope=test" -q 2>&1 | Out-Null
$classpath = (Get-Content $cpFile -Raw).Trim()
Pop-Location

$testClasses = "$benchDir\target\test-classes"
$mainClasses = "$benchDir\target\classes"
$fullCp = "$testClasses;$mainClasses;$classpath"

Write-Output "Classpath ready ($($fullCp.Length) chars)"

# Step 3: Run benchmarks
Write-Output "`n[3/4] Running benchmarks..."

# 3a: Serialization (no Redis needed)
Write-Output "`n--- SerializationBenchmark ---"
$serResult = "$resultsDir\serialization-$timestamp.txt"
& java -cp $fullCp org.openjdk.jmh.Main "io.github.streammq.benchmark.SerializationBenchmark" -wi 2 -i 3 -w 2s -r 3s -f 1 -o $serResult
Write-Output "Done: $serResult"

# 3b: Send benchmarks (uses local Redis)
Write-Output "`n--- StreamMessageTemplateBenchmark ---"
$sendResult = "$resultsDir\send-$timestamp.txt"
& java -Dstreammq.redis.mode=local -cp $fullCp org.openjdk.jmh.Main "io.github.streammq.benchmark.StreamMessageTemplateBenchmark" -wi 2 -i 3 -w 2s -r 3s -f 1 -o $sendResult
Write-Output "Done: $sendResult"

# 3c: Consume benchmarks (uses local Redis)
Write-Output "`n--- StreamConsumerBenchmark ---"
$consResult = "$resultsDir\consume-$timestamp.txt"
& java -Dstreammq.redis.mode=local -cp $fullCp org.openjdk.jmh.Main "io.github.streammq.benchmark.StreamConsumerBenchmark" -wi 2 -i 3 -w 2s -r 3s -f 1 -o $consResult
Write-Output "Done: $consResult"

# Step 4: Summary
Write-Output "`n========================================"
Write-Output "ALL BENCHMARKS COMPLETE"
Write-Output "========================================"

# Show summary table
Write-Output "`n========== PERFORMANCE SUMMARY =========="

$allResults = @(
    @{Name="Serialization"; File=$serResult},
    @{Name="Message Send"; File=$sendResult},
    @{Name="Message Consume"; File=$consResult}
)

foreach ($r in $allResults) {
    Write-Output "`n--- $($r.Name) ---"
    if (Test-Path $r.File) {
        $content = Get-Content $r.File
        $inResult = $false
        foreach ($line in $content) {
            if ($line -match "Benchmark\s+Mode\s+Cnt") { $inResult = $true }
            if ($inResult) { Write-Output $line }
            if ($inResult -and $line -match "^$" -or ($inResult -and $line -match "Threads:")) { break }
        }
        # Also show threads info
        $threadLine = Select-String -Path $r.File -Pattern "Threads:" | Select-Object -First 1
        if ($threadLine) { Write-Output $threadLine.Line }
    }
}

Write-Output "`nResults saved in: $resultsDir"