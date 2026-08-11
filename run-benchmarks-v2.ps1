# StreamMQ JMH 基准测试运行脚本（修复版）
# 使用本地 Redis (127.0.0.1:6379, 无密码)
# 输出测试报告到 benchmark-results/

$ErrorActionPreference = "Stop"
$projectDir = "D:\learn\project\StreamMQ"
$benchDir = "$projectDir\streammq-benchmark"
$resultsDir = "$projectDir\benchmark-results"
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"

New-Item -ItemType Directory -Path $resultsDir -Force | Out-Null

Write-Output "========================================"
Write-Output "StreamMQ JMH 基准测试 (无 Fork 模式)"
Write-Output "时间: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Output "Redis: 127.0.0.1:6379 (本地)"
Write-Output "========================================"

# 编译 benchmark 模块
Write-Output "`n[1/5] 编译 benchmark 模块..."
Push-Location $projectDir
mvn test-compile -pl streammq-benchmark -q 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Error "编译失败"
    Pop-Location
    exit 1
}
Write-Output "编译成功"

# 构建 classpath
$cpFile = "$benchDir\target\test-classpath.txt"
Push-Location "$benchDir"
mvn dependency:build-classpath "-Dmdep.outputFile=$cpFile" "-Dmdep.includeScope=test" -q 2>&1 | Out-Null
$classpath = Get-Content $cpFile -Raw
Pop-Location

# 编译后的测试类路径
$testClasses = "$benchDir\target\test-classes"
$mainClasses = "$benchDir\target\classes"
$fullCp = "$testClasses;$mainClasses;$classpath"

Write-Output "Classpath 构建完成"

# 运行各基准测试（禁用 Fork，使用同进程）
$benchmarks = @(
    @{Name = "SerializationBenchmark"; File = "serialization-$timestamp"},
    @{Name = "StreamMessageTemplateBenchmark"; File = "send-$timestamp"; JvmArgs = "-Dstreammq.redis.mode=local"},
    @{Name = "StreamConsumerBenchmark"; File = "consume-$timestamp"; JvmArgs = "-Dstreammq.redis.mode=local"}
)

foreach ($bench in $benchmarks) {
    Write-Output "`n========================================"
    Write-Output "运行: $($bench.Name)"
    Write-Output "========================================"

    $resultFile = "$resultsDir\$($bench.File).txt"
    $javaArgs = @(
        "-cp", $fullCp,
        $bench.JvmArgs,
        $bench.Name,
        "-wi", "2",
        "-i", "3",
        "-w", "2s",
        "-r", "3s",
        "-f", "1",
        "-o", $resultFile,
        "-rf"
    )

    & java @javaArgs 2>&1
    Write-Output "结果已保存: $resultFile"
}

Write-Output "`n========================================"
Write-Output "所有基准测试完成！"
Write-Output "========================================"

# 生成摘要
Write-Output "`n========== 性能测试摘要 =========="

foreach ($bench in $benchmarks) {
    $resultFile = "$resultsDir\$($bench.File).txt"
    Write-Output "`n--- $($bench.Name) ---"
    if (Test-Path $resultFile) {
        $content = Get-Content $resultFile -Raw
        # 提取 Benchmark 结果表
        if ($content -match "Benchmark\s+Mode") {
            $lines = $content -split "`n"
            $inTable = $false
            foreach ($line in $lines) {
                if ($line -match "Benchmark\s+Mode") { $inTable = $true }
                if ($inTable) { Write-Output $line.TrimEnd() }
                if ($inTable -and $line -match "---") { break }
            }
        }
        # 提取 "Threads" 行
        Select-String -Path $resultFile -Pattern "Threads:" | ForEach-Object { $_.Line.Trim() }
    } else {
        Write-Output "  (无结果)"
    }
}