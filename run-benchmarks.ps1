# StreamMQ JMH 基准测试运行脚本
# 使用本地 Redis (127.0.0.1:6379, 无密码)
# 输出测试报告到 benchmark-results/

$ErrorActionPreference = "Stop"
$projectDir = "D:\learn\project\StreamMQ"
$resultsDir = "$projectDir\benchmark-results"
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"

New-Item -ItemType Directory -Path $resultsDir -Force | Out-Null

Write-Output "========================================"
Write-Output "StreamMQ JMH 基准测试"
Write-Output "时间: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Output "Redis: 127.0.0.1:6379 (本地)"
Write-Output "========================================"

# Step 1: 编译 benchmark 模块
Write-Output "`n[1/4] 编译 benchmark 模块..."
Push-Location $projectDir
mvn compile test-compile -pl streammq-benchmark -q 2>&1 | Out-Null
if ($LASTEXITCODE -ne 0) {
    Write-Error "编译失败"
    Pop-Location
    exit 1
}
Write-Output "编译成功"

# Step 2: 运行序列化基准（无需 Redis）
Write-Output "`n[2/4] 运行序列化基准测试..."
$serFile = "$resultsDir\serialization-$timestamp.txt"
Push-Location "$projectDir\streammq-benchmark"
mvn exec:java "-Dexec.mainClass=io.github.streammq.benchmark.SerializationBenchmark" "-Dexec.classpathScope=test" 2>&1 | Tee-Object $serFile
Pop-Location
Write-Output "序列化基准完成，结果: $serFile"

# Step 3: 运行发送基准（使用本地 Redis）
Write-Output "`n[3/4] 运行消息发送基准测试..."
$sendFile = "$resultsDir\send-$timestamp.txt"
Push-Location "$projectDir\streammq-benchmark"
mvn exec:java "-Dexec.mainClass=io.github.streammq.benchmark.StreamMessageTemplateBenchmark" "-Dexec.classpathScope=test" "-Dstreammq.redis.mode=local" 2>&1 | Tee-Object $sendFile
Pop-Location
Write-Output "发送基准完成，结果: $sendFile"

# Step 4: 运行消费基准（使用本地 Redis）
Write-Output "`n[4/4] 运行消息消费基准测试..."
$consFile = "$resultsDir\consume-$timestamp.txt"
Push-Location "$projectDir\streammq-benchmark"
mvn exec:java "-Dexec.mainClass=io.github.streammq.benchmark.StreamConsumerBenchmark" "-Dexec.classpathScope=test" "-Dstreammq.redis.mode=local" 2>&1 | Tee-Object $consFile
Pop-Location
Write-Output "消费基准完成，结果: $consFile"

# Step 5: 汇总
Write-Output "`n========================================"
Write-Output "所有基准测试完成！"
Write-Output "结果文件:"
Write-Output "  序列化: $serFile"
Write-Output "  发送:   $sendFile"
Write-Output "  消费:   $consFile"
Write-Output "========================================"

# 输出关键指标摘要
Write-Output "`n--- 关键指标摘要 ---"
Write-Output "`n[序列化性能]"
if (Test-Path $serFile) {
    Select-String -Path $serFile -Pattern "Score|acc" | ForEach-Object { $_.Line.Trim() }
}

Write-Output "`n[消息发送性能]"
if (Test-Path $sendFile) {
    Select-String -Path $sendFile -Pattern "Score|acc" | ForEach-Object { $_.Line.Trim() }
}

Write-Output "`n[消息消费性能]"
if (Test-Path $consFile) {
    Select-String -Path $consFile -Pattern "Score|acc" | ForEach-Object { $_.Line.Trim() }
}