/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.benchmark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;

/**
 * 消费基准口径记录：由 {@code consumeThroughput} 的每个 fork 在 trial 结束时落盘一份，{@link
 * StreamConsumerBenchmark#main} 汇总后判定该轮数字是否可用于容量规划与回归对比。
 *
 * <p>判定规则（B1 修复后，取代 R4-38 的"补货端 ≥ 3 × 消费端"——该规则在"每条消息一次同步 XADD 补货"的口径下 结构性必然失败：稳态消费吞吐只能等于补货吞吐）：
 *
 * <ol>
 *   <li>{@code starvedReads > 0} → 无效：测量期间出现过空读，说明预灌积压被消费耗尽、补货没跟上，该轮数字是 "补货端下界"而非消费能力（提示加大 {@code
 *       -Dstreammq.benchmark.backlog}）；
 *   <li>{@code batches <= 0} → 无效：整轮没有读到任何消息（记录缺失或 harness 没真正跑起来）；
 *   <li>平均批大小 &lt; {@value #SUPPLY_TIGHT_RATIO} × {@value #BATCH_SIZE} 只<b>告警</b>不判无效：供应侧吃紧
 *       会削薄批大小，需人工判断，但不把一次可用测量误杀成失败。
 * </ol>
 *
 * <p>并发：{@code recordBatch}/{@code recordStarvedRead} 由消费线程调用，{@code recordReplenished} 由补货线程
 * 调用，其余由 trial 收尾线程读取——全部走同步方法，避免 64 位计数撕裂。
 */
final class ConsumeValidityReport {

    /** 单条 XREADGROUP 的批大小（与 {@link StreamConsumerBenchmark} 一致），用于评估供应是否吃紧。 */
    static final int BATCH_SIZE = 100;

    /** 平均批大小低于该比例即认为"测量期间供应侧吃紧"（告警阈值，不参与有效性判定）。 */
    static final double SUPPLY_TIGHT_RATIO = 0.9;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final int payloadSize;
    private final int backlog;
    private final int feederThreads;

    private long preloadMillis;
    private long trialMillis;
    private long consumed;
    private long replenished;
    private long batches;
    private long starvedReads;

    ConsumeValidityReport(int payloadSize, int backlog, int feederThreads) {
        this.payloadSize = payloadSize;
        this.backlog = backlog;
        this.feederThreads = feederThreads;
    }

    /** 预灌阶段耗时（毫秒）：用于打印"补货端吞吐"参考指标。 */
    synchronized void recordPreloadMillis(long millis) {
        this.preloadMillis = millis;
    }

    /** 测量窗口（预热 + 测量）耗时（毫秒）：补货速率的参考分母。 */
    synchronized void recordTrialMillis(long millis) {
        this.trialMillis = millis;
    }

    /** 消费端每读完一批（非空 XREADGROUP）记一次，{@code size} 为该批实际处理条数。 */
    synchronized void recordBatch(int size) {
        this.consumed += size;
        this.batches++;
    }

    /** 消费端遇到空读（XREADGROUP 未返回任何消息）——积压被耗尽的直接证据。 */
    synchronized void recordStarvedRead() {
        this.starvedReads++;
    }

    /** 补货线程每成功提交一批记一次。 */
    synchronized void recordReplenished(int size) {
        this.replenished += size;
    }

    /** 当前未读积压估计（预灌 + 已补货 - 已消费）：补货线程据此做低水位判断。 */
    synchronized long unread() {
        return (long) backlog + replenished - consumed;
    }

    int payloadSize() {
        return payloadSize;
    }

    int backlog() {
        return backlog;
    }

    int feederThreads() {
        return feederThreads;
    }

    synchronized long preloadMillis() {
        return preloadMillis;
    }

    synchronized long trialMillis() {
        return trialMillis;
    }

    synchronized long consumed() {
        return consumed;
    }

    synchronized long replenished() {
        return replenished;
    }

    synchronized long batches() {
        return batches;
    }

    synchronized long starvedReads() {
        return starvedReads;
    }

    /** 平均批大小：≈ {@value #BATCH_SIZE} 说明消费端始终有满批可读（供应不构成瓶颈）。 */
    synchronized double avgBatchSize() {
        return batches == 0 ? 0.0 : (double) consumed / batches;
    }

    /** 积压构建阶段吞吐（补货端参考指标，不参与判定）。 */
    synchronized double preloadMsgPerSec() {
        return preloadMillis <= 0 ? 0.0 : backlog * 1000.0 / preloadMillis;
    }

    /** 测量窗口内补货线程的持续吞吐（参考指标：应不低于消费吞吐，否则会看到空读）。 */
    synchronized double replenishMsgPerSec() {
        return trialMillis <= 0 ? 0.0 : replenished * 1000.0 / trialMillis;
    }

    /** 平均批大小是否被供应侧削薄（告警，不判无效）。 */
    synchronized boolean supplyTight() {
        return batches > 0 && avgBatchSize() < SUPPLY_TIGHT_RATIO * BATCH_SIZE;
    }

    /** 无效原因；返回 {@code null} 表示该轮数字有效。 */
    synchronized String invalidReason() {
        if (batches <= 0) {
            return "no consume batch was recorded: consumeThroughput did not run (or the validity"
                    + " record is incomplete), so this run cannot be certified.";
        }
        if (starvedReads > 0) {
            return "the pre-loaded backlog ran dry ("
                    + starvedReads
                    + " empty XREADGROUP during the trial with backlog="
                    + backlog
                    + "): the refill path, not the consumer, capped consumeThroughput."
                    + " Raise -Dstreammq.benchmark.backlog (currently "
                    + backlog
                    + ") and/or -Dstreammq.benchmark.feederThreads (currently "
                    + feederThreads
                    + "), then re-run.";
        }
        return null;
    }

    synchronized boolean valid() {
        return invalidReason() == null;
    }

    /** 单行摘要：main() 打印、人工回填报告时可直接引用。 */
    synchronized String summaryLine() {
        return String.format(
                Locale.ROOT,
                "payload=%dB backlog=%d feederThreads=%d preload=%.0f msg/s trial=%.1fs consumed=%d"
                    + " replenished=%d (%.0f msg/s) avgBatch=%.1f/%d starvedReads=%d supplyTight=%s"
                    + " valid=%s",
                payloadSize,
                backlog,
                feederThreads,
                preloadMsgPerSec(),
                trialMillis / 1000.0,
                consumed,
                replenished,
                replenishMsgPerSec(),
                avgBatchSize(),
                BATCH_SIZE,
                starvedReads,
                supplyTight(),
                valid());
    }

    /** 落盘为 JSON（供 main() 跨 fork 汇总；每个 payloadSize 一份，避免 fork 之间互相覆盖）。 */
    synchronized void writeTo(Path file) throws IOException {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("payloadSize", payloadSize);
        node.put("backlog", backlog);
        node.put("feederThreads", feederThreads);
        node.put("preloadMillis", preloadMillis);
        node.put("preloadMsgPerSec", preloadMsgPerSec());
        node.put("trialMillis", trialMillis);
        node.put("consumed", consumed);
        node.put("replenished", replenished);
        node.put("replenishMsgPerSec", replenishMsgPerSec());
        node.put("batches", batches);
        node.put("avgBatchSize", avgBatchSize());
        node.put("starvedReads", starvedReads);
        node.put("supplyTight", supplyTight());
        node.put("valid", valid());
        node.put("invalidReason", invalidReason());
        if (file.getParent() != null) {
            Files.createDirectories(file.getParent());
        }
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(file.toFile(), node);
    }

    /** 读取落盘记录；缺失字段按 0 处理（{@code batches <= 0} 会被判为无效，不会伪装成有效轮次）。 */
    static ConsumeValidityReport read(Path file) throws IOException {
        JsonNode node = MAPPER.readTree(Files.readAllBytes(file));
        if (!node.isObject()) {
            throw new IOException("not a consume validity JSON object: " + file);
        }
        ConsumeValidityReport report =
                new ConsumeValidityReport(
                        node.path("payloadSize").asInt(0),
                        node.path("backlog").asInt(0),
                        node.path("feederThreads").asInt(0));
        report.preloadMillis = node.path("preloadMillis").asLong(0);
        report.trialMillis = node.path("trialMillis").asLong(0);
        report.consumed = node.path("consumed").asLong(0);
        report.replenished = node.path("replenished").asLong(0);
        report.batches = node.path("batches").asLong(0);
        report.starvedReads = node.path("starvedReads").asLong(0);
        return report;
    }
}
