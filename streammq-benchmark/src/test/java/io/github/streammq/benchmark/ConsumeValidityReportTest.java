/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.benchmark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * B1 回归用例：消费基准的口径判定必须是"积压被消费耗尽 → INVALID（非零退出）"，且记录缺失/损坏一律 fail closed（不允许把无法证明有效的轮次当成有效结论）。
 *
 * <p>被替代的旧规则（R4-38：{@code feeder ≥ 3 × consume}）在"每条消息一次同步 XADD 补货"的口径下结构性 必然失败（仓库自有数据
 * 0.66–0.78×），因此这些用例也在锁定"不再用补货/消费比例判有效性"。
 */
class ConsumeValidityReportTest {

    private static final int PAYLOAD_SIZE = 1024;
    private static final int BACKLOG = 50_000;
    private static final int FEEDER_THREADS = 2;

    @TempDir Path resultDir;

    @AfterEach
    void clearBenchmarkProperties() {
        System.clearProperty(StreamConsumerBenchmark.BACKLOG_PROPERTY);
        System.clearProperty(StreamConsumerBenchmark.FEEDER_THREADS_PROPERTY);
    }

    /** 稳态健康轮次：8200 条消息 / 82 批 = 满批，测量期间零空读。 */
    private static ConsumeValidityReport healthyReport() {
        ConsumeValidityReport report =
                new ConsumeValidityReport(PAYLOAD_SIZE, BACKLOG, FEEDER_THREADS);
        report.recordPreloadMillis(1_000);
        report.recordTrialMillis(11_000);
        report.recordReplenished(7_000);
        for (int i = 0; i < 82; i++) {
            report.recordBatch(100);
        }
        return report;
    }

    @Test
    void backlogNeverExhaustedIsValidAndCertified() throws IOException {
        ConsumeValidityReport report = healthyReport();

        assertTrue(report.valid());
        assertNull(report.invalidReason());
        assertEquals(100.0, report.avgBatchSize());
        assertFalse(report.supplyTight());

        report.writeTo(resultDir.resolve(validityFileName(PAYLOAD_SIZE)));
        assertEquals(
                0,
                StreamConsumerBenchmark.evaluateValidity(resultDir),
                "测量期间积压未被耗尽 → 必须判为有效并以 0 退出");
    }

    @Test
    void validityFileIsPerPayloadSizeSoForksDoNotOverwrite() {
        assertEquals(
                Path.of(
                        StreamConsumerBenchmark.RESULT_DIR,
                        StreamConsumerBenchmark.VALIDITY_FILE_PREFIX
                                + "1024"
                                + StreamConsumerBenchmark.VALIDITY_FILE_SUFFIX),
                StreamConsumerBenchmark.validityFile(1024));
        assertFalse(
                StreamConsumerBenchmark.validityFile(1024)
                        .equals(StreamConsumerBenchmark.validityFile(10240)),
                "每个 payloadSize 各一份记录：两个 fork 不能互相覆盖");
    }

    private static String validityFileName(int payloadSize) {
        return StreamConsumerBenchmark.VALIDITY_FILE_PREFIX
                + payloadSize
                + StreamConsumerBenchmark.VALIDITY_FILE_SUFFIX;
    }

    @Test
    void driedBacklogInvalidatesRunAndPointsToBacklogKnob() throws IOException {
        ConsumeValidityReport report = healthyReport();
        report.recordStarvedRead();

        assertFalse(report.valid());
        String reason = report.invalidReason();
        assertNotNull(reason);
        assertTrue(
                reason.contains("streammq.benchmark.backlog"),
                "无效原因必须提示加大 -Dstreammq.benchmark.backlog，实际：" + reason);

        report.writeTo(resultDir.resolve(validityFileName(PAYLOAD_SIZE)));
        assertEquals(
                1,
                StreamConsumerBenchmark.evaluateValidity(resultDir),
                "积压被耗尽 → 必须判为无效并以非零码退出（这正是 CI 不能静默放行的场景）");
    }

    @Test
    void recordWithoutConsumeBatchesIsInvalid() throws IOException {
        ConsumeValidityReport report =
                new ConsumeValidityReport(PAYLOAD_SIZE, BACKLOG, FEEDER_THREADS);
        report.recordPreloadMillis(1_000);

        assertFalse(report.valid());
        assertTrue(report.invalidReason().contains("no consume batch"));

        report.writeTo(resultDir.resolve(validityFileName(PAYLOAD_SIZE)));
        assertEquals(1, StreamConsumerBenchmark.evaluateValidity(resultDir));
    }

    @Test
    void missingRecordFailsClosed() {
        assertEquals(
                1,
                StreamConsumerBenchmark.evaluateValidity(resultDir),
                "没有任何口径记录时必须 fail closed，不能把未证明有效的轮次当有效");
    }

    @Test
    void unreadableRecordFailsClosed() throws IOException {
        Files.writeString(
                resultDir.resolve(StreamConsumerBenchmark.VALIDITY_FILE_PREFIX + "broken.json"),
                "{ not json",
                StandardCharsets.UTF_8);

        assertEquals(1, StreamConsumerBenchmark.evaluateValidity(resultDir));
    }

    @Test
    void recordRoundTripsThroughJson() throws IOException {
        ConsumeValidityReport report = healthyReport();
        report.recordStarvedRead();
        Path file = resultDir.resolve("round-trip.json");

        report.writeTo(file);
        ConsumeValidityReport restored = ConsumeValidityReport.read(file);

        assertEquals(report.payloadSize(), restored.payloadSize());
        assertEquals(report.backlog(), restored.backlog());
        assertEquals(report.feederThreads(), restored.feederThreads());
        assertEquals(report.preloadMillis(), restored.preloadMillis());
        assertEquals(report.trialMillis(), restored.trialMillis());
        assertEquals(report.consumed(), restored.consumed());
        assertEquals(report.replenished(), restored.replenished());
        assertEquals(report.batches(), restored.batches());
        assertEquals(report.starvedReads(), restored.starvedReads());
        assertFalse(restored.valid());
    }

    @Test
    void staleRecordsAreClearedButJmhResultFilesSurvive() throws IOException {
        Files.writeString(
                resultDir.resolve(StreamConsumerBenchmark.VALIDITY_FILE_PREFIX + "1024.json"),
                "{}",
                StandardCharsets.UTF_8);
        Files.writeString(
                resultDir.resolve(StreamConsumerBenchmark.VALIDITY_FILE_PREFIX + "10240.json"),
                "{}",
                StandardCharsets.UTF_8);
        Files.writeString(resultDir.resolve("jmh-consumer.json"), "[]", StandardCharsets.UTF_8);

        StreamConsumerBenchmark.clearStaleValidityRecords(resultDir);

        assertTrue(StreamConsumerBenchmark.listValidityRecords(resultDir).isEmpty());
        assertTrue(Files.exists(resultDir.resolve("jmh-consumer.json")), "清理残留记录不得误删 JMH 结果文件");
    }

    @Test
    void thinnedBatchesWarnButDoNotInvalidate() {
        ConsumeValidityReport report =
                new ConsumeValidityReport(PAYLOAD_SIZE, BACKLOG, FEEDER_THREADS);
        report.recordTrialMillis(11_000);
        for (int i = 0; i < 200; i++) {
            report.recordBatch(50);
        }

        assertTrue(report.supplyTight(), "平均批大小 50/100 应触发供应吃紧告警");
        assertTrue(report.valid(), "批被削薄是告警而非无效判定（避免把可用测量误杀成失败）");
    }

    @Test
    void unreadEstimateTracksBacklogAndReplenishment() {
        ConsumeValidityReport report =
                new ConsumeValidityReport(PAYLOAD_SIZE, BACKLOG, FEEDER_THREADS);

        assertEquals(BACKLOG, report.unread());
        report.recordReplenished(500);
        assertEquals(BACKLOG + 500, report.unread());
        report.recordBatch(100);
        assertEquals(BACKLOG + 400, report.unread());
    }

    @Test
    void referenceRatesComeFromPreloadAndTrialWindows() {
        ConsumeValidityReport report = healthyReport();

        assertEquals(50_000.0, report.preloadMsgPerSec(), 0.5);
        assertEquals(7_000.0 / 11.0, report.replenishMsgPerSec(), 0.5);
    }

    @Test
    void illegalSystemPropertiesFallBackToDefaults() {
        assertEquals(
                StreamConsumerBenchmark.DEFAULT_BACKLOG,
                StreamConsumerBenchmark.intProperty(
                        StreamConsumerBenchmark.BACKLOG_PROPERTY, 50_000));

        System.setProperty(StreamConsumerBenchmark.BACKLOG_PROPERTY, "200000");
        assertEquals(
                200_000,
                StreamConsumerBenchmark.intProperty(
                        StreamConsumerBenchmark.BACKLOG_PROPERTY, 50_000));

        for (String illegal : new String[] {"abc", "0", "-5", " "}) {
            System.setProperty(StreamConsumerBenchmark.BACKLOG_PROPERTY, illegal);
            assertEquals(
                    50_000,
                    StreamConsumerBenchmark.intProperty(
                            StreamConsumerBenchmark.BACKLOG_PROPERTY, 50_000),
                    "非法值 " + illegal + " 必须回退默认值（不能变成空跑或 OOM）");
        }
    }
}
