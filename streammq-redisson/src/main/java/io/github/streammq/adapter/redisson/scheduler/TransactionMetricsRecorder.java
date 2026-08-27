/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.scheduler;

import io.github.streammq.core.metrics.StreamMQMetrics;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 事务指标记录器：封装 commit / rollback / check 三类指标调用。
 *
 * <p>所有方法对 {@code metrics} 为 null 友好（no-op），避免业务调用方反复判空。 指标收集器自身抛出异常时记录 DEBUG 日志，
 * 不影响业务主流程。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class TransactionMetricsRecorder {

    private static final Logger LOG = LoggerFactory.getLogger(TransactionMetricsRecorder.class);

    private final StreamMQMetrics metrics;

    public TransactionMetricsRecorder(StreamMQMetrics metrics) {
        this.metrics = metrics;
    }

    /** 记录事务提交指标。 */
    public void recordCommit(String txGroup) {
        if (Objects.isNull(metrics)) {
            return;
        }
        try {
            metrics.recordTransactionCommit(txGroup);
        } catch (Exception ex) {
            LOG.debug("Metrics collection failed for transaction.commit: txGroup={}", txGroup, ex);
        }
    }

    /** 记录事务回滚指标。 */
    public void recordRollback(String txGroup) {
        if (Objects.isNull(metrics)) {
            return;
        }
        try {
            metrics.recordTransactionRollback(txGroup);
        } catch (Exception ex) {
            LOG.debug(
                    "Metrics collection failed for transaction.rollback: txGroup={}", txGroup, ex);
        }
    }

    /**
     * 记录事务回查指标。
     *
     * @param txGroup 事务组
     * @param result 回查结果（COMMIT / ROLLBACK / UNKNOWN 等标签）
     */
    public void recordCheck(String txGroup, String result) {
        if (Objects.isNull(metrics)) {
            return;
        }
        try {
            metrics.recordTransactionCheck(txGroup, result);
        } catch (Exception ex) {
            LOG.debug(
                    "Metrics collection failed for transaction.check: txGroup={}, result={}",
                    txGroup,
                    result,
                    ex);
        }
    }
}
