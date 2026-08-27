/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.diagnostics.support;

import io.github.streammq.core.trace.TraceRecord;
import io.github.streammq.core.trace.TraceType;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 追踪记录过滤工具，供各 {@code Analyzer} 共享使用。
 *
 * <p>所有方法均为纯函数无副作用，可安全并发调用。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class TraceRecordFilters {

    private TraceRecordFilters() {}

    /**
     * 从追踪记录列表中过滤出指定消费者组的消费记录。
     *
     * @param records 追踪记录列表
     * @param group 消费者组
     * @return 消费记录列表
     */
    public static List<TraceRecord> filterConsumeByGroup(
            List<TraceRecord> records, String group) {
        List<TraceRecord> result = new ArrayList<>();
        for (TraceRecord record : records) {
            if (Objects.nonNull(record.type())
                    && record.type() == TraceType.CONSUME
                    && Objects.equals(record.group(), group)) {
                result.add(record);
            }
        }
        return result;
    }

    /**
     * 从追踪记录列表中过滤出发送记录。
     *
     * @param records 追踪记录列表
     * @return 发送记录列表
     */
    public static List<TraceRecord> filterSend(List<TraceRecord> records) {
        List<TraceRecord> result = new ArrayList<>();
        for (TraceRecord record : records) {
            if (Objects.nonNull(record.type()) && record.type() == TraceType.SEND) {
                result.add(record);
            }
        }
        return result;
    }

    /**
     * 从追踪记录列表中过滤出失败的消费记录。
     *
     * @param records 追踪记录列表
     * @return 失败的消费记录列表
     */
    public static List<TraceRecord> filterFailedConsume(List<TraceRecord> records) {
        List<TraceRecord> result = new ArrayList<>();
        for (TraceRecord record : records) {
            if (Objects.nonNull(record.type())
                    && record.type() == TraceType.CONSUME
                    && !record.success()) {
                result.add(record);
            }
        }
        return result;
    }
}
