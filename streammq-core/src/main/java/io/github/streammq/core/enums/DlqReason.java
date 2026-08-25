/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.enums;

/**
 * 死信（DLQ）原因枚举，定义消息进入死信队列的标准化原因标识。
 *
 * <p>该值会作为 Stream Entry 字段 {@code dlqReason} 写入消息属性， 是跨模块的线上协议契约： 生产侧（重试调度器、DLQ
 * 处理器）写入，消费/展示侧（管理端点、拓扑追踪）读取。
 *
 * @author StreamMQ Contributors
 * @since 1.1.0
 */
public enum DlqReason {

    /** 超过最大重试次数（并发消费） */
    MAX_RETRY("maxRetry"),

    /** 超过最大重试次数（顺序消费） */
    MAX_RETRY_ORDERLY("maxRetryOrderly"),

    /** 二级死信队列转移 */
    SECONDARY_DLQ("secondaryDlq"),

    /** 反序列化失败（毒丸消息，无法转换为业务消息） */
    DESERIALIZE("deserialize"),

    /** 原因未知（字段缺失或无法识别） */
    UNKNOWN("unknown");

    private final String code;

    DlqReason(String code) {
        this.code = code;
    }

    /**
     * 返回线上协议使用的原因编码。
     *
     * @return 原因编码字符串
     */
    public String getCode() {
        return code;
    }

    /**
     * 根据协议编码解析枚举，未匹配时返回 {@link #UNKNOWN}。
     *
     * @param code 协议编码
     * @return 对应的 DlqReason
     */
    public static DlqReason ofCode(String code) {
        for (DlqReason reason : values()) {
            if (reason.code.equals(code)) {
                return reason;
            }
        }
        return UNKNOWN;
    }
}
