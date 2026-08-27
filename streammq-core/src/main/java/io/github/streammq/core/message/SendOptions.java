/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.message;

import io.github.streammq.core.template.StreamMessageTemplate;
import java.io.Serial;
import java.io.Serializable;
import lombok.Getter;

/**
 * 消息发送选项（Builder 模式），用于统一替代 {@link StreamMessageTemplate} 中的多个参数重载方法。
 *
 * <p>使用示例：
 *
 * <pre>{@code
 * SendOptions options = SendOptions.builder()
 *     .timeoutMillis(5000)
 *     .retryTimes(3)
 *     .build();
 * template.syncSend(message, options);
 * }</pre>
 *
 * <p>所有字段均为可选，未设置时使用 {@link StreamMessageTemplate} 的默认值。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Getter
public final class SendOptions implements Serializable {

    @Serial private static final long serialVersionUID = 1L;

    /** 默认发送超时（毫秒） */
    public static final long DEFAULT_TIMEOUT_MILLIS =
            StreamMessageTemplate.DEFAULT_SEND_TIMEOUT_MILLIS;

    /** 默认同步重试次数 */
    public static final int DEFAULT_RETRY_TIMES = StreamMessageTemplate.DEFAULT_SYNC_RETRY_TIMES;

    /** 发送超时毫秒数（-1 表示未设置，其余取值必须 &gt; 0） */
    private final long timeoutMillis;

    /** 同步发送重试次数（-1 表示未设置，其余取值必须 &gt;= 0） */
    private final int retryTimes;

    private SendOptions(long timeoutMillis, int retryTimes) {
        this.timeoutMillis = timeoutMillis;
        this.retryTimes = retryTimes;
    }

    /**
     * 返回使用全部默认值（超时 3000ms、同步重试 2 次）的共享实例。
     *
     * <p>实例不可变且语义等价，可安全复用。
     *
     * @return 默认 SendOptions
     */
    public static SendOptions defaults() {
        return DEFAULT;
    }

    /** 共享的默认实例（全字段未设置，按默认值生效）。 */
    private static final SendOptions DEFAULT = new SendOptions(-1, -1);

    /**
     * 以指定超时与重试次数创建选项。
     *
     * <p>参数契约与 Builder 一致：{@code timeoutMillis} 必须为 -1（未设置）或 &gt; 0；{@code retryTimes} 必须为 -1（未设置）或
     * &gt;= 0。0 不再是合法的超时值（此前 0 会被静默替换为默认值，语义含糊）。
     *
     * @param timeoutMillis 超时毫秒数（-1 使用默认）
     * @param retryTimes 重试次数（-1 使用默认，0 表示字面上的零次重试）
     * @return SendOptions
     * @throws IllegalArgumentException 如果取值不在合法范围内
     */
    public static SendOptions of(long timeoutMillis, int retryTimes) {
        if (timeoutMillis != -1 && timeoutMillis <= 0) {
            throw new IllegalArgumentException(
                    "timeoutMillis must be -1 (unset) or positive, got: " + timeoutMillis);
        }
        if (retryTimes != -1 && retryTimes < 0) {
            throw new IllegalArgumentException(
                    "retryTimes must be -1 (unset) or >= 0, got: " + retryTimes);
        }
        return new SendOptions(timeoutMillis, retryTimes);
    }

    /**
     * 创建 Builder。
     *
     * @return 新的 Builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * 返回实际生效的超时毫秒数（将 <= 0 的值替换为默认值）。
     *
     * @return 有效的超时毫秒数
     */
    public long effectiveTimeoutMillis() {
        return timeoutMillis > 0 ? timeoutMillis : DEFAULT_TIMEOUT_MILLIS;
    }

    /**
     * 返回实际生效的重试次数（将 < 0 的值替换为默认值）。
     *
     * @return 有效的重试次数
     */
    public int effectiveRetryTimes() {
        return retryTimes >= 0 ? retryTimes : DEFAULT_RETRY_TIMES;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SendOptions that)) {
            return false;
        }
        return timeoutMillis == that.timeoutMillis && retryTimes == that.retryTimes;
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(timeoutMillis, retryTimes);
    }

    @Override
    public String toString() {
        return "SendOptions{timeoutMillis=" + timeoutMillis + ", retryTimes=" + retryTimes + '}';
    }

    /** SendOptions Builder。 */
    public static final class Builder {
        private long timeoutMillis = -1;
        private int retryTimes = -1;

        private Builder() {}

        /**
         * 设置发送超时毫秒数。
         *
         * @param timeoutMillis 超时毫秒数（必须 &gt; 0；不调用则使用默认值 3000ms）
         * @return this
         * @throws IllegalArgumentException 如果 {@code timeoutMillis <= 0}
         */
        public Builder timeoutMillis(long timeoutMillis) {
            if (timeoutMillis <= 0) {
                throw new IllegalArgumentException(
                        "timeoutMillis must be positive, got: " + timeoutMillis);
            }
            this.timeoutMillis = timeoutMillis;
            return this;
        }

        /**
         * 设置同步发送重试次数。
         *
         * @param retryTimes 重试次数（必须 &gt;= 0，0 表示字面上的零次重试；不调用则使用默认值 2 次）
         * @return this
         * @throws IllegalArgumentException 如果 {@code retryTimes < 0}
         */
        public Builder retryTimes(int retryTimes) {
            if (retryTimes < 0) {
                throw new IllegalArgumentException("retryTimes must be >= 0, got: " + retryTimes);
            }
            this.retryTimes = retryTimes;
            return this;
        }

        /**
         * 构建 SendOptions。
         *
         * @return SendOptions 实例
         */
        public SendOptions build() {
            return new SendOptions(timeoutMillis, retryTimes);
        }
    }
}
