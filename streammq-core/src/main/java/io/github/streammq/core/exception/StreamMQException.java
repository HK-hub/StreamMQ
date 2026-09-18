/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.exception;

import java.io.Serial;

/**
 * StreamMQ 异常基类，所有 StreamMQ 内部异常均继承此类。
 *
 * <p>继承 {@link RuntimeException}，所有方法签名不强制声明 throws。
 *
 * <p><b>异常层次结构（06-1.3）：</b>
 *
 * <ul>
 *   <li>{@link StreamMQException} - 基类，所有 StreamMQ 异常的父类
 *   <li>{@link StreamMQClientException} - 客户端配置错误（{@code streammq.*} 配置项非法、必填配置缺失等）
 *   <li>{@link StreamMQBrokerException} - Broker 端错误（Redis 操作失败、连接超时等）
 *   <li>{@link TransactionException} - 事务消息相关异常
 *   <li>{@link SerializationException} - 序列化/反序列化异常
 *   <li>{@link ProducerTimeoutException} - 生产者发送超时
 *   <li>{@link ConsumerInterruptedException} - 消费线程被中断（容器停止 / 消费超时），由适配层在与 Broker 交互的消费路径抛出，core
 *       仅提供类型
 *   <li>{@link OrderlyShardBusyException} - 顺序消费分片锁被其它实例持有：本条消息未被处理，
 *       稍后重试且不消耗业务重试预算，由适配层的分片锁管理器抛出，core 仅提供类型
 * </ul>
 *
 * <p><b>异常口径（0.1.2 起准确化）：</b>
 *
 * <ul>
 *   <li><b>参数 / 契约违反</b>使用标准 JDK 异常（{@link IllegalArgumentException} / {@link
 *       IllegalStateException} / {@link NullPointerException}），与 JDK 集合 / Builder
 *       生态一致，调用方无需捕获框架类型即可防御性编程
 *   <li><b>框架运行时错误</b>（Broker 交互、序列化、事务、发送超时、消费中断）使用 {@link StreamMQException} 子类，业务层可统一捕获 {@code
 *       StreamMQException} 处理框架错误
 *   <li><b>配置错误</b>（外部配置项非法，用户无法通过改调用参数修复）使用 {@link StreamMQClientException}
 *   <li>内部异常应通过 cause 链传递，保留原始异常信息
 *   <li>公开 API 不应返回 null
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StreamMQException extends RuntimeException {

    @Serial private static final long serialVersionUID = 1L;

    /**
     * 构造异常。
     *
     * @param message 错误信息
     */
    public StreamMQException(String message) {
        super(message);
    }

    /**
     * 构造异常。
     *
     * @param message 错误信息
     * @param cause 原始异常
     */
    public StreamMQException(String message, Throwable cause) {
        super(message, cause);
    }
}
