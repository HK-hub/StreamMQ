package io.github.streammq.core.policy;

import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.message.Message;

/**
 * 死信队列消费失败处理器 SPI（对齐 RocketMQ 死信终端 + 告警介入理念）。
 *
 * <p>当 {@code dlqMode=true} 的消费者消费死信消息也失败（返回
 * {@link io.github.streammq.core.enums.ConsumeAction#RECONSUME_LATER} / DEFER 或抛出异常）时，
 * 框架不再将死信消息再次写入重试/DLQ 循环（避免无限循环），而是：
 * <ol>
 *   <li>调用本处理器的 {@link #handleFailure} 执行告警/持久化/重放等自定义逻辑</li>
 *   <li>随后 ACK 丢弃该死信消息（从 DLQ PEL 移除）</li>
 * </ol>
 *
 * <p>默认实现 {@code LogAndDropDlqFailureHandler} 仅打印 ERROR 日志。
 * 用户可实现本接口接入钉钉/飞书告警、写入持久存储、转人工队列等，并通过
 * {@code @StreamMQConsumer(dlqFailureHandler = MyHandler.class)} 或全局配置
 * {@code streammq.dlq.failure-handler} 注入。
 *
 * <p>实现需注意：
 * <ul>
 *   <li>{@code handleFailure} 抛出的异常会被框架捕获并降级为日志，不会阻止 ACK 丢弃</li>
 *   <li>本处理器在死信消费线程内同步执行，应避免耗时阻塞</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface DlqFailureHandler {

    /**
     * 处理 DLQ 消费失败的消息。
     *
     * @param message 消费失败的死信消息
     * @param reg Listener 注册信息（{@code reg.isDlqMode() == true}）
     * @param cause 失败原因；消费者返回 RECONSUME_LATER/DEFER 时为 {@code null}，抛出异常时为该异常
     */
    void handleFailure(Message<?> message, ListenerRegistration<?> reg, Throwable cause);

    /**
     * 处理器名称（用于监控与日志）。
     *
     * @return 名称
     */
    default String name() {
        return getClass().getSimpleName();
    }
}
