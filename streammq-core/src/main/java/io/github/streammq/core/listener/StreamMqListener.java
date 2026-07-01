package io.github.streammq.core.listener;

import io.github.streammq.core.enums.Action;
import io.github.streammq.core.message.Message;

/**
 * 自动 ACK 消费回调接口（并发消费）。
 *
 * <p>实现此接口并在类上标注 {@link io.github.streammq.core.annotation.StreamMqListener} 注解即可注册为并发消费者。
 * Listener 返回 {@link Action} 控制后续流程：
 * <ul>
 *   <li>{@link Action#SUCCESS} - 自动 ACK</li>
 *   <li>{@link Action#RECONSUME_LATER} - 进入重试</li>
 * </ul>
 *
 * <p>抛出 {@link RuntimeException} 等价于返回 {@link Action#RECONSUME_LATER}。
 *
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMqListener<T> {

    /**
     * 处理单条消息。
     *
     * @param message 消息载体
     * @param context 消费上下文
     * @return 处理结果动作
     * @throws Exception 业务异常，框架将其视为 {@link Action#RECONSUME_LATER}
     */
    Action onMessage(Message<T> message, ConsumerContext context) throws Exception;
}
