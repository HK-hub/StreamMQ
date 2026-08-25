/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.service;

import io.github.streammq.core.template.StreamMessageTemplate;

/**
 * MQ 消息发送服务接口，封装 {@link StreamMessageTemplate} 提供更简洁的 API。
 *
 * <p>用户无需手动构造消息对象，只需传入 body 和 topic 即可发送。 类似 RocketMQ 的 DefaultMQPushProducer 封装层。
 *
 * <p><b>接口设计</b>：本接口继承了多个细粒度接口，用户可以按需注入：
 *
 * <ul>
 *   <li>{@link BasicMessageService} - 基础同步发送
 *   <li>{@link AsyncMessageService} - 异步发送
 *   <li>{@link OnewayMessageService} - 单向发送
 *   <li>{@link BatchMessageService} - 批量发送
 *   <li>{@link DelayMessageService} - 延时消息
 *   <li>{@link TransactionMessageService} - 事务消息
 * </ul>
 *
 * <p>遵循「依赖接口而非实现」原则，业务代码应注入 {@link StreamMessageService}， 默认实现为 {@link
 * DefaultStreamMessageService}。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 * @see DefaultStreamMessageService
 */
public interface StreamMessageService
        extends BasicMessageService,
                AsyncMessageService,
                OnewayMessageService,
                BatchMessageService,
                DelayMessageService,
                TransactionMessageService {}
