/**
 * StreamMQ Spring Cloud Stream Binder 实现，允许 Spring Cloud Stream 用户以零代码改动接入 StreamMQ 作为消息后端。
 *
 * <p>本包基于 Spring Cloud Stream Binder SPI 实现，核心类说明：
 * <ul>
 *   <li>{@link io.github.streammq.spring.cloud.stream.binder.StreamMQMessageBinder} - Binder 核心，
 *       桥接 Spring Cloud Stream 与 StreamMQ 的生产/消费 API</li>
 *   <li>{@link io.github.streammq.spring.cloud.stream.binder.StreamMQMessageHandler} - 生产端处理器，
 *       将 Spring Messaging 消息转换为 StreamMQ 消息并发送</li>
 *   <li>{@link io.github.streammq.spring.cloud.stream.binder.StreamMQMessageProducer} - 消费端生产者，
 *       注册 StreamMQ 消费者并将收到的消息转换为 Spring Integration 消息输出</li>
 *   <li>{@link io.github.streammq.spring.cloud.stream.binder.StreamMQBinderConfiguration} - Spring Boot 自动装配</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
package io.github.streammq.spring.cloud.stream.binder;
