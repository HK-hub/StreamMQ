/**
 * StreamMQ Spring Cloud Stream Binder 实现，允许 Spring Cloud Stream 用户以零代码改动接入 StreamMQ 作为消息后端。
 *
 * <h2>依赖说明</h2>
 *
 * <p>本模块直接依赖 {@code streammq-redisson}（非仅 {@code streammq-spring-boot-starter}）， 因此使用本 Binder 的用户会传递性引入
 * Redisson 作为 Redis 客户端。如果你的应用已经使用 Lettuce（Spring Boot 3 默认）， 需要明确接受两个 Redis 客户端共存—— 二者连接到同一 Redis
 * 实例是安全的，但会增加少量 jar 体积。
 *
 * <h2>当前限制</h2>
 *
 * <ul>
 *   <li><b>不支持分区生产</b>（{@code partitioned=true}）：配置后启动即报错，需改用 {@code shardingKey} 扩展属性实现路由； 未来 V2.0
 *       计划支持。
 *   <li><b>无独立 Broker 协议</b>：复用 StreamMQ 的 Redisson 适配层，不暴露 Spring Cloud Stream 抽象下的独立 connector——这与
 *       Kafka/RabbitMQ Binder 不同。
 *   <li><b>默认鉴权器为 DenyAll</b>：与 {@code streammq-spring-boot-starter} 对齐；调用方需注册自定义 {@code
 *       ManagementAuthenticator} Bean 以开放管理端点。
 * </ul>
 *
 * <h2>核心类</h2>
 *
 * <ul>
 *   <li>{@link io.github.streammq.spring.cloud.stream.binder.StreamMQMessageBinder} - Binder 核心， 桥接
 *       Spring Cloud Stream 与 StreamMQ 的生产/消费 API
 *   <li>{@link io.github.streammq.spring.cloud.stream.binder.StreamMQMessageHandler} - 生产端处理器， 将
 *       Spring Messaging 消息转换为 StreamMQ 消息并发送
 *   <li>{@link io.github.streammq.spring.cloud.stream.binder.StreamMQMessageProducer} - 消费端生产者， 注册
 *       StreamMQ 消费者并将收到的消息转换为 Spring Integration 消息输出
 *   <li>{@link io.github.streammq.spring.cloud.stream.binder.StreamMQBinderConfiguration} - Spring
 *       Boot 自动装配
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
package io.github.streammq.spring.cloud.stream.binder;
