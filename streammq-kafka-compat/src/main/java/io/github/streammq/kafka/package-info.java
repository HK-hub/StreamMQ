/**
 * StreamMQ Kafka API 风格兼容层。
 *
 * <h2>设计目标</h2>
 * <p>提供与 Kafka Client 同名的类（{@code KafkaProducer}/{@code KafkaConsumer}/{@code ProducerRecord}/
 * {@code ConsumerRecord}/{@code KafkaCompatTemplate}），使已有 Kafka 业务代码只需修改 import 即可迁移到
 * StreamMQ（Redis Stream 底层）。
 *
 * <h2>核心类</h2>
 * <ul>
 *   <li>{@link io.github.streammq.kafka.KafkaProducer} - 对齐 Kafka {@code KafkaProducer}，底层委托 {@code StreamMessageTemplate}</li>
 *   <li>{@link io.github.streammq.kafka.KafkaConsumer} - 对齐 Kafka {@code KafkaConsumer}，底层通过 {@code StreamMQListenerFactory} 创建 {@code RedissonStreamListener}</li>
 *   <li>{@link io.github.streammq.kafka.ProducerRecord} - 对齐 Kafka {@code ProducerRecord}，封装 topic/key/value/headers</li>
 *   <li>{@link io.github.streammq.kafka.ConsumerRecord} - 对齐 Kafka {@code ConsumerRecord}，封装 topic/partition/offset/key/value/headers</li>
 *   <li>{@link io.github.streammq.kafka.KafkaCompatTemplate} - 对齐 Spring Kafka {@code KafkaTemplate}，提供简化发送 API</li>
 * </ul>
 *
 * <h2>限制说明</h2>
 * <p>本模块仅提供 API 风格兼容，不实现 Kafka 线网协议。不支持以下 Kafka 特性：
 * <ul>
 *   <li>事务消息（请直接使用 {@code StreamMessageTemplate#executeInTransaction}）</li>
 *   <li>Rebalance / Group Coordinator 协议</li>
 *   <li>Exactly-Once 语义（Kafka EOS）</li>
 *   <li>压缩（compression）</li>
 *   <li>拦截器（interceptors）</li>
 * </ul>
 *
 * <h2>使用示例</h2>
 * <pre>{@code
 * // === 生产者 ===
 * StreamMessageTemplate template = ...; // 由业务层注入
 * KafkaProducer<String, String> producer = new KafkaProducer<>(template);
 * ProducerRecord<String, String> record = new ProducerRecord<>("my-topic", "hello");
 * SendResult result = producer.send(record);
 * producer.close();
 *
 * // === 消费者 ===
 * StreamMQListenerFactory listenerFactory = new RedissonStreamListenerFactory(redisson, converter);
 * KafkaConsumer<String, String> consumer = new KafkaConsumer<>(listenerFactory, "my-group");
 * consumer.subscribe(List.of("my-topic"));
 * List<ConsumerRecord<String, String>> records = consumer.poll(Duration.ofSeconds(5));
 * for (ConsumerRecord<String, String> r : records) {
 *     System.out.println(r.value());
 * }
 * consumer.close();
 *
 * // === Template ===
 * KafkaCompatTemplate<String, String> kt = new KafkaCompatTemplate<>(template, "default-topic");
 * kt.send("my-topic", "hello");       // 发送到指定 topic
 * kt.sendDefault("world");            // 发送到默认 topic
 * }</pre>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
package io.github.streammq.kafka;
