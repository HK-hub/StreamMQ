package io.github.streammq.amqp;

import io.github.streammq.core.listener.ListenerConfig;
import io.github.streammq.core.listener.StreamMQListener;
import io.github.streammq.core.listener.StreamMQListenerFactory;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.MessageId;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.template.StreamMessageTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * AMQP 风格 Channel 抽象，提供 RabbitMQ / AMQP 风格的消息收发 API。
 *
 * <p>概念映射（AMQP → StreamMQ）：
 * <ul>
 *   <li>Exchange → Topic（消息发送目标）</li>
 *   <li>Queue → ConsumerGroup（消费组）</li>
 *   <li>Binding（Exchange → Queue + RoutingKey）→ ConsumerGroup 订阅 Topic，按 Tag 过滤</li>
 *   <li>Routing Key → Tag（消息二级标签，用于消费端过滤）</li>
 * </ul>
 *
 * <p>使用示例：
 * <pre>{@code
 * AmqpChannel ch = client.newChannel();
 * ch.exchangeDeclare("my-exchange", "direct", true);
 * ch.queueDeclare("my-queue", true, false, false, null);
 * ch.queueBind("my-queue", "my-exchange", "order.created");
 * ch.basicPublish("my-exchange", "order.created", null, body);
 * ch.basicConsume("my-queue", true, msg -> {
 *     System.out.println(new String(msg.getBody()));
 * });
 * ch.close();
 * }</pre>
 *
 * <p>线程安全：Channel 实例非线程安全（对齐 AMQP 语义），不建议多线程并发使用同一 Channel。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class AmqpChannel implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpChannel.class);

    /** 默认 poll 批量大小 */
    private static final int DEFAULT_POLL_BATCH_SIZE = 32;

    /** 默认 poll 阻塞超时 */
    private static final long DEFAULT_POLL_TIMEOUT_MS = 1000L;

    /** 默认消费者线程名前缀 */
    private static final String CONSUMER_THREAD_PREFIX = "streammq-amqp-consumer-";

    // ===================== 内部数据结构 =====================

    /** Exchange 信息：名称 → 元数据 */
    private final Map<String, ExchangeDeclare> exchanges = new LinkedHashMap<>();

    /** Queue 信息：名称 → 元数据 */
    private final Map<String, QueueDeclare> queues = new LinkedHashMap<>();

    /** Binding 列表：（Exchange, Queue, RoutingKey） */
    private final List<Binding> bindings = new CopyOnWriteArrayList<>();

    // ===================== 底层依赖 =====================

    /** StreamMQ 消息模板，用于发送消息 */
    private final StreamMessageTemplate template;

    /** StreamMQ 监听器工厂，用于创建消费者 */
    private final StreamMQListenerFactory listenerFactory;

    /** 命名空间 */
    private final String namespace;

    /** 默认 Exchange（basicPublish 时可省略 exchange 参数） */
    private final String defaultExchange;

    /** 投递标签递增计数器 */
    private final AtomicLong deliveryTagCounter = new AtomicLong(0);

    /** 活跃的消费线程（用于 close 时安全终止） */
    private final List<ConsumerThread> consumerThreads = new ArrayList<>();

    /** 是否已关闭 */
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * 构造 AmqpChannel（由 {@link AmqpClient#newChannel()} 调用）。
     *
     * @param template        StreamMQ 消息模板
     * @param listenerFactory StreamMQ 监听器工厂
     * @param namespace       命名空间
     * @param defaultExchange 默认 Exchange
     * @throws NullPointerException 如果 template 或 listenerFactory 为 null
     */
    AmqpChannel(StreamMessageTemplate template, StreamMQListenerFactory listenerFactory,
                String namespace, String defaultExchange) {
        this.template = Objects.requireNonNull(template, "template");
        this.listenerFactory = Objects.requireNonNull(listenerFactory, "listenerFactory");
        this.namespace = (namespace != null) ? namespace : "";
        this.defaultExchange = (defaultExchange != null) ? defaultExchange : "";
    }

    // ===================== Exchange 操作 =====================

    /**
     * 声明 Exchange。
     *
     * <p>在 StreamMQ 中，Exchange 映射为 Topic。声明 Exchange 仅在本 Channel
     * 内部记录元数据，不会在 Redis 中创建任何结构（Topic 在首次发送时自动创建）。
     *
     * <p>当前支持的 exchange type：
     * <ul>
     *   <li>{@code direct} - 精确匹配 routingKey（等同于 tag 精确匹配）</li>
     *   <li>{@code fanout} - 忽略 routingKey，所有绑定的 Queue 均收到消息</li>
     * </ul>
     *
     * @param exchange Exchange 名称（必填，不可为空）
     * @param type     Exchange 类型（必填，如 "direct"、"fanout"）
     * @param durable  是否持久化（当前版本忽略，StreamMQ 默认持久化）
     * @return 声明结果信息
     * @throws IllegalArgumentException 如果 exchange 为空
     * @throws IllegalStateException    如果 Channel 已关闭
     */
    public String exchangeDeclare(String exchange, String type, boolean durable) {
        ensureOpen();
        String ex = validateNotEmpty(exchange, "exchange");
        String tp = validateNotEmpty(type, "type");

        if (exchanges.containsKey(ex)) {
            ExchangeDeclare existing = exchanges.get(ex);
            if (!existing.type.equalsIgnoreCase(tp)) {
                LOG.warn("Exchange '{}' already declared with type '{}', re-declaring with type '{}'",
                        ex, existing.type, tp);
            }
        }
        exchanges.put(ex, new ExchangeDeclare(ex, tp, durable));
        LOG.debug("Exchange declared: name={}, type={}, durable={}", ex, tp, durable);
        return ex;
    }

    // ===================== Queue 操作 =====================

    /**
     * 声明 Queue。
     *
     * <p>在 StreamMQ 中，Queue 映射为 ConsumerGroup。声明 Queue 仅在本 Channel
     * 内部记录元数据，不会在 Redis 中创建任何结构（ConsumerGroup 在首次消费时自动创建）。
     *
     * @param queue      Queue 名称（必填，不可为空）
     * @param durable    是否持久化（当前版本忽略）
     * @param exclusive  是否独占（当前版本忽略）
     * @param autoDelete 是否自动删除（当前版本忽略）
     * @param args       额外参数（当前版本忽略）
     * @return 声明结果信息
     * @throws IllegalArgumentException 如果 queue 为空
     * @throws IllegalStateException    如果 Channel 已关闭
     */
    public String queueDeclare(String queue, boolean durable, boolean exclusive,
                               boolean autoDelete, Map<String, Object> args) {
        ensureOpen();
        String q = validateNotEmpty(queue, "queue");
        queues.put(q, new QueueDeclare(q, durable, exclusive, autoDelete, args));
        LOG.debug("Queue declared: name={}, durable={}, exclusive={}, autoDelete={}",
                q, durable, exclusive, autoDelete);
        return q;
    }

    // ===================== Binding 操作 =====================

    /**
     * 绑定 Queue 到 Exchange。
     *
     * <p>概念映射：ConsumerGroup（queue）订阅 Topic（exchange），
     * routingKey 对应 Tag 过滤器。
     *
     * <p>当 {@code routingKey} 为空字符串时，不进行 Tag 过滤，Queue 接收该 Exchange 的全部消息。
     *
     * @param queue      Queue 名称（必填）
     * @param exchange   Exchange 名称（必填）
     * @param routingKey 路由键（可为 null 或空字符串，表示不过滤）
     * @throws IllegalArgumentException 如果 queue 或 exchange 为空
     * @throws IllegalStateException    如果 Channel 已关闭
     */
    public void queueBind(String queue, String exchange, String routingKey) {
        ensureOpen();
        String q = validateNotEmpty(queue, "queue");
        String ex = validateNotEmpty(exchange, "exchange");
        String rk = (routingKey != null) ? routingKey : "";

        bindings.add(new Binding(ex, q, rk));
        LOG.debug("Binding created: exchange={} → queue={}, routingKey={}", ex, q, rk);
    }

    // ===================== 消息发送 =====================

    /**
     * 发送消息到指定 Exchange。
     *
     * <p>概念映射：
     * <ul>
     *   <li>exchange → StreamMQ Topic</li>
     *   <li>routingKey → StreamMQ Tag</li>
     *   <li>props（Map 中的字符串键值对）→ StreamMQ userProperties</li>
     * </ul>
     *
     * <p>当 exchange 为空字符串时，使用构造时指定的默认 Exchange。
     *
     * @param exchange   Exchange 名称（为空时使用默认 Exchange）
     * @param routingKey 路由键（可为 null）
     * @param props      消息属性（可为 null）
     * @param body       消息体（必填）
     * @return 发送结果
     * @throws NullPointerException 如果 body 为 null
     * @throws IllegalStateException 如果 Channel 已关闭
     * @throws io.github.streammq.core.exception.StreamMQException 发送失败
     */
    public SendResult basicPublish(String exchange, String routingKey,
                                   Map<String, Object> props, byte[] body) {
        ensureOpen();
        Objects.requireNonNull(body, "body");

        String ex = resolveExchange(exchange);
        String rk = (routingKey != null) ? routingKey : "";

        MessageBuilder<byte[]> builder = MessageBuilder.<byte[]>withTopic(ex)
                .body(body)
                .tag(rk.isEmpty() ? null : rk);

        // 将 AMQP properties 写入 userProperties
        if (props != null && !props.isEmpty()) {
            for (Map.Entry<String, Object> entry : props.entrySet()) {
                if (entry.getValue() != null) {
                    builder.userProperty(entry.getKey(), entry.getValue().toString());
                }
            }
        }

        Message<byte[]> message = builder.build();
        LOG.debug("Publishing to exchange={}, routingKey={}, bodyLength={}",
                ex, rk, body.length);
        return template.syncSend(message);
    }

    // ===================== 消息消费 =====================

    /**
     * 开始消费 Queue 中的消息。
     *
     * <p>为 Queue 的每个 Binding 创建后台消费线程，从对应的 Exchange（Topic / Stream）
     * 拉取消息，按 routingKey（Tag）过滤后回调 {@link ConsumerCallback}。
     *
     * <p><b>autoAck 语义</b>：
     * <ul>
     *   <li>{@code true} - 消息从 Stream 拉取后立即 ACK，然后回调业务逻辑</li>
     *   <li>{@code false} - 当前版本不支持（抛出 UnsupportedOperationException）</li>
     * </ul>
     *
     * <p>返回的 consumerTag 可用于后续取消消费（当前版本 cancel 尚未实现）。
     *
     * @param queue    Queue 名称（必填）
     * @param autoAck  是否自动确认（当前版本仅支持 true）
     * @param callback 消费回调（必填）
     * @return consumerTag 消费者标签
     * @throws IllegalArgumentException      如果 queue 为空
     * @throws NullPointerException           如果 callback 为 null
     * @throws UnsupportedOperationException  如果 autoAck 为 false
     * @throws IllegalStateException          如果 Channel 已关闭，或 Queue 无 Binding
     */
    public String basicConsume(String queue, boolean autoAck, ConsumerCallback callback) {
        ensureOpen();
        String q = validateNotEmpty(queue, "queue");
        Objects.requireNonNull(callback, "callback");

        if (!autoAck) {
            throw new UnsupportedOperationException(
                    "Manual ack (autoAck=false) is not supported in this version. " +
                            "Use autoAck=true or manually ack via StreamMQListener.");
        }

        // 查找该 Queue 的所有 Binding
        List<Binding> queueBindings = new ArrayList<>();
        for (Binding b : bindings) {
            if (b.queue.equals(q)) {
                queueBindings.add(b);
            }
        }

        if (queueBindings.isEmpty()) {
            throw new IllegalStateException(
                    "Queue '" + q + "' has no bindings. Call queueBind() before basicConsume().");
        }

        String consumerTag = q + "-" + UUID.randomUUID().toString().substring(0, 8);
        LOG.info("Starting consumer: queue={}, consumerTag={}, bindings={}, autoAck={}",
                q, consumerTag, queueBindings.size(), autoAck);

        // 为每个 Binding 创建 Listener 和消费线程
        for (Binding binding : queueBindings) {
            ListenerConfig config = ListenerConfig.builder()
                    .topic(binding.exchange)
                    .consumerGroup(q)
                    .namespace(namespace)
                    .targetBodyType(byte[].class)
                    .build();

            StreamMQListener listener = listenerFactory.createListener(config);

            ConsumerThread ct = new ConsumerThread(
                    listener, binding, consumerTag, autoAck, callback);
            consumerThreads.add(ct);
            ct.start();
        }

        return consumerTag;
    }

    // ===================== 生命周期 =====================

    /**
     * 关闭 Channel，释放所有资源。
     *
     * <p>依次执行：
     * <ol>
     *   <li>停止所有消费线程</li>
     *   <li>关闭所有底层 Listener</li>
     *   <li>清理内部状态</li>
     * </ol>
     *
     * <p>注意：此方法不会关闭底层的 {@link StreamMessageTemplate} 或
     * {@link StreamMQListenerFactory}（由 {@link AmqpClient} 管理其生命周期）。
     * 重复调用安全（幂等）。
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        LOG.info("Closing AmqpChannel...");
        // 停止所有消费线程
        for (ConsumerThread ct : consumerThreads) {
            ct.shutdown();
        }
        consumerThreads.clear();

        // 清理内部状态
        exchanges.clear();
        queues.clear();
        bindings.clear();

        LOG.info("AmqpChannel closed");
    }

    // ===================== 内部辅助方法 =====================

    /**
     * 确保 Channel 未关闭。
     *
     * @throws IllegalStateException 如果已关闭
     */
    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("AmqpChannel is closed");
        }
    }

    /**
     * 解析 Exchange 名称（空字符串时回退到默认 Exchange）。
     *
     * @param exchange Exchange 名称
     * @return 解析后的 Exchange 名称
     * @throws IllegalArgumentException 如果 exchange 为空且未设置默认 Exchange
     */
    private String resolveExchange(String exchange) {
        if (exchange != null && !exchange.isEmpty()) {
            return exchange;
        }
        if (defaultExchange.isEmpty()) {
            throw new IllegalArgumentException(
                    "Exchange is empty and no default exchange is configured");
        }
        return defaultExchange;
    }

    /**
     * 验证字符串参数非空。
     *
     * @param value 参数值
     * @param name  参数名
     * @return 去除首尾空白后的值
     * @throws IllegalArgumentException 如果 value 为 null 或空字符串
     */
    private static String validateNotEmpty(String value, String name) {
        Objects.requireNonNull(value, name);
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be empty");
        }
        return trimmed;
    }

    // ===================== 内部类型定义 =====================

    /**
     * Exchange 声明元数据。
     */
    private static class ExchangeDeclare {
        final String name;
        final String type;
        final boolean durable;

        ExchangeDeclare(String name, String type, boolean durable) {
            this.name = name;
            this.type = type;
            this.durable = durable;
        }
    }

    /**
     * Queue 声明元数据。
     */
    private static class QueueDeclare {
        final String name;
        final boolean durable;
        final boolean exclusive;
        final boolean autoDelete;
        final Map<String, Object> args;

        QueueDeclare(String name, boolean durable, boolean exclusive,
                     boolean autoDelete, Map<String, Object> args) {
            this.name = name;
            this.durable = durable;
            this.exclusive = exclusive;
            this.autoDelete = autoDelete;
            this.args = args != null ? new LinkedHashMap<>(args) : new LinkedHashMap<>();
        }
    }

    /**
     * Binding 关系：（Exchange, Queue, RoutingKey）。
     */
    private static class Binding {
        final String exchange;
        final String queue;
        final String routingKey;

        Binding(String exchange, String queue, String routingKey) {
            this.exchange = exchange;
            this.queue = queue;
            this.routingKey = routingKey;
        }
    }

    /**
     * 消费回调函数接口。
     *
     * <p>当 Queue 收到消息时，Channel 通过此回调将消息分发给业务方。
     * 对齐 RabbitMQ {@code DeliverCallback} 风格。
     */
    @FunctionalInterface
    public interface ConsumerCallback {

        /**
         * 处理一条投递消息。
         *
         * @param message 消息（包含 body、exchange、routingKey、properties 等）
         */
        void handleDelivery(AmqpMessage message);
    }

    /**
     * 消费线程：封装 Listener + 轮询逻辑 + 回调分发。
     */
    private class ConsumerThread {
        private final StreamMQListener listener;
        private final Binding binding;
        private final String consumerTag;
        private final boolean autoAck;
        private final ConsumerCallback callback;
        private final AtomicBoolean running = new AtomicBoolean(true);
        private Thread thread;

        ConsumerThread(StreamMQListener listener, Binding binding, String consumerTag,
                       boolean autoAck, ConsumerCallback callback) {
            this.listener = listener;
            this.binding = binding;
            this.consumerTag = consumerTag;
            this.autoAck = autoAck;
            this.callback = callback;
        }

        /**
         * 启动消费线程（daemon 线程）。
         */
        void start() {
            String threadName = CONSUMER_THREAD_PREFIX + consumerTag + "-" + binding.exchange;
            thread = new Thread(this::runLoop, threadName);
            thread.setDaemon(true);
            thread.start();
        }

        /**
         * 主轮询循环。
         */
        @SuppressWarnings("unchecked")
        private void runLoop() {
            LOG.debug("Consumer thread started: {}", Thread.currentThread().getName());
            while (running.get() && !closed.get()) {
                try {
                    List<Message<?>> messages = listener.pullBlock(
                            DEFAULT_POLL_BATCH_SIZE, Duration.ofMillis(DEFAULT_POLL_TIMEOUT_MS));

                    if (messages.isEmpty()) {
                        continue;
                    }

                    for (Message<?> msg : messages) {
                        // Tag 过滤：routingKey 非空时，仅处理 tag 匹配的消息
                        if (!binding.routingKey.isEmpty()) {
                            String tag = msg.getTag();
                            if (tag == null || !tag.equals(binding.routingKey)) {
                                LOG.trace("Message filtered: expectedTag={}, actualTag={}",
                                        binding.routingKey, tag);
                                // 不匹配的消息也需 ACK（否则会一直留在 PEL 中）
                                if (autoAck && msg.getMessageId() != null) {
                                    listener.ack(msg.getMessageId());
                                }
                                continue;
                            }
                        }

                        // 转换为 AmqpMessage
                        AmqpMessage amqpMsg = toAmqpMessage((Message<byte[]>) msg);

                        // autoAck：ACK 在回调前执行（对齐 AMQP autoAck 语义）
                        if (autoAck && msg.getMessageId() != null) {
                            listener.ack(msg.getMessageId());
                        }

                        // 回调业务逻辑
                        try {
                            callback.handleDelivery(amqpMsg);
                        } catch (Exception ex) {
                            LOG.warn("Consumer callback threw exception: consumerTag={}, exchange={}, msgId={}: {}",
                                    consumerTag, binding.exchange,
                                    msg.getMessageId(), ex.getMessage(), ex);
                        }
                    }
                } catch (Exception ex) {
                    if (running.get() && !closed.get()) {
                        LOG.warn("Poll error in consumer thread {}: {}",
                                Thread.currentThread().getName(), ex.getMessage(), ex);
                        // 短暂退避后继续
                        try {
                            Thread.sleep(100L);
                        } catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            }

            // 关闭 Listener
            try {
                listener.close();
                LOG.debug("Listener closed: exchange={}, consumerTag={}",
                        binding.exchange, consumerTag);
            } catch (Exception ex) {
                LOG.warn("Error closing listener for exchange={}: {}",
                        binding.exchange, ex.getMessage(), ex);
            }
            LOG.debug("Consumer thread stopped: {}", Thread.currentThread().getName());
        }

        /**
         * 转换为 AmqpMessage。
         */
        private AmqpMessage toAmqpMessage(Message<byte[]> msg) {
            long deliveryTag = deliveryTagCounter.incrementAndGet();

            AmqpMessage.Builder builder = AmqpMessage.builder()
                    .body(msg.getBody())
                    .exchange(binding.exchange)
                    .routingKey(msg.getTag() != null ? msg.getTag() : "")
                    .deliveryTag(deliveryTag)
                    .consumerTag(consumerTag)
                    .redelivered(msg.getReconsumeTimes() > 0);

            // 将 userProperties 转换为 AmqpMessage properties
            Map<String, String> userProps = msg.getUserProperties();
            if (!userProps.isEmpty()) {
                for (Map.Entry<String, String> entry : userProps.entrySet()) {
                    builder.property(entry.getKey(), entry.getValue());
                }
            }

            return builder.build();
        }

        /**
         * 安全关闭消费线程。
         */
        void shutdown() {
            running.set(false);
            if (thread != null && thread.isAlive()) {
                thread.interrupt();
                try {
                    thread.join(5000L);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    LOG.warn("Interrupted while waiting for consumer thread to stop");
                }
            }
        }
    }
}
