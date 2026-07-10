package io.github.streammq.sample.quickstart;

import io.github.streammq.core.message.BatchMessage;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.MessageMetadataBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.producer.SendCallback;
import io.github.streammq.core.service.StreamMessageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * 订单消息生产者示例。
 *
 * <p>演示 StreamMQ 的完整发送能力：
 * <ul>
 *   <li>同步发送（基础 API + MessageBuilder）</li>
 *   <li>异步发送（CompletableFuture + SendCallback）</li>
 *   <li>单向发送（fire-and-forget）</li>
 *   <li>批量发送（BatchMessage）</li>
 *   <li>MessageMetadataBuilder 模式（统一封装附加参数）</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Component
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    private final StreamMessageService service;

    public OrderProducer(StreamMessageService service) {
        this.service = service;
    }

    // ===================== 同步发送 =====================

    public SendResult createOrder(String orderId, String content) {
        log.info("Producing order message: orderId={}, content={}", orderId, content);
        SendResult result = service.send("order-topic", content, "created", orderId);
        log.info("Order message sent successfully: orderId={}, msgId={}, status={}",
                orderId, result.getMessageId(), result.getSendStatus());
        return result;
    }

    public SendResult createOrderWithBuilder(String orderId, String content) {
        Message<String> message = MessageBuilder.<String>withTopic("order-topic")
                .tag("created")
                .keys(orderId)
                .body(content)
                .userProperty("source", "quickstart-sample")
                .build();
        log.info("Producing order message via builder: orderId={}", orderId);
        SendResult result = service.send(message);
        log.info("Order message sent successfully: msgId={}", result.getMessageId());
        return result;
    }

    // ===================== 异步发送（CompletableFuture） =====================

    /**
     * 异步发送消息，返回 CompletableFuture。
     *
     * @param orderId 订单 ID
     * @param content 订单内容
     * @return 异步发送结果
     */
    public CompletableFuture<SendResult> createOrderAsync(String orderId, String content) {
        log.info("Producing order message asynchronously: orderId={}", orderId);
        CompletableFuture<SendResult> future = service.asyncSend("order-topic", content, "async", orderId);

        future.thenAccept(result ->
                log.info("Async send completed: orderId={}, msgId={}", orderId, result.getMessageId()))
                .exceptionally(ex -> {
                    log.error("Async send failed: orderId={}, error={}", orderId, ex.getMessage());
                    return null;
                });

        return future;
    }

    /**
     * 异步发送消息（带超时）。
     *
     * @param orderId 订单 ID
     * @param content 订单内容
     * @param timeoutMillis 超时毫秒数
     * @return 异步发送结果
     */
    public CompletableFuture<SendResult> createOrderAsyncWithTimeout(String orderId, String content, long timeoutMillis) {
        log.info("Producing order message asynchronously with timeout: orderId={}, timeout={}ms", orderId, timeoutMillis);
        return service.asyncSend("order-topic", content, timeoutMillis);
    }

    // ===================== 异步发送（SendCallback） =====================

    /**
     * 异步发送消息，使用 SendCallback 回调。
     *
     * @param orderId 订单 ID
     * @param content 订单内容
     */
    public void createOrderWithCallback(String orderId, String content) {
        log.info("Producing order message with callback: orderId={}", orderId);

        SendCallback callback = new SendCallback() {
            @Override
            public void onSuccess(SendResult result) {
                log.info("Callback send success: orderId={}, msgId={}", orderId, result.getMessageId());
            }

            @Override
            public void onException(Throwable ex) {
                log.error("Callback send failed: orderId={}, error={}", orderId, ex.getMessage(), ex);
            }
        };

        service.asyncSend("order-topic", content, "callback", callback);
    }

    // ===================== 单向发送 =====================

    /**
     * 单向发送消息（fire-and-forget），不等待响应。
     *
     * <p>适用于对可靠性要求不高的场景（如日志采集、监控上报），性能最高。
     *
     * @param orderId 订单 ID
     * @param content 订单内容
     */
    public void createOrderOneway(String orderId, String content) {
        log.info("Producing order message oneway: orderId={}", orderId);
        service.sendOneway("order-topic", content, "oneway", orderId);
        log.info("Oneway message sent (no response): orderId={}", orderId);
    }

    // ===================== 批量发送 =====================

    /**
     * 批量发送订单消息。
     *
     * <p>底层通过 Redisson RBatch（Pipeline）一次性 XADD 多条消息，减少 RTT。
     *
     * @param orderIds 订单 ID 列表
     * @param contents 订单内容列表
     * @return 每条消息的发送结果
     */
    public List<SendResult> createOrdersBatch(List<String> orderIds, List<String> contents) {
        log.info("Producing batch order messages: count={}", orderIds.size());

        BatchMessage.Builder<String> builder = BatchMessage.<String>withTopic("order-topic");
        for (int i = 0; i < orderIds.size(); i++) {
            Message<String> msg = MessageBuilder.<String>withTopic("order-topic")
                    .tag("batch")
                    .keys(orderIds.get(i))
                    .body(contents.get(i))
                    .userProperty("batchIndex", String.valueOf(i))
                    .build();
            builder.add(msg);
        }

        List<SendResult> results = service.sendBatch(builder.build());
        log.info("Batch messages sent: successCount={}",
                results.stream().filter(SendResult::isSuccess).count());
        return results;
    }

    /**
     * 使用简化的批量发送 API。
     *
     * @param contents 订单内容列表
     * @return 每条消息的发送结果
     */
    public List<SendResult> createOrdersBatchSimple(List<String> contents) {
        log.info("Producing batch order messages (simple API): count={}", contents.size());
        List<SendResult> results = service.sendBatch("order-topic", "simple-batch", contents);
        log.info("Simple batch messages sent: count={}", results.size());
        return results;
    }

    // ===================== MessageMetadataBuilder 模式 =====================

    /**
     * 使用 MessageMetadataBuilder 封装所有附加参数。
     *
     * <p>统一封装 Tag、Keys、ShardingKey、用户属性等，避免方法重载数量爆炸。
     *
     * @param orderId 订单 ID
     * @param content 订单内容
     * @param source 消息来源
     * @return 发送结果
     */
    public SendResult createOrderWithMetadata(String orderId, String content, String source) {
        log.info("Producing order message with metadata: orderId={}, source={}", orderId, source);

        MessageMetadataBuilder metadata = MessageMetadataBuilder.create()
                .tag("metadata")
                .keys(orderId)
                .shardingKey(orderId)
                .userProperty("source", source)
                .userProperty("traceId", "trace-" + System.currentTimeMillis());

        SendResult result = service.send("order-topic", content, metadata);
        log.info("Metadata message sent: orderId={}, msgId={}", orderId, result.getMessageId());
        return result;
    }

    /**
     * 使用 MessageMetadataBuilder + 指定超时和重试次数。
     *
     * @param orderId 订单 ID
     * @param content 订单内容
     * @param timeoutMillis 超时毫秒数
     * @param retryTimes 重试次数
     * @return 发送结果
     */
    public SendResult createOrderWithTimeoutAndRetry(String orderId, String content, long timeoutMillis, int retryTimes) {
        log.info("Producing order message with timeout={}ms and retry={}: orderId={}",
                timeoutMillis, retryTimes, orderId);

        MessageMetadataBuilder metadata = MessageMetadataBuilder.create()
                .tag("timeout-retry")
                .keys(orderId);

        SendResult result = service.send("order-topic", content, metadata, timeoutMillis, retryTimes);
        log.info("Timeout/retry message sent: orderId={}, msgId={}", orderId, result.getMessageId());
        return result;
    }
}
