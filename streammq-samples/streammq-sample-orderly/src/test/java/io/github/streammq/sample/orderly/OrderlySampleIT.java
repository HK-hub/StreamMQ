package io.github.streammq.sample.orderly;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.ConsumeOrderlyContext;
import io.github.streammq.core.consumer.StreamMessageOrderlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.MessageModel;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.SendResult;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

/**
 * 顺序消息示例集成测试。
 *
 * <p>启动完整 Spring Boot 上下文，通过 {@link OrderlyMessageProducer} 发送消息， 由测试专用的 {@link
 * TestOrderlyMessageCollector} 按顺序接收，验证同一 shardingKey 的消息严格按发送顺序消费。
 *
 * <p>测试场景：
 *
 * <ul>
 *   <li>{@code sendOrderlyMessage_verifiedInOrder} 同步发送同 shardingKey 消息 → 验证顺序
 *   <li>{@code sendOrderStatusFlow_allStatusesArriveInOrder} 订单状态流转 → 验证完整生命周期顺序
 *   <li>{@code sendBatchOrderlyMessages_allMessagesArriveInOrder} 批量发送 → 验证所有消息按序到达
 *   <li>{@code multipleShardingKeys_messagesPerShardInOrder} 多 shardingKey 并行 → 验证每个分片内有序
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@SpringBootTest(classes = OrderlySampleApplication.class)
@ActiveProfiles("it")
@Import(OrderlySampleIT.TestOrderlyMessageCollector.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@TestPropertySource(properties = {"spring.redis.host=127.0.0.1", "spring.redis.port=6379"})
@DisplayName("顺序消息示例集成测试")
class OrderlySampleIT {

    private static final String TEST_CONSUMER_GROUP = "test-orderly-consumer-group";

    @Autowired private OrderlyMessageProducer producer;

    @Autowired private TestOrderlyMessageCollector testCollector;

    @BeforeEach
    void clearReceivedMessages() {
        testCollector.receivedMessages.clear();
    }

    @Test
    @DisplayName("同一 shardingKey 发送多条消息后按顺序接收")
    void sendOrderlyMessage_verifiedInOrder() {
        String orderId = "IT-ORDER-001";

        SendResult result1 = producer.sendOrderlyMessage(orderId, "{\"step\":\"first\"}", 1);
        SendResult result2 = producer.sendOrderlyMessage(orderId, "{\"step\":\"second\"}", 2);
        SendResult result3 = producer.sendOrderlyMessage(orderId, "{\"step\":\"third\"}", 3);

        assertThat(result1.isSuccess()).isTrue();
        assertThat(result2.isSuccess()).isTrue();
        assertThat(result3.isSuccess()).isTrue();

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            List<Message<String>> messages =
                                    new ArrayList<>(testCollector.receivedMessages);
                            assertThat(messages).hasSize(3);
                            assertThat(messages.get(0).getKeys()).isEqualTo(orderId);
                            assertThat(messages.get(0).getUserProperties().get("sequence"))
                                    .isEqualTo("1");
                            assertThat(messages.get(1).getKeys()).isEqualTo(orderId);
                            assertThat(messages.get(1).getUserProperties().get("sequence"))
                                    .isEqualTo("2");
                            assertThat(messages.get(2).getKeys()).isEqualTo(orderId);
                            assertThat(messages.get(2).getUserProperties().get("sequence"))
                                    .isEqualTo("3");
                        });
    }

    @Test
    @DisplayName("订单状态流转消息按正确顺序到达")
    void sendOrderStatusFlow_allStatusesArriveInOrder() {
        String orderId = "IT-ORDER-FLOW-001";

        producer.sendOrderStatusFlow(orderId);

        await().atMost(15, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            List<Message<String>> messages =
                                    new ArrayList<>(testCollector.receivedMessages);
                            assertThat(messages).hasSize(4);
                            assertThat(messages.get(0).getBody())
                                    .isEqualTo("{\"status\":\"created\"}");
                            assertThat(messages.get(1).getBody())
                                    .isEqualTo("{\"status\":\"paid\"}");
                            assertThat(messages.get(2).getBody())
                                    .isEqualTo("{\"status\":\"shipped\"}");
                            assertThat(messages.get(3).getBody())
                                    .isEqualTo("{\"status\":\"completed\"}");
                        });
    }

    @Test
    @DisplayName("批量顺序消息全部按序接收")
    void sendBatchOrderlyMessages_allMessagesArriveInOrder() {
        String orderId = "IT-BATCH-001";
        int count = 5;

        producer.sendBatchOrderlyMessages(orderId, count);

        await().atMost(15, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            List<Message<String>> messages =
                                    new ArrayList<>(testCollector.receivedMessages);
                            assertThat(messages).hasSize(count);
                            for (int i = 0; i < count; i++) {
                                assertThat(messages.get(i).getUserProperties().get("sequence"))
                                        .isEqualTo(String.valueOf(i + 1));
                                assertThat(messages.get(i).getKeys()).isEqualTo(orderId);
                            }
                        });
    }

    @Test
    @DisplayName("多 shardingKey 并行发送，每个 shard 内消息有序")
    void multipleShardingKeys_messagesPerShardInOrder() {
        String orderId1 = "IT-PARALLEL-001";
        String orderId2 = "IT-PARALLEL-002";
        String orderId3 = "IT-PARALLEL-003";

        producer.sendBatchOrderlyMessages(orderId1, 3);
        producer.sendBatchOrderlyMessages(orderId2, 3);
        producer.sendBatchOrderlyMessages(orderId3, 3);

        await().atMost(20, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            List<Message<String>> messages =
                                    new ArrayList<>(testCollector.receivedMessages);
                            assertThat(messages).hasSize(9);

                            List<Message<String>> order1Messages =
                                    messages.stream()
                                            .filter(m -> orderId1.equals(m.getKeys()))
                                            .toList();
                            List<Message<String>> order2Messages =
                                    messages.stream()
                                            .filter(m -> orderId2.equals(m.getKeys()))
                                            .toList();
                            List<Message<String>> order3Messages =
                                    messages.stream()
                                            .filter(m -> orderId3.equals(m.getKeys()))
                                            .toList();

                            assertThat(order1Messages).hasSize(3);
                            assertThat(order2Messages).hasSize(3);
                            assertThat(order3Messages).hasSize(3);

                            assertThat(order1Messages.get(0).getUserProperties().get("sequence"))
                                    .isEqualTo("1");
                            assertThat(order1Messages.get(1).getUserProperties().get("sequence"))
                                    .isEqualTo("2");
                            assertThat(order1Messages.get(2).getUserProperties().get("sequence"))
                                    .isEqualTo("3");

                            assertThat(order2Messages.get(0).getUserProperties().get("sequence"))
                                    .isEqualTo("1");
                            assertThat(order2Messages.get(1).getUserProperties().get("sequence"))
                                    .isEqualTo("2");
                            assertThat(order2Messages.get(2).getUserProperties().get("sequence"))
                                    .isEqualTo("3");

                            assertThat(order3Messages.get(0).getUserProperties().get("sequence"))
                                    .isEqualTo("1");
                            assertThat(order3Messages.get(1).getUserProperties().get("sequence"))
                                    .isEqualTo("2");
                            assertThat(order3Messages.get(2).getUserProperties().get("sequence"))
                                    .isEqualTo("3");
                        });
    }

    @Test
    @DisplayName("消息元数据正确性验证：topic/tag/shardingKey/userProperties")
    void sendOrderlyMessage_metadataIsCorrect() {
        String orderId = "IT-META-001";

        SendResult result = producer.sendOrderlyMessage(orderId, "{\"meta\":\"test\"}", 42);

        assertThat(result.isSuccess()).isTrue();

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            List<Message<String>> messages =
                                    new ArrayList<>(testCollector.receivedMessages);
                            assertThat(messages).hasSize(1);

                            Message<String> received = messages.get(0);
                            assertThat(received.getTopic()).isEqualTo("orderly-order-topic");
                            assertThat(received.getTag()).isEqualTo("orderly");
                            assertThat(received.getKeys()).isEqualTo(orderId);
                            assertThat(received.getShardingKey()).isEqualTo(orderId);
                            assertThat(received.getBody()).isEqualTo("{\"meta\":\"test\"}");
                            assertThat(received.getUserProperties())
                                    .containsEntry("sequence", "42");
                            assertThat(received.getUserProperties())
                                    .containsEntry("source", "orderly-sample");
                        });
    }

    @StreamMQConsumer(
            topic = SampleConstants.TOPIC,
            consumerGroup = TEST_CONSUMER_GROUP,
            messageModel = MessageModel.ORDERLY,
            shardCount = 8)
    static class TestOrderlyMessageCollector implements StreamMessageOrderlyConsumer<String> {

        final ConcurrentLinkedQueue<Message<String>> receivedMessages =
                new ConcurrentLinkedQueue<>();

        @Override
        public ConsumeAction onMessage(Message<String> message, ConsumeOrderlyContext context) {
            receivedMessages.add(message);
            return ConsumeAction.SUCCESS;
        }
    }
}
