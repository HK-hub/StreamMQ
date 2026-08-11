package io.github.streammq.sample.transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.LocalTransactionState;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.message.SendStatus;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.core.transaction.TransactionCallback;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

@SpringBootTest(classes = TransactionSampleApplication.class)
@Import(TransactionSampleIT.TestMessageCollector.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@TestPropertySource(properties = {"spring.redis.host=127.0.0.1", "spring.redis.port=6379"})
@DisplayName("事务消息示例集成测试")
class TransactionSampleIT {

    private static final String TEST_CONSUMER_GROUP = "test-tx-consumer-group";

    @Autowired private OrderTransactionProducer producer;

    @Autowired private StreamMessageTemplate template;

    @Autowired private TestMessageCollector testCollector;

    @BeforeEach
    void clearReceivedMessages() {
        testCollector.receivedMessages.clear();
    }

    @Test
    @DisplayName("事务消息 COMMIT 流程：发送事务消息后消费者接收")
    void commit_flow_consumer_receives_message() {
        String content = "test-commit-order-001";

        SendResult result = producer.sendOrderTransaction(content);

        assertThat(result).isNotNull();
        assertThat(result.isSuccess()).isTrue();

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            assertThat(testCollector.receivedMessages).hasSize(1);
                            Message<String> received = testCollector.receivedMessages.peek();
                            assertThat(received).isNotNull();
                            assertThat(received.getBody()).isEqualTo(content);
                            assertThat(received.getTag()).isEqualTo("transaction");
                        });
    }

    @Test
    @DisplayName("事务消息 ROLLBACK 流程：本地事务失败返回不成功结果")
    void rollback_flow_returns_unsuccessful_result() {
        String content = "test-rollback-order-001";

        Message<String> msg =
                MessageBuilder.<String>withTopic("order-topic")
                        .tag("transaction")
                        .body(content)
                        .build();

        TransactionCallback<String> callback =
                (message, ctx) -> LocalTransactionState.ROLLBACK_MESSAGE;

        SendResult result = template.executeInTransaction(msg, callback);

        assertThat(result).isNotNull();
        assertThat(result.isSuccess()).isFalse();
        assertThat(result.getSendStatus()).isEqualTo(SendStatus.SEND_FAILED);
        assertThat(result.getErrorMessage()).contains("rolled back");
    }

    @StreamMQConsumer(topic = "order-topic", consumerGroup = TEST_CONSUMER_GROUP)
    static class TestMessageCollector implements StreamMessageConcurrentlyConsumer<String> {

        final ConcurrentLinkedQueue<Message<String>> receivedMessages =
                new ConcurrentLinkedQueue<>();

        @Override
        public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
            receivedMessages.add(message);
            return ConsumeAction.SUCCESS;
        }
    }
}
