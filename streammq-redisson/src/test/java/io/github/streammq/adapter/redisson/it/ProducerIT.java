package io.github.streammq.adapter.redisson.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.streammq.adapter.redisson.producer.RedissonStreamProducer;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.enums.DelayLevel;
import io.github.streammq.core.exception.StreamMQBrokerException;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.*;
import org.redisson.config.Config;

/**
 * {@link RedissonStreamProducer} 的 Redis 联动集成测试。
 *
 * <p>覆盖同步/异步/批量/延时消息发送,并直接读取 Redis Stream 验证写入正确性。
 */
@DisplayName("RedissonStreamProducer 集成测试")
class ProducerIT extends AbstractRedisIT {

  private RedissonStreamProducer producer;
  private static final String TOPIC = "producer-test-topic";
  private static final String GROUP = "producer-test-group";

  @BeforeEach
  void setUpProducer() {
    producer = new RedissonStreamProducer(redisson, namespace, GROUP, converter, 3000L, 0, 0, 0);
  }

  @AfterEach
  void tearDownProducer() {
    if (producer != null) {
      producer.close();
    }
  }

  @Test
  @DisplayName("syncSend 单条消息后 Stream 中存在该消息且 messageId 非空")
  void syncSend_singleMessage_writesToStream() {
    Message<String> msg = buildMessage(TOPIC, "tag1", "key1", "hello");

    SendResult result = producer.syncSend(msg);

    assertThat(result).isNotNull();
    assertThat(result.getMessageId()).isNotNull();
    assertThat(result.getMessageId().getStreamEntryId()).isNotEmpty();
    assertThat(result.getTopic()).isEqualTo(TOPIC);
    assertThat(result.getTag()).isEqualTo("tag1");
    assertThat(result.isSuccess()).isTrue();

    RStream<String, String> stream = redisson.getStream(StreamMQKeys.topicStream(namespace, TOPIC));
    assertThat(stream.size()).isEqualTo(1L);
  }

  @Test
  @DisplayName("syncSend 消息内容验证:反序列化后字段一致")
  void syncSend_messageContentMatches() {
    Message<String> msg =
        MessageBuilder.<String>withTopic(TOPIC)
            .tag("created")
            .keys("order-123")
            .shardingKey("shard-1")
            .body("payload-content")
            .withUserProperty("traceId", "trace-001")
            .build();

    SendResult result = producer.syncSend(msg);
    assertThat(result.isSuccess()).isTrue();

    RStream<String, String> stream = redisson.getStream(StreamMQKeys.topicStream(namespace, TOPIC));
    Map<StreamMessageId, Map<String, String>> range =
        stream.range(1, StreamMessageId.MIN, StreamMessageId.MAX);
    assertThat(range).hasSize(1);
    Map<String, String> fields = range.values().iterator().next();
    assertThat(fields).containsEntry("tag", "created");
    assertThat(fields).containsEntry("keys", "order-123");
    assertThat(fields).containsEntry("shardingKey", "shard-1");
    assertThat(fields.get("body")).isNotEmpty();
    assertThat(fields).containsEntry("bodyType", String.class.getName());
    assertThat(fields.get("props")).contains("traceId");
  }

  @Test
  @DisplayName("asyncSend 异步发送完成后 SendResult 非空")
  void asyncSend_completesWithResult() {
    Message<String> msg = buildMessage(TOPIC, "async-tag", "async-key", "async-body");

    CompletableFuture<SendResult> future = producer.asyncSend(msg);

    SendResult result = await().atMost(5, TimeUnit.SECONDS).until(future::join, r -> r != null);
    assertThat(result.getMessageId()).isNotNull();
    assertThat(result.getTopic()).isEqualTo(TOPIC);

    RStream<String, String> stream = redisson.getStream(StreamMQKeys.topicStream(namespace, TOPIC));
    assertThat(stream.size()).isEqualTo(1L);
  }

  @Test
  @DisplayName("asyncSend 异常场景:Redisson 客户端关闭后 CompletableFuture 异常完成")
  void asyncSend_clientShutdown_completesExceptionally() {
    Config config = new Config();
    config.useSingleServer().setAddress("redis://localhost:6379").setDatabase(0);
    RedissonClient failingClient = Redisson.create(config);
    RedissonStreamProducer failingProducer =
        new RedissonStreamProducer(
            failingClient, namespace, GROUP + "-fail", converter, 3000L, 0, 0, 0);
    failingClient.shutdown();

    Message<String> msg = buildMessage(TOPIC, "fail-tag", "fail-key", "fail-body");
    CompletableFuture<SendResult> future = failingProducer.asyncSend(msg);

    await().atMost(5, TimeUnit.SECONDS).until(future::isDone);
    assertThat(future).isCompletedExceptionally();
    failingProducer.close();
  }

  @Test
  @DisplayName("syncSendBatch 批量发送 3 条消息后 Stream 中存在 3 条 entry")
  void syncSendBatch_threeMessages_writesAllToStream() {
    List<Message<String>> messages = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      messages.add(buildMessage(TOPIC, "batch-tag", "batch-key-" + i, "batch-body-" + i));
    }

    List<SendResult> results = producer.syncSendBatch(messages);

    assertThat(results).hasSize(3);
    RStream<String, String> stream = redisson.getStream(StreamMQKeys.topicStream(namespace, TOPIC));
    assertThat(stream.size()).isEqualTo(3L);
  }

  @Test
  @DisplayName("syncSendBatch 返回结果:每个 SendResult 含独立 messageId")
  void syncSendBatch_eachResultHasDistinctMessageId() {
    List<Message<String>> messages = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      messages.add(buildMessage(TOPIC, "batch-tag", "batch-key-" + i, "batch-body-" + i));
    }

    List<SendResult> results = producer.syncSendBatch(messages);

    assertThat(results).hasSize(3);
    long distinctIds =
        results.stream().map(r -> r.getMessageId().getStreamEntryId()).distinct().count();
    assertThat(distinctIds).isEqualTo(3L);
  }

  @Test
  @DisplayName("syncSendBatch 空列表抛 IllegalArgumentException")
  void syncSendBatch_emptyList_throws() {
    assertThatThrownBy(() -> producer.syncSendBatch(List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("empty");
  }

  @Test
  @DisplayName("sendDelayMessage 写入延时 ZSet 与 payload Hash")
  void sendDelayMessage_writesToZSetAndHash() {
    Message<String> msg =
        MessageBuilder.<String>withTopic(TOPIC)
            .tag("delay-tag")
            .keys("delay-key")
            .body("delay-body")
            .delayLevel(DelayLevel.SECOND_1)
            .build();

    SendResult result = producer.syncSend(msg);

    assertThat(result).isNotNull();
    assertThat(result.getMessageId()).isNotNull();

    String zsetKey = StreamMQKeys.delayZSet(namespace, DelayLevel.SECOND_1.name());
    RScoredSortedSet<String> zset = redisson.getScoredSortedSet(zsetKey);
    assertThat(zset.size()).isEqualTo(1);

    String msgId = zset.iterator().next();
    String payloadKey = StreamMQKeys.delayPayloadHash(namespace, msgId);
    RMap<String, String> payload = redisson.getMap(payloadKey);
    assertThat(payload).isNotEmpty();
    assertThat(payload).containsEntry("targetTopic", TOPIC);
    assertThat(payload).containsEntry("tag", "delay-tag");
    assertThat(payload).containsKey("deliverAt");
  }

  @Test
  @DisplayName("sendDelayMessage 不同 delayLevel 写入不同 ZSet")
  void sendDelayMessage_differentLevels_separateZSets() {
    Message<String> msg1 =
        MessageBuilder.<String>withTopic(TOPIC)
            .body("delay-1")
            .delayLevel(DelayLevel.SECOND_1)
            .build();
    Message<String> msg2 =
        MessageBuilder.<String>withTopic(TOPIC)
            .body("delay-5")
            .delayLevel(DelayLevel.SECOND_5)
            .build();

    producer.syncSend(msg1);
    producer.syncSend(msg2);

    RScoredSortedSet<String> zset1 =
        redisson.getScoredSortedSet(StreamMQKeys.delayZSet(namespace, DelayLevel.SECOND_1.name()));
    RScoredSortedSet<String> zset5 =
        redisson.getScoredSortedSet(StreamMQKeys.delayZSet(namespace, DelayLevel.SECOND_5.name()));
    assertThat(zset1.size()).isEqualTo(1);
    assertThat(zset5.size()).isEqualTo(1);
  }

  @Test
  @DisplayName("sendOneway 不阻塞且消息最终写入 Stream")
  void sendOneway_eventuallyWrittenToStream() {
    Message<String> msg = buildMessage(TOPIC, "oneway-tag", "oneway-key", "oneway-body");

    producer.sendOneway(msg);

    await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              RStream<String, String> stream =
                  redisson.getStream(StreamMQKeys.topicStream(namespace, TOPIC));
              assertThat(stream.size()).isEqualTo(1L);
            });
  }

  @Test
  @DisplayName("关闭 Producer 后发送抛 IllegalStateException")
  void syncSend_afterClose_throws() {
    producer.close();
    Message<String> msg = buildMessage(TOPIC, "x", "y", "z");
    assertThatThrownBy(() -> producer.syncSend(msg))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("closed");
  }

  @Test
  @DisplayName("syncSend 异常被包装为 StreamMQBrokerException")
  void syncSend_streamError_wrapsInBrokerException() {
    Config config = new Config();
    config.useSingleServer().setAddress("redis://localhost:6379").setDatabase(0);
    RedissonClient closedClient = Redisson.create(config);
    RedissonStreamProducer closedProducer =
        new RedissonStreamProducer(
            closedClient, namespace, GROUP + "-closed", converter, 3000L, 0, 0, 0);
    closedClient.shutdown();

    Message<String> msg = buildMessage(TOPIC, "x", "y", "z");
    assertThatThrownBy(() -> closedProducer.syncSend(msg))
        .isInstanceOf(StreamMQBrokerException.class);
    closedProducer.close();
  }

  private Message<String> buildMessage(String topic, String tag, String keys, String body) {
    return MessageBuilder.<String>withTopic(topic).tag(tag).keys(keys).body(body).build();
  }
}
