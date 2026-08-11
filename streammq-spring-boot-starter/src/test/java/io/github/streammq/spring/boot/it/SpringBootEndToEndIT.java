package io.github.streammq.spring.boot.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.streammq.adapter.redisson.scheduler.DelayMessageScheduler;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.consumer.ConsumeContext;
import io.github.streammq.core.consumer.StreamMessageConcurrentlyConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.message.SendResult;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.spring.boot.autoconfigure.StreamMQCoreAutoConfiguration;
import io.github.streammq.spring.boot.properties.StreamMQProperties;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

/**
 * Spring Boot 端到端集成测试。
 *
 * <p>启动完整的 Spring Boot 上下文,通过 {@link StreamMessageTemplate} 发送消息, 由 {@code @StreamMQConsumer} 注解驱动的
 * Listener 自动消费,验证生产→存储→消费全链路。
 *
 * <p>覆盖场景:
 *
 * <ul>
 *   <li>syncSend 同步发送 → Listener 接收
 *   <li>asyncSend 异步发送 → Listener 接收
 *   <li>sendOneway 单向发送 → Listener 接收
 *   <li>延时消息:发送延时消息 → DelayMessageScheduler 转投 → Listener 接收
 * </ul>
 *
 * <p>使用 {@link TestInstance.Lifecycle#PER_CLASS} 以便在 {@link AfterAll} 中清理 Redis 数据。
 */
@SpringBootTest
@ActiveProfiles("it")
@ContextConfiguration(classes = {RedissonTestConfig.class, SpringBootEndToEndIT.E2EConfig.class})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Spring Boot 端到端集成测试")
class SpringBootEndToEndIT {

  /** 端到端测试 Topic */
  private static final String E2E_TOPIC = "e2e-topic";

  /** 端到端测试消费者组 */
  private static final String E2E_GROUP = "e2e-group";

  @Autowired
  @SuppressWarnings("rawtypes")
  private StreamMessageTemplate streamMessageTemplate;

  @Autowired private E2EStringListener e2eStringListener;

  @Autowired private RedissonClient redissonClient;

  @Autowired private StreamMQProperties properties;

  @Autowired private DelayMessageScheduler delayMessageScheduler;

  /** 每个测试前清空已接收消息队列,避免测试间干扰。 */
  @BeforeEach
  void clearReceivedMessages() {
    e2eStringListener.receivedBodies.clear();
  }

  /** 所有测试完成后清理 Redis 中的测试数据。 */
  @AfterAll
  void cleanupRedis() {
    if (redissonClient != null && !redissonClient.isShutdown()) {
      redissonClient.getKeys().deleteByPattern("streammq:" + properties.getNamespace() + ":*");
    }
  }

  // ===================== 端到端测试 =====================

  @Test
  @DisplayName("syncSend 同步发送消息后 Listener 接收并处理")
  void syncSend_listenerReceivesMessage() {
    String body = "e2e-sync-" + UUID.randomUUID();
    Message<String> msg =
        MessageBuilder.<String>withTopic(E2E_TOPIC)
            .tag("sync-tag")
            .keys("sync-key")
            .body(body)
            .build();

    @SuppressWarnings("unchecked")
    SendResult result = streamMessageTemplate.syncSend(msg);

    assertThat(result).isNotNull();
    assertThat(result.isSuccess()).isTrue();

    await()
        .atMost(10, TimeUnit.SECONDS)
        .until(() -> e2eStringListener.receivedBodies.contains(body));
  }

  @Test
  @DisplayName("syncSend 消息内容验证:Listener 接收到的 body 与发送一致")
  void syncSend_messageContentMatches() {
    String body = "e2e-content-" + UUID.randomUUID();
    Message<String> msg =
        MessageBuilder.<String>withTopic(E2E_TOPIC)
            .tag("content-tag")
            .keys("content-key")
            .body(body)
            .build();

    @SuppressWarnings("unchecked")
    SendResult result = streamMessageTemplate.syncSend(msg);
    assertThat(result.isSuccess()).isTrue();

    await()
        .atMost(10, TimeUnit.SECONDS)
        .until(() -> e2eStringListener.receivedBodies.contains(body));
    assertThat(e2eStringListener.receivedBodies).contains(body);
  }

  @Test
  @DisplayName("asyncSend 异步发送消息后 Listener 接收并处理")
  void asyncSend_listenerReceivesMessage() {
    String body = "e2e-async-" + UUID.randomUUID();
    Message<String> msg = MessageBuilder.<String>withTopic(E2E_TOPIC).body(body).build();

    @SuppressWarnings("unchecked")
    java.util.concurrent.CompletableFuture<SendResult> future =
        streamMessageTemplate.asyncSend(msg);

    SendResult result = await().atMost(5, TimeUnit.SECONDS).until(future::join, r -> r != null);
    assertThat(result.isSuccess()).isTrue();

    await()
        .atMost(10, TimeUnit.SECONDS)
        .until(() -> e2eStringListener.receivedBodies.contains(body));
  }

  @Test
  @DisplayName("sendOneway 单向发送消息后 Listener 最终接收")
  @SuppressWarnings("unchecked")
  void sendOneway_listenerReceivesMessage() {
    String body = "e2e-oneway-" + UUID.randomUUID();
    Message<String> msg = MessageBuilder.<String>withTopic(E2E_TOPIC).body(body).build();

    streamMessageTemplate.sendOneway(msg);

    await()
        .atMost(10, TimeUnit.SECONDS)
        .until(() -> e2eStringListener.receivedBodies.contains(body));
  }

  @Test
  @DisplayName("连续发送 3 条消息后 Listener 全部接收")
  void syncSendBatch_threeMessages_allReceived() {
    String body1 = "e2e-batch-1-" + UUID.randomUUID();
    String body2 = "e2e-batch-2-" + UUID.randomUUID();
    String body3 = "e2e-batch-3-" + UUID.randomUUID();

    for (String body : new String[] {body1, body2, body3}) {
      Message<String> msg = MessageBuilder.<String>withTopic(E2E_TOPIC).body(body).build();
      @SuppressWarnings("unchecked")
      SendResult result = streamMessageTemplate.syncSend(msg);
      assertThat(result.isSuccess()).isTrue();
    }

    await()
        .atMost(15, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              assertThat(e2eStringListener.receivedBodies).contains(body1, body2, body3);
            });
  }

  @Test
  @DisplayName("延时消息:发送后由 DelayMessageScheduler 转投到目标 Stream 并被 Listener 消费")
  void delayMessage_schedulerTransfersAndListenerConsumes() {
    String body = "e2e-delay-" + UUID.randomUUID();
    Message<String> msg =
        MessageBuilder.<String>withTopic(E2E_TOPIC)
            .body(body)
            .delayLevel(io.github.streammq.core.enums.DelayLevel.SECOND_1)
            .build();

    @SuppressWarnings("unchecked")
    SendResult result = streamMessageTemplate.syncSend(msg);
    assertThat(result).isNotNull();
    assertThat(result.getMessageId()).isNotNull();

    // 验证延时 ZSet 中存在该消息
    String delayZSetKey =
        StreamMQKeys.delayZSet(
            properties.getNamespace(), io.github.streammq.core.enums.DelayLevel.SECOND_1.name());
    RScoredSortedSet<String> zset = redissonClient.getScoredSortedSet(delayZSetKey);
    assertThat(zset.size()).isEqualTo(1);

    // 等待 DelayMessageScheduler 转投 + Listener 消费(延时 1 秒 + 调度间隔 500ms)
    await()
        .atMost(15, TimeUnit.SECONDS)
        .until(() -> e2eStringListener.receivedBodies.contains(body));

    // 转投后延时 ZSet 应清空
    await()
        .atMost(5, TimeUnit.SECONDS)
        .untilAsserted(
            () -> {
              RScoredSortedSet<String> zsetAfter = redissonClient.getScoredSortedSet(delayZSetKey);
              assertThat(zsetAfter.size()).isZero();
            });
  }

  // ===================== 测试配置与 Listener =====================

  /**
   * 测试配置:注册端到端测试用 Listener Bean。
   *
   * <p>namespace 由 {@link StreamMQCoreAutoConfiguration#streamMQTemplate} 自动注入
   * (已修复:defaultProperties 含 namespace,Producer 与 ListenerContainer 使用相同 namespace)。
   */
  @TestConfiguration
  static class E2EConfig {

    @Bean
    E2EStringListener e2EStringListener() {
      return new E2EStringListener();
    }
  }

  /**
   * 端到端测试用 Listener,实现 {@link StreamMessageConcurrentlyConsumer} 接口, 标注 {@code @StreamMQConsumer}
   * 注解,由 Spring 自动扫描注册。
   *
   * <p>使用 {@link ConcurrentLinkedQueue} 收集所有接收到的消息 body,线程安全。
   */
  @StreamMQConsumer(topic = E2E_TOPIC, consumerGroup = E2E_GROUP)
  public static class E2EStringListener implements StreamMessageConcurrentlyConsumer<String> {

    /** 已接收的消息 body 集合 */
    final ConcurrentLinkedQueue<String> receivedBodies = new ConcurrentLinkedQueue<>();

    @Override
    public ConsumeAction onMessage(Message<String> message, ConsumeContext context) {
      if (message.getBody() != null) {
        receivedBodies.add(message.getBody());
      }
      return ConsumeAction.SUCCESS;
    }
  }
}
