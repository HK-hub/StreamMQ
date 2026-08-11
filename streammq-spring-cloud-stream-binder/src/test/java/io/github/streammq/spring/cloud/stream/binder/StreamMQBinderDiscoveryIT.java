package io.github.streammq.spring.cloud.stream.binder;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.binder.Binder;
import org.springframework.cloud.stream.binder.BinderFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** StreamMQ Binder 发现与注册测试，验证 Binder 是否被 Spring Cloud Stream 正确发现。 */
@SpringBootTest(
    classes = StreamMQBinderDiscoveryIT.TestApp.class,
    properties = {
      "spring.application.name=binder-discovery-test",
      "streammq.enabled=true",
      "streammq.namespace=binder-discovery",
      "streammq.producer.group=binder-discovery-producer",
      "redisson.singleServerConfig.address=redis://127.0.0.1:6379",
      "redisson.singleServerConfig.database=3",
      "spring.cloud.stream.default-binder=streammq"
    })
@DirtiesContext
@DisplayName("StreamMQ Binder 发现测试")
class StreamMQBinderDiscoveryIT {

  @DynamicPropertySource
  static void redisPassword(DynamicPropertyRegistry registry) {
    String password =
        System.getProperty(
            "test.redis.password",
            System.getenv().getOrDefault("STREAMMQ_TEST_REDIS_PASSWORD", ""));
    if (!password.isEmpty()) {
      registry.add("redisson.singleServerConfig.password", () -> password);
    }
  }

  @Autowired private ApplicationContext context;

  @Autowired private BinderFactory binderFactory;

  @Test
  @DisplayName("BinderFactory 应能获取 streammq 类型的 Binder")
  void shouldDiscoverStreamMQBinder() {
    Binder<?, ?, ?> binder =
        binderFactory.getBinder("streammq", org.springframework.messaging.MessageChannel.class);
    assertThat(binder).isNotNull();
    assertThat(binder).isInstanceOf(StreamMQMessageBinder.class);
  }

  @Test
  @DisplayName("ApplicationContext 中应存在 StreamMQMessageBinder Bean")
  void shouldHaveBinderBean() {
    // Binder 在子上下文中创建，检查 BinderFactory 是否能找到它
    Binder<?, ?, ?> binder =
        binderFactory.getBinder(null, org.springframework.messaging.MessageChannel.class);
    assertThat(binder).isNotNull();
  }

  @SpringBootApplication
  static class TestApp {}
}
