package io.github.streammq.spring.boot.it;

import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.adapter.redisson.scheduler.DelayMessageScheduler;
import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.adapter.redisson.scheduler.TransactionScanner;
import io.github.streammq.core.listener.StreamMQListenerFactory;
import io.github.streammq.core.producer.StreamMessageProducerFactory;
import io.github.streammq.core.spi.MessageConverter;
import io.github.streammq.core.spi.MessageSerializer;
import io.github.streammq.core.spi.RetryPolicy;
import io.github.streammq.core.template.StreamMessageTemplate;
import io.github.streammq.spring.boot.autoconfigure.*;
import io.github.streammq.spring.boot.properties.StreamMQProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Spring Boot 自动装配集成测试。
 *
 * <p>启动完整的 Spring Boot 上下文,验证 StreamMQ 所有核心 Bean 均被正确装配,
 * 包括序列化器、转换器、重试策略、生产者/消费者工厂、Template、Listener 容器、
 * 调度器(Retry/Delay/Transaction)、SmartLifecycle 生命周期管理及健康检查。
 *
 * <p>测试策略:
 * <ul>
 *   <li>使用 {@code @SpringBootTest} 启动完整上下文</li>
 *   <li>通过 {@code @ActiveProfiles("it")} 加载 {@code application-it.yml}</li>
 *   <li>通过 {@link RedissonTestConfig} 提供 RedissonClient Bean</li>
 *   <li>验证 Bean 存在性、类型、配置属性绑定及生命周期相位</li>
 * </ul>
 */
@SpringBootTest
@ActiveProfiles("it")
@ContextConfiguration(classes = RedissonTestConfig.class)
@DisplayName("Spring Boot 自动装配集成测试")
class SpringBootAutoConfigIT {

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private StreamMQProperties properties;

    @Autowired
    private RedissonClient redissonClient;

    // ===================== 核心 Bean 装配验证 =====================

    @Test
    @DisplayName("StreamMqAutoConfiguration 主配置类被装配")
    void streamMqAutoConfiguration_isLoaded() {
        assertThat(applicationContext.containsBeanDefinition(
            StreamMQAutoConfiguration.class.getName())).isTrue();
    }

    @Test
    @DisplayName("StreamMqCoreAutoConfiguration 核心配置类被装配")
    void coreAutoConfiguration_isLoaded() {
        assertThat(applicationContext.containsBeanDefinition(
            StreamMQCoreAutoConfiguration.class.getName())).isTrue();
    }

    @Test
    @DisplayName("StreamMqSchedulerAutoConfiguration 调度器配置类被装配")
    void schedulerAutoConfiguration_isLoaded() {
        assertThat(applicationContext.containsBeanDefinition(
            StreamMQSchedulerAutoConfiguration.class.getName())).isTrue();
    }

    @Test
    @DisplayName("StreamMqListenerContainerAutoConfiguration 容器配置类被装配")
    void listenerContainerAutoConfiguration_isLoaded() {
        assertThat(applicationContext.containsBeanDefinition(
            StreamMQListenerContainerAutoConfiguration.class.getName())).isTrue();
    }

    @Test
    @DisplayName("StreamMqHealthAutoConfiguration 健康检查配置类被装配")
    void healthAutoConfiguration_isLoaded() {
        assertThat(applicationContext.containsBeanDefinition(
            StreamMQHealthAutoConfiguration.class.getName())).isTrue();
    }

    // ===================== 核心 Bean 实例验证 =====================

    @Test
    @DisplayName("MessageSerializer Bean 存在且为 JacksonJsonSerializer")
    void messageSerializer_beanExists() {
        MessageSerializer<?> serializer = applicationContext.getBean(MessageSerializer.class);
        assertThat(serializer).isNotNull();
        assertThat(serializer.getClass().getName())
            .isEqualTo("io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer");
    }

    @Test
    @DisplayName("MessageConverter Bean 存在且为 DefaultMessageConverter")
    void messageConverter_beanExists() {
        MessageConverter converter = applicationContext.getBean(MessageConverter.class);
        assertThat(converter).isNotNull();
        assertThat(converter.getClass().getName())
            .isEqualTo("io.github.streammq.adapter.redisson.converter.DefaultMessageConverter");
    }

    @Test
    @DisplayName("RetryPolicy Bean 存在且为 FixedArrayRetryPolicy")
    void retryPolicy_beanExists() {
        RetryPolicy policy = applicationContext.getBean(RetryPolicy.class);
        assertThat(policy).isNotNull();
        assertThat(policy.getClass().getName())
            .isEqualTo("io.github.streammq.adapter.redisson.retry.FixedArrayRetryPolicy");
    }

    @Test
    @DisplayName("StreamMqProducerFactory Bean 存在")
    void producerFactory_beanExists() {
        StreamMessageProducerFactory factory = applicationContext.getBean(StreamMessageProducerFactory.class);
        assertThat(factory).isNotNull();
    }

    @Test
    @DisplayName("StreamMqListenerFactory Bean 存在")
    void consumerFactory_beanExists() {
        StreamMQListenerFactory factory = applicationContext.getBean(StreamMQListenerFactory.class);
        assertThat(factory).isNotNull();
    }

    @Test
    @DisplayName("StreamMessageTemplate Bean 存在且为 DefaultStreamMessageTemplate")
    void streamMqTemplate_beanExists() {
        StreamMessageTemplate template = applicationContext.getBean(StreamMessageTemplate.class);
        assertThat(template).isNotNull();
        assertThat(template.getClass().getName())
            .isEqualTo("io.github.streammq.adapter.redisson.template.DefaultStreamMessageTemplate");
    }

    // ===================== Listener 容器 Bean 验证 =====================

    @Test
    @DisplayName("DefaultStreamMqListenerContainer Bean 存在")
    void listenerContainer_beanExists() {
        DefaultStreamMQListenerContainer container =
            applicationContext.getBean(DefaultStreamMQListenerContainer.class);
        assertThat(container).isNotNull();
    }

    @Test
    @DisplayName("StreamMqListenerContainerLifecycle Bean 存在且为 SmartLifecycle")
    void listenerContainerLifecycle_beanExists() {
        StreamMQListenerContainerLifecycle lifecycle =
            applicationContext.getBean(StreamMQListenerContainerLifecycle.class);
        assertThat(lifecycle).isNotNull();
        assertThat(lifecycle).isInstanceOf(SmartLifecycle.class);
    }

    // ===================== 调度器 Bean 验证 =====================

    @Test
    @DisplayName("RetryScheduler Bean 存在")
    void retryScheduler_beanExists() {
        RetryScheduler scheduler = applicationContext.getBean(RetryScheduler.class);
        assertThat(scheduler).isNotNull();
    }

    @Test
    @DisplayName("DelayMessageScheduler Bean 存在")
    void delayMessageScheduler_beanExists() {
        DelayMessageScheduler scheduler = applicationContext.getBean(DelayMessageScheduler.class);
        assertThat(scheduler).isNotNull();
    }

    @Test
    @DisplayName("TransactionScanner Bean 存在")
    void transactionScanner_beanExists() {
        TransactionScanner scanner = applicationContext.getBean(TransactionScanner.class);
        assertThat(scanner).isNotNull();
    }

    @Test
    @DisplayName("StreamMqSchedulerLifecycle Bean 存在且为 SmartLifecycle")
    void schedulerLifecycle_beanExists() {
        StreamMQSchedulerLifecycle lifecycle =
            applicationContext.getBean(StreamMQSchedulerLifecycle.class);
        assertThat(lifecycle).isNotNull();
        assertThat(lifecycle).isInstanceOf(SmartLifecycle.class);
    }

    // ===================== 健康检查 Bean 验证 =====================

    @Test
    @DisplayName("HealthIndicator Bean 存在(name=streamMqHealthIndicator)")
    void healthIndicator_beanExists() {
        HealthIndicator indicator = applicationContext.getBean("streamMqHealthIndicator", HealthIndicator.class);
        assertThat(indicator).isNotNull();
    }

    // ===================== 配置属性绑定验证 =====================

    @Test
    @DisplayName("配置属性 namespace 正确绑定")
    void properties_namespaceBound() {
        assertThat(properties.getNamespace()).isEqualTo("it-starter");
    }

    @Test
    @DisplayName("配置属性 producer.group 正确绑定")
    void properties_producerGroupBound() {
        assertThat(properties.getProducer().getGroup()).isEqualTo("starter-producer");
    }

    @Test
    @DisplayName("配置属性 producer.sendMessageTimeout 正确绑定")
    void properties_producerTimeoutBound() {
        assertThat(properties.getProducer().getSendMessageTimeout()).isEqualTo(3000L);
    }

    @Test
    @DisplayName("配置属性 retry.scanInterval 正确绑定为 Duration")
    void properties_retryScanIntervalBound() {
        assertThat(properties.getRetry().getScanInterval()).isEqualTo(java.time.Duration.ofMillis(500));
    }

    @Test
    @DisplayName("配置属性 delay.enabled 正确绑定")
    void properties_delayEnabledBound() {
        assertThat(properties.getDelay().isEnabled()).isTrue();
    }

    @Test
    @DisplayName("配置属性 transaction.defaultGroup 正确绑定")
    void properties_transactionDefaultGroupBound() {
        assertThat(properties.getTransaction().getDefaultGroup()).isEqualTo("starter-tx-group");
    }

    @Test
    @DisplayName("配置属性 transaction.maxCheckTimes 正确绑定")
    void properties_transactionMaxCheckTimesBound() {
        assertThat(properties.getTransaction().getMaxCheckTimes()).isEqualTo(3);
    }

    @Test
    @DisplayName("配置属性 health.enabled 正确绑定")
    void properties_healthEnabledBound() {
        assertThat(properties.getHealth().isEnabled()).isTrue();
    }

    // ===================== 生命周期相位验证 =====================

    @Test
    @DisplayName("SchedulerLifecycle 相位为 Integer.MAX_VALUE - 100(高于 ListenerContainer)")
    void schedulerLifecycle_phaseHigherThanListener() {
        StreamMQSchedulerLifecycle schedulerLifecycle =
            applicationContext.getBean(StreamMQSchedulerLifecycle.class);
        StreamMQListenerContainerLifecycle listenerLifecycle =
            applicationContext.getBean(StreamMQListenerContainerLifecycle.class);
        assertThat(schedulerLifecycle.getPhase()).isEqualTo(Integer.MAX_VALUE - 100);
        assertThat(listenerLifecycle.getPhase()).isEqualTo(Integer.MAX_VALUE - 200);
        assertThat(schedulerLifecycle.getPhase()).isGreaterThan(listenerLifecycle.getPhase());
    }

    @Test
    @DisplayName("SmartLifecycle Bean 启动后 isRunning 为 true")
    void smartLifecycle_runningAfterStart() {
        StreamMQSchedulerLifecycle schedulerLifecycle =
            applicationContext.getBean(StreamMQSchedulerLifecycle.class);
        StreamMQListenerContainerLifecycle listenerLifecycle =
            applicationContext.getBean(StreamMQListenerContainerLifecycle.class);
        assertThat(schedulerLifecycle.isRunning()).isTrue();
        assertThat(listenerLifecycle.isRunning()).isTrue();
    }

    // ===================== RedissonClient Bean 验证 =====================

    @Test
    @DisplayName("RedissonClient Bean 存在且已连接")
    void redissonClient_beanExistsAndConnected() {
        assertThat(redissonClient).isNotNull();
        assertThat(redissonClient.isShutdown()).isFalse();
    }
}
