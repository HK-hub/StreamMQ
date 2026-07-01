package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.scheduler.DelayMessageScheduler;
import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.adapter.redisson.scheduler.TransactionScanner;
import io.github.streammq.core.spi.MessageConverter;
import io.github.streammq.spring.boot.properties.StreamMqProperties;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 调度器自动装配：注册 {@link RetryScheduler}、{@link DelayMessageScheduler}、
 * {@link TransactionScanner}，并通过统一 {@link SmartLifecycle} 管理启停顺序。
 *
 * <p>启动相位 {@code Integer.MAX_VALUE - 100}（高于 Listener 容器），
 * 确保调度器在 Listener 容器启动前已就绪。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "streammq", name = "enabled", havingValue = "true", matchIfMissing = true)
@ConditionalOnClass({RetryScheduler.class, RedissonClient.class})
public class StreamMqSchedulerAutoConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMqSchedulerAutoConfiguration.class);

    /** 启动相位：高于 Listener 容器，确保先启动 */
    public static final int PHASE = Integer.MAX_VALUE - 100;

    /**
     * 重试调度器：当 {@code streammq.retry.enabled=true}（默认）时注册。
     *
     * @param redisson Redisson 客户端
     * @param properties 配置
     * @return 重试调度器
     */
    @Bean
    @ConditionalOnMissingBean(RetryScheduler.class)
    @ConditionalOnProperty(prefix = "streammq.retry", name = "enabled", havingValue = "true", matchIfMissing = true)
    public RetryScheduler streamMqRetryScheduler(RedissonClient redisson, StreamMqProperties properties) {
        Duration interval = properties.getRetry().getScanInterval();
        int batchSize = properties.getRetry().getBatchSize();
        LOG.info("Creating RetryScheduler: scanInterval={}, batchSize={}", interval, batchSize);
        return new RetryScheduler(redisson, properties.getNamespace(),
            interval.toMillis(), batchSize);
    }

    /**
     * 延时消息调度器：当 {@code streammq.delay.enabled=true} 时注册。
     *
     * @param redisson Redisson 客户端
     * @param properties 配置
     * @return 延时调度器
     */
    @Bean
    @ConditionalOnMissingBean(DelayMessageScheduler.class)
    @ConditionalOnProperty(prefix = "streammq.delay", name = "enabled", havingValue = "true", matchIfMissing = true)
    public DelayMessageScheduler streamMqDelayMessageScheduler(RedissonClient redisson,
                                                                StreamMqProperties properties) {
        Duration interval = properties.getDelay().getScanInterval();
        int batchSize = properties.getDelay().getBatchSize();
        LOG.info("Creating DelayMessageScheduler: scanInterval={}, batchSize={}", interval, batchSize);
        return new DelayMessageScheduler(redisson, properties.getNamespace(),
            interval.toMillis(), batchSize);
    }

    /**
     * 事务回查调度器：当 {@code streammq.transaction.enabled=true}（默认）时注册。
     *
     * @param redisson Redisson 客户端
     * @param messageConverter 消息转换器
     * @param properties 配置
     * @return 事务回查调度器
     */
    @Bean
    @ConditionalOnMissingBean(TransactionScanner.class)
    @ConditionalOnProperty(prefix = "streammq.transaction", name = "enabled", havingValue = "true", matchIfMissing = true)
    public TransactionScanner streamMqTransactionScanner(RedissonClient redisson,
                                                          MessageConverter messageConverter,
                                                          StreamMqProperties properties) {
        Duration interval = properties.getTransaction().getCheckInterval();
        int maxCheck = properties.getTransaction().getMaxCheckTimes();
        LOG.info("Creating TransactionScanner: checkInterval={}, maxCheckTimes={}", interval, maxCheck);
        return new TransactionScanner(redisson, properties.getNamespace(), messageConverter,
            interval.toMillis(), maxCheck, TransactionScanner.DEFAULT_BATCH_SIZE);
    }

    /**
     * 调度器统一生命周期管理：在 Spring 容器启动时按顺序启动调度器。
     *
     * <p>所有调度器均为可选，通过 {@code ObjectProvider} 注入，避免单个调度器被条件注解禁用时
     * 导致 Lifecycle Bean 创建失败。
     *
     * @param retrySchedulerProvider 重试调度器（可选）
     * @param delaySchedulerProvider 延时调度器（可选）
     * @param transactionScannerProvider 事务回查调度器（可选）
     * @return SmartLifecycle
     */
    @Bean
    @ConditionalOnMissingBean(name = "streamMqSchedulerLifecycle")
    public SmartLifecycle streamMqSchedulerLifecycle(
            org.springframework.beans.factory.ObjectProvider<RetryScheduler> retrySchedulerProvider,
            org.springframework.beans.factory.ObjectProvider<DelayMessageScheduler> delaySchedulerProvider,
            org.springframework.beans.factory.ObjectProvider<TransactionScanner> transactionScannerProvider) {
        List<Object> schedulers = new ArrayList<>(3);
        RetryScheduler retryScheduler = retrySchedulerProvider.getIfAvailable();
        if (retryScheduler != null) {
            schedulers.add(retryScheduler);
        }
        DelayMessageScheduler delay = delaySchedulerProvider.getIfAvailable();
        if (delay != null) {
            schedulers.add(delay);
        }
        TransactionScanner scanner = transactionScannerProvider.getIfAvailable();
        if (scanner != null) {
            schedulers.add(scanner);
        }
        LOG.info("Creating StreamMqSchedulerLifecycle with {} scheduler(s)", schedulers.size());
        return new StreamMqSchedulerLifecycle(schedulers);
    }
}
