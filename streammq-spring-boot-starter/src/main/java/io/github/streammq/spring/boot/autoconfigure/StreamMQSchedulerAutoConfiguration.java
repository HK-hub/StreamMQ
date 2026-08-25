/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.scheduler.DelayMessageScheduler;
import io.github.streammq.adapter.redisson.scheduler.PelClaimScheduler;
import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.adapter.redisson.scheduler.TransactionScanner;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.metrics.StreamMQMetrics;
import io.github.streammq.core.scheduler.StreamMQScheduler;
import io.github.streammq.spring.boot.StreamMQSpringConstants;
import io.github.streammq.spring.boot.properties.StreamMQProperties;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 调度器自动装配：注册 {@link RetryScheduler}、{@link DelayMessageScheduler}、 {@link TransactionScanner}，并通过统一
 * {@link SmartLifecycle} 管理启停顺序。
 *
 * <p>启动相位 {@code Integer.MAX_VALUE - 300}（低于 Listener 容器的 {@code Integer.MAX_VALUE - 200}）， 确保调度器在
 * Listener 容器启动前已就绪。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(
        prefix = StreamMQSpringConstants.PROP_PREFIX,
        name = StreamMQSpringConstants.PROP_NAME_ENABLED,
        havingValue = StreamMQSpringConstants.PROP_VALUE_TRUE,
        matchIfMissing = true)
@ConditionalOnClass({RetryScheduler.class, RedissonClient.class})
public class StreamMQSchedulerAutoConfiguration {

    private static final Logger LOG =
            LoggerFactory.getLogger(StreamMQSchedulerAutoConfiguration.class);

    /** 启动相位：高于 Listener 容器，确保先启动 */
    public static final int PHASE = StreamMQSchedulerLifecycle.PHASE;

    /**
     * 重试调度器：当 {@code streammq.retry.enabled=true}（默认）时注册。
     *
     * @param redisson Redisson 客户端
     * @param properties 配置
     * @return 重试调度器
     */
    @Bean
    @ConditionalOnMissingBean(RetryScheduler.class)
    @ConditionalOnProperty(
            prefix = StreamMQSpringConstants.PROP_PREFIX_RETRY,
            name = StreamMQSpringConstants.PROP_NAME_ENABLED,
            havingValue = StreamMQSpringConstants.PROP_VALUE_TRUE,
            matchIfMissing = true)
    public RetryScheduler streamMQRetryScheduler(
            RedissonClient redisson,
            StreamMQProperties properties,
            ObjectProvider<StreamMQMetrics> metricsProvider) {
        Duration interval = properties.getRetry().getScanInterval();
        int batchSize = properties.getRetry().getBatchSize();
        int streamMaxLen = properties.getRetry().getStreamMaxLen();
        LOG.info(
                "Creating RetryScheduler: scanInterval={}, batchSize={}, streamMaxLen={}",
                interval,
                batchSize,
                streamMaxLen);
        // RetryScheduler 当前未暴露指标埋点接口；重试指标由 DefaultRetryAndDlqHandler 在调度重试时记录。
        RetryScheduler retryScheduler =
                new RetryScheduler(
                        redisson,
                        properties.getNamespace(),
                        interval.toMillis(),
                        batchSize,
                        streamMaxLen);
        retryScheduler.setFailureRequeueBackoffMs(
                properties.getRetry().getFailureRequeueBackoffMs());
        return retryScheduler;
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
    @ConditionalOnProperty(
            prefix = StreamMQSpringConstants.PROP_PREFIX_DELAY,
            name = StreamMQSpringConstants.PROP_NAME_ENABLED,
            havingValue = StreamMQSpringConstants.PROP_VALUE_TRUE,
            matchIfMissing = true)
    public DelayMessageScheduler streamMQDelayMessageScheduler(
            RedissonClient redisson,
            StreamMQProperties properties,
            ObjectProvider<StreamMQMetrics> metricsProvider) {
        Duration interval = properties.getDelay().getScanInterval();
        int batchSize = properties.getDelay().getBatchSize();
        LOG.info(
                "Creating DelayMessageScheduler: scanInterval={}, batchSize={}",
                interval,
                batchSize);
        DelayMessageScheduler scheduler =
                new DelayMessageScheduler(
                        redisson, properties.getNamespace(), interval.toMillis(), batchSize);
        scheduler.setFailureRequeueBackoffMs(properties.getDelay().getFailureRequeueBackoffMs());
        StreamMQMetrics metrics = metricsProvider.getIfAvailable();
        if (metrics != null) {
            scheduler.setMetrics(metrics);
            LOG.info(
                    "StreamMQMetrics injected into DelayMessageScheduler: delay delivery metrics"
                            + " enabled");
        }
        return scheduler;
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
    @ConditionalOnProperty(
            prefix = StreamMQSpringConstants.PROP_PREFIX_TRANSACTION,
            name = StreamMQSpringConstants.PROP_NAME_ENABLED,
            havingValue = StreamMQSpringConstants.PROP_VALUE_TRUE,
            matchIfMissing = true)
    public TransactionScanner streamMQTransactionScanner(
            RedissonClient redisson,
            MessageConverter messageConverter,
            StreamMQProperties properties,
            ObjectProvider<StreamMQMetrics> metricsProvider) {
        Duration interval = properties.getTransaction().getCheckInterval();
        int maxCheck = properties.getTransaction().getMaxCheckTimes();
        LOG.info(
                "Creating TransactionScanner: checkInterval={}, maxCheckTimes={}",
                interval,
                maxCheck);
        TransactionScanner scanner =
                new TransactionScanner(
                        redisson,
                        properties.getNamespace(),
                        messageConverter,
                        interval.toMillis(),
                        maxCheck,
                        TransactionScanner.DEFAULT_BATCH_SIZE);
        StreamMQMetrics metrics = metricsProvider.getIfAvailable();
        if (metrics != null) {
            scanner.setMetrics(metrics);
            LOG.info(
                    "StreamMQMetrics injected into TransactionScanner: transaction metrics"
                            + " enabled");
        }
        return scanner;
    }

    /**
     * 顺序消费 PEL 认领调度器：当 {@code streammq.retry.enabled=true}（默认）时注册。
     *
     * <p>负责恢复顺序消费 SUSPEND/崩溃后遗留的 PEL 消息。目标由 {@code
     * DefaultStreamMQListenerContainer#registerPelClaimTargets} 在容器启动时注册。
     *
     * @param redisson Redisson 客户端
     * @param properties 配置
     * @return PEL 认领调度器
     */
    @Bean
    @ConditionalOnMissingBean(PelClaimScheduler.class)
    @ConditionalOnProperty(
            prefix = StreamMQSpringConstants.PROP_PREFIX_RETRY,
            name = StreamMQSpringConstants.PROP_NAME_ENABLED,
            havingValue = StreamMQSpringConstants.PROP_VALUE_TRUE,
            matchIfMissing = true)
    public PelClaimScheduler streamMQPelClaimScheduler(
            RedissonClient redisson, StreamMQProperties properties) {
        long intervalMs = properties.getRetry().getPelClaimScanInterval().toMillis();
        long minIdleMs = properties.getRetry().getPelClaimMinIdleMs();
        int batchSize = properties.getRetry().getBatchSize();
        LOG.info(
                "Creating PelClaimScheduler: scanInterval={}ms, minIdleMs={}ms, batchSize={}",
                intervalMs,
                minIdleMs,
                batchSize);
        return new PelClaimScheduler(
                redisson, properties.getNamespace(), intervalMs, batchSize, minIdleMs);
    }

    /**
     * 调度器统一生命周期管理：在 Spring 容器启动时按顺序启动调度器。
     *
     * <p>所有调度器均为可选，通过 {@code ObjectProvider} 注入，避免单个调度器被条件注解禁用时 导致 Lifecycle Bean 创建失败。
     *
     * @param retrySchedulerProvider 重试调度器（可选）
     * @param delaySchedulerProvider 延时调度器（可选）
     * @param transactionScannerProvider 事务回查调度器（可选）
     * @param pelClaimSchedulerProvider PEL 认领调度器（可选）
     * @return SmartLifecycle
     */
    @Bean
    @ConditionalOnMissingBean(name = StreamMQSpringConstants.BEAN_SCHEDULER_LIFECYCLE)
    public SmartLifecycle streamMQSchedulerLifecycle(
            org.springframework.beans.factory.ObjectProvider<RetryScheduler> retrySchedulerProvider,
            org.springframework.beans.factory.ObjectProvider<DelayMessageScheduler>
                    delaySchedulerProvider,
            org.springframework.beans.factory.ObjectProvider<TransactionScanner>
                    transactionScannerProvider,
            org.springframework.beans.factory.ObjectProvider<PelClaimScheduler>
                    pelClaimSchedulerProvider) {
        List<StreamMQScheduler> schedulers = new ArrayList<>(4);
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
        PelClaimScheduler pelClaim = pelClaimSchedulerProvider.getIfAvailable();
        if (pelClaim != null) {
            schedulers.add(pelClaim);
        }
        LOG.info("Creating StreamMQSchedulerLifecycle with {} scheduler(s)", schedulers.size());
        return new StreamMQSchedulerLifecycle(schedulers);
    }
}
