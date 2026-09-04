/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.listener.RedissonBroadcastGroupRegistry;
import io.github.streammq.adapter.redisson.scheduler.BroadcastGroupSweeper;
import io.github.streammq.adapter.redisson.scheduler.DelayMessageScheduler;
import io.github.streammq.adapter.redisson.scheduler.PelClaimScheduler;
import io.github.streammq.adapter.redisson.scheduler.RetryScheduler;
import io.github.streammq.adapter.redisson.scheduler.TransactionScanner;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.listener.BroadcastGroupRegistry;
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
 * <p><b>P1-B 修复：</b>广播消费者组僵尸回收由独立的 {@link BroadcastGroupSweeper} 负责，只要 StreamMQ 启用即运行， 与 {@link
 * PelClaimScheduler}（顺序消费专属）是否启用解耦，消除「禁用 PelClaimScheduler 时广播组永久泄漏」的风险。
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
     * <p><b>P1-B：</b>{@code registryProvider} 传入的 {@link BroadcastGroupRegistry} 不再被本调度器使用
     * （广播组僵尸回收已解耦为 {@link BroadcastGroupSweeper}），此处仅为兼容历史构造签名保留。
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
            RedissonClient redisson,
            StreamMQProperties properties,
            ObjectProvider<BroadcastGroupRegistry> registryProvider) {
        long intervalMs = properties.getRetry().getPelClaimScanInterval().toMillis();
        long minIdleMs = properties.getRetry().getPelClaimMinIdleMs();
        int batchSize = properties.getRetry().getBatchSize();
        LOG.info(
                "Creating PelClaimScheduler: scanInterval={}ms, minIdleMs={}ms, batchSize={}",
                intervalMs,
                minIdleMs,
                batchSize);
        return new PelClaimScheduler(
                redisson,
                properties.getNamespace(),
                intervalMs,
                batchSize,
                minIdleMs,
                registryProvider.getIfAvailable());
    }

    /**
     * 广播消费者组注册表（僵尸组回收策略）。默认实现为 Redisson；用户可覆盖。
     *
     * @param redisson Redisson 客户端
     * @param properties 配置
     * @return 广播组注册表
     */
    @Bean
    @ConditionalOnMissingBean(BroadcastGroupRegistry.class)
    public BroadcastGroupRegistry streamMQBroadcastGroupRegistry(
            RedissonClient redisson, StreamMQProperties properties) {
        return new RedissonBroadcastGroupRegistry(redisson, properties.getNamespace());
    }

    /**
     * 广播消费者组僵尸回收调度器（独立生命周期，与 {@link PelClaimScheduler} 解耦）。
     *
     * <p>只要 StreamMQ 启用即运行，确保广播模式在任意装配形态下（包括 PelClaimScheduler 被禁用时） 均能可靠回收 Redis 僵尸消费者组，避免 Redis
     * 内存随容器重启次数单调增长。
     *
     * @param redisson Redisson 客户端
     * @param properties 配置
     * @param registryProvider 广播组注册表（可选，默认回落 Redisson 实现）
     * @return 广播组僵尸回收调度器
     */
    @Bean
    @ConditionalOnMissingBean(BroadcastGroupSweeper.class)
    public BroadcastGroupSweeper streamMQBroadcastGroupSweeper(
            RedissonClient redisson,
            StreamMQProperties properties,
            ObjectProvider<BroadcastGroupRegistry> registryProvider) {
        BroadcastGroupRegistry registry = registryProvider.getIfAvailable();
        BroadcastGroupSweeper sweeper =
                new BroadcastGroupSweeper(
                        redisson,
                        properties.getNamespace(),
                        io.github.streammq.core.StreamMQConstants
                                .DEFAULT_BROADCAST_SWEEP_INTERVAL_MS,
                        registry);
        LOG.info(
                "Creating BroadcastGroupSweeper: scanIntervalMs={}",
                io.github.streammq.core.StreamMQConstants.DEFAULT_BROADCAST_SWEEP_INTERVAL_MS);
        return sweeper;
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
     * @param broadcastGroupSweeperProvider 广播组僵尸回收调度器（可选）
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
                    pelClaimSchedulerProvider,
            org.springframework.beans.factory.ObjectProvider<BroadcastGroupSweeper>
                    broadcastGroupSweeperProvider) {
        List<StreamMQScheduler> schedulers = new ArrayList<>(5);
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
        BroadcastGroupSweeper sweeper = broadcastGroupSweeperProvider.getIfAvailable();
        if (sweeper != null) {
            schedulers.add(sweeper);
        }
        LOG.info("Creating StreamMQSchedulerLifecycle with {} scheduler(s)", schedulers.size());
        return new StreamMQSchedulerLifecycle(schedulers);
    }
}
