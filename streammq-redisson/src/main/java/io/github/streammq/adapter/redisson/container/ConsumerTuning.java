/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

/**
 * 消费拉取运行参数读视图。
 *
 * <p><b>设计模式：Parameter Object。</b>实现类 {@link DefaultConsumerTuning} 提供带下界保护的写入方法；消费循环与注册流程仅依赖本接口。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface ConsumerTuning {

    int defaultPullBatchSize();

    long defaultPullBlockTimeoutMillis();

    int inflightCapacity();

    /** 解析生效的拉取批量：注解显式指定优先，否则全局默认，夹取上界。 */
    int effectivePullBatchSize(int annotationValue);

    /** 解析生效的拉取间隔：注解显式指定优先，否则全局默认。 */
    long effectivePullInterval(long annotationValue);

    /**
     * 解析生效的顺序消费超时（毫秒）：注解显式指定优先，否则全局默认。
     *
     * <p><b>为什么需要全局默认值通道：</b>{@code @StreamMQConsumer#orderlyConsumeTimeout()} 注解默认 0（不启用），
     * 是刻意为之的安全默认值——顺序消费超时后走串行重试，耗尽即进 DLQ，默认开启会把所有存量 顺序消费者的慢消息系统性误杀。但代价是用户若想全局开启，需在每一个消费者注解上重复声明。
     * 本方法提供「注解显式值优先、否则回落全局默认」的解析通道，与 {@link #effectivePullInterval} 同构。
     *
     * @param annotationValue 注解声明值（0 表示未显式指定）
     * @return 生效的超时毫秒数；0 表示不启用顺序消费超时
     */
    long effectiveOrderlyConsumeTimeoutMillis(long annotationValue);
}
