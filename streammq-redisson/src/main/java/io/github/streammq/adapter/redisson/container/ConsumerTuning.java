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
}
