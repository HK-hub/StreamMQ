package io.github.streammq.cloud.k8s;

import java.util.Objects;
import lombok.extern.slf4j.Slf4j;

/**
 * {@link StreamMQConfigRefresher} 的空操作实现。
 *
 * <p>当用户未提供自定义 {@link StreamMQConfigRefresher} Bean 时使用此默认实现， 所有方法均为空操作，仅记录 debug 级别日志，不产生任何副作用。
 *
 * <p>用户可通过实现 {@link StreamMQConfigRefresher} 接口并注册为 Spring Bean 来覆盖此默认行为，实现真正的配置热更新。
 *
 * <p>通过 {@link CloudK8sAutoConfiguration} 的 {@code @Bean} 方法装配， 当容器中存在其它 {@link
 * StreamMQConfigRefresher} Bean 时自动退让。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Slf4j
public class NoopConfigRefresher implements StreamMQConfigRefresher {

    @Override
    public void refreshRetryPolicy(int maxReconsumeTimes, long[] retryIntervals) {
        log.debug(
                "NoopConfigRefresher#refreshRetryPolicy called but no-op implemented "
                        + "(maxReconsumeTimes={}, retryIntervals length={})",
                maxReconsumeTimes,
                Objects.isNull(retryIntervals) ? 0 : retryIntervals.length);
    }

    @Override
    public void refreshConsumerThreads(int min, int max) {
        log.debug(
                "NoopConfigRefresher#refreshConsumerThreads called but no-op implemented (min={},"
                        + " max={})",
                min,
                max);
    }

    @Override
    public void refreshScanInterval(long retryScanMs, long delayScanMs) {
        log.debug(
                "NoopConfigRefresher#refreshScanInterval called but no-op implemented "
                        + "(retryScanMs={}, delayScanMs={})",
                retryScanMs,
                delayScanMs);
    }
}
