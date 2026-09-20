/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.cloud.k8s;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.github.streammq.cloud.k8s.autoscaler.HpaAutoScaler;
import io.github.streammq.cloud.k8s.config.ConfigMapConfigRefresher;
import io.github.streammq.cloud.k8s.operator.StreamMQClusterController;
import io.github.streammq.core.listener.StreamMQListenerContainer;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * {@link CloudK8sProperties} 属性校验与自动装配接线测试。
 *
 * <p><b>锁定的红队发现：item-7（属性名/访问器与 {@link CloudK8sAutoConfiguration} 使用不一致会静默失效）与 K2 的相位/超时边界</b>
 *
 * <p><b>失败即红说明：</b>
 *
 * <ul>
 *   <li>用例一（item-7，本轮新增修复）：文档/CRD yaml/WARN 文案统一使用点号写法 {@code
 *       streammq.cloud.k8s.operator.watch-all-namespaces}，但扁平字段 {@code operatorWatchAllNamespaces}
 *       只能被 kebab 写法 {@code operator-watch-all-namespaces} 绑定——按文档配置时两个开关被静默忽略（收敛模式失效、仍要求
 *       ClusterRole）。修复方式：开关收进嵌套的 {@code CloudK8sProperties.Operator} 子对象，点号写法生效，扁平访问器
 *       保留为委托（自动装配调用点不变）。撤销修复后 {@code isOperatorWatchAllNamespaces()} 回落到默认 true，用例一立即红；
 *   <li>用例二（item-7）：兼容写法 kebab 必须继续生效（避免修复引入回归）；
 *   <li>用例三（K2）：{@code getPhase()} 必须是 {@code Integer.MAX_VALUE - 150} 且大于 starter 容器生命周期的 {@code
 *       Integer.MAX_VALUE - 200}，否则停止顺序反转（先停容器再 pause，pause 成为死代码）；
 *   <li>用例四（K2/属性校验）：{@code graceful-shutdown-timeout-ms} 为负值时不得等待、不得抛异常（旧实现用该值直接睡满，
 *       负值语义混乱）；正超时下未实现 {@code InFlightAware} 的容器也不得睡满整窗口（旧实现无条件睡满 30s）；
 *   <li>用例五（K2）：{@code stop()} 后再 {@code destroy()} 幂等，容器只被 pause 一次。
 * </ul>
 *
 * <p>依赖假设：用例一的 {@link ApplicationContextRunner} 来自 spring-boot-test（自动装配本身的真实接线）； 用例二~四用 Mockito
 * 桩容器 + 计时上界断言（不依赖 fabric8，也不依赖容器实现）。
 *
 * @author StreamMQ Contributors
 */
@DisplayName("CloudK8sProperties 属性校验与自动装配接线测试（item-7/K2）")
class CloudK8sPropertiesWiringTest {

    @Test
    @DisplayName("item-7 - 文档中的点号写法 operator.watch-* 必须生效并接线到 Operator/HPA/ConfigMap 刷新器")
    void documentedDottedPropertiesAreBoundAndPropagatedToBeans() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(CloudK8sAutoConfiguration.class))
                .withPropertyValues(
                        "streammq.cloud.k8s.enabled=true",
                        "streammq.cloud.k8s.operator.watch-all-namespaces=false",
                        "streammq.cloud.k8s.operator.watch-namespaces[0]=ns-a",
                        "streammq.cloud.k8s.operator.watch-namespaces[1]=ns-b",
                        "streammq.cloud.k8s.config-refresh-enabled=true",
                        "streammq.cloud.k8s.config-watch-namespaces[0]=cm-ns",
                        "streammq.cloud.k8s.graceful-shutdown-timeout-ms=-1")
                .run(
                        context -> {
                            assertThat(context).hasNotFailed();
                            CloudK8sProperties properties =
                                    context.getBean(CloudK8sProperties.class);
                            assertThat(properties.isOperatorWatchAllNamespaces())
                                    .as(
                                            "watch-all-namespaces 必须能绑定为 false：文档/CRD yaml"
                                                    + " 用的就是点号写法，绑定失败会让收敛模式静默失效并继续要求 ClusterRole")
                                    .isFalse();
                            assertThat(properties.getOperatorWatchNamespaces())
                                    .containsExactly("ns-a", "ns-b");
                            assertThat(properties.getConfigWatchNamespaces())
                                    .containsExactly("cm-ns");
                            assertThat(properties.getGracefulShutdownTimeoutMs()).isEqualTo(-1L);

                            StreamMQClusterController controller =
                                    context.getBean(StreamMQClusterController.class);
                            assertThat(readField(controller, "watchAllNamespaces"))
                                    .isEqualTo(false);
                            assertThat(readField(controller, "watchNamespaces"))
                                    .isEqualTo(List.of("ns-a", "ns-b"));

                            HpaAutoScaler scaler = context.getBean(HpaAutoScaler.class);
                            assertThat(readField(scaler, "watchAllNamespaces"))
                                    .as("K9：HPA 扫描范围必须与 operator watch 语义同一开关")
                                    .isEqualTo(false);
                            assertThat(readField(scaler, "watchNamespaces"))
                                    .isEqualTo(List.of("ns-a", "ns-b"));

                            ConfigMapConfigRefresher refresher =
                                    context.getBean(ConfigMapConfigRefresher.class);
                            assertThat(readField(refresher, "watchNamespaces"))
                                    .as("config-watch-namespaces 必须收敛到 ConfigMap 刷新器的 watch 列表")
                                    .isEqualTo(List.of("cm-ns"));
                        });
    }

    @Test
    @DisplayName("item-7 - 兼容写法：kebab（operator-watch-*）同样必须绑定生效")
    void legacyKebabPropertiesRemainBound() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(CloudK8sAutoConfiguration.class))
                .withPropertyValues(
                        "streammq.cloud.k8s.enabled=true",
                        "streammq.cloud.k8s.operator-watch-all-namespaces=false",
                        "streammq.cloud.k8s.operator-watch-namespaces[0]=legacy-ns")
                .run(
                        context -> {
                            assertThat(context).hasNotFailed();
                            CloudK8sProperties properties =
                                    context.getBean(CloudK8sProperties.class);
                            assertThat(properties.isOperatorWatchAllNamespaces()).isFalse();
                            assertThat(properties.getOperatorWatchNamespaces())
                                    .containsExactly("legacy-ns");
                            assertThat(
                                            readField(
                                                    context.getBean(HpaAutoScaler.class),
                                                    "watchNamespaces"))
                                    .isEqualTo(List.of("legacy-ns"));
                        });
    }

    @Test
    @DisplayName("item-7 - 默认值：enabled=false、watch 全命名空间、config-refresh 关闭")
    void defaultsMatchDocumentedBehaviour() {
        CloudK8sProperties properties = new CloudK8sProperties();
        assertThat(properties.isEnabled()).isFalse();
        assertThat(properties.isOperatorWatchAllNamespaces()).isTrue();
        assertThat(properties.getOperatorWatchNamespaces()).isNull();
        assertThat(properties.isConfigRefreshEnabled()).isFalse();
        assertThat(properties.getGracefulShutdownTimeoutMs())
                .isEqualTo(CloudK8sProperties.DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_MS);
    }

    @Test
    @DisplayName("K2 - getPhase 必须先于容器生命周期停止（MAX_VALUE-150 且大于 MAX_VALUE-200）")
    void gracefulShutdownPhasePrecedesContainerLifecycle() {
        GracefulShutdownHandler handler =
                new GracefulShutdownHandler(
                        new SingleContainerProvider(null), new CloudK8sProperties());
        assertThat(handler.getPhase()).isEqualTo(Integer.MAX_VALUE - 150);
        assertThat(GracefulShutdownHandler.PHASE)
                .as("降序停止时 phase 更大者先执行：必须大于 starter 容器生命周期的 MAX_VALUE-200")
                .isGreaterThan(Integer.MAX_VALUE - 200);
        assertThat(handler.isAutoStartup()).isTrue();
    }

    @Test
    @DisplayName("K2 - stop() 必须 pause 容器且不无条件睡满 grace（未实现 InFlightAware 的容器直接跳过等待）")
    void stopPausesContainerWithoutSleepingFullGraceWindow() {
        StreamMQListenerContainer container = mock(StreamMQListenerContainer.class);
        CloudK8sProperties properties = new CloudK8sProperties();
        properties.setGracefulShutdownTimeoutMs(30_000L);
        GracefulShutdownHandler handler =
                new GracefulShutdownHandler(new SingleContainerProvider(container), properties);

        long startNanos = System.nanoTime();
        handler.stop();
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        verify(container).pause();
        verify(container).stop();
        assertThat(handler.isShuttingDown()).isTrue();
        assertThat(handler.isRunning()).isFalse();
        assertThat(elapsedMillis)
                .as("K2：旧实现在此处无条件睡满 30s，叠加容器停止时间必然超过 K8s terminationGracePeriodSeconds")
                .isLessThan(1_000L);
    }

    @Test
    @DisplayName("K2 - 负超时语义：不等待、不抛异常，仍完成 pause → stop")
    void negativeGracefulTimeoutIsTreatedAsNoWait() {
        StreamMQListenerContainer container = mock(StreamMQListenerContainer.class);
        CloudK8sProperties properties = new CloudK8sProperties();
        properties.setGracefulShutdownTimeoutMs(-1L);
        GracefulShutdownHandler handler =
                new GracefulShutdownHandler(new SingleContainerProvider(container), properties);

        long startNanos = System.nanoTime();
        handler.stop();
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        verify(container).pause();
        verify(container).stop();
        assertThat(elapsedMillis).isLessThan(1_000L);
    }

    @Test
    @DisplayName("K2 - stop() 后再 destroy() 幂等：容器只被 pause/stop 一次")
    void stopThenDestroyPausesContainerExactlyOnce() {
        StreamMQListenerContainer container = mock(StreamMQListenerContainer.class);
        CloudK8sProperties properties = new CloudK8sProperties();
        properties.setGracefulShutdownTimeoutMs(0L);
        GracefulShutdownHandler handler =
                new GracefulShutdownHandler(new SingleContainerProvider(container), properties);

        handler.stop();
        handler.destroy();
        handler.destroy();

        verify(container, times(1)).pause();
        verify(container, times(1)).stop();
    }

    private static Object readField(Object target, String name) {
        try {
            Field field = target.getClass().getDeclaredField(name);
            field.setAccessible(true);
            return field.get(target);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Cannot read field " + name, e);
        }
    }

    /** 固定返回同一容器的 {@link ObjectProvider} 桩（避免裸类型转换触发的 unchecked 告警）。 */
    private static final class SingleContainerProvider
            implements ObjectProvider<StreamMQListenerContainer> {

        private final StreamMQListenerContainer container;

        SingleContainerProvider(StreamMQListenerContainer container) {
            this.container = container;
        }

        @Override
        public StreamMQListenerContainer getObject() {
            return container;
        }

        @Override
        public StreamMQListenerContainer getObject(Object... args) {
            return container;
        }

        @Override
        public StreamMQListenerContainer getIfAvailable() {
            return container;
        }

        @Override
        public StreamMQListenerContainer getIfUnique() {
            return container;
        }

        @Override
        public Iterator<StreamMQListenerContainer> iterator() {
            return container == null
                    ? List.<StreamMQListenerContainer>of().iterator()
                    : List.of(container).iterator();
        }
    }
}
