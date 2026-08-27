/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.cloud.k8s;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** {@link CloudK8sProperties} 单元测试，验证默认值与属性设置。 */
@DisplayName("K8s 云原生配置属性测试")
class CloudK8sPropertiesTest {

    @Test
    @DisplayName("默认 enabled 为 false（模块默认关闭，需显式开启）")
    void defaultEnabledIsFalse() {
        CloudK8sProperties properties = new CloudK8sProperties();
        assertThat(properties.isEnabled()).isFalse();
    }

    @Test
    @DisplayName("默认 operatorWatchAllNamespaces 为 true 且 watch 列表为空")
    void defaultOperatorWatchSettings() {
        CloudK8sProperties properties = new CloudK8sProperties();
        assertThat(properties.isOperatorWatchAllNamespaces()).isTrue();
        assertThat(properties.getOperatorWatchNamespaces()).isNull();

        properties.setOperatorWatchAllNamespaces(false);
        properties.setOperatorWatchNamespaces(java.util.List.of("ns-a", "ns-b"));
        assertThat(properties.isOperatorWatchAllNamespaces()).isFalse();
        assertThat(properties.getOperatorWatchNamespaces()).containsExactly("ns-a", "ns-b");
    }

    @Test
    @DisplayName("默认 gracefulShutdownTimeoutMs 为 30000")
    void defaultGracefulShutdownTimeoutMs() {
        CloudK8sProperties properties = new CloudK8sProperties();
        assertThat(properties.getGracefulShutdownTimeoutMs()).isEqualTo(30000L);
    }

    @Test
    @DisplayName("默认 healthEndpointEnabled 为 true")
    void defaultHealthEndpointEnabledIsTrue() {
        CloudK8sProperties properties = new CloudK8sProperties();
        assertThat(properties.isHealthEndpointEnabled()).isTrue();
    }

    @Test
    @DisplayName("默认 configRefreshEnabled 为 false")
    void defaultConfigRefreshEnabledIsFalse() {
        CloudK8sProperties properties = new CloudK8sProperties();
        assertThat(properties.isConfigRefreshEnabled()).isFalse();
    }

    @Test
    @DisplayName("属性可正确设置与读取")
    void canSetAndReadProperties() {
        CloudK8sProperties properties = new CloudK8sProperties();
        properties.setEnabled(false);
        properties.setGracefulShutdownTimeoutMs(60000L);
        properties.setHealthEndpointEnabled(false);
        properties.setConfigRefreshEnabled(true);
        assertThat(properties.isEnabled()).isFalse();
        assertThat(properties.getGracefulShutdownTimeoutMs()).isEqualTo(60000L);
        assertThat(properties.isHealthEndpointEnabled()).isFalse();
        assertThat(properties.isConfigRefreshEnabled()).isTrue();
    }
}
