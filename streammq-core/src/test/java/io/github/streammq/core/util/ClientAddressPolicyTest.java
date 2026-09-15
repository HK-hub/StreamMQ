/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link WebRequestAuthSupport.ClientAddressPolicy} 单元测试。
 *
 * <p>该策略是"是否信任 {@code X-Forwarded-For}"的安全判定核心，此前逻辑埋在静态方法里、依赖真实请求上下文而无法 单测；改为值对象后可以逐条固定安全语义（尤其
 * fail-closed 方向）。
 */
@DisplayName("客户端地址可信策略")
class ClientAddressPolicyTest {

    @Test
    @DisplayName("安全默认：不信任 X-Forwarded-For，且无可信代理 CIDR")
    void defaultPolicyIsFailClosed() {
        WebRequestAuthSupport.ClientAddressPolicy policy =
                WebRequestAuthSupport.ClientAddressPolicy.DEFAULT;
        assertThat(policy.trustForwardedHeaders()).isFalse();
        assertThat(policy.trustedProxyCidrs()).isEmpty();
    }

    @Test
    @DisplayName("回环地址始终视为可信对端（本机代理）")
    void loopbackIsAlwaysTrustedPeer() {
        WebRequestAuthSupport.ClientAddressPolicy policy =
                new WebRequestAuthSupport.ClientAddressPolicy(true, Set.of("10.0.0.0/8"));
        assertThat(policy.isTrustedPeer("127.0.0.1")).isTrue();
        assertThat(policy.isTrustedPeer("::1")).isTrue();
    }

    @Test
    @DisplayName("仅命中可信 CIDR 的对端才被视为可信代理")
    void cidrMatchDeterminesTrust() {
        WebRequestAuthSupport.ClientAddressPolicy policy =
                new WebRequestAuthSupport.ClientAddressPolicy(true, Set.of("10.0.0.0/8"));
        assertThat(policy.isTrustedPeer("10.1.2.3")).isTrue();
        assertThat(policy.isTrustedPeer("192.168.1.1")).isFalse();
    }

    @Test
    @DisplayName("地址族不一致 / 非法地址 / 空地址一律不可信（fail-closed）")
    void malformedOrMismatchedAddressIsNeverTrusted() {
        WebRequestAuthSupport.ClientAddressPolicy policy =
                new WebRequestAuthSupport.ClientAddressPolicy(true, Set.of("2001:db8::/32"));
        assertThat(policy.isTrustedPeer("10.0.0.1")).isFalse(); // IPv4 vs IPv6
        assertThat(policy.isTrustedPeer("not-an-ip")).isFalse();
        assertThat(policy.isTrustedPeer(null)).isFalse();
        assertThat(policy.isTrustedPeer("")).isFalse();
        assertThat(policy.isTrustedPeer("2001:db8::5")).isTrue();
    }

    @Test
    @DisplayName("CIDR 集合不可变，null 视为空集（安全退化）")
    void cidrSetIsImmutableAndNullSafe() {
        WebRequestAuthSupport.ClientAddressPolicy fromNull =
                new WebRequestAuthSupport.ClientAddressPolicy(true, null);
        assertThat(fromNull.trustedProxyCidrs()).isEmpty();

        Set<String> mutable = new HashSet<>(Set.of("10.0.0.0/8"));
        WebRequestAuthSupport.ClientAddressPolicy policy =
                new WebRequestAuthSupport.ClientAddressPolicy(true, mutable);
        mutable.add("192.168.0.0/16");
        // 构造后不受外部修改影响（Set.copyOf 快照）
        assertThat(policy.trustedProxyCidrs()).containsExactly("10.0.0.0/8");
        assertThatThrownBy(() -> policy.trustedProxyCidrs().add("1.2.3.4/32"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
