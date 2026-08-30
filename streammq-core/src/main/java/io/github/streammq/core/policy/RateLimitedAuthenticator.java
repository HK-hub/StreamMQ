/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.policy;

import io.github.streammq.core.util.StringUtils;
import io.github.streammq.core.util.WebRequestAuthSupport;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 基于失败次数的客户端限流装饰器。
 *
 * <p>包装任意 {@link ManagementAuthenticator}：同一客户端（按请求来源地址聚合，见 {@link
 * WebRequestAuthSupport#getClientAddressFromRequest()}）在时间窗口内鉴权失败超过阈值后进入锁定期，期间所有
 * 鉴权直接拒绝；鉴权成功后复位计数。用于抵御针对管理/诊断端点的暴力破解。
 *
 * <p>设计约束：
 *
 * <ul>
 *   <li>客户端地址不可信（可伪造 {@code X-Forwarded-For}）：因此状态表<b>有界</b>，超出上限时淘汰过期条目， 防止来源地址抖动导致无界增长
 *   <li>非 Web 环境无法识别来源时退化为全局计数（{@code "unknown"}），仍具备基本防护
 *   <li>限流只拦截失败的暴力尝试，不影响正常凭据的成功鉴权（成功后复位）
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class RateLimitedAuthenticator implements ManagementAuthenticator {

    private static final Logger LOG = LoggerFactory.getLogger(RateLimitedAuthenticator.class);

    /** 无法识别来源时的客户端标识 */
    private static final String UNKNOWN_CLIENT = "unknown";

    /** 默认窗口内允许的最大失败次数 */
    private static final int DEFAULT_MAX_FAILURES = 10;

    /** 默认统计窗口（毫秒）：1 分钟 */
    private static final long DEFAULT_WINDOW_MILLIS = 60_000L;

    /** 默认锁定时长（毫秒）：5 分钟 */
    private static final long DEFAULT_LOCKOUT_MILLIS = 300_000L;

    /** 客户端状态表上限（超出时淘汰过期条目） */
    private static final int DEFAULT_MAX_CLIENTS = 10_000;

    /** 每次调用最多淘汰的过期条目数（限制摊还成本） */
    private static final int MAX_EVICT_PER_CALL = 128;

    private final ManagementAuthenticator delegate;
    private final int maxFailures;
    private final long windowMillis;
    private final long lockoutMillis;
    private final int maxClients;

    private final ConcurrentHashMap<String, ClientState> states = new ConcurrentHashMap<>();

    /**
     * 使用默认参数构造：10 次失败/60s 窗口，锁定 5 分钟，状态表上限 10000。
     *
     * @param delegate 被包装的鉴权器（不允许为 null）
     */
    public RateLimitedAuthenticator(ManagementAuthenticator delegate) {
        this(
                delegate,
                DEFAULT_MAX_FAILURES,
                DEFAULT_WINDOW_MILLIS,
                DEFAULT_LOCKOUT_MILLIS,
                DEFAULT_MAX_CLIENTS);
    }

    /**
     * 全参构造。
     *
     * @param delegate 被包装的鉴权器（不允许为 null）
     * @param maxFailures 窗口内允许的最大失败次数（必须 &gt; 0）
     * @param windowMillis 统计窗口毫秒（必须 &gt; 0）
     * @param lockoutMillis 锁定时长毫秒（必须 &gt;= 0）
     * @param maxClients 状态表上限（必须 &gt; 0）
     */
    public RateLimitedAuthenticator(
            ManagementAuthenticator delegate,
            int maxFailures,
            long windowMillis,
            long lockoutMillis,
            int maxClients) {
        if (delegate == null) {
            throw new IllegalArgumentException("delegate must not be null");
        }
        if (maxFailures <= 0 || windowMillis <= 0 || lockoutMillis < 0 || maxClients <= 0) {
            throw new IllegalArgumentException(
                    "maxFailures/windowMillis/maxClients must be > 0, lockoutMillis must be >= 0");
        }
        this.delegate = delegate;
        this.maxFailures = maxFailures;
        this.windowMillis = windowMillis;
        this.lockoutMillis = lockoutMillis;
        this.maxClients = maxClients;
    }

    @Override
    public boolean authenticate(String username, String password, String resource) {
        String clientId = resolveClientId();
        ClientState state = states.compute(clientId, (k, v) -> v == null ? new ClientState() : v);
        long now = System.currentTimeMillis();
        if (state.isLocked(now)) {
            return false;
        }
        boolean ok = delegate.authenticate(username, password, resource);
        if (ok) {
            state.reset();
        } else {
            if (state.recordFailureAndLock(now, maxFailures, windowMillis, lockoutMillis)) {
                LOG.warn(
                        "Management auth rate limit triggered for client '{}' (resource={}):"
                                + " locked for {}ms",
                        clientId,
                        resource,
                        lockoutMillis);
            }
        }
        evictIfNeeded(now);
        return ok;
    }

    @Override
    public String name() {
        return "rate-limited-" + delegate.name();
    }

    /** 解析客户端标识：优先真实地址，无法识别时退化为全局标识。 */
    private static String resolveClientId() {
        String addr = WebRequestAuthSupport.getClientAddressFromRequest();
        return StringUtils.isNotEmpty(addr) ? addr : UNKNOWN_CLIENT;
    }

    /** 状态表超限时淘汰非锁定且窗口过期的条目，限制内存占用。 */
    private void evictIfNeeded(long now) {
        if (states.size() <= maxClients) {
            return;
        }
        int evicted = 0;
        for (Iterator<Map.Entry<String, ClientState>> it = states.entrySet().iterator();
                it.hasNext() && evicted < MAX_EVICT_PER_CALL; ) {
            Map.Entry<String, ClientState> e = it.next();
            if (!e.getValue().isLocked(now) && !e.getValue().isActiveInWindow(now, windowMillis)) {
                it.remove();
                evicted++;
            }
        }
    }

    /** 单客户端鉴权状态（线程安全）。 */
    private static final class ClientState {
        private long windowStart;
        private int failures;
        private long lockedUntil;

        synchronized boolean isLocked(long now) {
            return now < lockedUntil;
        }

        synchronized boolean isActiveInWindow(long now, long windowMillis) {
            return now - windowStart <= windowMillis && failures > 0;
        }

        /** 记录一次失败；触发锁定时返回 true。 */
        synchronized boolean recordFailureAndLock(
                long now, int maxFailures, long windowMillis, long lockoutMillis) {
            if (now - windowStart > windowMillis) {
                windowStart = now;
                failures = 0;
            }
            failures++;
            if (failures > maxFailures) {
                lockedUntil = now + lockoutMillis;
                return true;
            }
            return false;
        }

        synchronized void reset() {
            windowStart = 0L;
            failures = 0;
            lockedUntil = 0L;
        }
    }
}
