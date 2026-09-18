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
import java.util.Objects;
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
 *   <li>客户端地址不可信（可伪造 {@code X-Forwarded-For}）：因此状态表<b>有界</b>，超出上限时淘汰过期条目， 防止来源地址抖动导致无界增长；
 *       若淘汰后仍无空间，则<b>拒绝新客户端</b>（fail-closed）并节流告警，绝不无界增长
 *   <li>淘汰与扫描均为有界操作（{@link #MAX_EVICT_SCAN_PER_CALL} / {@link #MAX_EVICT_PER_CALL}），
 *       单次鉴权请求的成本为常数级，不会被"海量来源地址"放大
 *   <li>非 Web 环境无法识别来源时退化为全局计数（{@code "unknown"}），仍具备基本防护
 *   <li>限流只拦截失败的暴力尝试，不影响正常凭据的成功鉴权（成功后复位）
 *   <li>统计窗口与锁定期的起始时间取<b>鉴权返回之后</b>的时间戳，慢鉴权不会让窗口计时偏早
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

    /**
     * 每次调用最多<b>扫描</b>的条目数（限制单次请求的最坏成本）。
     *
     * <p>历史缺陷：只限制淘汰数（{@link #MAX_EVICT_PER_CALL}）而不限制扫描数，表满时每个请求都要遍历整张表 （最坏
     * O(表大小)），攻击者可用海量来源地址把鉴权路径放大成 CPU 放大器。 现在扫描与淘汰双上限，单请求成本为常数级。
     */
    private static final int MAX_EVICT_SCAN_PER_CALL = 256;

    /** "客户端表已满并拒绝新客户端"告警的最小间隔（毫秒），避免 fail-closed 期间日志洪泛 */
    private static final long TABLE_FULL_WARN_INTERVAL_MILLIS = 60_000L;

    private final ManagementAuthenticator delegate;
    private final int maxFailures;
    private final long windowMillis;
    private final long lockoutMillis;
    private final int maxClients;

    /** 客户端地址可信策略（装配层注入；替代此前的静态全局配置）。 */
    private final WebRequestAuthSupport.ClientAddressPolicy addressPolicy;

    private final ConcurrentHashMap<String, ClientState> states = new ConcurrentHashMap<>();

    /** 上次"表满拒绝新客户端"告警时间（毫秒），用于告警节流；仅告警路径读写，无需原子性保证。 */
    private volatile long lastTableFullWarnMillis;

    /**
     * 使用默认参数构造：10 次失败/60s 窗口，锁定 5 分钟，状态表上限 10000。
     *
     * @param delegate 被包装的鉴权器（不允许为 null）
     */
    public RateLimitedAuthenticator(ManagementAuthenticator delegate) {
        this(delegate, WebRequestAuthSupport.ClientAddressPolicy.DEFAULT);
    }

    /**
     * 构造并指定客户端地址可信策略。
     *
     * @param delegate 被包装的鉴权器（不允许为 null）
     * @param addressPolicy 客户端地址可信策略（不允许为 null）
     */
    public RateLimitedAuthenticator(
            ManagementAuthenticator delegate,
            WebRequestAuthSupport.ClientAddressPolicy addressPolicy) {
        this(
                delegate,
                addressPolicy,
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
        this(
                delegate,
                WebRequestAuthSupport.ClientAddressPolicy.DEFAULT,
                maxFailures,
                windowMillis,
                lockoutMillis,
                maxClients);
    }

    /**
     * 全参构造（含客户端地址可信策略）。
     *
     * @param delegate 被包装的鉴权器（不允许为 null）
     * @param addressPolicy 客户端地址可信策略（不允许为 null）
     * @param maxFailures 窗口内允许的最大失败次数（必须 &gt; 0）
     * @param windowMillis 统计窗口毫秒（必须 &gt; 0）
     * @param lockoutMillis 锁定时长毫秒（必须 &gt;= 0）
     * @param maxClients 状态表上限（必须 &gt; 0）
     */
    public RateLimitedAuthenticator(
            ManagementAuthenticator delegate,
            WebRequestAuthSupport.ClientAddressPolicy addressPolicy,
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
        this.addressPolicy = java.util.Objects.requireNonNull(addressPolicy, "addressPolicy");
        this.maxFailures = maxFailures;
        this.windowMillis = windowMillis;
        this.lockoutMillis = lockoutMillis;
        this.maxClients = maxClients;
    }

    @Override
    public boolean authenticate(String username, String password, String resource) {
        String clientId = resolveClientId();
        ClientState state = states.get(clientId);
        if (Objects.isNull(state)) {
            // fail-closed：状态表已满且有限扫描无法释放空间时，拒绝尚未登记的新客户端
            if (!admitNewClient(System.currentTimeMillis(), clientId, resource)) {
                return false;
            }
            state = states.computeIfAbsent(clientId, k -> new ClientState());
        }
        long nowBefore = System.currentTimeMillis();
        if (state.isLocked(nowBefore)) {
            return false;
        }
        boolean ok = delegate.authenticate(username, password, resource);
        // 时间基准在 delegate 之后重取：鉴权本身可能很慢（远端校验 / 慢哈希），
        // 用调用前的时间会让窗口计时与锁定期整体偏早，等于放宽了限流。
        long now = System.currentTimeMillis();
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

    /** 解析客户端标识：优先真实地址，无法识别时退化为全局标识。地址解析遵循注入的可信策略。 */
    private String resolveClientId() {
        String addr = WebRequestAuthSupport.getClientAddressFromRequest(addressPolicy);
        return StringUtils.isNotEmpty(addr) ? addr : UNKNOWN_CLIENT;
    }

    /**
     * 新客户端准入判定（fail-closed 兜底）。
     *
     * <p>表未满直接放行；已满时先做一次<b>有界</b>扫描尝试淘汰过期条目（见 {@link #evictExpired(long)}），
     * 仍无空间则拒绝该新客户端，避免无界内存增长——状态表无界比拒绝少量新来源更危险。拒绝时按 {@link #TABLE_FULL_WARN_INTERVAL_MILLIS} 节流告警一次。
     *
     * @param now 当前时间毫秒
     * @param clientId 客户端标识
     * @param resource 受保护资源
     * @return true 允许登记该客户端
     */
    private boolean admitNewClient(long now, String clientId, String resource) {
        if (states.size() < maxClients) {
            return true;
        }
        evictExpired(now);
        if (states.size() < maxClients) {
            return true;
        }
        long lastWarn = lastTableFullWarnMillis;
        if (now - lastWarn >= TABLE_FULL_WARN_INTERVAL_MILLIS) {
            lastTableFullWarnMillis = now;
            LOG.warn(
                    "Management auth client table is full ({} entries, detected within a {}-entry"
                            + " bounded scan) and no expired entry could be evicted: rejecting new"
                            + " client '{}' (resource={}, limiter=fail-closed)",
                    maxClients,
                    MAX_EVICT_SCAN_PER_CALL,
                    clientId,
                    resource);
        }
        return false;
    }

    /** 状态表超限时淘汰非锁定且窗口过期的条目（扫描与淘汰均为有界，单请求成本常数级）。 */
    private void evictIfNeeded(long now) {
        if (states.size() <= maxClients) {
            return;
        }
        evictExpired(now);
    }

    /** 有界扫描并淘汰过期条目：最多检查 {@link #MAX_EVICT_SCAN_PER_CALL} 个、最多淘汰 {@link #MAX_EVICT_PER_CALL} 个。 */
    private void evictExpired(long now) {
        int scanned = 0;
        int evicted = 0;
        for (Iterator<Map.Entry<String, ClientState>> it = states.entrySet().iterator();
                it.hasNext()
                        && scanned < MAX_EVICT_SCAN_PER_CALL
                        && evicted < MAX_EVICT_PER_CALL; ) {
            Map.Entry<String, ClientState> e = it.next();
            scanned++;
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
