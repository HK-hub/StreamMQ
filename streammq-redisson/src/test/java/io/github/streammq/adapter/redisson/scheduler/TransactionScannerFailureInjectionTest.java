/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.enums.LocalTransactionState;
import io.github.streammq.core.exception.StreamMQBrokerException;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.transaction.TransactionChecker;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.stubbing.Answer;
import org.redisson.api.RMap;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RScript;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.client.codec.StringCodec;
import org.slf4j.LoggerFactory;

/**
 * {@link TransactionScanner} 失败注入单元测试（红队 F-03 / U-04 / U-05 缺口）。
 *
 * <p>覆盖此前 0 行覆盖的事务终结 / 降级路径：
 *
 * <ul>
 *   <li>{@code forceFinalizeStuckCommit}：卡在 COMMITTING 且转投持续失败时的强制终结
 *   <li>预算耗尽后状态置 ROLLBACK，并写入 {@code .failureReason} 供业务对账
 *   <li>{@code forceFinalizeStuckRollback}：卡在 ROLLBACKING 超限时强制终结为 ROLLBACK
 *   <li>{@code invokeCheckerWithTimeout}：回查器抛异常 / 超时均降级 UNKNOWN 并重新入 ZSet
 * </ul>
 *
 * <p>驱动方式：直接调用包内可见的 {@code scanTimeoutHalf(txGroup)}（周期任务共用该实现）。
 *
 * <p>用内存 Map 模拟 Redis Hash、按脚本内容模拟 Lua 语义（CAS 状态机、HINCRBY 计数）。
 *
 * <p>不修改任何 main 代码可见性，也不捏造状态机：断言全部落在生产分支的真实副作用上。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
@DisplayName("TransactionScanner 事务终结与降级失败注入测试")
class TransactionScannerFailureInjectionTest {

    private static final String NAMESPACE = "tx-failure-ns";
    private static final String TX_GROUP = "tx-failure-group";
    private static final String TX_ID = "tx-0001";
    private static final String TARGET_TOPIC = "tx-target-topic";
    private static final String HALF_ID = "1000-0";
    private static final long CHECK_INTERVAL_MS = 1000L;
    private static final int MAX_CHECK_TIMES = 3;
    private static final int BATCH_SIZE = 10;

    private static final String STATE_HASH_KEY =
            StreamMQKeys.transactionStateHash(NAMESPACE, TX_GROUP);
    private static final String CHECK_ZSET_KEY =
            StreamMQKeys.transactionCheckZSet(NAMESPACE, TX_GROUP);
    private static final String COUNTER_KEY =
            StreamMQKeys.transactionCheckCounter(NAMESPACE, TX_GROUP);
    private static final String HALF_STREAM_KEY = StreamMQKeys.halfStream(NAMESPACE, TX_GROUP);

    /** 内存 txstate Hash（模拟 HSET/HGET/HDEL 语义，使状态机读写可观测）。 */
    private final Map<String, String> stateStore = new HashMap<>();

    /** 内存回查计数 Hash（模拟 HINCRBY 语义）。 */
    private final Map<String, String> counterStore = new HashMap<>();

    /** 重新入回查 ZSet 时记录的 score。 */
    private final AtomicReference<Double> rescheduledScore = new AtomicReference<>();

    private RedissonClient redisson;
    private RScoredSortedSet<String> checkZset;
    private RStream<String, String> halfStream;
    private MessageConverter converter;
    private TransactionScanner scanner;
    private Logger scannerLogger;
    private ListAppender<ILoggingEvent> logAppender;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        redisson = mock(RedissonClient.class);
        checkZset = mock(RScoredSortedSet.class);
        halfStream = mock(RStream.class);
        converter = mock(MessageConverter.class);

        doReturn(mapView(stateStore)).when(redisson).getMap(STATE_HASH_KEY, StringCodec.INSTANCE);
        doReturn(mapView(counterStore)).when(redisson).getMap(COUNTER_KEY, StringCodec.INSTANCE);
        doReturn(checkZset).when(redisson).getScoredSortedSet(CHECK_ZSET_KEY, StringCodec.INSTANCE);
        doReturn(halfStream).when(redisson).getStream(HALF_STREAM_KEY, StringCodec.INSTANCE);
        doReturn(luaScript()).when(redisson).getScript(StringCodec.INSTANCE);

        // 回查 ZSet 每轮都返回同一超时 txId（等价于"该事务一直到期未终结"）
        doReturn(List.of(TX_ID))
                .when(checkZset)
                .valueRange(anyDouble(), eq(true), anyDouble(), eq(true), anyInt(), anyInt());
        doAnswer(
                        invocation -> {
                            rescheduledScore.set(
                                    ((Number) invocation.getArgument(0)).doubleValue());
                            return true;
                        })
                .when(checkZset)
                .add(anyDouble(), anyString());

        scanner =
                new TransactionScanner(
                        redisson,
                        NAMESPACE,
                        converter,
                        CHECK_INTERVAL_MS,
                        MAX_CHECK_TIMES,
                        BATCH_SIZE);

        scannerLogger = (Logger) LoggerFactory.getLogger(TransactionScanner.class);
        logAppender = new ListAppender<>();
        logAppender.start();
        scannerLogger.addAppender(logAppender);
    }

    @AfterEach
    void tearDown() {
        if (scannerLogger != null && logAppender != null) {
            scannerLogger.detachAppender(logAppender);
            logAppender.stop();
        }
    }

    // ===================== 场景 A：COMMITTING 超限强制终结 =====================

    @Test
    @DisplayName("COMMITTING 持续转投失败：预算耗尽前不终结，耗尽后强制 ROLLBACK 并写 failureReason")
    void commitStuck_exceedsBudget_forceFinalizesAsRollback() {
        stateStore.put(TX_ID, TransactionScanner.STATE_COMMITTING);
        stateStore.put(TX_ID + ".target", TARGET_TOPIC);
        stateStore.put(TX_ID + ".halfId", HALF_ID);

        // 第 1~maxCheckTimes 轮：仅递增恢复尝试计数，事务仍停留在 COMMITTING（转投持续失败）
        for (int round = 1; round <= MAX_CHECK_TIMES; round++) {
            assertThatCode(() -> scanner.scanTimeoutHalf(TX_GROUP)).doesNotThrowAnyException();
            assertThat(stateStore).containsEntry(TX_ID, TransactionScanner.STATE_COMMITTING);
            assertThat(counterStore).containsEntry(TX_ID, Integer.toString(round));
        }
        // 失败原因被记录（扫描入口不吞异常，降级为 WARN 后继续下一轮）
        assertThat(warnEvents())
                .anySatisfy(
                        event ->
                                assertThat(event.getFormattedMessage())
                                        .contains("triggerCheck failed")
                                        .contains("simulated commit script failure"));

        // 第 maxCheckTimes+1 轮：预算耗尽 → 强制终结（状态非 COMMIT 即等价于"未发布"）
        scanner.scanTimeoutHalf(TX_GROUP);

        assertThat(stateStore)
                .containsEntry(TX_ID, TransactionScanner.STATE_ROLLBACK)
                .containsEntry(TX_ID + ".failureReason", "COMMIT_FAILED_FORCE_ROLLBACK")
                .containsKey(TX_ID + ".done")
                .doesNotContainKeys(TX_ID + ".target", TX_ID + ".halfId");
        assertThat(counterStore).doesNotContainKey(TX_ID);
        verify(checkZset).remove(TX_ID);
        // 尽力 XDEL 半消息，避免半消息流残留
        verify(halfStream).remove(new StreamMessageId(1000L, 0L));
    }

    // ===================== 场景 B：ROLLBACKING 超限强制终结 =====================

    @Test
    @DisplayName("ROLLBACKING 超限：强制终结为 ROLLBACK 并写 failureReason，不再重试 XDEL")
    void rollbackStuck_exceedsBudget_forceFinalizesRollback() {
        stateStore.put(TX_ID, TransactionScanner.STATE_ROLLBACKING);
        stateStore.put(TX_ID + ".halfId", HALF_ID);
        stateStore.put(TX_ID + ".target", TARGET_TOPIC);
        counterStore.put(TX_ID, Integer.toString(MAX_CHECK_TIMES));

        scanner.scanTimeoutHalf(TX_GROUP);

        assertThat(stateStore)
                .containsEntry(TX_ID, TransactionScanner.STATE_ROLLBACK)
                .containsEntry(TX_ID + ".failureReason", "ROLLBACK_FAILED_FORCE_FINALIZE")
                .containsKey(TX_ID + ".done");
        assertThat(counterStore).doesNotContainKey(TX_ID);
        verify(checkZset).remove(TX_ID);
        // 该分支只终结状态：半消息残留由保留期维护任务/人工清理，因此不再尝试 XDEL
        verify(halfStream, never()).remove(ArgumentMatchers.<StreamMessageId>any());
        assertThat(errorEvents())
                .anySatisfy(
                        event ->
                                assertThat(event.getFormattedMessage())
                                        .contains("stuck in ROLLBACKING")
                                        .contains("force-finalizing as ROLLBACK"));
    }

    // ===================== 场景 C：回查器抛异常 / 持续 UNKNOWN =====================

    @Test
    @DisplayName("回查器抛异常：状态降级 UNKNOWN、计数递增并重新入回查 ZSet")
    void checkerThrows_degradesToUnknownAndReschedules() {
        givenReadableHalfMessage();
        long before = System.currentTimeMillis();
        TransactionChecker<String> throwingChecker =
                (message, context) -> {
                    throw new IllegalStateException("local transaction db down");
                };
        scanner.registerChecker(TX_GROUP, throwingChecker);

        scanner.scanTimeoutHalf(TX_GROUP);

        assertThat(stateStore)
                // 计数未耗尽：降级为 UNKNOWN 等待下轮，.halfId 保留供下轮回查
                .containsEntry(TX_ID, TransactionScanner.STATE_UNKNOWN)
                .containsEntry(TX_ID + ".halfId", HALF_ID);
        assertThat(counterStore).containsEntry(TX_ID, "1");
        assertThat(rescheduledScore.get()).isNotNull().isGreaterThan((double) before);
        verify(checkZset).add(anyDouble(), eq(TX_ID));
        assertThat(warnEvents())
                .anySatisfy(
                        event ->
                                assertThat(event.getFormattedMessage())
                                        .contains("treated as UNKNOWN")
                                        .contains("local transaction db down"));
    }

    @Test
    @DisplayName("回查持续 UNKNOWN 达 maxCheckTimes：走回滚而非无限重查")
    void checkerUnknownBeyondBudget_rollsBack() {
        givenReadableHalfMessage();
        TransactionChecker<String> unknownChecker =
                (message, context) -> LocalTransactionState.UNKNOWN;
        scanner.registerChecker(TX_GROUP, unknownChecker);
        counterStore.put(TX_ID, Integer.toString(MAX_CHECK_TIMES));

        scanner.scanTimeoutHalf(TX_GROUP);

        assertThat(stateStore)
                .containsEntry(TX_ID, TransactionScanner.STATE_ROLLBACK)
                .doesNotContainKeys(TX_ID + ".halfId", TX_ID + ".target");
        assertThat(counterStore).doesNotContainKey(TX_ID);
        verify(halfStream).remove(new StreamMessageId(1000L, 0L));
    }

    // ===================== 场景 D：回查器超时 =====================

    @Test
    @DisplayName("回查器超过 checkerTimeoutMillis：不阻塞扫描线程，按 UNKNOWN 有界重查")
    void checkerTimeout_treatedAsUnknownWithoutBlockingScan() {
        givenReadableHalfMessage();
        TransactionChecker<String> slowChecker =
                (message, context) -> {
                    Thread.sleep(2_000L);
                    return LocalTransactionState.COMMIT_MESSAGE;
                };
        scanner.registerChecker(TX_GROUP, slowChecker);
        scanner.setCheckerTimeoutMillis(200L);

        long start = System.nanoTime();
        scanner.scanTimeoutHalf(TX_GROUP);
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000L;

        // 未等待业务回查的 2s 完整耗时（放弃等待，孤儿回查线程的返回值被丢弃）
        assertThat(elapsedMillis).isLessThan(1_500L);
        assertThat(stateStore).containsEntry(TX_ID, TransactionScanner.STATE_UNKNOWN);
        assertThat(counterStore).containsEntry(TX_ID, "1");
        assertThat(warnEvents())
                .anySatisfy(
                        event ->
                                assertThat(event.getFormattedMessage())
                                        .contains("TransactionChecker timed out")
                                        .contains("treated as UNKNOWN"));
    }

    // ===================== 场景 F：R2-2① 终态早退补 .done =====================

    @Test
    @DisplayName("R2-2①：已终态事务的回查早退分支必须补写 .done（否则保留期清理永远扫不到）")
    void terminalEarlyExit_marksDoneForRetention() {
        // 模拟「终态脚本已执行（COMMIT 已写入），实例在 markTerminalDone 前崩溃」：
        // 状态为终态但缺少 .done，若早退分支不补写，该字段永远不会被 sweepExpiredTerminalStates 清理。
        stateStore.put(TX_ID, TransactionScanner.STATE_COMMIT);

        scanner.scanTimeoutHalf(TX_GROUP);

        assertThat(stateStore).as("早退分支必须产生可清理标记 .done").containsKey(TX_ID + ".done");
        verify(checkZset).remove(TX_ID);
        assertThat(counterStore).doesNotContainKey(TX_ID);
    }

    // ===================== 场景 G：R2-3 孤儿回查线程有界 =====================

    @Test
    @DisplayName("R2-3：回查超时后不为同一事务重复起线程，线程结束后标记清除并可再次回查")
    void timedOutChecker_boundsConcurrentCheckerThreads() throws Exception {
        givenReadableHalfMessage();
        java.util.concurrent.atomic.AtomicInteger invocations =
                new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        TransactionChecker<String> slowChecker =
                (message, context) -> {
                    invocations.incrementAndGet();
                    release.await(5, java.util.concurrent.TimeUnit.SECONDS);
                    return LocalTransactionState.UNKNOWN;
                };
        // 提高回查预算：本用例关注线程有界性，不触发耗尽强制回滚
        TransactionScanner boundedScanner =
                new TransactionScanner(
                        redisson, NAMESPACE, converter, CHECK_INTERVAL_MS, 10, BATCH_SIZE);
        boundedScanner.registerChecker(TX_GROUP, slowChecker);
        boundedScanner.setCheckerTimeoutMillis(100L);

        for (int round = 1; round <= 3; round++) {
            boundedScanner.scanTimeoutHalf(TX_GROUP);
        }

        assertThat(invocations.get()).as("超时期间不得为同一 txId 重复启动回查线程（旧实现每轮起一个 → 无界）").isEqualTo(1);
        assertThat(boundedScanner.getOrphanCheckerCount()).isEqualTo(1);
        assertThat(boundedScanner.getOrphanCheckerTotal()).isEqualTo(1);

        release.countDown();
        long deadline = System.currentTimeMillis() + 5_000L;
        while (boundedScanner.getOrphanCheckerCount() > 0
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(20L);
        }
        assertThat(boundedScanner.getOrphanCheckerCount()).as("孤儿线程结束后必须摘除标记").isZero();

        // 标记清除后，同一事务恢复可回查
        boundedScanner.scanTimeoutHalf(TX_GROUP);
        assertThat(invocations.get()).isEqualTo(2);
    }

    // ===================== 场景 E：MISSING 状态（B-16） =====================

    @Test
    @DisplayName("状态字段缺失时 markCommit：ERROR 告警 + 降级 UNKNOWN 并消耗回查预算（不再静默吞掉提交）")
    void markCommitOnMissingState_degradesToUnknownInsteadOfSilentReturn() {
        // stateStore 中不存在该 txId：CAS 脚本返回 MISSING（等价于"从未注册 / 注册期元数据丢失"）。
        // 旧实现把 MISSING 归入"已终态"debug 静默返回——commit 请求被忽略，半消息既不投递也不清理。
        scanner.markCommit(TX_ID, TX_GROUP);

        assertThat(stateStore)
                .as("缺失状态必须显式降级为 UNKNOWN 走有界回查，而不是被静默忽略")
                .containsEntry(TX_ID, TransactionScanner.STATE_UNKNOWN);
        assertThat(counterStore).as("降级必须消耗回查预算（计数），耗尽后由强制终结兜底").containsEntry(TX_ID, "1");
        verify(checkZset).add(anyDouble(), eq(TX_ID));
        assertThat(errorEvents())
                .anySatisfy(
                        event ->
                                assertThat(event.getFormattedMessage())
                                        .contains("markCommit on missing txstate entry")
                                        .contains("degrading to UNKNOWN"));
        // 半消息 ID 未知：此分支不猜测条目位置，孤儿半消息由保留期维护任务清理
        verify(halfStream, never()).remove(ArgumentMatchers.<StreamMessageId>any());

        // 后续扫描按 UNKNOWN 继续回查：无元数据（halfId 缺失）→ 以 ROLLBACK 明确终结，
        // 保证"要么投递、要么明确失败"，不会留下既未投递又未被清理的半消息
        TransactionChecker<Object> unknownChecker =
                (message, context) -> LocalTransactionState.UNKNOWN;
        scanner.registerChecker(TX_GROUP, unknownChecker);
        scanner.scanTimeoutHalf(TX_GROUP);
        assertThat(stateStore).containsEntry(TX_ID, TransactionScanner.STATE_ROLLBACK);
        assertThat(warnEvents())
                .anySatisfy(
                        event ->
                                assertThat(event.getFormattedMessage())
                                        .contains("Half message not found in half stream"));
    }

    // ===================== 夹具 =====================

    /** 布置"PREPARE + 半消息可读"的事务，使回查器能被真正调用。 */
    private void givenReadableHalfMessage() {
        stateStore.put(TX_ID, TransactionScanner.STATE_PREPARE);
        stateStore.put(TX_ID + ".target", TARGET_TOPIC);
        stateStore.put(TX_ID + ".halfId", HALF_ID);
        Map<StreamMessageId, Map<String, String>> entries =
                Map.of(
                        new StreamMessageId(1000L, 0L),
                        Map.of(DefaultMessageConverter.FIELD_BODY, "half-payload"));
        when(halfStream.range(1, new StreamMessageId(1000L, 0L), new StreamMessageId(1000L, 0L)))
                .thenReturn(entries);
        Message<String> halfMessage =
                MessageBuilder.<String>withTopic(TARGET_TOPIC).body("half-payload").build();
        doReturn(halfMessage)
                .when(converter)
                .fromStreamFields(
                        ArgumentMatchers.<Map<String, String>>any(),
                        ArgumentMatchers.<Class<String>>any(),
                        anyString());
    }

    /** 用内存 Map 模拟 {@link RMap}（get/put/remove 直通）。 */
    private static RMap<String, String> mapView(Map<String, String> store) {
        @SuppressWarnings("unchecked")
        RMap<String, String> map = mock(RMap.class);
        when(map.get(anyString())).thenAnswer(invocation -> store.get(invocation.getArgument(0)));
        when(map.put(anyString(), anyString()))
                .thenAnswer(
                        invocation ->
                                store.put(invocation.getArgument(0), invocation.getArgument(1)));
        when(map.remove(anyString()))
                .thenAnswer(invocation -> store.remove(invocation.getArgument(0)));
        return map;
    }

    /**
     * 模拟 Lua 语义的 {@link RScript} mock，按脚本内容分派。
     *
     * <p>分派标记必须唯一且判定顺序固定：{@code LUA_CAS_STATE} 也含字面量 {@code 'UNKNOWN'}，先判它会注入错误语义。
     *
     * <ul>
     *   <li>{@code HINCRBY}：原子递增回查计数
     *   <li>{@code XRANGE}：提交转投脚本，抛异常模拟"持续失败"（状态停留 COMMITTING）
     *   <li>{@code 'MISSING'}（带引号，区别于 {@code 'HALF_MISSING'}）：CAS 状态抢占
     *   <li>{@code 'UNKNOWN'}：CAS 置 UNKNOWN（终态不覆盖）
     *   <li>其余：显式失败，让脚本契约漂移暴露为测试红灯
     * </ul>
     */
    private RScript luaScript() {
        RScript script = mock(RScript.class);
        Answer<Object> lua =
                invocation -> {
                    List<Object> argv = flattenArguments(invocation.getArguments());
                    String luaScript = String.valueOf(argv.get(1));
                    List<Object> scriptArgs = argv.subList(4, argv.size());
                    if (luaScript.contains("HINCRBY")) {
                        String id = String.valueOf(scriptArgs.get(0));
                        int next = Integer.parseInt(counterStore.getOrDefault(id, "0")) + 1;
                        counterStore.put(id, Integer.toString(next));
                        return next;
                    }
                    if (luaScript.contains("XRANGE")) {
                        String txId = String.valueOf(scriptArgs.get(1));
                        throw new StreamMQBrokerException(
                                "simulated commit script failure, txId=" + txId, null, null);
                    }
                    if (luaScript.contains("'MISSING'")) {
                        return casState(scriptArgs);
                    }
                    if (luaScript.contains("'UNKNOWN'")) {
                        return casToUnknown(scriptArgs);
                    }
                    if (luaScript.contains("STATE=")) {
                        return casFinalizeStuck(scriptArgs);
                    }
                    throw new IllegalStateException(
                            "Unexpected Lua script dispatched by TransactionScanner: " + luaScript);
                };
        doAnswer(lua)
                .when(script)
                .eval(
                        any(RScript.Mode.class),
                        anyString(),
                        any(RScript.ReturnType.class),
                        anyList(),
                        any());
        doAnswer(lua)
                .when(script)
                .eval(
                        any(RScript.Mode.class),
                        anyString(),
                        any(RScript.ReturnType.class),
                        anyList(),
                        any(),
                        any());
        // 强制终结状态 CAS 脚本：5 个 ARGV（txId / 期望中间态 / 原因字段 / 原因值 / 终态）
        doAnswer(lua)
                .when(script)
                .eval(
                        any(RScript.Mode.class),
                        anyString(),
                        any(RScript.ReturnType.class),
                        anyList(),
                        any(),
                        any(),
                        any(),
                        any(),
                        any());
        return script;
    }

    /**
     * 展开 Mockito 记录的调用参数，使 {@code [Mode, script, ReturnType, keys, argv...]} 与可变参数个数无关。
     *
     * <p>不同重载/版本下末位可变参数既可能已展开也可能是嵌套数组，统一展平后按位取值。
     */
    private static List<Object> flattenArguments(Object[] recordedArguments) {
        List<Object> flattened = new ArrayList<>(recordedArguments.length);
        for (Object argument : recordedArguments) {
            if (argument instanceof Object[] nested) {
                flattened.addAll(Arrays.asList(nested));
            } else {
                flattened.add(argument);
            }
        }
        return flattened;
    }

    /**
     * 复刻 {@code LUA_CAS_STATE} 语义：{@code txId} 缺失返回 {@code MISSING}；终态/中间态原样返回；其余状态原子写入目标状态并返回旧状态。
     *
     * @param scriptArgs 脚本参数：{@code [0]=txId, [1]=targetState}
     */
    private String casState(List<Object> scriptArgs) {
        String id = String.valueOf(scriptArgs.get(0));
        String targetState = String.valueOf(scriptArgs.get(1));
        String current = stateStore.get(id);
        if (Objects.isNull(current)) {
            return "MISSING";
        }
        if (TransactionScanner.STATE_COMMIT.equals(current)
                || TransactionScanner.STATE_ROLLBACK.equals(current)
                || TransactionScanner.STATE_COMMITTING.equals(current)
                || TransactionScanner.STATE_ROLLBACKING.equals(current)) {
            return current;
        }
        stateStore.put(id, targetState);
        return current;
    }

    /**
     * 复刻 {@code LUA_CAS_TO_UNKNOWN} 语义：终态不覆盖，其余状态置 {@code UNKNOWN} 并返回 {@code OK}。
     *
     * @param scriptArgs 脚本参数：{@code [0]=txId}
     */
    private String casToUnknown(List<Object> scriptArgs) {
        String id = String.valueOf(scriptArgs.get(0));
        String current = stateStore.get(id);
        if (TransactionScanner.STATE_COMMIT.equals(current)
                || TransactionScanner.STATE_ROLLBACK.equals(current)) {
            return current;
        }
        stateStore.put(id, TransactionScanner.STATE_UNKNOWN);
        return "OK";
    }

    /**
     * 复刻 {@code LUA_CAS_FINALIZE_STUCK} 语义（R2-1）：仅当状态仍等于期望中间态时才写终态 + 失败原因； 否则原样返回当前状态（{@code
     * STATE=...}），绝不改写。
     *
     * @param scriptArgs 脚本参数：{@code [0]=txId, [1]=期望中间态, [2]=原因字段, [3]=原因值, [4]=终态}
     */
    private String casFinalizeStuck(List<Object> scriptArgs) {
        String id = String.valueOf(scriptArgs.get(0));
        String expected = String.valueOf(scriptArgs.get(1));
        String reasonField = String.valueOf(scriptArgs.get(2));
        String reasonValue = String.valueOf(scriptArgs.get(3));
        String terminal = String.valueOf(scriptArgs.get(4));
        String current = stateStore.get(id);
        if (!Objects.equals(current, expected)) {
            return Objects.isNull(current) ? "STATE=absent" : "STATE=" + current;
        }
        stateStore.put(reasonField, reasonValue);
        stateStore.put(id, terminal);
        return "OK";
    }

    private List<ILoggingEvent> warnEvents() {
        return eventsOf(Level.WARN);
    }

    private List<ILoggingEvent> errorEvents() {
        return eventsOf(Level.ERROR);
    }

    private List<ILoggingEvent> eventsOf(Level level) {
        return logAppender.list.stream().filter(event -> level.equals(event.getLevel())).toList();
    }
}
