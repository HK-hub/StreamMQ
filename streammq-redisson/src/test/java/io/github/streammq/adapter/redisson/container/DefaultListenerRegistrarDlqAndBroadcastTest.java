/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.container;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.github.streammq.core.StreamMQConstants;
import io.github.streammq.core.annotation.StreamMQConsumer;
import io.github.streammq.core.annotation.StreamMQDlqConsumer;
import io.github.streammq.core.enums.ConsumeAction;
import io.github.streammq.core.enums.ConsumeFromWhere;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.enums.MessageModel;
import io.github.streammq.core.enums.SecondaryDlqMode;
import io.github.streammq.core.enums.SelectorType;
import io.github.streammq.core.listener.ListenerRegistration;
import io.github.streammq.core.listener.ListenerType;
import io.github.streammq.core.policy.DlqFailureStrategy;
import io.github.streammq.core.policy.RebalanceStrategy;
import io.github.streammq.core.policy.RetryPolicy;
import io.github.streammq.core.serializer.MessageSerializer;
import java.lang.reflect.Proxy;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 注册期不变量的回归测试（发布前红队审查 R4-x2 / R4-x3）。
 *
 * <ul>
 *   <li><b>R4-x2：</b>DLQ 注册（{@code @StreamMQDlqConsumer} 与 {@code @StreamMQConsumer(dlqMode=true)}
 *       两条路径）的起始位点必须固定为 {@code CONSUME_FROM_FIRST}——否则首次建组以 LAST($) 钉住位点， 建组前已存在的全部死信被静默跳过（不在 PEL
 *       里，认领机制也无从恢复）；
 *   <li><b>R4-x3：</b>ORDERLY + BROADCASTING 组合必须在注册期被拒绝——该组合的 PEL 认领目标会被登记到 基组名，而广播消费实际跑在 {@code
 *       {group}:{group}-{instanceId}} 上，导致认领循环每轮对不存在的组 listPending 刷 WARN、且 PEL 恢复完全失效。
 * </ul>
 */
@DisplayName("监听器注册：DLQ 起始位点固定 FIRST + ORDERLY/BROADCASTING 拒绝")
class DefaultListenerRegistrarDlqAndBroadcastTest {

    private static final String TOPIC = "trade-topic";
    private static final String GROUP = "trade-group";
    private static final String NS = "ns";
    private static final String INSTANCE_TOKEN = "inst-1";

    private DefaultRegistrationStore store;
    private DefaultListenerRegistrar registrar;

    @BeforeEach
    void setUp() {
        store = new DefaultRegistrationStore();
        ConsumerTuning tuning = mock(ConsumerTuning.class);
        // 注册的拉取批量必须 >= 1，其余 effective* 方法返回 0 均落在合法区间（>=0 / >=1）
        when(tuning.effectivePullBatchSize(anyInt())).thenReturn(32);
        // 全局默认位点刻意设为 LAST：DLQ 必须"显式覆盖"为 FIRST，而不是恰好跟随全局配置
        registrar =
                new DefaultListenerRegistrar(
                        mock(ContainerStateMachine.class),
                        store,
                        mock(PerConsumerSpiResolver.class),
                        tuning,
                        NS,
                        INSTANCE_TOKEN,
                        ConsumeFromWhere.CONSUME_FROM_LAST,
                        (defaultNs, topic, group, ns, shardCount) -> List.of(),
                        reg -> {});
    }

    @Test
    @DisplayName("R4-x2：@StreamMQDlqConsumer 注册固定 consumeFromWhere=FIRST（覆盖全局 LAST）")
    void dlqAnnotationRegistration_consumesFromFirst() {
        registrar.registerDlq((msg, ctx) -> {}, dlqAnnotation(GROUP));

        ListenerRegistration<?> reg = onlyRegistration();
        assertThat(reg.isDlqMode()).isTrue();
        assertThat(reg.getType()).isEqualTo(ListenerType.AUTO_ACK);
        assertThat(reg.getConsumeFromWhere()).isEqualTo(ConsumeFromWhere.CONSUME_FROM_FIRST);
    }

    @Test
    @DisplayName("R4-x2：@StreamMQConsumer(dlqMode=true) 注册同样固定 FIRST（两条构建路径同口径）")
    void concurrentDlqModeRegistration_consumesFromFirst() {
        registrar.registerConcurrent(
                (msg, ctx) -> ConsumeAction.SUCCESS,
                consumerAnnotation(
                        TOPIC, GROUP, ConsumeMode.CLUSTERING, MessageModel.CONCURRENT, true));

        ListenerRegistration<?> reg = onlyRegistration();
        assertThat(reg.isDlqMode()).isTrue();
        assertThat(reg.getConsumeFromWhere()).isEqualTo(ConsumeFromWhere.CONSUME_FROM_FIRST);
    }

    @Test
    @DisplayName("R4-x2 反向：非 DLQ 并发注册仍跟随全局默认 LAST（修复未过度外溢）")
    void normalConcurrentRegistration_stillFollowsGlobalDefault() {
        registrar.registerConcurrent(
                (msg, ctx) -> ConsumeAction.SUCCESS,
                consumerAnnotation(
                        TOPIC, GROUP, ConsumeMode.CLUSTERING, MessageModel.CONCURRENT, false));

        assertThat(onlyRegistration().getConsumeFromWhere())
                .isEqualTo(ConsumeFromWhere.CONSUME_FROM_LAST);
    }

    @Test
    @DisplayName("R4-x3：ORDERLY + BROADCASTING 注册期抛 IllegalArgumentException，信息可定位")
    void orderlyBroadcastingRegistration_rejected() {
        assertThatThrownBy(
                        () ->
                                registrar.registerOrderly(
                                        (msg, ctx) -> ConsumeAction.SUCCESS,
                                        consumerAnnotation(
                                                TOPIC,
                                                GROUP,
                                                ConsumeMode.BROADCASTING,
                                                MessageModel.ORDERLY,
                                                false)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("ORDERLY")
                .hasMessageContaining("BROADCASTING")
                .hasMessageContaining(TOPIC)
                .hasMessageContaining(GROUP);
        // 拒绝必须发生在入库之前：不能留下半成品注册项
        assertThat(store.registrations()).isEmpty();
    }

    @Test
    @DisplayName("R4-x3 反向：ORDERLY + CLUSTERING 仍正常注册")
    void orderlyClusteringRegistration_succeeds() {
        registrar.registerOrderly(
                (msg, ctx) -> ConsumeAction.SUCCESS,
                consumerAnnotation(
                        TOPIC, GROUP, ConsumeMode.CLUSTERING, MessageModel.ORDERLY, false));

        ListenerRegistration<?> reg = onlyRegistration();
        assertThat(reg.getType()).isEqualTo(ListenerType.ORDERLY);
        assertThat(reg.getConsumeMode()).isEqualTo(ConsumeMode.CLUSTERING);
    }

    // ===================== 辅助 =====================

    private ListenerRegistration<?> onlyRegistration() {
        return store.registrations().stream()
                .findFirst()
                .orElseThrow(() -> new AssertionError("expected exactly one registration"));
    }

    /** 通过动态代理构造 {@link StreamMQConsumer} 注解（与容器扫描产出的注解实例等价）。 */
    private static StreamMQConsumer consumerAnnotation(
            String topic, String group, ConsumeMode mode, MessageModel model, boolean dlqMode) {
        return (StreamMQConsumer)
                Proxy.newProxyInstance(
                        StreamMQConsumer.class.getClassLoader(),
                        new Class<?>[] {StreamMQConsumer.class},
                        (proxy, method, args) ->
                                switch (method.getName()) {
                                    case "topic" -> topic;
                                    case "consumerGroup" -> group;
                                    case "consumeMode" -> mode;
                                    case "messageModel" -> model;
                                    case "dlqMode" -> dlqMode;
                                    case "maxReconsumeTimes" -> 16;
                                    case "pullBatchSize" -> 32;
                                    case "namespace" -> "";
                                    case "consumeFromWhere" -> ConsumeFromWhere.ANNOTATION_DEFAULT;
                                    case "selectorExpression" -> "*";
                                    case "serializer" -> MessageSerializer.class;
                                    case "retryPolicy" -> RetryPolicy.class;
                                    case "messageConverter" ->
                                            io.github.streammq.core.converter.MessageConverter
                                                    .class;
                                    case "rebalanceStrategy" -> RebalanceStrategy.class;
                                    case "selectorType" -> SelectorType.TAG;
                                    case "annotationType" -> StreamMQConsumer.class;
                                    case "hashCode" -> (topic + group + mode + dlqMode).hashCode();
                                    case "equals" ->
                                            args != null && args.length > 0 && proxy == args[0];
                                    case "toString" ->
                                            "@StreamMQConsumer(topic="
                                                    + topic
                                                    + ", consumerGroup="
                                                    + group
                                                    + ")";
                                    default -> defaultAnnotationValue(method.getReturnType());
                                });
    }

    /** 通过动态代理构造 {@link StreamMQDlqConsumer} 注解。 */
    private static StreamMQDlqConsumer dlqAnnotation(String group) {
        return (StreamMQDlqConsumer)
                Proxy.newProxyInstance(
                        StreamMQDlqConsumer.class.getClassLoader(),
                        new Class<?>[] {StreamMQDlqConsumer.class},
                        (proxy, method, args) ->
                                switch (method.getName()) {
                                    case "consumerGroup" -> group;
                                    case "namespace" -> "";
                                    case "failureStrategy" -> DlqFailureStrategy.class;
                                    // DLQ 数值属性必须返回**注解的真实默认值**（哨兵），
                                    // 否则代理会退化成 0/false/null —— 那不是合法注解取值，
                                    // 会让 toDlqConfigOverride 产出非法覆盖（如 alertThreshold=0）。
                                    case "maxDlqRetryAttempts" ->
                                            StreamMQConstants.ANNOTATION_UNSET_INT;
                                    case "dlqRetryDelayMs" ->
                                            StreamMQConstants.ANNOTATION_UNSET_LONG;
                                    case "secondaryDlqMode" -> SecondaryDlqMode.INHERIT;
                                    case "secondaryDlqKeyPrefix" ->
                                            StreamMQConstants.ANNOTATION_UNSET_STRING;
                                    case "dlqAlertThreshold" ->
                                            StreamMQConstants.ANNOTATION_UNSET_INT;
                                    case "dlqRetryBackoffMultiplier" ->
                                            StreamMQConstants.ANNOTATION_UNSET_DOUBLE;
                                    case "dlqRetryMaxDelayMs" ->
                                            StreamMQConstants.ANNOTATION_UNSET_LONG;
                                    case "annotationType" -> StreamMQDlqConsumer.class;
                                    case "hashCode" -> group.hashCode();
                                    case "equals" ->
                                            args != null && args.length > 0 && proxy == args[0];
                                    case "toString" ->
                                            "@StreamMQDlqConsumer(consumerGroup=" + group + ")";
                                    default -> defaultAnnotationValue(method.getReturnType());
                                });
    }

    /** 按返回类型给出注解属性默认值，避免注解新增属性时测试代理崩溃。 */
    private static Object defaultAnnotationValue(Class<?> returnType) {
        if (returnType == String.class) {
            return "";
        }
        if (returnType == int.class) {
            return 0;
        }
        if (returnType == long.class) {
            return 0L;
        }
        if (returnType == boolean.class) {
            return false;
        }
        if (returnType == double.class) {
            return 0.0d;
        }
        if (returnType == Class.class) {
            return null;
        }
        if (returnType.isEnum()) {
            return returnType.getEnumConstants()[0];
        }
        return null;
    }
}
