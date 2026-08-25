/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.diagnostics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import io.github.streammq.core.trace.StreamMQTraceService;
import io.github.streammq.core.trace.TraceRecord;
import io.github.streammq.core.trace.TraceType;
import io.github.streammq.diagnostics.model.MessageProfile;
import io.github.streammq.diagnostics.model.MessageStatus;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * {@link MessageProfileService} 单元测试，验证基于追踪记录构建消息画像的逻辑。
 *
 * <p>使用 Mockito 模拟 {@link StreamMQTraceService}，验证各种追踪数据场景下的画像构建行为。
 */
@DisplayName("消息画像服务测试")
@ExtendWith(MockitoExtension.class)
class MessageProfileServiceTest {

    @Mock private StreamMQTraceService traceService;

    private MessageProfileService profileService;

    @BeforeEach
    void setUp() {
        profileService = new MessageProfileService(traceService);
    }

    @Nested
    @DisplayName("getProfile - 按消息 ID 构建画像")
    class GetProfile {

        @Test
        @DisplayName("发送+消费成功 -> 最终状态 SUCCESS")
        void shouldReturnSuccessProfileWhenConsumeSucceeded() {
            Map<String, String> sendAttrs =
                    Map.of(
                            "tag",
                            "order",
                            "keys",
                            "order-key-1",
                            "bodyType",
                            "String",
                            "bornHost",
                            "localhost:8080");
            Map<String, String> consumeAttrs =
                    Map.of("consumerName", "consumer-1", "reconsumeTimes", "0");
            TraceRecord send = sendRecord("msg-1", "test-topic", 1000L, 10L, sendAttrs);
            TraceRecord consume =
                    consumeRecord(
                            "msg-1", "test-topic", "test-group", true, 2000L, 100L, consumeAttrs);

            when(traceService.queryByMessageId("msg-1")).thenReturn(List.of(send, consume));

            MessageProfile profile = profileService.getProfile("msg-1");

            assertThat(profile).isNotNull();
            assertThat(profile.messageId()).isEqualTo("msg-1");
            assertThat(profile.topic()).isEqualTo("test-topic");
            assertThat(profile.tag()).isEqualTo("order");
            assertThat(profile.keys()).isEqualTo("order-key-1");
            assertThat(profile.bodyType()).isEqualTo("String");
            assertThat(profile.bornHost()).isEqualTo("localhost:8080");
            assertThat(profile.bornTimestamp()).isEqualTo(1000L);
            assertThat(profile.sendDurationMillis()).isEqualTo(10L);
            assertThat(profile.consumeHistory()).hasSize(1);
            assertThat(profile.retryCount()).isEqualTo(0);
            assertThat(profile.finalStatus()).isEqualTo(MessageStatus.SUCCESS);
            assertThat(profile.routePath()).containsExactly("test-topic");
        }

        @Test
        @DisplayName("仅有发送记录 -> 最终状态 PROCESSING")
        void shouldReturnProcessingStatusWhenOnlySendRecord() {
            TraceRecord send = sendRecord("msg-2", "test-topic", 1000L, 10L, Map.of());

            when(traceService.queryByMessageId("msg-2")).thenReturn(List.of(send));

            MessageProfile profile = profileService.getProfile("msg-2");

            assertThat(profile).isNotNull();
            assertThat(profile.finalStatus()).isEqualTo(MessageStatus.PROCESSING);
            assertThat(profile.consumeHistory()).isEmpty();
            assertThat(profile.retryCount()).isEqualTo(0);
        }

        @Test
        @DisplayName("消费失败 -> 最终状态 FAILED")
        void shouldReturnFailedStatusWhenConsumeFailed() {
            TraceRecord send = sendRecord("msg-3", "test-topic", 1000L, 10L, Map.of());
            Map<String, String> failAttrs =
                    Map.of(
                            "consumerName",
                            "consumer-1",
                            "reconsumeTimes",
                            "0",
                            "errorMessage",
                            "NullPointerException");
            TraceRecord consume =
                    consumeRecord(
                            "msg-3", "test-topic", "test-group", false, 2000L, 100L, failAttrs);

            when(traceService.queryByMessageId("msg-3")).thenReturn(List.of(send, consume));

            MessageProfile profile = profileService.getProfile("msg-3");

            assertThat(profile).isNotNull();
            assertThat(profile.finalStatus()).isEqualTo(MessageStatus.FAILED);
            assertThat(profile.consumeHistory()).hasSize(1);
            assertThat(profile.consumeHistory().get(0).errorMessage())
                    .isEqualTo("NullPointerException");
            assertThat(profile.consumeHistory().get(0).success()).isFalse();
        }

        @Test
        @DisplayName("路由路径包含 DLQ 主题 -> 最终状态 DLQ")
        void shouldReturnDlqStatusWhenRoutePathContainsDlqTopic() {
            TraceRecord send = sendRecord("msg-4", "test-topic", 1000L, 10L, Map.of());
            TraceRecord dlqConsume =
                    consumeRecord(
                            "msg-4", "test-topic-dlq", "test-group", false, 3000L, 50L, Map.of());

            when(traceService.queryByMessageId("msg-4")).thenReturn(List.of(send, dlqConsume));

            MessageProfile profile = profileService.getProfile("msg-4");

            assertThat(profile).isNotNull();
            assertThat(profile.finalStatus()).isEqualTo(MessageStatus.DLQ);
            assertThat(profile.routePath()).containsExactly("test-topic", "test-topic-dlq");
        }

        @Test
        @DisplayName("重试场景 -> retryCount 正确计算")
        void shouldCalculateRetryCountCorrectly() {
            TraceRecord send = sendRecord("msg-5", "test-topic", 1000L, 10L, Map.of());
            TraceRecord fail1 =
                    consumeRecord(
                            "msg-5",
                            "test-topic",
                            "test-group",
                            false,
                            2000L,
                            100L,
                            Map.of(
                                    "consumerName",
                                    "c-1",
                                    "reconsumeTimes",
                                    "0",
                                    "errorMessage",
                                    "error1"));
            TraceRecord fail2 =
                    consumeRecord(
                            "msg-5",
                            "test-topic",
                            "test-group",
                            false,
                            3000L,
                            200L,
                            Map.of(
                                    "consumerName",
                                    "c-1",
                                    "reconsumeTimes",
                                    "1",
                                    "errorMessage",
                                    "error2"));
            TraceRecord success =
                    consumeRecord(
                            "msg-5",
                            "test-topic",
                            "test-group",
                            true,
                            4000L,
                            50L,
                            Map.of("consumerName", "c-1", "reconsumeTimes", "2"));

            when(traceService.queryByMessageId("msg-5"))
                    .thenReturn(List.of(send, success, fail1, fail2));

            MessageProfile profile = profileService.getProfile("msg-5");

            assertThat(profile).isNotNull();
            assertThat(profile.consumeHistory()).hasSize(3);
            assertThat(profile.retryCount()).isEqualTo(2);
            assertThat(profile.finalStatus()).isEqualTo(MessageStatus.SUCCESS);
            assertThat(profile.consumeHistory().get(0).timestamp()).isEqualTo(2000L);
            assertThat(profile.consumeHistory().get(2).timestamp()).isEqualTo(4000L);
        }

        @Test
        @DisplayName("无追踪记录 -> 返回 null")
        void shouldReturnNullWhenNoTraceRecords() {
            when(traceService.queryByMessageId("msg-empty")).thenReturn(Collections.emptyList());

            MessageProfile profile = profileService.getProfile("msg-empty");

            assertThat(profile).isNull();
        }

        @Test
        @DisplayName("空消息 ID -> 返回 null")
        void shouldReturnNullWhenMessageIdIsEmpty() {
            MessageProfile profile = profileService.getProfile("");

            assertThat(profile).isNull();
        }

        @Test
        @DisplayName("null 消息 ID -> 返回 null")
        void shouldReturnNullWhenMessageIdIsNull() {
            MessageProfile profile = profileService.getProfile(null);

            assertThat(profile).isNull();
        }
    }

    @Nested
    @DisplayName("getTopicProfiles - 按主题构建画像列表")
    class GetTopicProfiles {

        @Test
        @DisplayName("多消息场景 -> 返回多条画像")
        void shouldReturnMultipleProfilesForTopic() {
            TraceRecord send1 = sendRecord("msg-a", "test-topic", 1000L, 10L, Map.of());
            TraceRecord consume1 =
                    consumeRecord("msg-a", "test-topic", "group-1", true, 2000L, 100L, Map.of());
            TraceRecord send2 = sendRecord("msg-b", "test-topic", 3000L, 10L, Map.of());
            TraceRecord consume2 =
                    consumeRecord("msg-b", "test-topic", "group-1", true, 4000L, 200L, Map.of());

            when(traceService.queryByTopic(eq("test-topic"), anyLong(), anyLong()))
                    .thenReturn(List.of(send1, consume1, send2, consume2));

            List<MessageProfile> profiles =
                    profileService.getTopicProfiles("test-topic", 0L, 5000L);

            assertThat(profiles).hasSize(2);
            assertThat(profiles)
                    .extracting(MessageProfile::messageId)
                    .containsExactlyInAnyOrder("msg-a", "msg-b");
            assertThat(profiles).allMatch(p -> p.finalStatus() == MessageStatus.SUCCESS);
        }

        @Test
        @DisplayName("无追踪记录 -> 返回空列表")
        void shouldReturnEmptyListWhenNoRecords() {
            when(traceService.queryByTopic(eq("empty-topic"), anyLong(), anyLong()))
                    .thenReturn(Collections.emptyList());

            List<MessageProfile> profiles =
                    profileService.getTopicProfiles("empty-topic", 0L, 5000L);

            assertThat(profiles).isEmpty();
        }

        @Test
        @DisplayName("空主题 -> 返回空列表")
        void shouldReturnEmptyListWhenTopicIsEmpty() {
            List<MessageProfile> profiles = profileService.getTopicProfiles("", 0L, 5000L);

            assertThat(profiles).isEmpty();
        }
    }

    /** 创建发送追踪记录。 */
    private TraceRecord sendRecord(
            String messageId,
            String topic,
            long timestamp,
            long durationMillis,
            Map<String, String> attrs) {
        return new TraceRecord(
                messageId,
                topic,
                "producer-group",
                TraceType.SEND,
                true,
                timestamp,
                durationMillis,
                "trace-" + messageId,
                attrs);
    }

    /** 创建消费追踪记录。 */
    private TraceRecord consumeRecord(
            String messageId,
            String topic,
            String group,
            boolean success,
            long timestamp,
            long durationMillis,
            Map<String, String> attrs) {
        return new TraceRecord(
                messageId,
                topic,
                group,
                TraceType.CONSUME,
                success,
                timestamp,
                durationMillis,
                "trace-" + messageId,
                attrs);
    }
}
