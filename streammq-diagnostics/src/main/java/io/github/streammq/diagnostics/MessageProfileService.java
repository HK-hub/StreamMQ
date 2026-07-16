package io.github.streammq.diagnostics;

import io.github.streammq.core.trace.StreamMQTraceService;
import io.github.streammq.core.trace.TraceRecord;
import io.github.streammq.core.trace.TraceType;
import io.github.streammq.core.util.CollectionUtils;
import io.github.streammq.core.util.StringUtils;
import io.github.streammq.diagnostics.model.ConsumeAttempt;
import io.github.streammq.diagnostics.model.MessageProfile;
import io.github.streammq.diagnostics.model.MessageStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 消息画像服务，基于追踪数据构建消息完整生命周期画像。
 *
 * <p>通过 {@link StreamMQTraceService} 查询消息的发送与消费追踪记录，
 * 聚合为 {@link MessageProfile}，用于消息链路可视化与问题排查。
 *
 * <p>核心能力：
 * <ul>
 *   <li>{@link #getProfile(String)} - 按消息 ID 构建单条消息的完整生命周期画像</li>
 *   <li>{@link #getTopicProfiles(String, long, long)} - 按主题与时间范围构建所有消息的画像</li>
 * </ul>
 *
 * <p>当追踪数据不可用时，{@link #getProfile(String)} 返回 null，
 * {@link #getTopicProfiles(String, long, long)} 返回空列表，不抛出异常。
 *
 * @author StreamMQ Contributors
 * @since 1.0.0
 */
public class MessageProfileService {

    private static final Logger log = LoggerFactory.getLogger(MessageProfileService.class);

    /** 追踪记录扩展属性键：消息标签 */
    private static final String ATTR_TAG = "tag";
    /** 追踪记录扩展属性键：业务键 */
    private static final String ATTR_KEYS = "keys";
    /** 追踪记录扩展属性键：消息体类型 */
    private static final String ATTR_BODY_TYPE = "bodyType";
    /** 追踪记录扩展属性键：出生主机 */
    private static final String ATTR_BORN_HOST = "bornHost";
    /** 追踪记录扩展属性键：消费者实例名 */
    private static final String ATTR_CONSUMER_NAME = "consumerName";
    /** 追踪记录扩展属性键：重试次数 */
    private static final String ATTR_RECONSUME_TIMES = "reconsumeTimes";
    /** 追踪记录扩展属性键：错误信息 */
    private static final String ATTR_ERROR_MESSAGE = "errorMessage";

    private final StreamMQTraceService traceService;

    /**
     * 构造消息画像服务。
     *
     * @param traceService 追踪查询服务
     */
    public MessageProfileService(StreamMQTraceService traceService) {
        this.traceService = Objects.requireNonNull(traceService, "traceService");
    }

    /**
     * 按消息 ID 构建消息完整生命周期画像。
     *
     * <p>查询该消息的所有追踪记录（发送 + 消费），聚合为画像。
     *
     * @param messageId 消息 ID
     * @return 消息画像，若追踪数据不存在则返回 null
     */
    public MessageProfile getProfile(String messageId) {
        if (StringUtils.isEmpty(messageId)) {
            return null;
        }
        List<TraceRecord> records = traceService.queryByMessageId(messageId);
        if (CollectionUtils.isEmpty(records)) {
            log.debug("未找到消息 [{}] 的追踪记录", messageId);
            return null;
        }
        return buildProfile(records);
    }

    /**
     * 按主题与时间范围构建所有消息的画像。
     *
     * @param topic 主题
     * @param startMs 起始时间戳（毫秒，包含）
     * @param endMs 结束时间戳（毫秒，包含）
     * @return 该时间范围内所有消息的画像列表，若无数据则返回空列表
     */
    public List<MessageProfile> getTopicProfiles(String topic, long startMs, long endMs) {
        if (StringUtils.isEmpty(topic)) {
            return Collections.emptyList();
        }
        List<TraceRecord> records = traceService.queryByTopic(topic, startMs, endMs);
        if (CollectionUtils.isEmpty(records)) {
            return Collections.emptyList();
        }
        Map<String, List<TraceRecord>> grouped = groupByMessageId(records);
        List<MessageProfile> profiles = new ArrayList<>(grouped.size());
        for (Map.Entry<String, List<TraceRecord>> entry : grouped.entrySet()) {
            MessageProfile profile = buildProfile(entry.getValue());
            if (Objects.nonNull(profile)) {
                profiles.add(profile);
            }
        }
        return profiles;
    }

    /**
     * 将追踪记录按消息 ID 分组，保持插入顺序。
     *
     * @param records 追踪记录列表
     * @return 按消息 ID 分组的记录映射
     */
    private Map<String, List<TraceRecord>> groupByMessageId(List<TraceRecord> records) {
        Map<String, List<TraceRecord>> grouped = new LinkedHashMap<>();
        for (TraceRecord record : records) {
            if (Objects.isNull(record) || StringUtils.isEmpty(record.messageId())) {
                continue;
            }
            grouped.computeIfAbsent(record.messageId(), k -> new ArrayList<>()).add(record);
        }
        return grouped;
    }

    /**
     * 基于追踪记录列表构建消息画像。
     *
     * @param records 同一消息的所有追踪记录
     * @return 消息画像
     */
    private MessageProfile buildProfile(List<TraceRecord> records) {
        TraceRecord sendRecord = findSendRecord(records);
        List<TraceRecord> consumeRecords = filterConsumeRecords(records);

        String messageId = records.get(0).messageId();
        String topic = Objects.nonNull(sendRecord) ? sendRecord.topic() : records.get(0).topic();
        Map<String, String> sendAttrs = Objects.nonNull(sendRecord)
            ? safeAttrs(sendRecord.attributes()) : safeAttrs(records.get(0).attributes());

        String tag = sendAttrs.get(ATTR_TAG);
        String keys = sendAttrs.get(ATTR_KEYS);
        String bodyType = sendAttrs.get(ATTR_BODY_TYPE);
        String bornHost = sendAttrs.get(ATTR_BORN_HOST);
        long bornTimestamp = Objects.nonNull(sendRecord) ? sendRecord.timestamp() : records.get(0).timestamp();
        long sendDurationMillis = Objects.nonNull(sendRecord) ? sendRecord.durationMillis() : 0L;

        List<ConsumeAttempt> consumeHistory = buildConsumeHistory(consumeRecords);
        int retryCount = Math.max(0, consumeHistory.size() - 1);
        List<String> routePath = buildRoutePath(records);
        MessageStatus finalStatus = determineFinalStatus(consumeHistory, routePath);

        return new MessageProfile(
            messageId,
            topic,
            tag,
            keys,
            bornTimestamp,
            sendDurationMillis,
            consumeHistory,
            retryCount,
            finalStatus,
            routePath,
            bodyType,
            bornHost
        );
    }

    /**
     * 从追踪记录中查找发送记录（取第一条类型为 SEND 的记录）。
     *
     * @param records 追踪记录列表
     * @return 发送记录，若不存在则返回 null
     */
    private TraceRecord findSendRecord(List<TraceRecord> records) {
        for (TraceRecord record : records) {
            if (Objects.nonNull(record.type()) && record.type() == TraceType.SEND) {
                return record;
            }
        }
        return null;
    }

    /**
     * 过滤出消费记录（类型为 CONSUME 的记录），保持原始顺序。
     *
     * @param records 追踪记录列表
     * @return 消费记录列表
     */
    private List<TraceRecord> filterConsumeRecords(List<TraceRecord> records) {
        List<TraceRecord> consumeRecords = new ArrayList<>();
        for (TraceRecord record : records) {
            if (Objects.nonNull(record.type()) && record.type() == TraceType.CONSUME) {
                consumeRecords.add(record);
            }
        }
        return consumeRecords;
    }

    /**
     * 基于消费追踪记录构建消费历史。
     *
     * @param consumeRecords 消费追踪记录列表
     * @return 消费尝试列表（按时间升序）
     */
    private List<ConsumeAttempt> buildConsumeHistory(List<TraceRecord> consumeRecords) {
        if (CollectionUtils.isEmpty(consumeRecords)) {
            return Collections.emptyList();
        }
        List<TraceRecord> sorted = new ArrayList<>(consumeRecords);
        sorted.sort((a, b) -> Long.compare(a.timestamp(), b.timestamp()));

        List<ConsumeAttempt> history = new ArrayList<>(sorted.size());
        for (int i = 0; i < sorted.size(); i++) {
            TraceRecord record = sorted.get(i);
            Map<String, String> attrs = safeAttrs(record.attributes());
            String consumerName = attrs.get(ATTR_CONSUMER_NAME);
            int reconsumeTimes = parseReconsumeTimes(attrs.get(ATTR_RECONSUME_TIMES), i);
            String errorMessage = attrs.get(ATTR_ERROR_MESSAGE);

            history.add(new ConsumeAttempt(
                record.group(),
                consumerName,
                record.timestamp(),
                record.durationMillis(),
                record.success(),
                reconsumeTimes,
                errorMessage
            ));
        }
        return history;
    }

    /**
     * 构建路由路径，收集消息经过的所有主题（去重，保持顺序）。
     *
     * @param records 追踪记录列表
     * @return 路由路径列表
     */
    private List<String> buildRoutePath(List<TraceRecord> records) {
        List<String> routePath = new ArrayList<>();
        for (TraceRecord record : records) {
            if (StringUtils.isNotEmpty(record.topic()) && !routePath.contains(record.topic())) {
                routePath.add(record.topic());
            }
        }
        return routePath;
    }

    /**
     * 根据消费历史与路由路径推导消息最终状态。
     *
     * <p>判定逻辑：
     * <ul>
     *   <li>无消费记录：PROCESSING</li>
     *   <li>最后一次消费成功：SUCCESS</li>
     *   <li>路由路径中存在 DLQ 主题：DLQ</li>
     *   <li>最后一次消费失败：FAILED</li>
     *   <li>其他：UNKNOWN</li>
     * </ul>
     *
     * @param consumeHistory 消费历史
     * @param routePath 路由路径
     * @return 最终状态
     */
    private MessageStatus determineFinalStatus(List<ConsumeAttempt> consumeHistory, List<String> routePath) {
        if (CollectionUtils.isEmpty(consumeHistory)) {
            return MessageStatus.PROCESSING;
        }
        if (hasDlqInRoute(routePath)) {
            return MessageStatus.DLQ;
        }
        ConsumeAttempt lastAttempt = consumeHistory.get(consumeHistory.size() - 1);
        if (lastAttempt.success()) {
            return MessageStatus.SUCCESS;
        }
        return MessageStatus.FAILED;
    }

    /**
     * 判断路由路径中是否存在死信队列主题。
     *
     * @param routePath 路由路径
     * @return true 如果存在 DLQ 主题
     */
    private boolean hasDlqInRoute(List<String> routePath) {
        if (CollectionUtils.isEmpty(routePath)) {
            return false;
        }
        for (String topic : routePath) {
            if (StringUtils.isNotEmpty(topic) && topic.toLowerCase().contains("dlq")) {
                return true;
            }
        }
        return false;
    }

    /**
     * 安全获取追踪记录的扩展属性，避免 NPE。
     *
     * @param attrs 原始属性 Map
     * @return 非 null 的属性 Map
     */
    private Map<String, String> safeAttrs(Map<String, String> attrs) {
        if (CollectionUtils.isEmpty(attrs)) {
            return Collections.emptyMap();
        }
        return attrs;
    }

    /**
     * 解析重试次数，若属性不存在或解析失败则使用索引值。
     *
     * @param value 属性值
     * @param fallbackIndex 回退索引
     * @return 重试次数
     */
    private int parseReconsumeTimes(String value, int fallbackIndex) {
        if (StringUtils.isEmpty(value)) {
            return fallbackIndex;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return fallbackIndex;
        }
    }
}
