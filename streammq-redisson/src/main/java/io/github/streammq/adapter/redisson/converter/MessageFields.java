/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.converter;

import io.github.streammq.core.StreamMQConstants;

/**
 * Redis Stream Entry 字段名统一定义。
 *
 * <p>所有 {@link MessageConverter} 实现共享同一套线上协议字段名， 避免各转换器各自维护导致协议漂移。转换器内的 public 常量均委托到本类。
 *
 * @author StreamMQ Contributors
 * @since 1.1.0
 */
public final class MessageFields {

    /** Stream Entry 字段名：消息体 */
    public static final String BODY = "body";

    /** Stream Entry 字段名：消息体类型全限定名 */
    public static final String BODY_TYPE = "bodyType";

    /** Stream Entry 字段名：消息体类型简称 */
    public static final String BODY_TYPE_NAME = "bodyTypeName";

    /** Stream Entry 字段名：标签 */
    public static final String TAG = "tag";

    /** Stream Entry 字段名：业务键 */
    public static final String KEYS = "keys";

    /** Stream Entry 字段名：分片键 */
    public static final String SHARDING_KEY = "shardingKey";

    /** Stream Entry 字段名：属性 JSON（sys + user 合并） */
    public static final String PROPS = "props";

    /** Stream Entry 字段名：出生时间戳（毫秒） */
    public static final String BORN_TS = "bornTs";

    /** Stream Entry 字段名：出生主机 */
    public static final String BORN_HOST = "bornHost";

    /** Stream Entry 字段名：重试次数 */
    public static final String RETRY_TIMES = "retryTimes";

    /** Stream Entry 字段名：事务 ID */
    public static final String TX_ID = "txId";

    /** Stream Entry 字段名：原始 Topic（重试/DLQ 场景） */
    public static final String ORIGIN_TOPIC = "originTopic";

    /** Stream Entry 字段名：压缩算法标识（"gzip" 或旧格式 "true"） */
    public static final String COMPRESSED = "compressed";

    /** Stream Entry 字段名：进入 DLQ 的原因 */
    public static final String DLQ_REASON = StreamMQConstants.FIELD_DLQ_REASON;

    /** Stream Entry 字段名：原始消息 ID（DLQ / 重试场景） */
    public static final String ORIGINAL_MESSAGE_ID = StreamMQConstants.FIELD_ORIGINAL_MESSAGE_ID;

    /** JVM byte[] 类型的类描述符（bodyType 探测用） */
    public static final String BYTE_ARRAY_DESCRIPTOR = "[B";

    private MessageFields() {}
}
