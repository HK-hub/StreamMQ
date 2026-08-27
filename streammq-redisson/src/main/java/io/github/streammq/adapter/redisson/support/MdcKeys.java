/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.support;

import lombok.experimental.UtilityClass;

/** MDC 键名常量定义。 */
@UtilityClass
public class MdcKeys {

    public static final String TRACE_ID = "traceId";
    public static final String MSG_ID = "msgId";
    public static final String TOPIC = "topic";
    public static final String CONSUMER_GROUP = "consumerGroup";
    public static final String PRODUCER_GROUP = "producerGroup";
    public static final String SHARDING_KEY = "shardingKey";
    public static final String RECONSUME_TIMES = "reconsumeTimes";
}
