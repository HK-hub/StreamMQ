package io.github.streammq.adapter.redisson.support;

/** MDC 键名常量定义。 */
public final class MdcKeys {
  private MdcKeys() {}

  public static final String TRACE_ID = "traceId";
  public static final String MSG_ID = "msgId";
  public static final String TOPIC = "topic";
  public static final String CONSUMER_GROUP = "consumerGroup";
  public static final String PRODUCER_GROUP = "producerGroup";
  public static final String SHARDING_KEY = "shardingKey";
  public static final String RECONSUME_TIMES = "reconsumeTimes";
}
