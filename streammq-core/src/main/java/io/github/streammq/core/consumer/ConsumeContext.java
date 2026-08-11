package io.github.streammq.core.consumer;

import java.util.Map;

/**
 * 消费上下文，封装消费过程中的运行时元信息。
 *
 * <p>框架在调用 Consumer 时构造此对象，注入到 {@code onMessage} 第二参数。 消费结果统一由 {@code onMessage} 的返回值 （{@link
 * io.github.streammq.core.enums.ConsumeAction} / {@link ConsumeAction}）表达， 本接口仅提供消息元数据读取，不再提供手动
 * ACK/nack/defer 调用， 避免返回值与手动调用双模式冲突。
 *
 * <p>关键能力：
 *
 * <ul>
 *   <li>获取消息元信息：topic, consumerGroup, consumerName, reconsumeTimes, bornTimestamp, bornHost
 *   <li>获取消息追踪信息与扩展属性
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface ConsumeContext {

  /**
   * 返回当前消息所属 Topic。
   *
   * @return Topic
   */
  String topic();

  /**
   * 返回当前消费者组名。
   *
   * @return 消费者组名
   */
  String consumerGroup();

  /**
   * 返回当前 Consumer 实例名。
   *
   * @return Consumer 实例名
   */
  String consumerName();

  /**
   * 返回当前消息已重试消费次数（首次消费为 0）。
   *
   * @return 重试次数
   */
  int reconsumeTimes();

  /**
   * 返回消息出生时间戳（毫秒）。
   *
   * @return 出生时间戳
   */
  long bornTimestamp();

  /**
   * 返回消息出生主机（host:port）。
   *
   * @return 出生主机
   */
  String bornHost();

  /**
   * 返回消息追踪信息（含 traceId、spanId 等）。
   *
   * @return 不可修改的追踪信息 Map
   */
  Map<String, String> messageTrack();

  /**
   * 获取扩展属性。
   *
   * @param key 属性键
   * @return 属性值，不存在则返回 null
   */
  String ext(String key);
}
