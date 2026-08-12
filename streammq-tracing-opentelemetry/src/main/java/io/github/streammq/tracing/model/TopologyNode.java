package io.github.streammq.tracing.model;

/**
 * 拓扑节点，表示消息流中的一个生产者或消费者实例。
 *
 * <p>用于 {@link TopologyGraph} 中构建生产者-消费者拓扑关系图。
 *
 * @param name 节点名称（生产者组名 / 消费者类名或实例名）
 * @param type 节点类型字符串：{@code "PRODUCER"} 或 {@code "CONSUMER"}
 * @param topic 关联 Topic
 * @param group 消费者组名，生产者节点可为 {@code null}
 * @param active 是否活跃（在最近时间窗口内有追踪记录）
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public record TopologyNode(String name, String type, String topic, String group, boolean active) {}
