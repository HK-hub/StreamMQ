package io.github.streammq.adapter.redisson.container;

/**
 * 容器生命周期状态枚举。
 *
 * <p>状态流转：{@link #INIT} → {@link #STARTING} → {@link #RUNNING} → {@link #STOPPING} → {@link
 * #STOPPED}
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public enum ContainerState {
  /** 初始状态，尚未启动，允许注册 Listener */
  INIT,
  /** 启动中（状态过渡，用于防止重复启动） */
  STARTING,
  /** 运行中，消费循环活跃 */
  RUNNING,
  /** 停止中，正在取消消费任务并释放资源 */
  STOPPING,
  /** 已停止，资源已释放 */
  STOPPED
}
