package io.github.streammq.core.producer;

/**
 * StreamMQ 生产者工厂接口。
 *
 * <p>根据 {@link ProducerConfig} 创建 {@link StreamMessageProducer} 实例。 实现类位于 {@code
 * streammq-redisson-adapter} 模块。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMessageProducerFactory {

  /**
   * 创建生产者。
   *
   * @param config 生产者配置（组名、命名空间、发送超时等）
   * @return 生产者实例
   */
  StreamMessageProducer createProducer(ProducerConfig config);

  /** 关闭工厂，释放底层资源（如 Redisson 连接）。 */
  void close();

  /**
   * 返回工厂是否已关闭。
   *
   * @return true 如果已关闭
   */
  boolean isClosed();
}
