package io.github.streammq.core.consumer;

import io.github.streammq.core.annotation.StreamMqListener;
import io.github.streammq.core.annotation.StreamMqOrderlyListener;
import io.github.streammq.core.listener.StreamMqAckListener;

import java.util.Collection;

/**
 * StreamMQ 监听器容器接口。
 *
 * <p>管理所有 Listener 的生命周期、消费线程、Rebalance。
 * 实现类位于 {@code streammq-redisson-adapter} 模块，建议继承 Spring {@code SmartLifecycle}。
 *
 * <p>注册 Listener 时需提供注解元数据，框架据此创建对应的 Consumer 与消费线程。
 *
 * <p>注：本接口文件中，{@code io.github.streammq.core.annotation.StreamMqListener}（注解）与
 * {@code io.github.streammq.core.listener.StreamMqListener}（接口）同名，故接口类型采用全限定名以消除歧义。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface StreamMqListenerContainer {

    /**
     * 注册一个并发消费 Listener（自动 ACK）。
     *
     * @param listener Listener 实例（{@code io.github.streammq.core.listener.StreamMqListener}）
     * @param annotation 注解元数据（{@code io.github.streammq.core.annotation.StreamMqListener}）
     * @param <T> body 类型
     */
    <T> void registerListener(io.github.streammq.core.listener.StreamMqListener<T> listener,
                              StreamMqListener annotation);

    /**
     * 注册一个手动 ACK Listener（并发消费）。
     *
     * @param listener Listener 实例
     * @param annotation 注解元数据
     * @param <T> body 类型
     */
    <T> void registerAckListener(StreamMqAckListener<T> listener, StreamMqListener annotation);

    /**
     * 注册一个顺序消费 Listener。
     *
     * @param listener Listener 实例（{@code io.github.streammq.core.listener.StreamMqOrderlyListener}）
     * @param annotation 顺序消费注解元数据
     * @param <T> body 类型
     */
    <T> void registerOrderlyListener(io.github.streammq.core.listener.StreamMqOrderlyListener<T> listener,
                                     StreamMqOrderlyListener annotation);

    /**
     * 返回所有已注册的 Listener 元信息。
     *
     * @return 不可修改的元信息集合
     */
    Collection<ListenerMetadata> getListeners();

    /**
     * 启动所有 Listener。
     *
     * @throws io.github.streammq.core.exception.StreamMqException 如果启动失败
     */
    void start();

    /**
     * 停止所有 Listener，释放线程与连接。
     */
    void stop();

    /**
     * 暂停所有 Listener（不释放资源，可恢复）。
     */
    void pause();

    /**
     * 恢复所有暂停的 Listener。
     */
    void resume();

    /**
     * 返回容器是否正在运行。
     *
     * @return true 如果运行中
     */
    boolean isRunning();

    /**
     * Listener 元信息。
     *
     * @param topic 主题
     * @param consumerGroup 消费者组
     * @param listenerType listener 类型
     * @param bodyType body 类型
     */
    record ListenerMetadata(String topic, String consumerGroup, Class<?> listenerType, Class<?> bodyType) {
    }
}
