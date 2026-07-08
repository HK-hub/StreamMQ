package io.github.streammq.nativeapi;

import io.github.streammq.core.converter.MessageConverter;
import org.redisson.api.RedissonClient;

import java.util.Objects;

/**
 * {@link NativeStreamMQ} 的建造者，提供流式 API 设置必填/可选参数。
 *
 * <p>所有设置方法均返回 {@code this}，支持链式调用。build() 前必须设置
 * {@link #redisson(RedissonClient)} 和 {@link #converter(MessageConverter)}。
 *
 * <p>使用示例：
 * <pre>{@code
 * NativeStreamMQ streamMQ = NativeStreamMQ.builder()
 *     .redisson(redissonClient)
 *     .namespace("my-app")
 *     .converter(converter)
 *     .build();
 * }</pre>
 *
 * <p>线程安全：不可变状态，所有设置方法返回新的逻辑状态（仿不可变风格的单例角色复制），
 * 实际上每次设置修改内部字段。建议在单线程中构造。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class NativeStreamMQBuilder {

    private RedissonClient redisson;
    private String namespace;
    private MessageConverter converter;

    /**
     * 设置 Redisson 客户端（必填）。
     *
     * @param redisson Redisson 客户端实例，不能为 null
     * @return this（链式调用）
     * @throws NullPointerException 如果 redisson 为 null
     */
    public NativeStreamMQBuilder redisson(RedissonClient redisson) {
        this.redisson = Objects.requireNonNull(redisson, "redisson");
        return this;
    }

    /**
     * 设置命名空间（可选，默认空字符串）。
     *
     * <p>命名空间用于隔离不同应用/环境的 Stream Key 前缀，
     * 格式：{@code streammq:{namespace}:...}
     *
     * @param namespace 命名空间，可为 null（自动转为空字符串）
     * @return this（链式调用）
     */
    public NativeStreamMQBuilder namespace(String namespace) {
        this.namespace = (namespace == null) ? "" : namespace;
        return this;
    }

    /**
     * 设置消息转换器（必填）。
     *
     * <p>负责 {@link io.github.streammq.core.message.Message} 与 Redis Stream Entry 字段的双向转换。
     * 常用实现：
     * <ul>
     *   <li>{@code io.github.streammq.adapter.redisson.converter.DefaultMessageConverter}
     *       -- 完整字段映射（推荐）</li>
     *   <li>{@code io.github.streammq.adapter.redisson.converter.CompactMessageConverter}
     *       -- 紧凑模式，减少存储开销</li>
     * </ul>
     *
     * @param converter 消息转换器实例，不能为 null
     * @return this（链式调用）
     * @throws NullPointerException 如果 converter 为 null
     */
    public NativeStreamMQBuilder converter(MessageConverter converter) {
        this.converter = Objects.requireNonNull(converter, "converter");
        return this;
    }

    /**
     * 构建 {@link NativeStreamMQ} 实例。
     *
     * <p>构建前会校验必填参数：{@link #redisson} 和 {@link #converter}。
     * 验证通过后内部创建 {@code RedissonStreamProducerFactory} 和
     * {@code RedissonStreamListenerFactory}，完成初始化。
     *
     * @return 已初始化的 NativeStreamMQ 实例
     * @throws IllegalStateException 如果 redisson 或 converter 未设置
     */
    public NativeStreamMQ build() {
        if (redisson == null) {
            throw new IllegalStateException("redisson must not be null, call redisson(RedissonClient) before build()");
        }
        if (converter == null) {
            throw new IllegalStateException(
                    "converter must not be null, call converter(MessageConverter) before build()");
        }
        return new NativeStreamMQ(redisson, namespace, converter);
    }
}
