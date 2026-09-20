/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.serializer;

import io.github.streammq.core.exception.SerializationException;

/**
 * 消息序列化器 SPI，负责 Message body 与 byte[] 的双向转换。
 *
 * <p>元信息（topic/tag/keys/shardingKey/properties）始终为 String，不参与序列化。 仅 {@code body} 字段经由此接口序列化。
 *
 * <p><b>null / 空输入契约（0.1.2 起对全部内置实现统一，发布即冻结）：</b>
 *
 * <ul>
 *   <li>{@code serialize(null, type)} 返回 {@code null}（<b>全部</b>内置实现，直通型也不例外）
 *   <li>{@code deserialize(null, type)} 返回 {@code null}（不抛异常）
 *   <li>{@code deserialize(byte[0], type)}：<b>直通型</b>（{@code StringSerializer} / {@code
 *       ByteArraySerializer}）返回空值本身（{@code ""} / {@code new byte[0]}）；<b>结构化型</b>（Jackson / Fury /
 *       Protostuff / JDK / SBE / FlatBuffers / Protobuf 等）返回 {@code null}—— 结构化载荷编码后至少含框架/类型
 *       标记，永不为空数组，空数组只可能来自"未写入"或对端发了空 body
 * </ul>
 *
 * <p><b>端到端不变式：</b>发送空（非 null）body 后，消费端必须还原为空值——{@code send(topic, "")} 在消费端得到 {@code ""}（而不是
 * {@code null}）；"{@code body 字段不存在}" 与 "{@code body 字段存在但为空}" 是两种不同语义，
 * 字段存在性判定以"字段是否出现"为准，<b>不以"值是否非空"为准</b>（消费侧实现见各适配层转换器）。
 *
 * <p>内置实现的 null/空语义映射表：
 *
 * <table border="1">
 *   <caption>内置序列化器 null/空语义</caption>
 *   <tr><th>序列化器</th><th>类型</th><th>{@code serialize(null)}</th><th>{@code deserialize(null)}</th><th>{@code deserialize(byte[0])}</th></tr>
 *   <tr><td>{@code StringSerializer}</td><td>直通</td><td>{@code null}</td><td>{@code null}</td><td>{@code ""}</td></tr>
 *   <tr><td>{@code ByteArraySerializer}</td><td>直通</td><td>{@code null}</td><td>{@code null}</td><td>{@code new byte[0]}</td></tr>
 *   <tr><td>{@code JacksonJsonSerializer}</td><td>结构化</td><td>{@code null}</td><td>{@code null}</td><td>{@code null}</td></tr>
 *   <tr><td>{@code FurySerializer}</td><td>结构化</td><td>{@code null}</td><td>{@code null}</td><td>{@code null}</td></tr>
 *   <tr><td>{@code ProtostuffSerializer}</td><td>结构化</td><td>{@code null}</td><td>{@code null}</td><td>{@code null}</td></tr>
 *   <tr><td>{@code JdkSerializer}</td><td>结构化</td><td>{@code null}</td><td>{@code null}</td><td>{@code null}</td></tr>
 *   <tr><td>{@code SbeSerializer} / {@code FlatBuffersSerializer}</td><td>结构化</td><td>{@code null}</td><td>{@code null}</td><td>{@code null}</td></tr>
 * </table>
 *
 * <p>内置实现（默认使用 {@code JacksonJsonSerializer}：严格类型、无多态反序列化、无 gadget RCE 面）：
 *
 * <ul>
 *   <li>{@code FurySerializer} - 基于 Apache Fury 的二进制序列化（可选，高吞吐；见 {@link
 *       io.github.streammq.core.StreamMQConstants#DEFAULT_SERIALIZER}；吞吐约为 Jackson 的 7~13
 *       倍。<b>默认强制类注册白名单</b>， 需预注册业务消息体类型；仅 {@code new FurySerializer(false)} 的宽松模式在共享/多租户 Redis
 *       上有反序列化 RCE 面）
 *   <li>{@code ProtostuffSerializer} - 基于 Protostuff 的二进制序列化（schema 由目标类型决定，无 gadget RCE 面，需无参构造
 *       POJO）
 *   <li>{@code FlatBuffersSerializer} - 基于 FlatBuffers {@code FlexBuffers}
 *       的动态（schema-less）二进制序列化；零拷贝读取、免代码生成， 经反射处理任意 POJO，纯数据、无反序列化代码执行面（安全）。需添加 {@code
 *       com.google.flatbuffers:flatbuffers-java}（optional）
 *   <li>{@code SbeSerializer} - 基于 SBE（Simple Binary Encoding，FIX 社区标准）的信封模式二进制序列化；定长消息头 + 零解析拷贝，
 *       业务体以严格类型 Jackson 编码后嵌入单一 {@code varData} 字段，无 gadget RCE 面（安全）。需添加 {@code
 *       org.agrona:agrona}（optional）
 *   <li>{@code JacksonJsonSerializer} - 基于 Jackson 的 JSON 序列化（跨语言/可读性优先，严格类型、无多态反序列化）
 *   <li>{@code JdkSerializer} - 基于 JDK 原生序列化（备选，内置 JEP 290 白名单）
 *   <li>{@code StringSerializer} / {@code ByteArraySerializer} - 直通序列化
 * </ul>
 *
 * <p><b>⚠️ 安全提示：</b>{@code FurySerializer} <b>默认强制类注册白名单</b>（{@code
 * requireClassRegistration=true}），需先注册业务消息体类型； 仅当显式创建宽松实例（{@code new
 * FurySerializer(false)}，受系统属性门禁保护）时，Redis 中字节流才可被反序列化为 classpath 上任意类， 共享/多租户 Redis 场景即反序列化 RCE
 * 攻击面。默认可用的 {@code JacksonJsonSerializer}（严格类型）、{@code ProtostuffSerializer}（schema 由目标类型决定）、
 * {@code FlatBuffersSerializer}（FlexBuffers 纯数据、零拷贝读）与 {@code SbeSerializer}（信封模式、定长头 + 零解析拷贝）均无
 * gadget 面。{@code JdkSerializer} 内置 JEP 290 白名单，自定义业务 body 类型需显式加白，详见各实现类 Javadoc。
 *
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface MessageSerializer<T> {

    /**
     * 序列化对象为 byte[]。
     *
     * <p><b>null 输入契约（0.1.2 起对全部内置实现统一）：</b>{@code object == null} 时返回 {@code null}（不再返回 {@code
     * byte[0]}，直通序列化器也不例外）。调用方（消息转换器）在 body 为 null 时不写入 {@code body} 字段， 因此 {@code null} 返回值不会进入
     * Base64 编码路径；实现方仍需保证非 null 入参永远返回非 null 字节数组。
     *
     * @param object 待序列化对象（可为 null，为 null 时返回 null）
     * @param type 目标类型（用于多态场景）
     * @return 字节数组；{@code object == null} 时为 {@code null}
     * @throws SerializationException 序列化失败
     */
    byte[] serialize(T object, Class<T> type) throws SerializationException;

    /**
     * 反序列化 byte[] 为对象。
     *
     * <p><b>null / 空输入契约（0.1.2 起对全部内置实现统一）：</b>{@code bytes} 为 {@code null} 时返回 {@code
     * null}（不抛异常）；{@code bytes} 为空数组（{@code length == 0}）时：直通型序列化器返回空值本身（{@code ""} / {@code new
     * byte[0]}），结构化型序列化器返回 {@code null}。{@code type} 为 {@code null} 时抛 {@link
     * NullPointerException}。 内置实现只抛 {@code SerializationException}（包装内部异常），不向消费路径泄漏裸运行时异常。
     *
     * @param bytes 字节数组（可为 null；为空数组时语义见接口 javadoc 映射表）
     * @param type 目标类型，不能为 null
     * @param <R> 反序列化目标类型
     * @return 反序列化对象；{@code bytes} 为 null 时为 {@code null}；空数组时直通型返回空值、结构化型返回 {@code null}
     * @throws SerializationException 反序列化失败
     * @throws NullPointerException 如果 {@code type} 为 null
     */
    <R> R deserialize(byte[] bytes, Class<R> type) throws SerializationException;

    /**
     * 返回序列化器名称（用于 SPI 选择与监控）。
     *
     * @return 名称
     */
    default String name() {
        return getClass().getSimpleName();
    }
}
