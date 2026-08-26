/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.exception.SerializationException;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.util.StringUtils;
import java.util.*;
import java.util.function.Consumer;

/**
 * {@link MessageConverter} 模板基类，使用 Template Method 模式抽取 encode/decode 骨架。
 *
 * <h3>设计意图</h3>
 *
 * <p>三个 Converter 实现（{@link DefaultMessageConverter} / {@link CompactMessageConverter} / {@link
 * PassThroughMessageConverter}）在 {@code toStreamFields} 和 {@code fromStreamFields}
 * 中存在大量重复的结构化代码（null 检查、字段赋值、bornTs 解析等），唯一差异在于：
 *
 * <ul>
 *   <li>字段名（长字段名 vs 短字段名 vs 直通）
 *   <li>Body 序列化/反序列化方式（序列化器 vs toString）
 *   <li>Properties 存储策略（合并 JSON vs 分离 JSON）
 *   <li>扩展字段（延迟字段、topic 字段等）
 * </ul>
 *
 * <h3>模板方法结构</h3>
 *
 * <pre>
 * toStreamFields (final)
 *   ├── encodeBody           [abstract]  子类实现
 *   ├── writeTag/Keys/Shard  [concrete]  putField 封装
 *   ├── encodeProperties     [abstract]  子类实现
 *   ├── writeBornTs/Host/Tx  [concrete]  putField 封装
 *   ├── writeRetryTimes      [concrete]  条件写入
 *   └── encodeExtra          [hook]      子类可选覆写
 *
 * fromStreamFields (final)
 *   ├── readTopic            [concrete]  getField 封装
 *   ├── decodeBody           [abstract]  子类实现
 *   ├── readTag/Keys/Shard/Host/Tx [concrete]  getField 封装
 *   ├── decodeProperties     [abstract]  子类实现
 *   ├── parseBornTs          [concrete]  带容错解析
 *   ├── parseRetryTimes      [concrete]  条件解析
 *   └── decodeExtra          [hook]      子类可选覆写
 * </pre>
 *
 * <h3>子类实现清单</h3>
 *
 * <table>
 *   <tr><th>方法</th><th>是否必须</th><th>说明</th></tr>
 *   <tr><td>{@link #fieldBody()} ~ {@link #fieldBornHost()}</td><td>abstract</td><td>核心字段名</td></tr>
 *   <tr><td>{@link #fieldTopic()}</td><td>optional</td><td>仅 compact 格式覆写（topic 存于 Stream Entry 中）</td></tr>
 *   <tr><td>{@link #fieldRetryTimes()} / {@link #fieldTxId()}</td><td>optional</td><td>可选元数据字段</td></tr>
 *   <tr><td>{@link #encodeBody(Message, Map)}</td><td>abstract</td><td>消息体编码逻辑</td></tr>
 *   <tr><td>{@link #decodeBody(Map, Class, Message, String)}</td><td>abstract</td><td>消息体解码逻辑</td></tr>
 *   <tr><td>{@link #encodeProperties(Message, Map)}</td><td>abstract</td><td>属性编码逻辑</td></tr>
 *   <tr><td>{@link #decodeProperties(Message, Map)}</td><td>abstract</td><td>属性解码逻辑</td></tr>
 *   <tr><td>{@link #encodeExtra(Message, Map)}</td><td>optional</td><td>扩展字段编码（如延迟字段）</td></tr>
 *   <tr><td>{@link #decodeExtra(Message, Map)}</td><td>optional</td><td>扩展字段解码</td></tr>
 * </table>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public abstract class AbstractMessageConverter implements MessageConverter {

    /** 属性 JSON 序列化器，子类通过 {@link #writePropsJson} / {@link #readPropsJson} 共用 */
    protected final ObjectMapper propsMapper = new ObjectMapper();

    // ================================================================
    // 字段名 —— 子类覆写以定义各自 Stream Entry 字段名
    // ================================================================

    /**
     * 返回消息体字段名。
     *
     * <p>Default / PassThrough: {@code "body"}；Compact: {@code "b"}。
     *
     * @return 字段名，不能为 null
     */
    protected abstract String fieldBody();

    /**
     * 返回消息体类型字段名（存储 body 类全限定名，用于反序列化时类型定位）。
     *
     * <p>Default / PassThrough: {@code "bodyType"}；Compact: {@code "bt"}。
     *
     * @return 字段名，不能为 null
     */
    protected abstract String fieldBodyType();

    /**
     * 返回消息体类型简称字段名（可选，仅 Default 覆写）。
     *
     * @return 字段名，默认 null 表示不写入
     */
    protected String fieldBodyTypeName() {
        return null;
    }

    /**
     * 返回标签字段名。
     *
     * <p>Default / PassThrough: {@code "tag"}；Compact: {@code "g"}。
     *
     * @return 字段名，不能为 null
     */
    protected abstract String fieldTag();

    /**
     * 返回业务键字段名。
     *
     * <p>Default / PassThrough: {@code "keys"}；Compact: {@code "k"}。
     *
     * @return 字段名，不能为 null
     */
    protected abstract String fieldKeys();

    /**
     * 返回分片键字段名。
     *
     * <p>Default / PassThrough: {@code "shardingKey"}；Compact: {@code "s"}。
     *
     * @return 字段名，不能为 null
     */
    protected abstract String fieldShardingKey();

    /**
     * 返回出生时间戳字段名。
     *
     * <p>Default / PassThrough: {@code "bornTs"}；Compact: {@code "ts"}。
     *
     * @return 字段名，不能为 null
     */
    protected abstract String fieldBornTs();

    /**
     * 返回出生主机字段名。
     *
     * <p>Default / PassThrough: {@code "bornHost"}；Compact: {@code "h"}。
     *
     * @return 字段名，不能为 null
     */
    protected abstract String fieldBornHost();

    /**
     * 返回重试次数字段名（可选）。
     *
     * <p>返回 null 表示此 Converter 不支持重试次数字段。
     *
     * @return 字段名，默认 null
     */
    protected String fieldRetryTimes() {
        return null;
    }

    /**
     * 返回事务 ID 字段名（可选）。
     *
     * <p>返回 null 表示此 Converter 不支持事务 ID 字段。
     *
     * @return 字段名，默认 null
     */
    protected String fieldTxId() {
        return null;
    }

    /**
     * 返回原始 Topic 字段名（可选，仅重试/DLQ 场景）。
     *
     * @return 字段名，默认 null
     */
    protected String fieldOriginTopic() {
        return null;
    }

    /**
     * 返回延时级别字段名（可选，仅 Compact 覆写）。
     *
     * @return 字段名，默认 null
     */
    protected String fieldDelayLevel() {
        return null;
    }

    /**
     * 返回自定义延时毫秒字段名（可选，仅 Compact 覆写）。
     *
     * @return 字段名，默认 null
     */
    protected String fieldDelayTimeMillis() {
        return null;
    }

    /**
     * 返回 Topic 字段名（可选，仅 Compact 覆写，因其格式将 topic 存入 Stream Entry）。
     *
     * <p>Default / PassThrough 中 topic 由 Stream Key 表示，不存入 Entry。
     *
     * @return 字段名，默认 null 表示不读写 topic 字段
     */
    protected String fieldTopic() {
        return null;
    }

    // ================================================================
    // 抽象钩子 —— 子类实现差异化逻辑
    // ================================================================

    /**
     * 将消息体编码写入 Stream Entry 字段。
     *
     * <p>典型实现：序列化 body 为 byte[] → Base64 编码 → 写入字段。 同时应写入 {@link #fieldBodyType()} 字段以支持消费端类型定位。
     *
     * @param message 消息载体，body 可能为 null
     * @param fields 输出 Map，直接修改
     */
    protected abstract void encodeBody(Message<?> message, Map<String, String> fields);

    /**
     * 从 Stream Entry 字段解码消息体并写入装配草稿。
     *
     * <p>需处理两种来源：
     *
     * <ol>
     *   <li>SDK 发送方：{@code bodyTypeField} 非空 → body 为 Base64 编码的序列化字节
     *   <li>跨平台发送方：{@code bodyTypeField} 为空 → body 为原始字符串
     * </ol>
     *
     * @param fields Stream Entry 全部字段
     * @param targetType 目标 body 类型
     * @param draft 装配草稿（解码结果写入 {@code draft.body}）
     * @param bodyStr body 字段的原始字符串值（已从 Map 中取出）
     * @param <T> body 泛型类型
     */
    protected abstract <T> void decodeBody(
            Map<String, String> fields, Class<T> targetType, MessageDraft<T> draft, String bodyStr);

    /**
     * 将消息的系统属性和用户属性编码写入 Stream Entry 字段。
     *
     * <p>典型实现：调用 {@link #writePropsJson(Map, String, Map, Map)} 合并写入单个 JSON 字段， 或分别写入两个独立字段。
     *
     * @param message 消息载体
     * @param fields 输出 Map，直接修改
     */
    protected abstract void encodeProperties(Message<?> message, Map<String, String> fields);

    /**
     * 从 Stream Entry 字段解码属性并写入装配草稿。
     *
     * <p>典型实现：调用 {@link #readPropsJson(Map, String, Consumer)} 从 JSON 读取后合并进 {@code
     * draft.properties} / {@code draft.userProperties}。
     *
     * @param draft 装配草稿
     * @param fields Stream Entry 全部字段
     * @param <T> body 泛型类型
     */
    protected abstract <T> void decodeProperties(MessageDraft<T> draft, Map<String, String> fields);

    /**
     * 编码扩展字段（钩子方法，默认空实现）。
     *
     * <p>用于写入模板方法未覆盖的自定义字段，如延迟级别、自定义延时毫秒等。 在 {@link #toStreamFields(Message)} 末尾调用。
     *
     * @param message 消息载体
     * @param fields 输出 Map，直接修改
     */
    protected void encodeExtra(Message<?> message, Map<String, String> fields) {}

    /**
     * 解码扩展字段（钩子方法，默认空实现）。
     *
     * <p>用于读取模板方法未覆盖的自定义字段。 在 {@link #fromStreamFields(Map, Class, String)} 末尾调用。
     *
     * @param draft 装配草稿
     * @param fields Stream Entry 全部字段
     * @param <T> body 泛型类型
     */
    protected <T> void decodeExtra(MessageDraft<T> draft, Map<String, String> fields) {}

    // ================================================================
    // 模板方法：编码 —— final，子类不可覆写
    // ================================================================

    /**
     * {@inheritDoc}
     *
     * <p><b>模板骨架（final）：</b>
     *
     * <ol>
     *   <li>null 校验
     *   <li>调用 {@link #encodeBody(Message, Map)} — 子类实现
     *   <li>写入 tag / keys / shardingKey（通过 {@link #putField} 封装 null 安全）
     *   <li>调用 {@link #encodeProperties(Message, Map)} — 子类实现
     *   <li>写入 bornTs / bornHost / txId / retryTimes
     *   <li>调用 {@link #encodeExtra(Message, Map)} — 钩子
     * </ol>
     */
    @Override
    public final Map<String, String> toStreamFields(Message<?> message) {
        Objects.requireNonNull(message, "message");
        Map<String, String> fields = new HashMap<>(16);

        encodeBody(message, fields);
        putField(fields, fieldTag(), message.getTag());
        putField(fields, fieldKeys(), message.getKeys());
        putField(fields, fieldShardingKey(), message.getShardingKey());

        encodeProperties(message, fields);

        fields.put(fieldBornTs(), Long.toString(message.getBornTimestamp()));
        putField(fields, fieldBornHost(), message.getBornHost());
        putField(fields, fieldTxId(), message.getTransactionId());
        // 原始 Topic：随消息体进入重试 Stream / DLQ Stream 后仍可溯源（消费侧回填 draft.topic）
        putField(fields, fieldOriginTopic(), message.getTopic());

        if (message.getReconsumeTimes() > 0 && fieldRetryTimes() != null) {
            fields.put(fieldRetryTimes(), Integer.toString(message.getReconsumeTimes()));
        }

        encodeExtra(message, fields);
        return fields;
    }

    // ================================================================
    // 模板方法：解码 —— final，子类不可覆写
    // ================================================================

    /**
     * {@inheritDoc}
     *
     * <p>委托到 {@link #fromStreamFields(Map, Class, String)}，不允许字段缺失 Topic。
     */
    @Override
    public final <T> Message<T> fromStreamFields(Map<String, String> fields, Class<T> targetType) {
        return fromStreamFields(fields, targetType, null);
    }

    /**
     * {@inheritDoc}
     *
     * <p><b>模板骨架（final）：</b>
     *
     * <ol>
     *   <li>null 校验，创建装配草稿
     *   <li>读取 topic（若 Converter 支持）
     *   <li>调用 {@link #decodeBody(Map, Class, MessageDraft, String)} — 子类实现
     *   <li>读取 tag / keys / shardingKey / bornHost / txId（通过 {@link #getField} 封装 null 安全）
     *   <li>调用 {@link #decodeProperties(MessageDraft, Map)} — 子类实现
     *   <li>解析 bornTs / retryTimes（带容错）
     *   <li>调用 {@link #decodeExtra(MessageDraft, Map)} — 钩子
     *   <li>一次性构造不可变 Message（Topic 缺失时回填 fallbackTopic）
     * </ol>
     */
    @Override
    public final <T> Message<T> fromStreamFields(
            Map<String, String> fields, Class<T> targetType, String fallbackTopic) {
        Objects.requireNonNull(fields, "fields");
        Objects.requireNonNull(targetType, "targetType");

        MessageDraft<T> draft = new MessageDraft<>();

        getField(fields, fieldTopic(), value -> draft.topic = value);

        String bodyStr = fields.get(fieldBody());
        if (StringUtils.isNotEmpty(bodyStr)) {
            decodeBody(fields, targetType, draft, bodyStr);
        }

        getField(fields, fieldTag(), value -> draft.tag = value);
        getField(fields, fieldKeys(), value -> draft.keys = value);
        getField(fields, fieldShardingKey(), value -> draft.shardingKey = value);
        getField(fields, fieldBornHost(), value -> draft.bornHost = value);
        getField(fields, fieldTxId(), value -> draft.transactionId = value);
        // 原始 Topic（重试/DLQ 场景溯源）：优先于 fallbackTopic
        getField(fields, fieldOriginTopic(), value -> draft.topic = value);

        decodeProperties(draft, fields);

        String bornTs = fields.get(fieldBornTs());
        if (StringUtils.isNotEmpty(bornTs)) {
            try {
                draft.bornTimestamp = Long.parseLong(bornTs);
            } catch (NumberFormatException ex) {
                throw new SerializationException("Failed to parse bornTs: " + bornTs, ex);
            }
        }

        String retryField = fieldRetryTimes();
        if (retryField != null) {
            String retryStr = fields.get(retryField);
            if (StringUtils.isNotEmpty(retryStr)) {
                try {
                    draft.reconsumeTimes = Integer.parseInt(retryStr);
                } catch (NumberFormatException ex) {
                    throw new SerializationException("Failed to parse retryTimes: " + retryStr, ex);
                }
            }
        }

        decodeExtra(draft, fields);
        captureReservedFields(draft, fields);
        return draft.toMessage(fallbackTopic);
    }

    /**
     * 捕获 Stream Entry 中的 SDK 内部保留字段（{@code __} 前缀，如 {@code __dlqRetryCount}） 到用户属性，使其在 decode →
     * encode 往返中不丢失。
     *
     * <p>背景：DLQ 重试计数等调度元数据以顶层 Entry 字段写入，若解码时丢弃、失败处理时重新编码， 计数将永远为 0，导致 DLQ 重试上限与二级 DLQ
     * 策略失效（无限重试）。捕获后这些字段随 props JSON 往返持久化。
     *
     * @param draft 装配草稿（保留字段并入 {@code draft.userProperties}）
     * @param fields Stream Entry 全部字段
     * @param <T> body 泛型类型
     */
    private static <T> void captureReservedFields(
            MessageDraft<T> draft, Map<String, String> fields) {
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            String name = entry.getKey();
            if (Objects.nonNull(name)
                    && name.startsWith(
                            io.github.streammq.core.StreamMQConstants.RESERVED_PROPERTY_PREFIX)
                    && Objects.nonNull(entry.getValue())) {
                // 顶层 Entry 字段是权威载体（调度器写入的最新值），覆盖 props JSON 中可能存在的陈旧副本
                draft.userProperties.put(name, entry.getValue());
            }
        }
    }

    // ================================================================
    // 内部工具方法
    // ================================================================

    /**
     * null 安全的字段写入：name 或 value 为 null 时跳过。
     *
     * <p>消除子类中大量的 {@code if (Objects.nonNull(xxx)) fields.put(...)} 样板代码。
     *
     * @param fields 目标 Map
     * @param name 字段名，为 null 表示此 Converter 不支持该字段
     * @param value 字段值，为 null 表示此消息不携带该字段
     */
    protected static void putField(Map<String, String> fields, String name, String value) {
        if (name != null && value != null) {
            fields.put(name, value);
        }
    }

    /**
     * null 安全的字段读取：name 为 null 或字段不存在时跳过。
     *
     * <p>消除子类中大量的 {@code if (fields.containsKey(xxx)) draft.xxx = ...} 样板代码。
     *
     * @param fields Stream Entry 全部字段
     * @param name 字段名，为 null 表示此 Converter 不支持该字段
     * @param consumer 草稿字段赋值逻辑
     */
    protected static <T> void getField(
            Map<String, String> fields, String name, Consumer<String> setter) {
        if (name != null && fields.containsKey(name)) {
            setter.accept(fields.get(name));
        }
    }

    /**
     * 将系统属性和用户属性合并序列化为单个 JSON 字段。
     *
     * <p>Default 和 PassThrough Converter 共用此方法。 两个 Map 均可能为空，合并后若为空则不做任何写入。
     *
     * <p><b>键冲突规则：</b>系统属性优先（后写入覆盖）——SDK 内部元数据（如 trace 上下文）不可被业务同名用户属性静默篡改。
     *
     * @param fields 目标 Map
     * @param fieldName JSON 字段名（如 {@code "props"}）
     * @param sysProps 系统属性 Map（可空，不可变视图）
     * @param userProps 用户属性 Map（可空，防御性拷贝）
     * @throws SerializationException 当 JSON 序列化失败时
     */
    protected void writePropsJson(
            Map<String, String> fields,
            String fieldName,
            Map<String, String> sysProps,
            Map<String, String> userProps) {
        Map<String, String> merged = new HashMap<>(sysProps.size() + userProps.size());
        merged.putAll(userProps);
        merged.putAll(sysProps);
        if (merged.isEmpty()) {
            return;
        }
        try {
            fields.put(fieldName, propsMapper.writeValueAsString(merged));
        } catch (JsonProcessingException ex) {
            throw new SerializationException("Failed to serialize message properties", ex);
        }
    }

    /**
     * 从单个 JSON 字段反序列化属性并写入 message。
     *
     * <p>Default 和 PassThrough Converter 共用此方法。 字段不存在或为空时不做任何操作。
     *
     * @param fields Stream Entry 全部字段
     * @param fieldName JSON 字段名（如 {@code "props"}）
     * @param consumer 接收反序列化后的属性 Map 的消费者（如 {@code message::setUserProperties}）
     * @param <T> body 泛型类型
     * @throws SerializationException 当 JSON 反序列化失败时
     */
    @SuppressWarnings("unchecked")
    protected <T> void readPropsJson(
            Map<String, String> fields, String fieldName, Consumer<Map<String, String>> consumer) {
        String json = fields.get(fieldName);
        if (StringUtils.isEmpty(json)) {
            return;
        }
        try {
            consumer.accept(
                    propsMapper.readValue(json, new TypeReference<Map<String, String>>() {}));
        } catch (JsonProcessingException ex) {
            throw new SerializationException("Failed to deserialize message properties", ex);
        }
    }
}
