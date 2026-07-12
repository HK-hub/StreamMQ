package io.github.streammq.adapter.redisson.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.streammq.core.exception.SerializationException;
import io.github.streammq.core.serializer.MessageSerializer;

import java.util.Objects;

/**
 * 默认 JSON 序列化器，基于 Jackson 实现。
 *
 * <p>使用内置线程安全的 {@link ObjectMapper}（Jackson 文档保证 ObjectMapper 在配置完成后线程安全）。
 * 默认注册：
 * <ul>
 *   <li>{@link JavaTimeModule} - 支持 JSR-310 时间类型</li>
 *   <li>禁用 {@code WRITE_DATES_AS_TIMESTAMPS} - 时间以 ISO-8601 字符串表示</li>
 * </ul>
 *
 * <p>序列化为 byte[] 时使用 UTF-8 编码；反序列化时按目标类型构造。
 *
 * @param <T> body 类型
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class JacksonJsonSerializer<T> implements MessageSerializer<T> {

    /** 默认共享实例（不可变配置） */
    private static final ObjectMapper DEFAULT_MAPPER = JsonMapper.builder()
        .addModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        .build();

    private final ObjectMapper mapper;

    /**
     * 使用默认 {@link ObjectMapper} 构造。
     */
    public JacksonJsonSerializer() {
        this(DEFAULT_MAPPER);
    }

    /**
     * 使用自定义 {@link ObjectMapper} 构造（用于复用上层已配置的 mapper）。
     *
     * @param mapper Jackson ObjectMapper，不能为 null
     */
    public JacksonJsonSerializer(ObjectMapper mapper) {
        this.mapper = Objects.requireNonNull(mapper, "mapper");
    }

    @Override
    public byte[] serialize(T object, Class<T> type) throws SerializationException {
        if (Objects.isNull(object)) {
            return new byte[0];
        }
        try {
            return mapper.writeValueAsBytes(object);
        } catch (JsonProcessingException ex) {
            throw new SerializationException(
                "Jackson serialize failed for type " + (Objects.nonNull(type) ? type.getName() : "null"), ex);
        }
    }

    @Override
    public <R> R deserialize(byte[] bytes, Class<R> type) throws SerializationException {
        Objects.requireNonNull(type, "type");
        if (Objects.isNull(bytes) || bytes.length == 0) {
            return null;
        }
        try {
            return mapper.readValue(bytes, type);
        } catch (Exception ex) {
            throw new SerializationException(
                "Jackson deserialize failed for type " + type.getName(), ex);
        }
    }

    @Override
    public String name() {
        return "jackson-json";
    }
}
