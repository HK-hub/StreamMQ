package io.github.streammq.adapter.redisson.serializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.streammq.core.exception.SerializationException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link JacksonJsonSerializer} 单元测试，覆盖序列化/反序列化往返、null 处理、 异常场景、JavaTimeModule 支持与 name 方法。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("JacksonJsonSerializer Jackson 序列化器测试")
class JacksonJsonSerializerTest {

    /** 测试用 POJO（record，Jackson 2.18 原生支持） */
    public record Person(String name, int age) {}

    private final JacksonJsonSerializer<Object> serializer = new JacksonJsonSerializer<>();

    @Test
    @DisplayName("String 序列化/反序列化往返")
    void stringRoundTrip() {
        byte[] bytes = serializer.serialize("hello", Object.class);
        assertThat(bytes).isNotEmpty();
        assertThat(serializer.deserialize(bytes, String.class)).isEqualTo("hello");
    }

    @Test
    @DisplayName("自定义 POJO 序列化/反序列化往返")
    void pojoRoundTrip() {
        Person person = new Person("Alice", 30);
        byte[] bytes = serializer.serialize(person, Object.class);
        assertThat(serializer.deserialize(bytes, Person.class)).isEqualTo(person);
    }

    @Test
    @DisplayName("List 序列化/反序列化往返")
    void listRoundTrip() {
        List<String> list = List.of("a", "b", "c");
        byte[] bytes = serializer.serialize(list, Object.class);
        @SuppressWarnings("unchecked")
        List<String> result = (List<String>) serializer.deserialize(bytes, List.class);
        assertThat(result).containsExactly("a", "b", "c");
    }

    @Test
    @DisplayName("Map 序列化/反序列化往返")
    void mapRoundTrip() {
        Map<String, String> map = Map.of("k1", "v1", "k2", "v2");
        byte[] bytes = serializer.serialize(map, Object.class);
        @SuppressWarnings("unchecked")
        Map<String, String> result = (Map<String, String>) serializer.deserialize(bytes, Map.class);
        assertThat(result).containsEntry("k1", "v1").containsEntry("k2", "v2");
    }

    @Test
    @DisplayName("serialize(null) 返回空 byte[]")
    void serializeNull() {
        assertThat(serializer.serialize(null, Object.class)).isEmpty();
    }

    @Test
    @DisplayName("deserialize(null) 返回 null")
    void deserializeNull() {
        assertThat(serializer.deserialize(null, String.class)).isNull();
    }

    @Test
    @DisplayName("deserialize(空 byte[]) 返回 null")
    void deserializeEmpty() {
        assertThat(serializer.deserialize(new byte[0], String.class)).isNull();
    }

    @Test
    @DisplayName("deserialize 非法 JSON 抛出 SerializationException")
    void deserializeInvalidJson() {
        byte[] invalid = "not-a-json".getBytes();
        assertThatThrownBy(() -> serializer.deserialize(invalid, String.class))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("Jackson deserialize failed");
    }

    @Test
    @DisplayName("deserialize type 为 null 抛出 NullPointerException")
    void deserializeNullType() {
        byte[] bytes = serializer.serialize("hello", Object.class);
        assertThatThrownBy(() -> serializer.deserialize(bytes, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("type");
    }

    @Test
    @DisplayName("name 返回 jackson-json")
    void name() {
        assertThat(serializer.name()).isEqualTo("jackson-json");
    }

    @Test
    @DisplayName("包含 JavaTimeModule: 序列化 LocalDateTime 不报错")
    void serializeLocalDateTime() {
        LocalDateTime now = LocalDateTime.of(2026, 7, 1, 12, 0, 0);
        byte[] bytes = serializer.serialize(now, Object.class);
        assertThat(bytes).isNotEmpty();
    }

    @Test
    @DisplayName("LocalDateTime 序列化/反序列化往返")
    void localDateTimeRoundTrip() {
        LocalDateTime time = LocalDateTime.of(2026, 7, 1, 12, 30, 45);
        byte[] bytes = serializer.serialize(time, Object.class);
        assertThat(serializer.deserialize(bytes, LocalDateTime.class)).isEqualTo(time);
    }

    @Test
    @DisplayName("自定义 ObjectMapper 构造（不能为 null）")
    void customMapperNull() {
        assertThatThrownBy(() -> new JacksonJsonSerializer<Object>(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("mapper");
    }

    @Test
    @DisplayName("自定义 ObjectMapper 构造可用")
    void customMapper() {
        JacksonJsonSerializer<Object> custom = new JacksonJsonSerializer<>(new ObjectMapper());
        byte[] bytes = custom.serialize("test", Object.class);
        assertThat(custom.deserialize(bytes, String.class)).isEqualTo("test");
    }
}
