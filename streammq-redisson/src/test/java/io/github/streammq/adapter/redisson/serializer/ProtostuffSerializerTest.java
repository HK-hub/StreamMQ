package io.github.streammq.adapter.redisson.serializer;

import io.github.streammq.core.exception.SerializationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link ProtostuffSerializer} 单元测试，覆盖序列化/反序列化往返、null 处理、
 * 空 byte[] 处理、损坏字节流与 name 方法。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("ProtostuffSerializer Protostuff 序列化器测试")
class ProtostuffSerializerTest {

    /** 测试用 POJO（含 String/int/long 字段，无参构造 + getter/setter） */
    public static class MyData {
        private String name;
        private int age;
        private long timestamp;

        public MyData() {
        }

        public MyData(String name, int age, long timestamp) {
            this.name = name;
            this.age = age;
            this.timestamp = timestamp;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof MyData myData)) {
                return false;
            }
            return age == myData.age
                && timestamp == myData.timestamp
                && Objects.equals(name, myData.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, age, timestamp);
        }
    }

    private final ProtostuffSerializer<MyData> serializer = new ProtostuffSerializer<>();

    @Test
    @DisplayName("POJO 序列化/反序列化往返（String/int/long 字段）")
    void roundTrip() {
        MyData data = new MyData("Bob", 25, 1719800000L);
        byte[] bytes = serializer.serialize(data, MyData.class);
        assertThat(bytes).isNotEmpty();
        MyData result = serializer.deserialize(bytes, MyData.class);
        assertThat(result).isEqualTo(data);
        assertThat(result.getName()).isEqualTo("Bob");
        assertThat(result.getAge()).isEqualTo(25);
        assertThat(result.getTimestamp()).isEqualTo(1719800000L);
    }

    @Test
    @DisplayName("serialize(null) 返回空 byte[]")
    void serializeNull() {
        assertThat(serializer.serialize(null, MyData.class)).isEmpty();
    }

    @Test
    @DisplayName("deserialize(null) 返回 null")
    void deserializeNull() {
        assertThat(serializer.deserialize(null, MyData.class)).isNull();
    }

    @Test
    @DisplayName("deserialize(空 byte[]) 返回 null")
    void deserializeEmpty() {
        assertThat(serializer.deserialize(new byte[0], MyData.class)).isNull();
    }

    @Test
    @DisplayName("deserialize 损坏 byte[] 抛出 SerializationException")
    void deserializeCorrupted() {
        byte[] corrupted = {1, 2, 3, 4, 5, 6, 7, 8};
        assertThatThrownBy(() -> serializer.deserialize(corrupted, MyData.class))
            .isInstanceOf(SerializationException.class)
            .hasMessageContaining("Protostuff deserialize failed");
    }

    @Test
    @DisplayName("name 返回 protostuff")
    void name() {
        assertThat(serializer.name()).isEqualTo("protostuff");
    }
}
