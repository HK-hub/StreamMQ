package io.github.streammq.adapter.redisson.serializer;

import io.github.streammq.core.exception.SerializationException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link JdkSerializer} 单元测试，覆盖 JDK 原生序列化往返、null 处理、
 * 类型不匹配、损坏字节流与 name 方法。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("JdkSerializer JDK 序列化器测试")
class JdkSerializerTest {

    /** 测试用 Serializable POJO */
    public static class MyData implements Serializable {
        private static final long serialVersionUID = 1L;

        private String name;
        private int age;

        public MyData() {
        }

        public MyData(String name, int age) {
            this.name = name;
            this.age = age;
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

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof MyData myData)) {
                return false;
            }
            return age == myData.age && Objects.equals(name, myData.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, age);
        }
    }

    private final JdkSerializer<MyData> serializer = new JdkSerializer<>();

    @Test
    @DisplayName("Serializable POJO 序列化/反序列化往返")
    void roundTrip() {
        MyData data = new MyData("Bob", 25);
        byte[] bytes = serializer.serialize(data, MyData.class);
        assertThat(bytes).isNotEmpty();
        MyData result = serializer.deserialize(bytes, MyData.class);
        assertThat(result).isEqualTo(data);
        assertThat(result.getName()).isEqualTo("Bob");
        assertThat(result.getAge()).isEqualTo(25);
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
    @DisplayName("deserialize type 为 null 抛出 NullPointerException")
    void deserializeNullType() {
        byte[] bytes = serializer.serialize(new MyData("Bob", 25), MyData.class);
        assertThatThrownBy(() -> serializer.deserialize(bytes, null))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("type");
    }

    @Test
    @DisplayName("deserialize 类型不匹配抛出 SerializationException")
    void deserializeTypeMismatch() {
        byte[] bytes = serializer.serialize(new MyData("Bob", 25), MyData.class);
        assertThatThrownBy(() -> serializer.deserialize(bytes, String.class))
            .isInstanceOf(SerializationException.class)
            .hasMessageContaining("type mismatch");
    }

    @Test
    @DisplayName("deserialize 损坏 byte[] 抛出 SerializationException")
    void deserializeCorrupted() {
        byte[] corrupted = {1, 2, 3, 4};
        assertThatThrownBy(() -> serializer.deserialize(corrupted, MyData.class))
            .isInstanceOf(SerializationException.class)
            .hasMessageContaining("JDK deserialize failed");
    }

    @Test
    @DisplayName("name 返回 jdk")
    void name() {
        assertThat(serializer.name()).isEqualTo("jdk");
    }
}
