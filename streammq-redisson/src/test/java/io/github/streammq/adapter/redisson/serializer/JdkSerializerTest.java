/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.serializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.streammq.core.exception.SerializationException;
import java.io.Serializable;
import java.util.Objects;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link JdkSerializer} 单元测试，覆盖 JDK 原生序列化往返、null 处理、 类型不匹配、损坏字节流与 name 方法。
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

        public MyData() {}

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
    @DisplayName("deserialize 目标类型不在白名单时被过滤器拒绝（安全优先于事后类型检查）")
    void deserializeTypeMismatch() {
        byte[] bytes = serializer.serialize(new MyData("Bob", 25), MyData.class);
        // String 不携带 MyData 白名单项：readObject 阶段即被 JEP 290 过滤器拦截，
        // 而非旧实现的反序列化后类型不匹配检查
        assertThatThrownBy(() -> serializer.deserialize(bytes, String.class))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("JDK deserialize failed");
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

    /** 白名单外的测试用 POJO */
    public static class ForeignData implements Serializable {
        private static final long serialVersionUID = 1L;
    }

    @Test
    @DisplayName("默认过滤器拒绝白名单外的类（反序列化前拦截）")
    void filterRejectsNonWhitelistedClass() {
        JdkSerializer<ForeignData> foreign = new JdkSerializer<>();
        byte[] bytes = foreign.serialize(new ForeignData(), ForeignData.class);
        assertThatThrownBy(() -> serializer.deserialize(bytes, MyData.class))
                .isInstanceOf(SerializationException.class);
    }

    @Test
    @DisplayName("addAllowedClasses 放行指定类")
    void addAllowedClassesPermitsExplicitClass() {
        JdkSerializer<MyData> widened = new JdkSerializer<>();
        widened.addAllowedClasses(java.util.List.of(ForeignData.class.getName()));
        byte[] bytes =
                new JdkSerializer<ForeignData>().serialize(new ForeignData(), ForeignData.class);
        ForeignData restored = widened.deserialize(bytes, ForeignData.class);
        assertThat(restored).isNotNull();
    }

    @Test
    @DisplayName("unrestricted() 关闭过滤（仅供可信环境迁移）")
    void unrestrictedDisablesFiltering() {
        JdkSerializer<ForeignData> open = JdkSerializer.unrestricted();
        byte[] bytes = open.serialize(new ForeignData(), ForeignData.class);
        assertThat(open.deserialize(bytes, ForeignData.class)).isNotNull();
    }
}
