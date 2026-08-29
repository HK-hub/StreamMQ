/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.serializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Objects;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link FurySerializer} 单元测试，覆盖序列化/反序列化往返、null 处理、 空 byte[] 处理与 name 方法。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("FurySerializer Apache Fury 序列化器测试")
class FurySerializerTest {

    /** 测试用 POJO（含 String/int/long 字段，无参构造 + getter/setter） */
    public static class MyData {
        private String name;
        private int age;
        private long timestamp;

        public MyData() {}

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

    /** 默认实例（0.1.0 起 secure-by-default：强制类注册白名单） */
    private final FurySerializer<MyData> serializer = new FurySerializer<>(false);

    @org.junit.jupiter.api.BeforeAll
    static void allowUnrestricted() {
        // 单元测试需要"无白名单"路径，必须显式声明
        System.setProperty("streammq.security.allowUnrestrictedSerializer", "true");
    }

    @org.junit.jupiter.api.AfterAll
    static void disallowUnrestricted() {
        System.clearProperty("streammq.security.allowUnrestrictedSerializer");
    }

    @Test
    @DisplayName("POJO 序列化/反序列化往返（String/int/long 字段，关闭类注册校验）")
    void roundTrip() {
        MyData data = new MyData("Alice", 30, 1719800000L);
        byte[] bytes = serializer.serialize(data, MyData.class);
        assertThat(bytes).isNotEmpty();
        MyData result = serializer.deserialize(bytes, MyData.class);
        assertThat(result).isEqualTo(data);
        assertThat(result.getName()).isEqualTo("Alice");
        assertThat(result.getAge()).isEqualTo(30);
        assertThat(result.getTimestamp()).isEqualTo(1719800000L);
    }

    @Test
    @DisplayName("默认构造强制类注册：未注册类序列化被拒绝（secure-by-default）")
    void defaultRejectsUnregisteredClass() {
        FurySerializer<MyData> secure = new FurySerializer<>();
        MyData data = new MyData("Alice", 30, 1719800000L);
        assertThatThrownBy(() -> secure.serialize(data, MyData.class))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("not registered");
    }

    @Test
    @DisplayName("secure serializer exposes public registration API")
    void registerAllowsPojoRoundTrip() {
        FurySerializer<MyData> secure = new FurySerializer<>();
        secure.register(MyData.class);
        MyData data = new MyData("registered", 7, 42L);
        byte[] bytes = secure.serialize(data, MyData.class);
        assertThat(secure.deserialize(bytes, MyData.class)).isEqualTo(data);
    }

    @Test
    @DisplayName("constructor registers initial message types")
    void constructorRegistersTypes() {
        FurySerializer<MyData> secure = new FurySerializer<>(MyData.class);
        MyData data = new MyData("initial", 1, 2L);
        assertThat(secure.deserialize(secure.serialize(data, MyData.class), MyData.class))
                .isEqualTo(data);
    }

    @Test
    @DisplayName("FurySerializer(false) 默认抛 SecurityException（防止 foot-gun）")
    void requiresClassRegistrationFalseGatedBySystemProperty() {
        String previous = System.getProperty("streammq.security.allowUnrestrictedSerializer");
        System.clearProperty("streammq.security.allowUnrestrictedSerializer");
        try {
            assertThatThrownBy(() -> new FurySerializer<MyData>(false))
                    .isInstanceOf(SecurityException.class)
                    .hasMessageContaining("streammq.security.allowUnrestrictedSerializer");
        } finally {
            if (previous != null) {
                System.setProperty("streammq.security.allowUnrestrictedSerializer", previous);
            }
        }
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
    @DisplayName("name 返回 fury")
    void name() {
        assertThat(serializer.name()).isEqualTo("fury");
    }
}
