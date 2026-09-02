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
 * {@link FurySerializer} 单元测试，覆盖序列化/反序列化往返、null 处理、 空 byte[] 处理、默认宽松/强制白名单两种模式与 name 方法。
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

    /** 宽松模式实例（requireClassRegistration=false），任意 POJO 开箱即用 */
    private final FurySerializer<MyData> serializer = new FurySerializer<>(false);

    @org.junit.jupiter.api.BeforeAll
    static void allowUnrestricted() {
        // 显式确认属性：抑制宽松模式构造的 WARN 提醒，避免测试日志噪音
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
    @DisplayName("默认无参构造为宽松模式：任意 POJO 开箱即用（requireClassRegistration=false）")
    void defaultConstructorAllowsAnyPojo() {
        FurySerializer<MyData> open = new FurySerializer<>();
        assertThat(open.isRequireClassRegistration()).isFalse();
        MyData data = new MyData("Alice", 30, 1719800000L);
        byte[] bytes = open.serialize(data, MyData.class);
        assertThat(bytes).isNotEmpty();
        assertThat(open.deserialize(bytes, MyData.class)).isEqualTo(data);
    }

    @Test
    @DisplayName("显式 true（强制类注册白名单）：未注册类序列化被拒绝")
    void secureModeRejectsUnregisteredClass() {
        FurySerializer<MyData> secure = new FurySerializer<>(true);
        assertThat(secure.isRequireClassRegistration()).isTrue();
        MyData data = new MyData("Alice", 30, 1719800000L);
        assertThatThrownBy(() -> secure.serialize(data, MyData.class))
                .isInstanceOf(Exception.class)
                .hasMessageContaining("not registered");
    }

    @Test
    @DisplayName("secure serializer exposes public registration API")
    void registerAllowsPojoRoundTrip() {
        FurySerializer<MyData> secure = new FurySerializer<>(true);
        secure.register(MyData.class);
        MyData data = new MyData("registered", 7, 42L);
        byte[] bytes = secure.serialize(data, MyData.class);
        assertThat(secure.deserialize(bytes, MyData.class)).isEqualTo(data);
    }

    @Test
    @DisplayName("强制类注册白名单实例可直接处理 String body：框架默认 body 类型无需预注册")
    void stringBodyWorksWithoutRegistration() {
        // 回归保护：Fury 是框架默认序列化器，samples / starter E2E 全部使用 String body。
        // 即使开启类注册白名单，java.lang.String 等内置类型也应开箱可用，无需显式注册。
        FurySerializer<String> secure = new FurySerializer<>(true);
        byte[] bytes = secure.serialize("streammq-default", String.class);
        assertThat(bytes).isNotEmpty();
        assertThat(secure.deserialize(bytes, String.class)).isEqualTo("streammq-default");
    }

    @Test
    @DisplayName("constructor registers initial message types")
    void constructorRegistersTypes() {
        FurySerializer<MyData> secure = new FurySerializer<>(MyData.class);
        assertThat(secure.isRequireClassRegistration()).isTrue();
        MyData data = new MyData("initial", 1, 2L);
        assertThat(secure.deserialize(secure.serialize(data, MyData.class), MyData.class))
                .isEqualTo(data);
    }

    @Test
    @DisplayName("宽松模式无需 -D 系统属性即可工作（原 SecurityException 门控已移除为 WARN）")
    void unrestrictedModeWorksWithoutSystemProperty() {
        String previous = System.getProperty("streammq.security.allowUnrestrictedSerializer");
        System.clearProperty("streammq.security.allowUnrestrictedSerializer");
        try {
            FurySerializer<MyData> open = new FurySerializer<>(false);
            assertThat(open.isRequireClassRegistration()).isFalse();
            MyData data = new MyData("gate-free", 1, 1L);
            byte[] bytes = open.serialize(data, MyData.class);
            assertThat(open.deserialize(bytes, MyData.class)).isEqualTo(data);
        } finally {
            if (previous == null) {
                System.clearProperty("streammq.security.allowUnrestrictedSerializer");
            } else {
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
