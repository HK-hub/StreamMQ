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
import java.util.ArrayList;
import java.util.List;
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
    @DisplayName("serialize(null) 返回 null（统一 null 契约）")
    void serializeNull() {
        assertThat(serializer.serialize(null, MyData.class)).isNull();
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

    /** 两个字段引用同一 String 实例的 POJO（触发 JDK 线格式回引用 TC_REFERENCE）。 */
    public static class Twinned implements Serializable {
        private static final long serialVersionUID = 1L;

        private String left;
        private String right;

        public Twinned() {}

        public Twinned(String left, String right) {
            this.left = left;
            this.right = right;
        }

        public String getLeft() {
            return left;
        }

        public void setLeft(String left) {
            this.left = left;
        }

        public String getRight() {
            return right;
        }

        public void setRight(String right) {
            this.right = right;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Twinned twinned)) {
                return false;
            }
            return Objects.equals(left, twinned.left) && Objects.equals(right, twinned.right);
        }

        @Override
        public int hashCode() {
            return Objects.hash(left, right);
        }
    }

    @Test
    @DisplayName("含同一 String 实例 3 次的 List 往返成功（JEP 290 回引用检查不误判）")
    @SuppressWarnings("unchecked")
    void roundTripListWithSharedReferences() {
        // 回归保护：JDK 21 在处理 TC_REFERENCE（回引用）时会以 class=null、arrayLength=-1
        // 调用过滤器；旧实现对该调用返回 REJECTED，导致任何含回引用的合法载荷都抛
        // InvalidClassException: filter status: REJECTED。
        String shared = new String("shared-instance");
        List<String> list = new ArrayList<>();
        list.add(shared);
        list.add(shared);
        list.add(shared);

        JdkSerializer<ArrayList<String>> listSerializer = new JdkSerializer<>();
        @SuppressWarnings("unchecked")
        Class<ArrayList<String>> listType = (Class<ArrayList<String>>) (Class<?>) ArrayList.class;
        byte[] bytes = listSerializer.serialize((ArrayList<String>) list, listType);
        assertThat(bytes).isNotEmpty();

        ArrayList<String> restored = listSerializer.deserialize(bytes, listType);
        assertThat(restored).isEqualTo(list);
        // 回引用必须被真正还原为同一实例，否则上面的过滤器误判没有被覆盖到
        assertThat(restored.get(0)).isSameAs(restored.get(1));
        assertThat(restored.get(1)).isSameAs(restored.get(2));
    }

    @Test
    @DisplayName("两个字段同值的 POJO 往返成功（回引用不触发过滤器拒绝）")
    void roundTripPojoWithFieldBackReference() {
        String value = new String("same-value");
        Twinned data = new Twinned(value, value);
        JdkSerializer<Twinned> twinnedSerializer = new JdkSerializer<>();

        byte[] bytes = twinnedSerializer.serialize(data, Twinned.class);
        Twinned restored = twinnedSerializer.deserialize(bytes, Twinned.class);

        assertThat(restored).isEqualTo(data);
        assertThat(restored.getLeft()).isSameAs(restored.getRight());
    }

    @Test
    @DisplayName("含未放行类的载荷（即使同一实例被回引用）仍被过滤器拒绝（未放松安全）")
    @SuppressWarnings("unchecked")
    void filterStillRejectsNonWhitelistedClassWithBackReferences() {
        ForeignData shared = new ForeignData();
        List<Object> payload = new ArrayList<>();
        payload.add(shared);
        payload.add(shared);

        JdkSerializer<ArrayList<Object>> listSerializer = new JdkSerializer<>();
        @SuppressWarnings("unchecked")
        Class<ArrayList<Object>> listType = (Class<ArrayList<Object>>) (Class<?>) ArrayList.class;
        byte[] bytes = listSerializer.serialize((ArrayList<Object>) payload, listType);

        assertThatThrownBy(() -> listSerializer.deserialize(bytes, listType))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("JDK deserialize failed")
                .rootCause()
                .hasMessageContaining("REJECTED");
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
    @DisplayName("unrestricted() 关闭过滤（仅供可信环境迁移，需显式系统属性）")
    @SuppressWarnings("deprecation")
    void unrestrictedDisablesFiltering() {
        String previous = System.getProperty("streammq.security.allowUnrestrictedSerializer");
        System.setProperty("streammq.security.allowUnrestrictedSerializer", "true");
        try {
            JdkSerializer<ForeignData> open = JdkSerializer.unrestricted();
            byte[] bytes = open.serialize(new ForeignData(), ForeignData.class);
            assertThat(open.deserialize(bytes, ForeignData.class)).isNotNull();
        } finally {
            if (previous == null) {
                System.clearProperty("streammq.security.allowUnrestrictedSerializer");
            } else {
                System.setProperty("streammq.security.allowUnrestrictedSerializer", previous);
            }
        }
    }

    @Test
    @DisplayName("unrestricted() 默认抛 SecurityException（防止 foot-gun）")
    @SuppressWarnings("deprecation")
    void unrestrictedGatedBySystemProperty() {
        String previous = System.getProperty("streammq.security.allowUnrestrictedSerializer");
        System.clearProperty("streammq.security.allowUnrestrictedSerializer");
        try {
            assertThatThrownBy(JdkSerializer::unrestricted)
                    .isInstanceOf(SecurityException.class)
                    .hasMessageContaining("streammq.security.allowUnrestrictedSerializer");
        } finally {
            if (previous != null) {
                System.setProperty("streammq.security.allowUnrestrictedSerializer", previous);
            }
        }
    }
}
