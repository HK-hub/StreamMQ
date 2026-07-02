package io.github.streammq.core.util;

import io.github.streammq.core.consumer.*;
import io.github.streammq.core.enums.Action;
import io.github.streammq.core.message.Message;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link BodyTypeResolver} 单元测试。
 *
 * <p>验证从 Listener 实现类的泛型声明中解析 body 类型 T 的能力，覆盖：
 * <ul>
 *   <li>三种 Listener 接口（{@link StreamMessageConcurrentlyConsumer} / {@link StreamMessageManualAckConsumer} / {@link StreamMessageOrderlyConsumer}）</li>
 *   <li>直接实现 + 父类实现 + 嵌套泛型</li>
 *   <li>无泛型信息（裸接口实现）的回退行为</li>
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("BodyTypeResolver")
class BodyTypeResolverTest {

    // ===================== 测试用 Listener 类定义 =====================

    /** 直接实现 StreamMqConsumer&lt;String&gt; */
    static class StringListener implements StreamMessageConcurrentlyConsumer<String> {
        @Override
        public Action onMessage(Message<String> message, ConsumeConcurrentlyContext context) {
            return Action.SUCCESS;
        }
    }

    /** 直接实现 StreamMqConsumer&lt;Integer&gt; */
    static class IntegerListener implements StreamMessageConcurrentlyConsumer<Integer> {
        @Override
        public Action onMessage(Message<Integer> message, ConsumeConcurrentlyContext context) {
            return Action.SUCCESS;
        }
    }

    /** 直接实现 StreamMqAckConsumer&lt;Long&gt; */
    static class LongManualAckListener implements StreamMessageManualAckConsumer<Long> {
        @Override
        public void onMessage(Message<Long> message, ConsumeConcurrentlyContext context) {
        }
    }

    /** 直接实现 StreamMqOrderlyConsumer&lt;Map&gt; */
    static class MapOrderlyListener implements StreamMessageOrderlyConsumer<Map> {
        @Override
        public Action onMessage(Message<Map> message, ConsumeOrderlyContext context) {
            return Action.SUCCESS;
        }
    }

    /** 自定义 POJO */
    static class OrderDto {
        private String orderId;
        private long amount;
    }

    /** 实现 StreamMqConsumer&lt;OrderDto&gt;（自定义 POJO） */
    static class OrderDtoListener implements StreamMessageConcurrentlyConsumer<OrderDto> {
        @Override
        public Action onMessage(Message<OrderDto> message, ConsumeConcurrentlyContext context) {
            return Action.SUCCESS;
        }
    }

    /** 嵌套泛型：StreamMqConsumer&lt;List&lt;OrderDto&gt;&gt; → 应解析为 List.class */
    static class OrderListListener implements StreamMessageConcurrentlyConsumer<List<OrderDto>> {
        @Override
        public Action onMessage(Message<List<OrderDto>> message, ConsumeConcurrentlyContext context) {
            return Action.SUCCESS;
        }
    }

    /** 父类实现 StreamMqConsumer&lt;String&gt;，子类继承 */
    static class ParentStringListener implements StreamMessageConcurrentlyConsumer<String> {
        @Override
        public Action onMessage(Message<String> message, ConsumeConcurrentlyContext context) {
            return Action.SUCCESS;
        }
    }

    static class ChildOfStringListener extends ParentStringListener {
    }

    /** 裸泛型 T（未指定具体类型）→ 应返回 null */
    static class GenericListener<T> implements StreamMessageConcurrentlyConsumer<T> {
        @Override
        public Action onMessage(Message<T> message, ConsumeConcurrentlyContext context) {
            return Action.SUCCESS;
        }
    }

    /** 实现 Listener 但不指定泛型（raw type）→ 应返回 null */
    @SuppressWarnings("rawtypes")
    static class RawListener implements StreamMessageConcurrentlyConsumer {
        @Override
        public Action onMessage(Message message, ConsumeConcurrentlyContext context) {
            return Action.SUCCESS;
        }
    }

    /** 不实现任何 StreamMQ Listener 接口 */
    static class NotAListener {
    }

    // ===================== 测试用例 =====================

    @Nested
    @DisplayName("直接实现接口")
    class DirectImplementation {

        @Test
        @DisplayName("StreamMqConsumer<String> → String.class")
        void resolveStringListener() {
            Class<?> bodyType = BodyTypeResolver.resolve(new StringListener());
            assertThat(bodyType).isEqualTo(String.class);
        }

        @Test
        @DisplayName("StreamMqConsumer<Integer> → Integer.class")
        void resolveIntegerListener() {
            Class<?> bodyType = BodyTypeResolver.resolve(new IntegerListener());
            assertThat(bodyType).isEqualTo(Integer.class);
        }

        @Test
        @DisplayName("StreamMqAckConsumer<Long> → Long.class")
        void resolveLongAckListener() {
            Class<?> bodyType = BodyTypeResolver.resolve(new LongManualAckListener());
            assertThat(bodyType).isEqualTo(Long.class);
        }

        @Test
        @DisplayName("StreamMqOrderlyConsumer<Map> → Map.class")
        void resolveMapOrderlyListener() {
            Class<?> bodyType = BodyTypeResolver.resolve(new MapOrderlyListener());
            assertThat(bodyType).isEqualTo(Map.class);
        }

        @Test
        @DisplayName("StreamMqConsumer<OrderDto> → OrderDto.class（自定义 POJO）")
        void resolveCustomPojoListener() {
            Class<?> bodyType = BodyTypeResolver.resolve(new OrderDtoListener());
            assertThat(bodyType).isEqualTo(OrderDto.class);
        }
    }

    @Nested
    @DisplayName("复杂场景")
    class ComplexScenarios {

        @Test
        @DisplayName("嵌套泛型 List<OrderDto> → List.class（取原始类型）")
        void resolveNestedGeneric() {
            Class<?> bodyType = BodyTypeResolver.resolve(new OrderListListener());
            assertThat(bodyType).isEqualTo(List.class);
        }

        @Test
        @DisplayName("父类实现 StreamMqConsumer<String>，子类继承 → String.class")
        void resolveInheritedFromParent() {
            Class<?> bodyType = BodyTypeResolver.resolve(new ChildOfStringListener());
            assertThat(bodyType).isEqualTo(String.class);
        }
    }

    @Nested
    @DisplayName("回退行为")
    class FallbackBehavior {

        @Test
        @DisplayName("裸泛型 T（未指定具体类型）→ null")
        void resolveBareTypeVariable() {
            Class<?> bodyType = BodyTypeResolver.resolve(new GenericListener<>());
            assertThat(bodyType).isNull();
        }

        @Test
        @DisplayName("raw type（不指定泛型）→ null")
        void resolveRawType() {
            Class<?> bodyType = BodyTypeResolver.resolve(new RawListener());
            assertThat(bodyType).isNull();
        }

        @Test
        @DisplayName("非 Listener 实例 → null")
        void resolveNonListener() {
            Class<?> bodyType = BodyTypeResolver.resolve(new NotAListener());
            assertThat(bodyType).isNull();
        }

        @Test
        @DisplayName("null 输入 → null")
        void resolveNull() {
            Class<?> bodyType = BodyTypeResolver.resolve(null);
            assertThat(bodyType).isNull();
        }
    }

    @Test
    @DisplayName("跨平台场景模拟：Go 发送 JSON string，consumer 声明 StreamMqConsumer<String>")
    void crossPlatformScenario() {
        // 模拟消费者声明 StreamMqConsumer<String>
        // 期望 BodyTypeResolver 解析出 String.class，作为反序列化目标类型
        StreamMessageConcurrentlyConsumer<String> goInteropListener = new StringListener();
        Class<?> bodyType = BodyTypeResolver.resolve(goInteropListener);

        assertThat(bodyType)
            .as("跨平台场景：consumer 声明 StreamMqConsumer<String> 时应解析出 String.class")
            .isEqualTo(String.class);
    }
}
