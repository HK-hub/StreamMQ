package io.github.streammq.core.annotation;

import io.github.streammq.core.enums.AcknowledgeMode;
import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.enums.MessageModel;
import io.github.streammq.core.spi.MessageSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 注解默认值测试，覆盖 @StreamMqConsumer / @StreamMqOrderlyConsumer /
 * @StreamMqTransactionConsumer / @EnableStreamMq 的默认值与元注解。
 */
@DisplayName("StreamMQ 注解默认值测试")
class AnnotationTest {

    @StreamMQConsumer(topic = "t", consumerGroup = "g")
    static class ListenerSample {
    }

    @StreamMQOrderlyConsumer(topic = "t", consumerGroup = "g")
    static class OrderlyListenerSample {
    }

    @StreamMQTransactionConsumer(transactionGroup = "tg")
    static class TxListenerSample {
    }

    @EnableStreamMQ
    static class EnableSample {
    }

    private static Object defaultValue(Annotation annotation, String method) throws Exception {
        Method m = annotation.annotationType().getDeclaredMethod(method);
        return m.getDefaultValue();
    }

    @Nested
    @DisplayName("@StreamMqConsumer 默认值")
    class StreamMQConsumerDefaults {

        @Test
        @DisplayName("consumeMode 默认 CLUSTERING")
        void consumeModeDefault() throws Exception {
            StreamMQConsumer ann = ListenerSample.class.getAnnotation(StreamMQConsumer.class);
            assertThat(ann.consumeMode()).isEqualTo(ConsumeMode.CLUSTERING);
            assertThat(defaultValue(ann, "consumeMode")).isEqualTo(ConsumeMode.CLUSTERING);
        }

        @Test
        @DisplayName("messageModel 默认 CONCURRENT")
        void messageModelDefault() throws Exception {
            StreamMQConsumer ann = ListenerSample.class.getAnnotation(StreamMQConsumer.class);
            assertThat(ann.messageModel()).isEqualTo(MessageModel.CONCURRENT);
        }

        @Test
        @DisplayName("acknowledgeMode 默认 AUTO")
        void acknowledgeModeDefault() {
            StreamMQConsumer ann = ListenerSample.class.getAnnotation(StreamMQConsumer.class);
            assertThat(ann.acknowledgeMode()).isEqualTo(AcknowledgeMode.AUTO);
        }

        @Test
        @DisplayName("consumeThreadMin 默认 1，consumeThreadMax 默认 64")
        void threadDefaults() {
            StreamMQConsumer ann = ListenerSample.class.getAnnotation(StreamMQConsumer.class);
            assertThat(ann.consumeThreadMin()).isEqualTo(1);
            assertThat(ann.consumeThreadMax()).isEqualTo(64);
        }

        @Test
        @DisplayName("maxReconsumeTimes 默认 16")
        void maxReconsumeTimesDefault() {
            StreamMQConsumer ann = ListenerSample.class.getAnnotation(StreamMQConsumer.class);
            assertThat(ann.maxReconsumeTimes()).isEqualTo(16);
        }

        @Test
        @DisplayName("consumeTimeout 默认 30000L")
        void consumeTimeoutDefault() {
            StreamMQConsumer ann = ListenerSample.class.getAnnotation(StreamMQConsumer.class);
            assertThat(ann.consumeTimeout()).isEqualTo(30000L);
        }

        @Test
        @DisplayName("selectorExpression 默认 *")
        void selectorExpressionDefault() {
            StreamMQConsumer ann = ListenerSample.class.getAnnotation(StreamMQConsumer.class);
            assertThat(ann.selectorExpression()).isEqualTo("*");
        }

        @Test
        @DisplayName("serializer 默认 MessageSerializer.class")
        void serializerDefault() {
            StreamMQConsumer ann = ListenerSample.class.getAnnotation(StreamMQConsumer.class);
            assertThat(ann.serializer()).isEqualTo(MessageSerializer.class);
        }

        @Test
        @DisplayName("namespace 默认空字符串，enable 默认 true")
        void namespaceAndEnableDefault() {
            StreamMQConsumer ann = ListenerSample.class.getAnnotation(StreamMQConsumer.class);
            assertThat(ann.namespace()).isEmpty();
            assertThat(ann.enable()).isTrue();
        }

        @Test
        @DisplayName("topic 与 consumerGroup 必填值正确读取")
        void requiredValues() {
            StreamMQConsumer ann = ListenerSample.class.getAnnotation(StreamMQConsumer.class);
            assertThat(ann.topic()).isEqualTo("t");
            assertThat(ann.consumerGroup()).isEqualTo("g");
        }
    }

    @Nested
    @DisplayName("@StreamMqOrderlyConsumer 默认值")
    class StreamMessageOrderlyConsumerDefaults {

        @Test
        @DisplayName("selectorExpression 默认 *")
        void selectorExpressionDefault() {
            StreamMQOrderlyConsumer ann = OrderlyListenerSample.class.getAnnotation(StreamMQOrderlyConsumer.class);
            assertThat(ann.selectorExpression()).isEqualTo("*");
        }

        @Test
        @DisplayName("serializer 默认 MessageSerializer.class")
        void serializerDefault() {
            StreamMQOrderlyConsumer ann = OrderlyListenerSample.class.getAnnotation(StreamMQOrderlyConsumer.class);
            assertThat(ann.serializer()).isEqualTo(MessageSerializer.class);
        }

        @Test
        @DisplayName("consumeMode 默认 CLUSTERING")
        void consumeModeDefault() {
            StreamMQOrderlyConsumer ann = OrderlyListenerSample.class.getAnnotation(StreamMQOrderlyConsumer.class);
            assertThat(ann.consumeMode()).isEqualTo(ConsumeMode.CLUSTERING);
        }

        @Test
        @DisplayName("acknowledgeMode 默认 AUTO")
        void acknowledgeModeDefault() {
            StreamMQOrderlyConsumer ann = OrderlyListenerSample.class.getAnnotation(StreamMQOrderlyConsumer.class);
            assertThat(ann.acknowledgeMode()).isEqualTo(AcknowledgeMode.AUTO);
        }

        @Test
        @DisplayName("consumeThreadMin/Max 默认均为 1")
        void threadDefaults() {
            StreamMQOrderlyConsumer ann = OrderlyListenerSample.class.getAnnotation(StreamMQOrderlyConsumer.class);
            assertThat(ann.consumeThreadMin()).isEqualTo(1);
            assertThat(ann.consumeThreadMax()).isEqualTo(1);
        }

        @Test
        @DisplayName("maxReconsumeTimes 默认 Integer.MAX_VALUE")
        void maxReconsumeTimesDefault() {
            StreamMQOrderlyConsumer ann = OrderlyListenerSample.class.getAnnotation(StreamMQOrderlyConsumer.class);
            assertThat(ann.maxReconsumeTimes()).isEqualTo(Integer.MAX_VALUE);
        }

        @Test
        @DisplayName("consumeTimeout 默认 30000L")
        void consumeTimeoutDefault() {
            StreamMQOrderlyConsumer ann = OrderlyListenerSample.class.getAnnotation(StreamMQOrderlyConsumer.class);
            assertThat(ann.consumeTimeout()).isEqualTo(30000L);
        }

        @Test
        @DisplayName("shardCount 默认 4")
        void shardCountDefault() {
            StreamMQOrderlyConsumer ann = OrderlyListenerSample.class.getAnnotation(StreamMQOrderlyConsumer.class);
            assertThat(ann.shardCount()).isEqualTo(4);
        }

        @Test
        @DisplayName("namespace 默认空，enable 默认 true")
        void namespaceAndEnableDefault() {
            StreamMQOrderlyConsumer ann = OrderlyListenerSample.class.getAnnotation(StreamMQOrderlyConsumer.class);
            assertThat(ann.namespace()).isEmpty();
            assertThat(ann.enable()).isTrue();
        }
    }

    @Nested
    @DisplayName("@StreamMqTransactionConsumer 默认值")
    class StreamMQTransactionConsumerDefaults {

        @Test
        @DisplayName("transactionGroup 必填值正确读取")
        void transactionGroupRequired() {
            StreamMQTransactionConsumer ann = TxListenerSample.class
                .getAnnotation(StreamMQTransactionConsumer.class);
            assertThat(ann.transactionGroup()).isEqualTo("tg");
        }

        @Test
        @DisplayName("checkTimeout 默认 60000L")
        void checkTimeoutDefault() throws Exception {
            StreamMQTransactionConsumer ann = TxListenerSample.class
                .getAnnotation(StreamMQTransactionConsumer.class);
            assertThat(ann.checkTimeout()).isEqualTo(60000L);
            assertThat(defaultValue(ann, "checkTimeout")).isEqualTo(60000L);
        }

        @Test
        @DisplayName("namespace 默认空字符串")
        void namespaceDefault() {
            StreamMQTransactionConsumer ann = TxListenerSample.class
                .getAnnotation(StreamMQTransactionConsumer.class);
            assertThat(ann.namespace()).isEmpty();
        }
    }

    @Nested
    @DisplayName("@EnableStreamMq 默认值")
    class EnableStreamMQDefaults {

        @Test
        @DisplayName("mode 默认 STANDARD")
        void modeDefault() throws Exception {
            EnableStreamMQ ann = EnableSample.class.getAnnotation(EnableStreamMQ.class);
            assertThat(ann.mode()).isEqualTo("STANDARD");
            assertThat(defaultValue(ann, "mode")).isEqualTo("STANDARD");
        }

        @Test
        @DisplayName("scanBasePackages 默认空数组")
        void scanBasePackagesDefault() {
            EnableStreamMQ ann = EnableSample.class.getAnnotation(EnableStreamMQ.class);
            assertThat(ann.scanBasePackages()).isEmpty();
        }
    }
}
