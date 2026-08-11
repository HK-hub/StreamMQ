package io.github.streammq.core.annotation;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.streammq.core.enums.ConsumeMode;
import io.github.streammq.core.enums.MessageModel;
import io.github.streammq.core.serializer.MessageSerializer;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** 注解默认值测试，覆盖 @StreamMQConsumer / @StreamMQTransactionConsumer / @EnableStreamMQ 的默认值与元注解。 */
@DisplayName("StreamMQ 注解默认值测试")
class AnnotationTest {

    @StreamMQConsumer(topic = "t", consumerGroup = "g")
    static class ListenerSample {}

    @StreamMQConsumer(topic = "t", consumerGroup = "g", messageModel = MessageModel.ORDERLY)
    static class OrderlyListenerSample {}

    @StreamMQConsumer(topic = "dlq-topic", consumerGroup = "g")
    static class DlqListenerSample {}

    @StreamMQTransactionConsumer(transactionGroup = "tg")
    static class TxListenerSample {}

    @EnableStreamMQ
    static class EnableSample {}

    private static Object defaultValue(Annotation annotation, String method) throws Exception {
        Method m = annotation.annotationType().getDeclaredMethod(method);
        return m.getDefaultValue();
    }

    @Nested
    @DisplayName("@StreamMQConsumer 默认值")
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

        @Test
        @DisplayName("shardCount 默认 4")
        void shardCountDefault() {
            StreamMQConsumer ann = ListenerSample.class.getAnnotation(StreamMQConsumer.class);
            assertThat(ann.shardCount()).isEqualTo(4);
        }

        @Test
        @DisplayName("consumerName 默认空字符串")
        void consumerNameDefault() {
            StreamMQConsumer ann = ListenerSample.class.getAnnotation(StreamMQConsumer.class);
            assertThat(ann.consumerName()).isEmpty();
        }
    }

    @Nested
    @DisplayName("@StreamMQConsumer 顺序消费配置")
    class StreamMQConsumerOrderlyAndDlq {

        @Test
        @DisplayName("messageModel=ORDERLY 正确读取")
        void orderlyMessageModel() {
            StreamMQConsumer ann =
                    OrderlyListenerSample.class.getAnnotation(StreamMQConsumer.class);
            assertThat(ann.messageModel()).isEqualTo(MessageModel.ORDERLY);
        }
    }

    @Nested
    @DisplayName("@StreamMQTransactionConsumer 默认值")
    class StreamMQTransactionConsumerDefaults {

        @Test
        @DisplayName("transactionGroup 必填值正确读取")
        void transactionGroupRequired() {
            StreamMQTransactionConsumer ann =
                    TxListenerSample.class.getAnnotation(StreamMQTransactionConsumer.class);
            assertThat(ann.transactionGroup()).isEqualTo("tg");
        }

        @Test
        @DisplayName("checkTimeout 默认 60000L")
        void checkTimeoutDefault() throws Exception {
            StreamMQTransactionConsumer ann =
                    TxListenerSample.class.getAnnotation(StreamMQTransactionConsumer.class);
            assertThat(ann.checkTimeout()).isEqualTo(60000L);
            assertThat(defaultValue(ann, "checkTimeout")).isEqualTo(60000L);
        }

        @Test
        @DisplayName("namespace 默认空字符串")
        void namespaceDefault() {
            StreamMQTransactionConsumer ann =
                    TxListenerSample.class.getAnnotation(StreamMQTransactionConsumer.class);
            assertThat(ann.namespace()).isEmpty();
        }
    }

    @Nested
    @DisplayName("@EnableStreamMQ 默认值")
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
