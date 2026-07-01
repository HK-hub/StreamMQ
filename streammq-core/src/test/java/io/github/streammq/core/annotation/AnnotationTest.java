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
 * 注解默认值测试，覆盖 @StreamMqListener / @StreamMqOrderlyListener / @StreamMqProducer /
 * @StreamMqTransactionListener / @EnableStreamMq 的默认值与元注解。
 */
@DisplayName("StreamMQ 注解默认值测试")
class AnnotationTest {

    @StreamMqListener(topic = "t", consumerGroup = "g")
    static class ListenerSample {
    }

    @StreamMqOrderlyListener(topic = "t", consumerGroup = "g")
    static class OrderlyListenerSample {
    }

    @StreamMqProducer(group = "g")
    transient Object producerField;

    @StreamMqTransactionListener(transactionGroup = "tg")
    static class TxListenerSample {
    }

    @EnableStreamMq
    static class EnableSample {
    }

    private static Object defaultValue(Annotation annotation, String method) throws Exception {
        Method m = annotation.annotationType().getDeclaredMethod(method);
        return m.getDefaultValue();
    }

    @Nested
    @DisplayName("@StreamMqListener 默认值")
    class StreamMqListenerDefaults {

        @Test
        @DisplayName("consumeMode 默认 CLUSTERING")
        void consumeModeDefault() throws Exception {
            StreamMqListener ann = ListenerSample.class.getAnnotation(StreamMqListener.class);
            assertThat(ann.consumeMode()).isEqualTo(ConsumeMode.CLUSTERING);
            assertThat(defaultValue(ann, "consumeMode")).isEqualTo(ConsumeMode.CLUSTERING);
        }

        @Test
        @DisplayName("messageModel 默认 CONCURRENT")
        void messageModelDefault() throws Exception {
            StreamMqListener ann = ListenerSample.class.getAnnotation(StreamMqListener.class);
            assertThat(ann.messageModel()).isEqualTo(MessageModel.CONCURRENT);
        }

        @Test
        @DisplayName("acknowledgeMode 默认 AUTO")
        void acknowledgeModeDefault() {
            StreamMqListener ann = ListenerSample.class.getAnnotation(StreamMqListener.class);
            assertThat(ann.acknowledgeMode()).isEqualTo(AcknowledgeMode.AUTO);
        }

        @Test
        @DisplayName("consumeThreadMin 默认 1，consumeThreadMax 默认 64")
        void threadDefaults() {
            StreamMqListener ann = ListenerSample.class.getAnnotation(StreamMqListener.class);
            assertThat(ann.consumeThreadMin()).isEqualTo(1);
            assertThat(ann.consumeThreadMax()).isEqualTo(64);
        }

        @Test
        @DisplayName("maxReconsumeTimes 默认 16")
        void maxReconsumeTimesDefault() {
            StreamMqListener ann = ListenerSample.class.getAnnotation(StreamMqListener.class);
            assertThat(ann.maxReconsumeTimes()).isEqualTo(16);
        }

        @Test
        @DisplayName("consumeTimeout 默认 30000L")
        void consumeTimeoutDefault() {
            StreamMqListener ann = ListenerSample.class.getAnnotation(StreamMqListener.class);
            assertThat(ann.consumeTimeout()).isEqualTo(30000L);
        }

        @Test
        @DisplayName("selectorExpression 默认 *")
        void selectorExpressionDefault() {
            StreamMqListener ann = ListenerSample.class.getAnnotation(StreamMqListener.class);
            assertThat(ann.selectorExpression()).isEqualTo("*");
        }

        @Test
        @DisplayName("serializer 默认 MessageSerializer.class")
        void serializerDefault() {
            StreamMqListener ann = ListenerSample.class.getAnnotation(StreamMqListener.class);
            assertThat(ann.serializer()).isEqualTo(MessageSerializer.class);
        }

        @Test
        @DisplayName("namespace 默认空字符串，enable 默认 true")
        void namespaceAndEnableDefault() {
            StreamMqListener ann = ListenerSample.class.getAnnotation(StreamMqListener.class);
            assertThat(ann.namespace()).isEmpty();
            assertThat(ann.enable()).isTrue();
        }

        @Test
        @DisplayName("topic 与 consumerGroup 必填值正确读取")
        void requiredValues() {
            StreamMqListener ann = ListenerSample.class.getAnnotation(StreamMqListener.class);
            assertThat(ann.topic()).isEqualTo("t");
            assertThat(ann.consumerGroup()).isEqualTo("g");
        }
    }

    @Nested
    @DisplayName("@StreamMqOrderlyListener 默认值")
    class StreamMqOrderlyListenerDefaults {

        @Test
        @DisplayName("selectorExpression 默认 *")
        void selectorExpressionDefault() {
            StreamMqOrderlyListener ann = OrderlyListenerSample.class.getAnnotation(StreamMqOrderlyListener.class);
            assertThat(ann.selectorExpression()).isEqualTo("*");
        }

        @Test
        @DisplayName("serializer 默认 MessageSerializer.class")
        void serializerDefault() {
            StreamMqOrderlyListener ann = OrderlyListenerSample.class.getAnnotation(StreamMqOrderlyListener.class);
            assertThat(ann.serializer()).isEqualTo(MessageSerializer.class);
        }

        @Test
        @DisplayName("consumeMode 默认 CLUSTERING")
        void consumeModeDefault() {
            StreamMqOrderlyListener ann = OrderlyListenerSample.class.getAnnotation(StreamMqOrderlyListener.class);
            assertThat(ann.consumeMode()).isEqualTo(ConsumeMode.CLUSTERING);
        }

        @Test
        @DisplayName("acknowledgeMode 默认 AUTO")
        void acknowledgeModeDefault() {
            StreamMqOrderlyListener ann = OrderlyListenerSample.class.getAnnotation(StreamMqOrderlyListener.class);
            assertThat(ann.acknowledgeMode()).isEqualTo(AcknowledgeMode.AUTO);
        }

        @Test
        @DisplayName("consumeThreadMin/Max 默认均为 1")
        void threadDefaults() {
            StreamMqOrderlyListener ann = OrderlyListenerSample.class.getAnnotation(StreamMqOrderlyListener.class);
            assertThat(ann.consumeThreadMin()).isEqualTo(1);
            assertThat(ann.consumeThreadMax()).isEqualTo(1);
        }

        @Test
        @DisplayName("maxReconsumeTimes 默认 Integer.MAX_VALUE")
        void maxReconsumeTimesDefault() {
            StreamMqOrderlyListener ann = OrderlyListenerSample.class.getAnnotation(StreamMqOrderlyListener.class);
            assertThat(ann.maxReconsumeTimes()).isEqualTo(Integer.MAX_VALUE);
        }

        @Test
        @DisplayName("consumeTimeout 默认 30000L")
        void consumeTimeoutDefault() {
            StreamMqOrderlyListener ann = OrderlyListenerSample.class.getAnnotation(StreamMqOrderlyListener.class);
            assertThat(ann.consumeTimeout()).isEqualTo(30000L);
        }

        @Test
        @DisplayName("shardCount 默认 4")
        void shardCountDefault() {
            StreamMqOrderlyListener ann = OrderlyListenerSample.class.getAnnotation(StreamMqOrderlyListener.class);
            assertThat(ann.shardCount()).isEqualTo(4);
        }

        @Test
        @DisplayName("namespace 默认空，enable 默认 true")
        void namespaceAndEnableDefault() {
            StreamMqOrderlyListener ann = OrderlyListenerSample.class.getAnnotation(StreamMqOrderlyListener.class);
            assertThat(ann.namespace()).isEmpty();
            assertThat(ann.enable()).isTrue();
        }
    }

    @Nested
    @DisplayName("@StreamMqProducer 默认值")
    class StreamMqProducerDefaults {

        @Test
        @DisplayName("group 必填值正确读取")
        void groupRequired() throws Exception {
            StreamMqProducer ann = AnnotationTest.class
                .getDeclaredField("producerField").getAnnotation(StreamMqProducer.class);
            assertThat(ann.group()).isEqualTo("g");
        }

        @Test
        @DisplayName("namespace 默认空字符串")
        void namespaceDefault() throws Exception {
            StreamMqProducer ann = AnnotationTest.class
                .getDeclaredField("producerField").getAnnotation(StreamMqProducer.class);
            assertThat(ann.namespace()).isEmpty();
        }

        @Test
        @DisplayName("serializer 默认 MessageSerializer.class")
        void serializerDefault() throws Exception {
            StreamMqProducer ann = AnnotationTest.class
                .getDeclaredField("producerField").getAnnotation(StreamMqProducer.class);
            assertThat(ann.serializer()).isEqualTo(MessageSerializer.class);
        }

        @Test
        @DisplayName("sendMessageTimeout 默认 0L")
        void sendMessageTimeoutDefault() throws Exception {
            StreamMqProducer ann = AnnotationTest.class
                .getDeclaredField("producerField").getAnnotation(StreamMqProducer.class);
            assertThat(ann.sendMessageTimeout()).isEqualTo(0L);
        }

        @Test
        @DisplayName("retryTimes 默认 -1")
        void retryTimesDefault() throws Exception {
            StreamMqProducer ann = AnnotationTest.class
                .getDeclaredField("producerField").getAnnotation(StreamMqProducer.class);
            assertThat(ann.retryTimes()).isEqualTo(-1);
        }
    }

    @Nested
    @DisplayName("@StreamMqTransactionListener 默认值")
    class StreamMqTransactionListenerDefaults {

        @Test
        @DisplayName("transactionGroup 必填值正确读取")
        void transactionGroupRequired() {
            StreamMqTransactionListener ann = TxListenerSample.class
                .getAnnotation(StreamMqTransactionListener.class);
            assertThat(ann.transactionGroup()).isEqualTo("tg");
        }

        @Test
        @DisplayName("checkTimeout 默认 60000L")
        void checkTimeoutDefault() throws Exception {
            StreamMqTransactionListener ann = TxListenerSample.class
                .getAnnotation(StreamMqTransactionListener.class);
            assertThat(ann.checkTimeout()).isEqualTo(60000L);
            assertThat(defaultValue(ann, "checkTimeout")).isEqualTo(60000L);
        }

        @Test
        @DisplayName("namespace 默认空字符串")
        void namespaceDefault() {
            StreamMqTransactionListener ann = TxListenerSample.class
                .getAnnotation(StreamMqTransactionListener.class);
            assertThat(ann.namespace()).isEmpty();
        }
    }

    @Nested
    @DisplayName("@EnableStreamMq 默认值")
    class EnableStreamMqDefaults {

        @Test
        @DisplayName("mode 默认 STANDARD")
        void modeDefault() throws Exception {
            EnableStreamMq ann = EnableSample.class.getAnnotation(EnableStreamMq.class);
            assertThat(ann.mode()).isEqualTo("STANDARD");
            assertThat(defaultValue(ann, "mode")).isEqualTo("STANDARD");
        }

        @Test
        @DisplayName("scanBasePackages 默认空数组")
        void scanBasePackagesDefault() {
            EnableStreamMq ann = EnableSample.class.getAnnotation(EnableStreamMq.class);
            assertThat(ann.scanBasePackages()).isEmpty();
        }
    }
}
