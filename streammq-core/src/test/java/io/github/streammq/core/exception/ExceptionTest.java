package io.github.streammq.core.exception;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** 异常体系单元测试，覆盖各类异常构造器、继承关系与特有字段。 */
@DisplayName("StreamMQ 异常体系测试")
class ExceptionTest {

    @Nested
    @DisplayName("StreamMQException 基类")
    class StreamMQExceptionTest {

        @Test
        @DisplayName("带 message 构造")
        void withMessage() {
            StreamMQException ex = new StreamMQException("boom");
            assertThat(ex.getMessage()).isEqualTo("boom");
            assertThat(ex.getCause()).isNull();
        }

        @Test
        @DisplayName("带 message + cause 构造")
        void withMessageAndCause() {
            Throwable cause = new RuntimeException("root");
            StreamMQException ex = new StreamMQException("boom", cause);
            assertThat(ex.getMessage()).isEqualTo("boom");
            assertThat(ex.getCause()).isSameAs(cause);
        }

        @Test
        @DisplayName("StreamMQException 继承 RuntimeException")
        void extendsRuntimeException() {
            assertThat(RuntimeException.class).isAssignableFrom(StreamMQException.class);
        }
    }

    @Nested
    @DisplayName("StreamMQClientException")
    class StreamMQClientExceptionTest {

        @Test
        @DisplayName("继承 StreamMQException")
        void extendsStreamMQException() {
            assertThat(StreamMQException.class).isAssignableFrom(StreamMQClientException.class);
        }

        @Test
        @DisplayName("带 message 构造")
        void withMessage() {
            StreamMQClientException ex = new StreamMQClientException("bad config");
            assertThat(ex.getMessage()).isEqualTo("bad config");
            assertThat(ex).isInstanceOf(StreamMQException.class);
        }

        @Test
        @DisplayName("带 message + cause 构造")
        void withMessageAndCause() {
            Throwable cause = new IllegalStateException("root");
            StreamMQClientException ex = new StreamMQClientException("bad config", cause);
            assertThat(ex.getMessage()).isEqualTo("bad config");
            assertThat(ex.getCause()).isSameAs(cause);
        }
    }

    @Nested
    @DisplayName("StreamMQBrokerException")
    class StreamMQBrokerExceptionTest {

        @Test
        @DisplayName("继承 StreamMQException")
        void extendsStreamMQException() {
            assertThat(StreamMQException.class).isAssignableFrom(StreamMQBrokerException.class);
        }

        @Test
        @DisplayName("单参构造：errorCode 与 cause 均为 null")
        void singleArg() {
            StreamMQBrokerException ex = new StreamMQBrokerException("redis error");
            assertThat(ex.getMessage()).isEqualTo("redis error");
            assertThat(ex.getErrorCode()).isNull();
            assertThat(ex.getCause()).isNull();
        }

        @Test
        @DisplayName("带 message + errorCode 构造")
        void withErrorCode() {
            StreamMQBrokerException ex = new StreamMQBrokerException("redis error", "OOM");
            assertThat(ex.getMessage()).isEqualTo("redis error");
            assertThat(ex.getErrorCode()).isEqualTo("OOM");
            assertThat(ex.getCause()).isNull();
        }

        @Test
        @DisplayName("全参构造：message + errorCode + cause")
        void fullConstructor() {
            Throwable cause = new RuntimeException("conn reset");
            StreamMQBrokerException ex =
                    new StreamMQBrokerException("redis error", "LOADING", cause);
            assertThat(ex.getMessage()).isEqualTo("redis error");
            assertThat(ex.getErrorCode()).isEqualTo("LOADING");
            assertThat(ex.getCause()).isSameAs(cause);
        }
    }

    @Nested
    @DisplayName("SerializationException")
    class SerializationExceptionTest {

        @Test
        @DisplayName("继承 StreamMQException")
        void extendsStreamMQException() {
            assertThat(StreamMQException.class).isAssignableFrom(SerializationException.class);
        }

        @Test
        @DisplayName("带 message 构造")
        void withMessage() {
            SerializationException ex = new SerializationException("serialize fail");
            assertThat(ex.getMessage()).isEqualTo("serialize fail");
        }

        @Test
        @DisplayName("包装底层异常（message + cause）")
        void wrapsCause() {
            Throwable cause = new RuntimeException("bad json");
            SerializationException ex = new SerializationException("serialize fail", cause);
            assertThat(ex.getMessage()).isEqualTo("serialize fail");
            assertThat(ex.getCause()).isSameAs(cause);
        }
    }

    @Nested
    @DisplayName("ProducerTimeoutException")
    class ProducerTimeoutExceptionTest {

        @Test
        @DisplayName("继承 StreamMQException")
        void extendsStreamMQException() {
            assertThat(StreamMQException.class).isAssignableFrom(ProducerTimeoutException.class);
        }

        @Test
        @DisplayName("构造器：message + topic + timeoutMillis")
        void threeArg() {
            ProducerTimeoutException ex =
                    new ProducerTimeoutException("timeout", "order-topic", 3000L);
            assertThat(ex.getMessage()).isEqualTo("timeout");
            assertThat(ex.getTopic()).isEqualTo("order-topic");
            assertThat(ex.getTimeoutMillis()).isEqualTo(3000L);
            assertThat(ex.getCause()).isNull();
        }

        @Test
        @DisplayName("构造器：message + topic + timeoutMillis + cause")
        void fourArg() {
            Throwable cause = new RuntimeException("net");
            ProducerTimeoutException ex =
                    new ProducerTimeoutException("timeout", "topic", 5000L, cause);
            assertThat(ex.getTopic()).isEqualTo("topic");
            assertThat(ex.getTimeoutMillis()).isEqualTo(5000L);
            assertThat(ex.getCause()).isSameAs(cause);
        }
    }

    @Nested
    @DisplayName("ConsumerInterruptedException")
    class ConsumerInterruptedExceptionTest {

        @Test
        @DisplayName("继承 StreamMQException")
        void extendsStreamMQException() {
            assertThat(StreamMQException.class)
                    .isAssignableFrom(ConsumerInterruptedException.class);
        }

        @Test
        @DisplayName("构造器：message + topic + consumerGroup")
        void threeArg() {
            ConsumerInterruptedException ex =
                    new ConsumerInterruptedException("interrupted", "topic", "group-1");
            assertThat(ex.getMessage()).isEqualTo("interrupted");
            assertThat(ex.getTopic()).isEqualTo("topic");
            assertThat(ex.getConsumerGroup()).isEqualTo("group-1");
            assertThat(ex.getCause()).isNull();
        }

        @Test
        @DisplayName("构造器：message + topic + consumerGroup + cause")
        void fourArg() {
            Throwable cause = new InterruptedException("shutdown");
            ConsumerInterruptedException ex =
                    new ConsumerInterruptedException("interrupted", "t", "g", cause);
            assertThat(ex.getConsumerGroup()).isEqualTo("g");
            assertThat(ex.getCause()).isSameAs(cause);
        }
    }

    @Nested
    @DisplayName("TransactionException")
    class TransactionExceptionTest {

        @Test
        @DisplayName("继承 StreamMQException")
        void extendsStreamMQException() {
            assertThat(StreamMQException.class).isAssignableFrom(TransactionException.class);
        }

        @Test
        @DisplayName("构造器：message + transactionId + transactionGroup")
        void threeArg() {
            TransactionException ex = new TransactionException("tx fail", "tx-001", "tx-group");
            assertThat(ex.getMessage()).isEqualTo("tx fail");
            assertThat(ex.getTransactionId()).isEqualTo("tx-001");
            assertThat(ex.getTransactionGroup()).isEqualTo("tx-group");
            assertThat(ex.getCause()).isNull();
        }

        @Test
        @DisplayName("构造器：message + transactionId + transactionGroup + cause")
        void fourArg() {
            Throwable cause = new RuntimeException("db");
            TransactionException ex = new TransactionException("tx fail", "tx-1", "g", cause);
            assertThat(ex.getTransactionId()).isEqualTo("tx-1");
            assertThat(ex.getTransactionGroup()).isEqualTo("g");
            assertThat(ex.getCause()).isSameAs(cause);
        }
    }
}
