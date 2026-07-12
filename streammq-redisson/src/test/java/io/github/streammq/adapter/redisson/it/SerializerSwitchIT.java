package io.github.streammq.adapter.redisson.it;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.listener.RedissonStreamListener;
import io.github.streammq.adapter.redisson.producer.RedissonStreamProducer;
import io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer;
import io.github.streammq.adapter.redisson.serializer.JdkSerializer;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.message.Message;
import io.github.streammq.core.message.MessageBuilder;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.serializer.MessageSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RStream;
import org.redisson.api.StreamMessageId;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 序列化器切换(Jackson vs JDK)端到端 Redis 联动集成测试。
 *
 * <p>验证 {@link JacksonJsonSerializer} 与 {@link JdkSerializer} 在生产-消费全链路下的
 * 正确性:发送、写入 Stream Entry、读取、还原 body。
 * 同时验证两种序列化器产生的 Stream Entry {@code body} 字段格式不同但都能正确还原,
 * 并覆盖自定义对象({@link Person})的序列化/反序列化。
 */
@DisplayName("序列化器切换集成测试")
class SerializerSwitchIT extends AbstractRedisIT {

    /**
     * 测试用自定义对象,需实现 {@link Serializable} 以支持 JDK 序列化。
     */
    public static class Person implements Serializable {
        private static final long serialVersionUID = 1L;
        private String name;
        private int age;

        public Person() {
        }

        public Person(String name, int age) {
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
            if (!(o instanceof Person person)) {
                return false;
            }
            return age == person.age && java.util.Objects.equals(name, person.name);
        }

        @Override
        public int hashCode() {
            return java.util.Objects.hash(name, age);
        }
    }

    @Test
    @DisplayName("JacksonJsonSerializer:发送 String body 后消费还原内容一致")
    void jacksonSerializer_stringBody_roundTrip() {
        String topic = "ser-jackson-str-topic";
        String group = "ser-jackson-str-group";

        // 基类已使用 JacksonJsonSerializer 构建 converter,这里直接复用
        RedissonStreamProducer producer =
            new RedissonStreamProducer(redisson, namespace, group + "-p", converter, 3000L, 0, 0);
        RedissonStreamListener consumer =
            new RedissonStreamListener(redisson, namespace, topic, group, "c1", converter);
        createConsumerGroup(topic, group);

        try {
            producer.syncSend(MessageBuilder.<String>withTopic(topic)
                .tag("jackson-tag")
                .keys("jackson-key")
                .body("jackson-body")
                .build());

            List<Message<?>> messages = consumer.pull(1);
            assertThat(messages).hasSize(1);
            Message<?> received = messages.get(0);
            assertThat(received.getBody()).isEqualTo("jackson-body");
            assertThat(received.getTag()).isEqualTo("jackson-tag");
            assertThat(received.getKeys()).isEqualTo("jackson-key");
        } finally {
            producer.close();
            consumer.close();
        }
    }

    @Test
    @DisplayName("JdkSerializer:发送 String body 后消费还原内容一致")
    void jdkSerializer_stringBody_roundTrip() {
        String topic = "ser-jdk-str-topic";
        String group = "ser-jdk-str-group";

        // 使用 JdkSerializer 构造 converter(String 实现 Serializable)
        @SuppressWarnings("unchecked")
        MessageSerializer<Object> jdkSerializer = (MessageSerializer<Object>) (MessageSerializer<?>) new JdkSerializer<String>();
        MessageConverter jdkConverter = new DefaultMessageConverter(jdkSerializer);

        RedissonStreamProducer producer =
            new RedissonStreamProducer(redisson, namespace, group + "-p", jdkConverter, 3000L, 0, 0);
        RedissonStreamListener consumer =
            new RedissonStreamListener(redisson, namespace, topic, group, "c1", jdkConverter);
        createConsumerGroup(topic, group);

        try {
            producer.syncSend(MessageBuilder.<String>withTopic(topic)
                .tag("jdk-tag")
                .keys("jdk-key")
                .body("jdk-body")
                .build());

            List<Message<?>> messages = consumer.pull(1);
            assertThat(messages).hasSize(1);
            Message<?> received = messages.get(0);
            assertThat(received.getBody()).isEqualTo("jdk-body");
            assertThat(received.getTag()).isEqualTo("jdk-tag");
            assertThat(received.getKeys()).isEqualTo("jdk-key");
        } finally {
            producer.close();
            consumer.close();
        }
    }

    @Test
    @DisplayName("两种序列化器产生的 Stream Entry body 字段格式不同但都能正确还原")
    void bothSerializers_differentBodyFormat_bothRestored() {
        String topicJackson = "ser-cmp-jackson-topic";
        String topicJdk = "ser-cmp-jdk-topic";
        String group = "ser-cmp-group";

        // Jackson 序列化器
        MessageConverter jacksonConverter = converter;
        // JDK 序列化器
        @SuppressWarnings("unchecked")
        MessageSerializer<Object> jdkSerializer = (MessageSerializer<Object>) (MessageSerializer<?>) new JdkSerializer<String>();
        MessageConverter jdkConverter = new DefaultMessageConverter(jdkSerializer);

        // 分别用两个 producer 发送相同 body
        RedissonStreamProducer jacksonProducer =
            new RedissonStreamProducer(redisson, namespace, group + "-jp", jacksonConverter, 3000L, 0, 0);
        RedissonStreamProducer jdkProducer =
            new RedissonStreamProducer(redisson, namespace, group + "-jdp", jdkConverter, 3000L, 0, 0);

        try {
            jacksonProducer.syncSend(MessageBuilder.<String>withTopic(topicJackson).body("same-body").build());
            jdkProducer.syncSend(MessageBuilder.<String>withTopic(topicJdk).body("same-body").build());

            // 读取两个 Stream 的 entry 字段,对比 body 字段格式
            RStream<String, String> jacksonStream =
                redisson.getStream(StreamMQKeys.topicStream(namespace, topicJackson));
            RStream<String, String> jdkStream =
                redisson.getStream(StreamMQKeys.topicStream(namespace, topicJdk));

            Map<StreamMessageId, Map<String, String>> jacksonRange =
                jacksonStream.range(1, StreamMessageId.MIN, StreamMessageId.MAX);
            Map<StreamMessageId, Map<String, String>> jdkRange =
                jdkStream.range(1, StreamMessageId.MIN, StreamMessageId.MAX);

            assertThat(jacksonRange).hasSize(1);
            assertThat(jdkRange).hasSize(1);

            String jacksonBody = jacksonRange.values().iterator().next().get(DefaultMessageConverter.FIELD_BODY);
            String jdkBody = jdkRange.values().iterator().next().get(DefaultMessageConverter.FIELD_BODY);

            // body 字段都非空
            assertThat(jacksonBody).isNotEmpty();
            assertThat(jdkBody).isNotEmpty();

            // 两种序列化器产生的 Base64 编码字符串不同(Jackson 为 JSON 字节,JDK 为对象序列化字节)
            assertThat(jacksonBody).isNotEqualTo(jdkBody);

            // 分别用对应序列化器消费,都能正确还原 body
            createConsumerGroup(topicJackson, group + "-jc");
            createConsumerGroup(topicJdk, group + "-jc");

            RedissonStreamListener jacksonConsumer =
                new RedissonStreamListener(redisson, namespace, topicJackson, group + "-jc", "c1", jacksonConverter);
            RedissonStreamListener jdkConsumer =
                new RedissonStreamListener(redisson, namespace, topicJdk, group + "-jc", "c1", jdkConverter);
            try {
                List<Message<?>> jacksonMessages = jacksonConsumer.pull(1);
                List<Message<?>> jdkMessages = jdkConsumer.pull(1);

                assertThat(jacksonMessages).hasSize(1);
                assertThat(jdkMessages).hasSize(1);
                assertThat(jacksonMessages.get(0).getBody()).isEqualTo("same-body");
                assertThat(jdkMessages.get(0).getBody()).isEqualTo("same-body");
            } finally {
                jacksonConsumer.close();
                jdkConsumer.close();
            }
        } finally {
            jacksonProducer.close();
            jdkProducer.close();
        }
    }

    @Test
    @DisplayName("JacksonJsonSerializer:自定义对象 Person 的序列化/反序列化往返一致")
    void jacksonSerializer_personBody_roundTrip() {
        String topic = "ser-jackson-person-topic";
        String group = "ser-jackson-person-group";

        RedissonStreamProducer producer =
            new RedissonStreamProducer(redisson, namespace, group + "-p", converter, 3000L, 0, 0);
        RedissonStreamListener consumer =
            new RedissonStreamListener(redisson, namespace, topic, group, "c1", converter);
        createConsumerGroup(topic, group);

        try {
            Person person = new Person("alice", 30);
            producer.syncSend(MessageBuilder.<Person>withTopic(topic).body(person).build());

            List<Message<?>> messages = consumer.pull(1);
            assertThat(messages).hasSize(1);
            Message<?> received = messages.get(0);
            assertThat(received.getBody()).isInstanceOf(Person.class);
            Person receivedPerson = (Person) received.getBody();
            assertThat(receivedPerson.getName()).isEqualTo("alice");
            assertThat(receivedPerson.getAge()).isEqualTo(30);
        } finally {
            producer.close();
            consumer.close();
        }
    }

    @Test
    @DisplayName("JdkSerializer:自定义对象 Person 的序列化/反序列化往返一致")
    void jdkSerializer_personBody_roundTrip() {
        String topic = "ser-jdk-person-topic";
        String group = "ser-jdk-person-group";

        @SuppressWarnings("unchecked")
        MessageSerializer<Object> jdkSerializer = (MessageSerializer<Object>) (MessageSerializer<?>) new JdkSerializer<Person>();
        MessageConverter jdkConverter = new DefaultMessageConverter(jdkSerializer);

        RedissonStreamProducer producer =
            new RedissonStreamProducer(redisson, namespace, group + "-p", jdkConverter, 3000L, 0, 0);
        RedissonStreamListener consumer =
            new RedissonStreamListener(redisson, namespace, topic, group, "c1", jdkConverter);
        createConsumerGroup(topic, group);

        try {
            Person person = new Person("bob", 25);
            producer.syncSend(MessageBuilder.<Person>withTopic(topic).body(person).build());

            List<Message<?>> messages = consumer.pull(1);
            assertThat(messages).hasSize(1);
            Message<?> received = messages.get(0);
            assertThat(received.getBody()).isInstanceOf(Person.class);
            Person receivedPerson = (Person) received.getBody();
            assertThat(receivedPerson.getName()).isEqualTo("bob");
            assertThat(receivedPerson.getAge()).isEqualTo(25);
        } finally {
            producer.close();
            consumer.close();
        }
    }
}
