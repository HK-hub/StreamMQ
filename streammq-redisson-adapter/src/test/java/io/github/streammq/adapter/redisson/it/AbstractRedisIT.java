package io.github.streammq.adapter.redisson.it;

import io.github.streammq.adapter.redisson.converter.DefaultMessageConverter;
import io.github.streammq.adapter.redisson.serializer.JacksonJsonSerializer;
import io.github.streammq.adapter.redisson.support.StreamMQKeys;
import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.serializer.MessageSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.redisson.Redisson;
import org.redisson.api.RStream;
import org.redisson.api.RedissonClient;
import org.redisson.api.StreamMessageId;
import org.redisson.api.stream.StreamCreateGroupArgs;
import org.redisson.config.Config;

import java.util.UUID;

/**
 * Redis 集成测试基类,管理 Redisson 客户端和测试命名空间隔离。
 *
 * <p>每个测试类在 {@link #setUpRedis()} 中创建独立的 {@link RedissonClient} 与随机 namespace,
 * 在 {@link #tearDownRedis()} 中清理本命名空间所有 key 并关闭客户端,避免测试间相互干扰。
 */
public abstract class AbstractRedisIT {

    protected RedissonClient redisson;
    protected String namespace;
    protected MessageSerializer<Object> serializer;
    protected MessageConverter converter;

    @BeforeEach
    void setUpRedis() {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://localhost:6379").setDatabase(0);
        redisson = Redisson.create(config);
        namespace = "it-" + UUID.randomUUID().toString().substring(0, 8);
        serializer = new JacksonJsonSerializer<>();
        converter = new DefaultMessageConverter(serializer);
    }

    @AfterEach
    void tearDownRedis() {
        if (redisson != null) {
            redisson.getKeys().deleteByPattern("streammq:" + namespace + ":*");
            redisson.shutdown();
        }
    }

    /**
     * 显式创建消费者组,绕过主代码 RedissonStreamListener.ensureGroup() 中
     * 使用 StreamMessageId.MIN(序列化为 "-")导致 XGROUP CREATE 失败的 bug。
     *
     * <p>主代码 bug 记录:RedissonStreamListener.java:286 使用 {@code StreamMessageId.MIN}
     * 作为 XGROUP CREATE 的起始 ID,但 MIN 序列化为 "-" 在 Redis 中无效,
     * 应使用 {@code new StreamMessageId(0, 0)}(即 "0-0")。
     *
     * <p>本方法使用正确的 ID 创建组后,Consumer 的 ensureGroup() 会得到 BUSYGROUP
     * (已被主代码捕获并忽略),不会影响测试。
     *
     * @param topic 主题
     * @param group 消费者组名
     */
    protected void createConsumerGroup(String topic, String group) {
        RStream<String, String> stream = redisson.getStream(StreamMQKeys.topicStream(namespace, topic));
        stream.createGroup(StreamCreateGroupArgs.name(group).makeStream().id(new StreamMessageId(0, 0)));
    }
}
