package io.github.streammq.adapter.redisson.serializer;

import io.github.streammq.core.serializer.MessageSerializer;
import java.util.Objects;
import org.apache.fury.Fury;
import org.apache.fury.ThreadSafeFury;
import org.apache.fury.config.Language;

/**
 * 基于 Apache Fury 的高性能跨语言序列化器。
 *
 * <p>Fury 支持 Java 对象的高性能序列化，性能显著优于 JDK 序列化， 且支持跨语言场景（通过 {@link Language#XLANG} 模式）。
 *
 * <p>注意：Fury 序列化要求被序列化的类与反序列化端的类版本一致， 适合 StreamMQ 内部消息体（body）的序列化。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class FurySerializer<T> implements MessageSerializer<T> {

  private final ThreadSafeFury fury;

  public FurySerializer() {
    this.fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .requireClassRegistration(false)
            .buildThreadSafeFury();
  }

  @Override
  public byte[] serialize(T object, Class<T> type) {
    if (Objects.isNull(object)) {
      return new byte[0];
    }
    return fury.serialize(object);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <R> R deserialize(byte[] bytes, Class<R> type) {
    if (Objects.isNull(bytes) || bytes.length == 0) {
      return null;
    }
    return (R) fury.deserialize(bytes);
  }

  @Override
  public String name() {
    return "fury";
  }
}
