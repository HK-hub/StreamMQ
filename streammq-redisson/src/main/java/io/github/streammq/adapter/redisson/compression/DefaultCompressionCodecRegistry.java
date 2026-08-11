package io.github.streammq.adapter.redisson.compression;

import io.github.streammq.core.compression.CompressionCodec;
import io.github.streammq.core.compression.CompressionCodecRegistry;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * {@link CompressionCodecRegistry} 默认实现，线程安全。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class DefaultCompressionCodecRegistry implements CompressionCodecRegistry {

    private final ConcurrentMap<String, CompressionCodec> codecs = new ConcurrentHashMap<>();

    @Override
    public void register(CompressionCodec codec) {
        Objects.requireNonNull(codec, "codec");
        Objects.requireNonNull(codec.name(), "codec.name()");
        codecs.put(codec.name(), codec);
    }

    @Override
    public CompressionCodec lookup(String name) {
        if (name == null || name.isEmpty()) {
            return null;
        }
        return codecs.get(name);
    }

    @Override
    public Set<String> availableCodecs() {
        return Collections.unmodifiableSet(codecs.keySet());
    }
}
