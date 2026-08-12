package io.github.streammq.adapter.redisson.compression;

import io.github.streammq.core.compression.CompressionCodec;
import io.github.streammq.core.exception.StreamMQException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * 基于 GZIP 的 {@link CompressionCodec} 实现。
 *
 * <p>使用 JDK 内置 {@link GZIPOutputStream} / {@link GZIPInputStream}，无需额外依赖。 压缩比较高，但速度不如 LZ4 /
 * Snappy。适用于对压缩率敏感、对 CPU 开销不敏感的场景。
 *
 * <p>线程安全：无内部状态，可在多线程间共享。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class GzipCompressionCodec implements CompressionCodec {

    /** 默认缓冲区大小（4KB） */
    private static final int BUFFER_SIZE = 4096;

    @Override
    public byte[] compress(byte[] data) {
        Objects.requireNonNull(data, "data");
        if (data.length == 0) {
            return data;
        }
        try (ByteArrayOutputStream bos =
                        new ByteArrayOutputStream(Math.max(data.length / 2, BUFFER_SIZE));
                GZIPOutputStream gzip = new GZIPOutputStream(bos)) {
            gzip.write(data);
            gzip.finish();
            return bos.toByteArray();
        } catch (IOException ex) {
            throw new StreamMQException("GZIP compress failed", ex);
        }
    }

    @Override
    public byte[] decompress(byte[] data) {
        Objects.requireNonNull(data, "data");
        if (data.length == 0) {
            return data;
        }
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
                GZIPInputStream gzip = new GZIPInputStream(bis);
                ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int n;
            while ((n = gzip.read(buffer)) != -1) {
                bos.write(buffer, 0, n);
            }
            return bos.toByteArray();
        } catch (IOException ex) {
            throw new StreamMQException("GZIP decompress failed", ex);
        }
    }

    @Override
    public String name() {
        return "gzip";
    }
}
