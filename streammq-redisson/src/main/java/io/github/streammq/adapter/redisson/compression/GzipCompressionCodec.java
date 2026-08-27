/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.compression;

import io.github.streammq.core.compression.CompressionCodec;
import io.github.streammq.core.exception.SerializationException;
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

    /** Codec 协议标识（写入 {@code compressed} 字段的值） */
    public static final String CODEC_NAME = "gzip";

    /** 默认缓冲区大小（4KB） */
    private static final int BUFFER_SIZE = 4096;

    /**
     * 解压输出上限（16MB）：防御解压炸弹（zip bomb）。
     *
     * <p>恶意/损坏的 GZIP 流可用极小密文诱导解压器展开数 GB 明文导致 OOM。 解压累计输出超过该上限立即抛 {@link
     * SerializationException}——上游毒丸消息路径会将其路由到 DLQ，不会拖垮消费线程。
     */
    public static final long MAX_EXPANDED_BYTES = 16L * 1024 * 1024;

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
            long expanded = 0;
            while ((n = gzip.read(buffer)) != -1) {
                expanded += n;
                if (expanded > MAX_EXPANDED_BYTES) {
                    throw new SerializationException(
                            "decompressed payload exceeds limit " + MAX_EXPANDED_BYTES + " bytes");
                }
                bos.write(buffer, 0, n);
            }
            return bos.toByteArray();
        } catch (IOException ex) {
            throw new StreamMQException("GZIP decompress failed", ex);
        }
    }

    @Override
    public String name() {
        return CODEC_NAME;
    }
}
