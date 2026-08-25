/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.compression;

import java.util.Set;

/**
 * 压缩编解码器注册表，按名称管理所有可用 Codec。
 *
 * <p>消息在发送时会将 Codec 名称写入 {@code "compressed"} 字段（如 {@code "gzip"}）， 消费端通过此注册表按名称查找对应的 Codec 实例进行解压。
 *
 * <p>内置 Codec 在自动装配时注册；用户自定义 Codec 可通过实现 {@link CompressionCodec} 并注册 Spring Bean 的方式添加。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface CompressionCodecRegistry {

    /**
     * 注册一个 Codec。若同名 Codec 已存在则覆盖。
     *
     * @param codec 编解码器实例
     */
    void register(CompressionCodec codec);

    /**
     * 按名称查找 Codec。
     *
     * @param name Codec 名称（如 {@code "gzip"}）
     * @return 对应实例，未找到返回 {@code null}
     */
    CompressionCodec lookup(String name);

    /**
     * 返回所有已注册 Codec 的名称集合。
     *
     * @return 不可变名称集合
     */
    Set<String> availableCodecs();
}
