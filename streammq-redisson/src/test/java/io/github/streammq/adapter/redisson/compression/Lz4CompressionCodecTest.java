/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson.compression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.streammq.core.exception.SerializationException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * {@link Lz4CompressionCodec} 单元测试：覆盖 classpath 有/无 LZ4 两种场景。
 *
 * <p><b>测试矩阵：</b>
 *
 * <ul>
 *   <li>无论 LZ4 是否在 classpath：{@link Lz4CompressionCodecFactory#isAvailable()} 与 {@link
 *       Lz4CompressionCodecFactory#tryCreate()} 行为契约
 *   <li>仅当 LZ4 在 classpath（{@code streammq-redisson} 测试 scope 引入 {@code org.lz4:lz4-java}）：压缩/解压
 *       往返、空字节、null、压缩比、解压炸弹
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("Lz4CompressionCodec 条件性 LZ4 编解码器测试")
class Lz4CompressionCodecTest {

    /** classpath 是否存在 lz4-java，影响本测试类中嵌套类的行为。 */
    private static final boolean LZ4_AVAILABLE = Lz4CompressionCodecFactory.isAvailable();

    @BeforeAll
    static void reportAvailability() {
        // 仅在控制台输出一次，便于日志定位当前构建的 LZ4 状态
        System.out.printf("[test] LZ4 classpath probe: isAvailable=%s%n", LZ4_AVAILABLE);
    }

    // ================================================================
    // 工厂契约测试：不依赖 lz4-java 是否可用
    // ================================================================

    @Nested
    @DisplayName("Lz4CompressionCodecFactory 契约")
    class FactoryContract {

        @Test
        @DisplayName("isAvailable 与 classpath 实际状态一致")
        void isAvailableReflectsClasspath() {
            // 二次校验：必须等于 LZ4_AVAILABLE（避免缓存导致的不一致）
            assertThat(Lz4CompressionCodecFactory.isAvailable()).isEqualTo(LZ4_AVAILABLE);
        }

        @Test
        @DisplayName("tryCreate 返回值与 isAvailable 一致（null ↔ 非 null）")
        void tryCreateMatchesAvailability() {
            Lz4CompressionCodec codec = Lz4CompressionCodecFactory.tryCreate();
            if (LZ4_AVAILABLE) {
                assertThat(codec).isNotNull();
                assertThat(codec.name()).isEqualTo("lz4");
            } else {
                assertThat(codec).isNull();
            }
        }
    }

    // ================================================================
    // Codec 行为测试：仅当 LZ4 可用时执行
    // ================================================================

    @Nested
    @DisplayName("Lz4CompressionCodec 行为（仅当 LZ4 在 classpath 时执行）")
    @org.junit.jupiter.api.TestInstance(org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS)
    class CodecBehavior {

        private Lz4CompressionCodec codec;

        @BeforeAll
        void setUp() {
            Assumptions.assumeTrue(
                    LZ4_AVAILABLE,
                    "LZ4 (org.lz4:lz4-java) not on classpath; skipping LZ4 behavior tests");
            codec = Lz4CompressionCodecFactory.tryCreate();
        }

        @Test
        @DisplayName("name 返回 lz4")
        void nameReturnsLz4() {
            assertThat(codec.name()).isEqualTo(Lz4CompressionCodec.CODEC_NAME);
            assertThat(codec.name()).isEqualTo("lz4");
        }

        @Test
        @DisplayName("压缩并解压后数据一致（短字符串）")
        void compressDecompressShortString() {
            byte[] original = "hello world".getBytes(StandardCharsets.UTF_8);
            byte[] compressed = codec.compress(original);
            byte[] decompressed = codec.decompress(compressed);

            assertThat(decompressed).isEqualTo(original);
        }

        @Test
        @DisplayName("压缩并解压后数据一致（大文本，重复内容）")
        void compressDecompressLargeText() {
            StringBuilder sb = new StringBuilder(10000);
            for (int i = 0; i < 1000; i++) {
                sb.append("重复内容行 #").append(i).append("\n");
            }
            byte[] original = sb.toString().getBytes(StandardCharsets.UTF_8);
            byte[] compressed = codec.compress(original);
            byte[] decompressed = codec.decompress(compressed);

            assertThat(decompressed).isEqualTo(original);
            // 重复内容应有效压缩（4 字节 header + 压缩数据 < 原文）
            assertThat(compressed.length).isLessThan(original.length);
        }

        @Test
        @DisplayName("压缩并解压后数据一致（随机二进制数据）")
        void compressDecompressBinary() {
            byte[] original = new byte[512];
            for (int i = 0; i < original.length; i++) {
                original[i] = (byte) (i * 7 % 256);
            }
            byte[] compressed = codec.compress(original);
            byte[] decompressed = codec.decompress(compressed);

            assertThat(decompressed).isEqualTo(original);
        }

        @Test
        @DisplayName("压缩空字节数组返回空数组（无 4 字节 header）")
        void compressEmpty() {
            byte[] empty = new byte[0];
            byte[] compressed = codec.compress(empty);
            assertThat(compressed).isEqualTo(empty);
        }

        @Test
        @DisplayName("解压空字节数组返回空数组")
        void decompressEmpty() {
            byte[] empty = new byte[0];
            byte[] decompressed = codec.decompress(empty);
            assertThat(decompressed).isEqualTo(empty);
        }

        @Test
        @DisplayName("compress null 入参抛出 NullPointerException")
        void compressNull() {
            assertThatThrownBy(() -> codec.compress(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("data");
        }

        @Test
        @DisplayName("decompress null 入参抛出 NullPointerException")
        void decompressNull() {
            assertThatThrownBy(() -> codec.decompress(null))
                    .isInstanceOf(NullPointerException.class)
                    .hasMessageContaining("data");
        }

        @Test
        @DisplayName("压缩数据前 4 字节为原始长度（big-endian int）")
        void compressedHeaderEncodesOriginalLength() {
            byte[] original = "header-length-test-payload".getBytes(StandardCharsets.UTF_8);
            byte[] compressed = codec.compress(original);
            assertThat(compressed.length).isGreaterThan(Integer.BYTES);
            int declaredLength =
                    ((compressed[0] & 0xFF) << 24)
                            | ((compressed[1] & 0xFF) << 16)
                            | ((compressed[2] & 0xFF) << 8)
                            | (compressed[3] & 0xFF);
            assertThat(declaredLength).isEqualTo(original.length);
        }

        @Test
        @DisplayName("解压炸弹：头部声明超过 64MB 抛 SerializationException")
        void decompressBombRejected() {
            // 构造一个 4 字节头部声明超大长度（但不分配该长度的实际解压缓冲）
            int declared = (int) Lz4CompressionCodec.MAX_EXPANDED_BYTES + 1;
            byte[] headerOnly = new byte[Integer.BYTES];
            headerOnly[0] = (byte) ((declared >>> 24) & 0xFF);
            headerOnly[1] = (byte) ((declared >>> 16) & 0xFF);
            headerOnly[2] = (byte) ((declared >>> 8) & 0xFF);
            headerOnly[3] = (byte) (declared & 0xFF);

            assertThatThrownBy(() -> codec.decompress(headerOnly))
                    .isInstanceOf(SerializationException.class)
                    .hasMessageContaining("exceeds limit");
        }

        @Test
        @DisplayName("解压炸弹：负长度头部抛 SerializationException")
        void decompressNegativeLengthRejected() {
            byte[] headerOnly = new byte[Integer.BYTES];
            headerOnly[0] = (byte) 0xFF;
            headerOnly[1] = (byte) 0xFF;
            headerOnly[2] = (byte) 0xFF;
            headerOnly[3] = (byte) 0xFF; // -1

            assertThatThrownBy(() -> codec.decompress(headerOnly))
                    .isInstanceOf(SerializationException.class)
                    .hasMessageContaining("exceeds limit");
        }

        @Test
        @DisplayName("解压小于 4 字节 payload 抛 SerializationException")
        void decompressTooShortRejected() {
            byte[] tooShort = new byte[3];
            assertThatThrownBy(() -> codec.decompress(tooShort))
                    .isInstanceOf(SerializationException.class)
                    .hasMessageContaining("too short");
        }

        @Test
        @DisplayName("压缩比：1MB 全零数据压缩后应远小于 1MB")
        void compressionRatioOnHighlyCompressible() {
            byte[] original = new byte[1024 * 1024];
            byte[] compressed = codec.compress(original);
            // 4 字节 header + LZ4 极少量元数据；1000 字节以内合理
            assertThat(compressed.length).isLessThan(1024);
        }
    }
}
