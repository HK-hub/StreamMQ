package io.github.streammq.adapter.redisson.compression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.streammq.core.exception.StreamMQException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * {@link GzipCompressionCodec} 单元测试，覆盖压缩/解压往返、空数据、大数据与异常场景。
 *
 * @author StreamMQ Contributors
 * @since 1.0.0
 */
@DisplayName("GzipCompressionCodec GZIP 压缩编解码器测试")
class GzipCompressionCodecTest {

    private final GzipCompressionCodec codec = new GzipCompressionCodec();

    @Test
    @DisplayName("name 返回 gzip")
    void name() {
        assertThat(codec.name()).isEqualTo("gzip");
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
        // 重复内容应有效压缩
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
    @DisplayName("压缩空字节数组返回空数组")
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
    @DisplayName("解压非法数据抛出 StreamMQException")
    void decompressInvalidData() {
        byte[] invalid = "not a gzip stream".getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> codec.decompress(invalid)).isInstanceOf(StreamMQException.class);
    }

    @Test
    @DisplayName("压缩后数据以 GZIP 魔数开头（0x1f 0x8b）")
    void compressHasGzipMagic() {
        byte[] original = "test data for gzip magic number check".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = codec.compress(original);
        assertThat(compressed[0]).isEqualTo((byte) 0x1f);
        assertThat(compressed[1]).isEqualTo((byte) 0x8b);
    }
}
