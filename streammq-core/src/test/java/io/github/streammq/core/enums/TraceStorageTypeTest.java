/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.enums;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * {@link TraceStorageType} 单元测试，覆盖编码解析与 UNKNOWN 兜底约定（F-10 回归：不再返回 null）。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@DisplayName("TraceStorageType 追踪存储方式测试")
class TraceStorageTypeTest {

    @ParameterizedTest
    @CsvSource({"none, NONE", "redis, REDIS", "NONE, NONE", "REDIS, REDIS", "Redis, REDIS"})
    @DisplayName("合法编码（忽略大小写）解析为对应枚举")
    void resolvesKnownCodes(String code, TraceStorageType expected) {
        assertThat(TraceStorageType.ofCode(code)).isEqualTo(expected);
    }

    @ParameterizedTest
    @ValueSource(strings = {"bogus", "memory", "rocks"})
    @NullAndEmptySource
    @DisplayName("未知/空编码兜底返回 UNKNOWN 而非 null")
    void fallsBackToUnknown(String code) {
        assertThat(TraceStorageType.ofCode(code)).isEqualTo(TraceStorageType.UNKNOWN);
    }

    @Test
    @DisplayName("UNKNOWN 编码可往返且不与已知冲突")
    void unknownRoundTrip() {
        assertThat(TraceStorageType.UNKNOWN.getCode()).isEqualTo("unknown");
        assertThat(TraceStorageType.ofCode("unknown")).isEqualTo(TraceStorageType.UNKNOWN);
    }
}
