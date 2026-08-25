/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.enums;

import java.util.Arrays;

/**
 * 追踪存储方式枚举。
 *
 * <p>对应配置项 {@code streammq.trace.storage}：
 *
 * <ul>
 *   <li>{@link #NONE} - 默认，不启用追踪存储
 *   <li>{@link #REDIS} - 使用 Redis Stream 存储追踪数据
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 1.1.0
 */
public enum TraceStorageType {

    /** 不启用追踪存储。 */
    NONE("none"),

    /** 使用 Redis Stream 存储追踪数据。 */
    REDIS("redis");

    private final String code;

    TraceStorageType(String code) {
        this.code = code;
    }

    /**
     * 返回配置文件中使用的编码。
     *
     * @return 配置编码
     */
    public String getCode() {
        return code;
    }

    /**
     * 根据配置编码解析枚举。
     *
     * @param code 配置编码
     * @return 匹配的枚举；未匹配时返回 null
     */
    public static TraceStorageType ofCode(String code) {
        return Arrays.stream(values())
                .filter(t -> t.code.equalsIgnoreCase(code))
                .findFirst()
                .orElse(null);
    }
}
