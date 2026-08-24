package io.github.streammq.spring.cloud.stream.binder;

import io.github.streammq.core.enums.SelectorType;

/**
 * StreamMQ Spring Cloud Stream Binder 常量定义。
 *
 * <p>集中管理配置前缀、Bean 名称、消息头与健康检查详情 key。
 *
 * @author StreamMQ Contributors
 * @since 1.1.0
 */
public final class StreamMQBinderConstants {

    // ==================== 配置属性前缀 ====================
    /** Binder 全局配置前缀 */
    public static final String BINDER_PREFIX = "spring.cloud.stream.streammq.binder";

    /** 扩展绑定配置前缀 */
    public static final String EXTENDED_BINDINGS_PREFIX = "spring.cloud.stream.streammq.bindings";

    /** 全局默认值配置前缀 */
    public static final String DEFAULTS_PREFIX = "spring.cloud.stream.streammq.default";

    /** Binder 开关配置前缀 */
    public static final String ENABLED_PROPERTY_PREFIX = "spring.cloud.stream.streammq.binder";

    /** 开关属性名：enabled */
    public static final String PROP_NAME_ENABLED = "enabled";

    /** 开关属性值：true */
    public static final String PROP_VALUE_TRUE = "true";

    /** 可选依赖 FQCN：Spring Boot Actuator AbstractHealthIndicator（编译期解耦探测） */
    public static final String ABSTRACT_HEALTH_INDICATOR_CLASS_NAME =
            "org.springframework.boot.actuate.health.AbstractHealthIndicator";

    // ==================== Bean 名称 ====================
    /** Binder 健康检查器 Bean 名 */
    public static final String BEAN_BINDER_HEALTH_INDICATOR = "streamMQBinderHealthIndicator";

    // ==================== 消息头 ====================
    /** StreamMQ 自定义消息头前缀 */
    public static final String HEADER_PREFIX = "streammq_";

    // ==================== 内容类型 ====================
    /** 内容类型：纯文本 */
    public static final String CONTENT_TYPE_TEXT_PLAIN = "text/plain";

    /** 内容类型：JSON */
    public static final String CONTENT_TYPE_APPLICATION_JSON = "application/json";

    /** 内容类型：二进制流 */
    public static final String CONTENT_TYPE_OCTET_STREAM = "application/octet-stream";

    // ==================== 健康检查详情 Key ====================
    /** 健康详情：错误信息 */
    public static final String HEALTH_DETAIL_ERROR = "error";

    /** 健康详情：容器运行标志 */
    public static final String HEALTH_DETAIL_LC_RUNNING = "listenerContainer.running";

    /** 健康详情：容器消费者数量 */
    public static final String HEALTH_DETAIL_LC_CONSUMER_COUNT =
            "listenerContainer.consumerCount";

    /** 默认选择器类型编码（对应 {@link SelectorType#TAG}） */
    public static final String DEFAULT_SELECTOR_TYPE_CODE = SelectorType.TAG.name();

    private StreamMQBinderConstants() {}
}
