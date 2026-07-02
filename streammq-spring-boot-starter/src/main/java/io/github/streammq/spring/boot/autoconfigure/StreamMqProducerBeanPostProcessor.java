package io.github.streammq.spring.boot.autoconfigure;

import io.github.streammq.adapter.redisson.template.DefaultStreamMqTemplate;
import io.github.streammq.core.annotation.StreamMqProducer;
import io.github.streammq.core.producer.ProducerConfig;
import io.github.streammq.core.producer.StreamMqProducerFactory;
import io.github.streammq.core.spi.MessageConverter;
import io.github.streammq.core.template.StreamMqTemplate;
import io.github.streammq.spring.boot.properties.StreamMqProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 处理 {@link StreamMqProducer} 注解的 {@link BeanPostProcessor}，
 * 在 Bean 初始化后扫描字段并将对应的 {@link StreamMqTemplate} 实例注入。
 *
 * <p>对齐 RocketMQ Spring Starter 中 {@code @ExtRocketMQTemplateConfiguration} / 字段注入体验。
 *
 * <p>注入逻辑：
 * <ol>
 *   <li>扫描 Bean 中所有 {@code @StreamMqProducer} 标注的字段</li>
 *   <li>按注解 {@code group} 复用 {@link StreamMqTemplate} 实例（同 group 同实例）</li>
 *   <li>若用户已注册同名 {@code StreamMqTemplate} Bean，优先复用</li>
 *   <li>否则按注解 + 全局配置创建 {@link DefaultStreamMqTemplate}</li>
 * </ol>
 *
 * <p>支持泛型字段：{@code StreamMqTemplate<Order>} / {@code StreamMqTemplate<String>}，
 * 运行时统一为 {@code StreamMqTemplate<Object>}。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public class StreamMqProducerBeanPostProcessor implements BeanPostProcessor, ApplicationContextAware {

    private static final Logger LOG = LoggerFactory.getLogger(StreamMqProducerBeanPostProcessor.class);

    private final Map<String, StreamMqTemplate<?>> templatesByGroup = new ConcurrentHashMap<>();
    private final StreamMqProducerFactory producerFactory;
    private final MessageConverter messageConverter;
    private final StreamMqProperties properties;
    private ApplicationContext applicationContext;

    /**
     * 构造 BeanPostProcessor。
     *
     * @param producerFactory 生产者工厂
     * @param messageConverter 消息转换器
     * @param properties 全局配置
     */
    public StreamMqProducerBeanPostProcessor(StreamMqProducerFactory producerFactory,
                                             MessageConverter messageConverter,
                                             StreamMqProperties properties) {
        this.producerFactory = Objects.requireNonNull(producerFactory, "producerFactory");
        this.messageConverter = Objects.requireNonNull(messageConverter, "messageConverter");
        this.properties = Objects.requireNonNull(properties, "properties");
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> clazz = bean.getClass();
        for (Field field : clazz.getDeclaredFields()) {
            StreamMqProducer annotation = field.getAnnotation(StreamMqProducer.class);
            if (annotation == null) {
                continue;
            }
            if (!StreamMqTemplate.class.isAssignableFrom(field.getType())) {
                LOG.warn("@StreamMqProducer annotated field {} is not StreamMqTemplate type, ignored: {}.{}",
                    field.getName(), clazz.getName(), beanName);
                continue;
            }
            injectTemplate(bean, field, annotation, beanName);
        }
        return bean;
    }

    private void injectTemplate(Object bean, Field field, StreamMqProducer annotation, String beanName) {
        // 解析 ${} 占位符与 #{} SpEL 表达式（group / namespace）
        String group = resolvePlaceholders(annotation.group());
        StreamMqTemplate<?> template = resolveTemplate(group, annotation);
        try {
            field.setAccessible(true);
            field.set(bean, template);
            LOG.info("Injected StreamMqTemplate (group={}) into bean={}, field={}",
                group, beanName, field.getName());
        } catch (IllegalAccessException ex) {
            throw new IllegalStateException(
                "Failed to inject StreamMqTemplate into " + bean.getClass().getName() + "." + field.getName(), ex);
        }
    }

    /**
     * 解析字符串中的 ${...} 属性占位符。
     * 若 ApplicationContext 不可用则返回原值。
     *
     * @param value 原始值
     * @return 解析后的值
     */
    private String resolvePlaceholders(String value) {
        if (value == null || value.isEmpty() || applicationContext == null) {
            return value;
        }
        return applicationContext.getEnvironment().resolvePlaceholders(value);
    }

    private StreamMqTemplate<?> resolveTemplate(String group, StreamMqProducer annotation) {
        // 复用同 group 已创建的 Template
        StreamMqTemplate<?> cached = templatesByGroup.get(group);
        if (cached != null) {
            return cached;
        }
        synchronized (this) {
            cached = templatesByGroup.get(group);
            if (cached != null) {
                return cached;
            }
            // 优先查找用户已注册的 StreamMqTemplate Bean（按 group 命名约定 streamMqTemplate-{group}）
            String beanName = "streamMqTemplate-" + group;
            if (applicationContext.containsBean(beanName)) {
                StreamMqTemplate<?> userBean = applicationContext.getBean(beanName, StreamMqTemplate.class);
                templatesByGroup.put(group, userBean);
                return userBean;
            }
            // 创建 DefaultStreamMqTemplate
            String txGroup = properties.getTransaction().getDefaultGroup();
            ProducerConfig producerConfig = ProducerConfig.builder().group(group).build();
            DefaultStreamMqTemplate<Object> template = new DefaultStreamMqTemplate<>(
                producerFactory, group, messageConverter, producerConfig, txGroup);
            templatesByGroup.put(group, template);
            LOG.info("Created StreamMqTemplate for group={}", group);
            return template;
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}
