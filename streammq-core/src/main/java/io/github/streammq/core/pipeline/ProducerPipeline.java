/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.pipeline;

import io.github.streammq.core.converter.MessageConverter;
import io.github.streammq.core.filter.ProducerFilter;
import io.github.streammq.core.interceptor.ProducerInterceptor;
import java.util.List;

/**
 * 生产者管线配置接口：管理生产者拦截器链、过滤器链与消息转换器。
 *
 * <p>从 {@link io.github.streammq.core.template.StreamMessageTemplate} 拆分出来，遵循单一职责原则： {@code
 * StreamMessageTemplate} 专注发送语义，{@code ProducerPipeline} 专注管线配置。
 *
 * @author StreamMQ Contributors
 * @since 0.1.1
 */
public interface ProducerPipeline {

    /**
     * 返回消息转换器。
     *
     * @return 消息转换器
     */
    MessageConverter getMessageConverter();

    /**
     * 返回生产者拦截器链（不可修改快照）。
     *
     * @return 拦截器列表
     */
    List<ProducerInterceptor> getProducerInterceptors();

    /**
     * 设置生产者拦截器链（覆盖现有）。
     *
     * @param interceptors 拦截器列表
     */
    void setProducerInterceptors(List<ProducerInterceptor> interceptors);

    /**
     * 添加单个生产者拦截器。
     *
     * @param interceptor 拦截器
     */
    void addProducerInterceptor(ProducerInterceptor interceptor);

    /**
     * 添加单个生产者过滤器（发送前过滤）。
     *
     * @param filter 过滤器
     */
    void addProducerFilter(ProducerFilter filter);
}
