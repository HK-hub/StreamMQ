/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.cloud.k8s;

import io.github.streammq.core.listener.StreamMQListenerContainer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * StreamMQ K8s 健康探针 REST 端点。
 *
 * <p>基础路径：{@code /streammq/health}
 *
 * <p>提供以下端点供 Kubernetes 存活与就绪探针调用：
 *
 * <ul>
 *   <li>{@code GET /streammq/health/liveness} - 返回存活状态，含 {@code status}、{@code backend} 字段
 *   <li>{@code GET /streammq/health/readiness} - 返回就绪状态，含 {@code ready}、{@code consumerCount} 字段
 * </ul>
 *
 * <p>当 {@link StreamMQListenerContainer} 不存在时，就绪探针返回降级状态 （{@code ready=false}、{@code
 * consumerCount=0}），存活探针仍返回 UP。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@RestController
@RequestMapping(StreamMQHealthControllerConstants.BASE_PATH)
public class StreamMQHealthController {

    private final ObjectProvider<StreamMQListenerContainer> containerProvider;

    /**
     * 构造健康探针控制器。
     *
     * @param containerProvider 监听器容器的可选注入提供者
     */
    public StreamMQHealthController(ObjectProvider<StreamMQListenerContainer> containerProvider) {
        this.containerProvider = Objects.requireNonNull(containerProvider, "containerProvider");
    }

    /**
     * 存活探针端点。
     *
     * <p>K8s livenessProbe 调用，返回 Pod 是否存活。存活状态恒为 UP， 仅在后端不可达等极端场景下由上层网关降级。
     *
     * @return 存活状态响应，包含 {@code status} 与 {@code backend} 字段
     */
    @GetMapping(StreamMQHealthControllerConstants.PATH_LIVENESS)
    public ResponseEntity<Map<String, Object>> liveness() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put(
                StreamMQHealthControllerConstants.KEY_STATUS,
                StreamMQHealthControllerConstants.STATUS_UP);
        body.put(
                StreamMQHealthControllerConstants.KEY_BACKEND,
                StreamMQHealthControllerConstants.VALUE_UNKNOWN);
        return ResponseEntity.ok(body);
    }

    /**
     * 就绪探针端点。
     *
     * <p>K8s readinessProbe 调用，返回 Pod 是否准备好接收流量。当监听器容器 不存在或未运行时返回 {@code ready=false}，否则返回 {@code
     * ready=true} 与当前消费者数量。
     *
     * @return 就绪状态响应，包含 {@code ready} 与 {@code consumerCount} 字段
     */
    @GetMapping(StreamMQHealthControllerConstants.PATH_READINESS)
    public ResponseEntity<Map<String, Object>> readiness() {
        StreamMQListenerContainer container = containerProvider.getIfAvailable();
        Map<String, Object> body = new LinkedHashMap<>();
        if (Objects.isNull(container)) {
            body.put(StreamMQHealthControllerConstants.KEY_READY, false);
            body.put(StreamMQHealthControllerConstants.KEY_CONSUMER_COUNT, 0);
            return ResponseEntity.ok(body);
        }
        body.put(StreamMQHealthControllerConstants.KEY_READY, container.isRunning());
        body.put(
                StreamMQHealthControllerConstants.KEY_CONSUMER_COUNT,
                container.getConsumers().size());
        return ResponseEntity.ok(body);
    }
}
