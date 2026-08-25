/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.test;

/**
 * 兼容别名：0.1.0 起更名为 {@link ContainerizedRedisServer}，以如实反映其实现方式—— 基于 Testcontainers 的容器化 Redis（需要
 * Docker daemon），而非进程内嵌入服务。
 *
 * @deprecated 使用 {@link ContainerizedRedisServer}
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
@Deprecated
public class EmbeddedRedisServer extends ContainerizedRedisServer {}
