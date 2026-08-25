/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.core.message;

/**
 * 发送结果状态。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public enum SendStatus {

    /** 发送成功 */
    SEND_OK,

    /** 发送失败（异常、超时等） */
    SEND_FAILED,

    /** 从节点未就绪（多副本场景，0.1.0 暂不产生，预留） */
    SLAVE_NOT_READY,

    /** 刷盘超时（0.1.0 暂不产生，预留） */
    FLUSH_DISK_TIMEOUT
}
