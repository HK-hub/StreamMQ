package io.github.streammq.core.enums;

import java.util.Arrays;

/**
 * 事务扫描状态机：Redis 事务状态 Hash 中的线上协议状态值。
 *
 * <p>与 {@link LocalTransactionState}（回查器返回值）不同，本枚举描述事务消息 在 Redis 端的存储状态：
 *
 * <ul>
 *   <li>{@link #PREPARE} - 半消息已写入，等待本地事务结果或回查
 *   <li>{@link #COMMIT} - 已提交终态
 *   <li>{@link #ROLLBACK} - 已回滚终态
 *   <li>{@link #COMMITTING} - 提交中中间态（实例已原子抢占）
 *   <li>{@link #ROLLBACKING} - 回滚中中间态（实例已原子抢占）
 *   <li>{@link #UNKNOWN} - 状态未知（字段缺失或无法识别）
 * </ul>
 *
 * @author StreamMQ Contributors
 * @since 1.1.0
 */
public enum TransactionScanState {

    /** 半消息已写入，等待提交/回滚。 */
    PREPARE("PREPARE"),

    /** 终态：已提交。 */
    COMMIT("COMMIT"),

    /** 终态：已回滚。 */
    ROLLBACK("ROLLBACK"),

    /** 中间态：提交中（实例已原子抢占事务，正在执行转投）。 */
    COMMITTING("COMMITTING"),

    /** 中间态：回滚中（实例已原子抢占事务，正在执行删除）。 */
    ROLLBACKING("ROLLBACKING"),

    /** 状态未知。 */
    UNKNOWN("UNKNOWN");

    private final String code;

    TransactionScanState(String code) {
        this.code = code;
    }

    /**
     * 返回线上协议使用的状态编码。
     *
     * @return 状态编码字符串
     */
    public String getCode() {
        return code;
    }

    /**
     * 根据协议编码解析枚举。
     *
     * @param code 协议编码
     * @return 匹配的枚举；未匹配时返回 {@link #UNKNOWN}
     */
    public static TransactionScanState ofCode(String code) {
        return Arrays.stream(values())
                .filter(s -> s.code.equals(code))
                .findFirst()
                .orElse(UNKNOWN);
    }

    /**
     * 判断是否为中间处理状态（其它实例正在处理）。
     *
     * @return true 为中间态
     */
    public boolean isInFlight() {
        return this == COMMITTING || this == ROLLBACKING;
    }
}
