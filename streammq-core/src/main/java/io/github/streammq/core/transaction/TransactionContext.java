package io.github.streammq.core.transaction;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 事务上下文，封装事务消息执行过程中的运行时信息。
 *
 * <p>由框架在调用 {@link TransactionCallback#execute} 或 {@link TransactionChecker#check} 时构造。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public final class TransactionContext {

    /** 事务 ID（与半消息 ID 关联） */
    private final String transactionId;

    /** 事务组名 */
    private final String transactionGroup;

    /** 生产者组名 */
    private final String producerGroup;

    /** 半消息发送时间戳（毫秒） */
    private final long bornTimestamp;

    /** 扩展属性 */
    private final Map<String, String> extAttributes;

    /**
     * 构造事务上下文。
     *
     * @param transactionId 事务 ID
     * @param transactionGroup 事务组名
     * @param producerGroup 生产者组名
     * @param bornTimestamp 出生时间戳
     * @param extAttributes 扩展属性
     */
    public TransactionContext(String transactionId, String transactionGroup, String producerGroup,
                              long bornTimestamp, Map<String, String> extAttributes) {
        this.transactionId = Objects.requireNonNull(transactionId, "transactionId");
        this.transactionGroup = Objects.requireNonNull(transactionGroup, "transactionGroup");
        this.producerGroup = producerGroup;
        this.bornTimestamp = bornTimestamp;
        this.extAttributes = extAttributes == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new HashMap<>(extAttributes));
    }

    public String getTransactionId() {
        return transactionId;
    }

    public String getTransactionGroup() {
        return transactionGroup;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public long getBornTimestamp() {
        return bornTimestamp;
    }

    /**
     * 返回扩展属性（不可修改）。
     *
     * @return 扩展属性
     */
    public Map<String, String> getExtAttributes() {
        return extAttributes;
    }

    /**
     * 获取扩展属性。
     *
     * @param key 键
     * @return 值，不存在则 null
     */
    public String ext(String key) {
        return extAttributes.get(key);
    }

    @Override
    public String toString() {
        return "TransactionContext{"
            + "transactionId='" + transactionId + '\''
            + ", transactionGroup='" + transactionGroup + '\''
            + ", producerGroup='" + producerGroup + '\''
            + ", bornTimestamp=" + bornTimestamp
            + ", extAttributes.size=" + extAttributes.size()
            + '}';
    }
}
