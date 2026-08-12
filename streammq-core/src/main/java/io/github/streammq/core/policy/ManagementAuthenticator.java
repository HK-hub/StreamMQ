package io.github.streammq.core.policy;

/**
 * 运维管理鉴权 SPI（0.1.0 提供）。
 *
 * <p>用于运维 REST 端点的鉴权，业务方实现此接口接入企业鉴权系统（如 OAuth2 / SSO / LDAP）。
 *
 * <p>默认实现为 {@code DenyAllAuthenticator}（拒绝所有访问），业务方注册自定义实现覆盖。
 *
 * @author StreamMQ Contributors
 * @since 0.1.0
 */
public interface ManagementAuthenticator {

    /**
     * 校验用户对资源的访问权限。
     *
     * @param username 用户名
     * @param password 密码或 Token
     * @param resource 资源标识（如 {@code "topic:order-topic"} / {@code "dlq:order-cg"}）
     * @return true 鉴权通过
     */
    boolean authenticate(String username, String password, String resource);

    /**
     * 鉴权器名称。
     *
     * @return 名称
     */
    default String name() {
        return getClass().getSimpleName();
    }
}
