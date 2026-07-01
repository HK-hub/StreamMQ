package io.github.streammq.core.spi;

/**
 * 运维管理鉴权 SPI（v1.0 GA 提供）。
 *
 * <p>用于运维 REST 端点的鉴权，业务方实现此接口接入企业鉴权系统（如 OAuth2 / SSO / LDAP）。
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
