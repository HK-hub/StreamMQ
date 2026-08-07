package io.github.streammq.cloud.k8s;

import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 测试用 Spring Boot 应用入口，为 {@link org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest}
 * 提供 {@code @SpringBootConfiguration} 上下文。
 *
 * <p>仅在测试范围内使用，不参与生产打包。
 */
@SpringBootApplication
class TestApplication {
}
