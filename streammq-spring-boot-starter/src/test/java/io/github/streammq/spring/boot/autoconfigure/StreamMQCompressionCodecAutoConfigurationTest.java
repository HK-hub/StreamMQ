/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.spring.boot.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import io.github.streammq.adapter.redisson.compression.DefaultCompressionCodecRegistry;
import io.github.streammq.adapter.redisson.compression.GzipCompressionCodec;
import io.github.streammq.adapter.redisson.container.DefaultStreamMQListenerContainer;
import io.github.streammq.core.compression.CompressionCodec;
import io.github.streammq.core.compression.CompressionCodecRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * 多 {@link CompressionCodec} 注册与"默认 Codec"消歧回归测试（R6-S3，失败即红）。
 *
 * <p>回归背景：{@code StreamMQCoreAutoConfiguration} 用 {@code ObjectProvider#getIfAvailable()} 解析默认
 * Codec——注册第二个自定义 Codec 且无 {@code @Primary} 时直接抛 {@code NoUniqueBeanDefinitionException}， 而同一处
 * javadoc 又鼓励用户注册自定义 Codec。现改为：注册表全量注册；默认 Codec 按 {@code @Primary} → {@code
 * streammq.producer.compression-codec} 精确匹配 → 唯一候选解析；多候选无法消歧时<b>不静默取任意值</b>。
 */
@DisplayName("多 Codec 注册与默认 Codec 消歧测试（S3）")
class StreamMQCompressionCodecAutoConfigurationTest {

    private final ApplicationContextRunner runner =
            new ApplicationContextRunner()
                    .withConfiguration(AutoConfigurations.of(StreamMQAutoConfiguration.class))
                    .withBean(RedissonClient.class, () -> mock(RedissonClient.class))
                    .withBean(
                            DefaultStreamMQListenerContainer.class,
                            () -> mock(DefaultStreamMQListenerContainer.class));

    @Test
    @DisplayName("注册 2 个自定义 Codec 能正常启动，且都能按名从注册表解析")
    void twoCustomCodecs_startAndResolveByName() {
        runner.withUserConfiguration(TwoCodecs.class)
                .run(
                        context -> {
                            assertThat(context).hasNotFailed();
                            CompressionCodecRegistry registry =
                                    context.getBean(CompressionCodecRegistry.class);
                            assertThat(registry.lookup("codec-a")).isNotNull();
                            assertThat(registry.lookup("codec-b")).isNotNull();
                            assertThat(registry.availableCodecs())
                                    .contains("gzip", "codec-a", "codec-b");
                        });
    }

    @Test
    @DisplayName("2 个 Codec + 未开压缩：不静默选默认 Codec，但仍能正常启动")
    void twoCodecsWithoutPrimary_compressionDisabled_startsWithoutSilentPick() {
        runner.withUserConfiguration(TwoCodecs.class)
                .run(
                        context -> {
                            assertThat(context).hasNotFailed();
                            // 未开压缩 → 无默认 Codec 也不影响可用性
                            assertThat(context).hasBean("streamMQCompressionCodecRegistry");
                        });
    }

    @Test
    @DisplayName("2 个 Codec + 已开压缩 + 无 @Primary：启动失败并列出候选（不得静默取任意一个）")
    void twoCodecsWithoutPrimary_compressionEnabled_failsWithCandidates() {
        runner.withUserConfiguration(TwoCodecs.class)
                .withPropertyValues("streammq.producer.compress-threshold=1")
                .run(
                        context -> {
                            assertThat(context).hasFailed();
                            assertThat(context.getStartupFailure())
                                    .hasMessageContaining("none is @Primary")
                                    .hasMessageContaining("codec-a")
                                    .hasMessageContaining("codec-b");
                        });
    }

    @Test
    @DisplayName("2 个 Codec + 配置名精确匹配：按名选中，压缩开启也能启动")
    void configuredCodecName_resolvesExactly() {
        runner.withUserConfiguration(TwoCodecs.class)
                .withPropertyValues(
                        "streammq.producer.compress-threshold=1",
                        "streammq.producer.compression-codec=codec-b")
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    @DisplayName("配置名不存在：启动失败并列出全部可选名称")
    void unknownConfiguredCodecName_failsListingCandidates() {
        runner.withUserConfiguration(TwoCodecs.class)
                .withPropertyValues("streammq.producer.compression-codec=zstd")
                .run(
                        context -> {
                            assertThat(context).hasFailed();
                            assertThat(context.getStartupFailure())
                                    .hasMessageContaining(
                                            "streammq.producer.compression-codec=zstd")
                                    .hasMessageContaining("codec-a")
                                    .hasMessageContaining("gzip");
                        });
    }

    @Test
    @DisplayName("2 个 Codec + @Primary：@Primary 候选被选中，压缩开启也能启动")
    void primaryCodec_isSelected() {
        runner.withUserConfiguration(TwoCodecsWithPrimary.class)
                .withPropertyValues("streammq.producer.compress-threshold=1")
                .run(context -> assertThat(context).hasNotFailed());
    }

    @Test
    @DisplayName("选择顺序单元验证：@Primary → 配置名 → 唯一候选 → 无法消歧时失败/返回 null")
    void selectionOrder_unitLevel() {
        CodecA a = new CodecA();
        CodecB b = new CodecB();
        DefaultCompressionCodecRegistry registry = new DefaultCompressionCodecRegistry();
        registry.register(new GzipCompressionCodec());

        // @Primary 优先
        assertThat(
                        StreamMQBeanResolution.defaultCompressionCodec(
                                java.util.List.of(a, b), b, registry, "", true, "test"))
                .isSameAs(b);
        // 配置名精确匹配优先于唯一/多候选判定
        assertThat(
                        StreamMQBeanResolution.defaultCompressionCodec(
                                java.util.List.of(a, b), null, registry, "codec-a", true, "test"))
                .isSameAs(a);
        // 配置名可命中注册表内置 Codec
        assertThat(
                        StreamMQBeanResolution.defaultCompressionCodec(
                                java.util.List.of(a, b), null, registry, "gzip", true, "test"))
                .isInstanceOf(GzipCompressionCodec.class);
        // 唯一候选
        assertThat(
                        StreamMQBeanResolution.defaultCompressionCodec(
                                java.util.List.of(a), null, registry, "", true, "test"))
                .isSameAs(a);
        // 多候选 + 已开压缩 → 失败并列出候选（类名 + codec 名）
        assertThatThrownBy(
                        () ->
                                StreamMQBeanResolution.defaultCompressionCodec(
                                        java.util.List.of(a, b), null, registry, "", true, "test"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("none is @Primary")
                .hasMessageContaining("codec-a")
                .hasMessageContaining("codec-b");
        // 多候选 + 未开压缩 → 返回 null（不静默取任意一个）
        assertThat(
                        StreamMQBeanResolution.defaultCompressionCodec(
                                java.util.List.of(a, b), null, registry, "", false, "test"))
                .isNull();
        // 无候选
        assertThat(
                        StreamMQBeanResolution.defaultCompressionCodec(
                                java.util.List.of(), null, registry, "", true, "test"))
                .isNull();
        // 配置名未知 → 失败并列出候选与注册表名称
        assertThatThrownBy(
                        () ->
                                StreamMQBeanResolution.defaultCompressionCodec(
                                        java.util.List.of(a, b),
                                        null,
                                        registry,
                                        "zstd",
                                        true,
                                        "test"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("zstd")
                .hasMessageContaining("codec-a")
                .hasMessageContaining("gzip");
    }

    @Test
    @DisplayName("uniqueOrNull：多候选无 @Primary 时抛可定位异常并列出冲突 Bean 名")
    void uniqueOrNull_multipleCandidates_reportsBeanNames() {
        org.springframework.beans.factory.support.DefaultListableBeanFactory beanFactory =
                new org.springframework.beans.factory.support.DefaultListableBeanFactory();
        beanFactory.registerSingleton("codecA", new CodecA());
        beanFactory.registerSingleton("codecB", new CodecB());
        ObjectProvider<CompressionCodec> provider =
                beanFactory.getBeanProvider(CompressionCodec.class);

        assertThatThrownBy(() -> StreamMQBeanResolution.uniqueOrNull(provider, "CompressionCodec"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CompressionCodec")
                .hasMessageContaining("codecA")
                .hasMessageContaining("codecB");
    }

    @Test
    @DisplayName("uniqueOrNull：@Primary 候选在多候选时被采纳")
    void uniqueOrNull_primaryCandidateWins() {
        org.springframework.beans.factory.support.DefaultListableBeanFactory beanFactory =
                new org.springframework.beans.factory.support.DefaultListableBeanFactory();
        beanFactory.registerSingleton("codecA", new CodecA());
        beanFactory.registerBeanDefinition(
                "codecB",
                org.springframework.beans.factory.support.BeanDefinitionBuilder.rootBeanDefinition(
                                CodecB.class)
                        .setPrimary(true)
                        .getBeanDefinition());
        ObjectProvider<CompressionCodec> provider =
                beanFactory.getBeanProvider(CompressionCodec.class);

        assertThat(StreamMQBeanResolution.uniqueOrNull(provider, "CompressionCodec"))
                .isInstanceOf(CodecB.class);
    }

    /** 用户配置：注册两个自定义 Codec（无 @Primary）。 */
    @Configuration(proxyBeanMethods = false)
    static class TwoCodecs {

        @Bean
        CompressionCodec codecA() {
            return new CodecA();
        }

        @Bean
        CompressionCodec codecB() {
            return new CodecB();
        }
    }

    /** 用户配置：注册两个自定义 Codec，其中 codec-b 为 {@code @Primary}。 */
    @Configuration(proxyBeanMethods = false)
    static class TwoCodecsWithPrimary {

        @Bean
        CompressionCodec codecA() {
            return new CodecA();
        }

        @Bean
        @Primary
        CompressionCodec codecB() {
            return new CodecB();
        }
    }

    /** 测试用 Codec A（无参构造，供 Bean 工厂实例化）。 */
    public static class CodecA implements CompressionCodec {

        @Override
        public byte[] compress(byte[] data) {
            return data;
        }

        @Override
        public byte[] decompress(byte[] data) {
            return data;
        }

        @Override
        public String name() {
            return "codec-a";
        }
    }

    /** 测试用 Codec B（无参构造，供 Bean 工厂实例化）。 */
    public static class CodecB implements CompressionCodec {

        @Override
        public byte[] compress(byte[] data) {
            return data;
        }

        @Override
        public byte[] decompress(byte[] data) {
            return data;
        }

        @Override
        public String name() {
            return "codec-b";
        }
    }
}
