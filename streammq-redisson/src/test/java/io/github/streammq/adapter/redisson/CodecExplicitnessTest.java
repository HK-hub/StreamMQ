/*
 * Copyright 2026 StreamMQ Contributors (https://github.com/HK-hub/StreamMQ)
 *
 * Licensed under the MIT License.
 */
package io.github.streammq.adapter.redisson;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 编解码显式性守卫（架构级静态检查）。
 *
 * <p><b>背景（真实回归）：</b>SDK 自有 Redis 键全部以字符串语义存取，因此必须显式传 {@code StringCodec}， 不能依赖下游 {@code
 * RedissonClient} 的全局 codec。曾出现两次真实缺陷：
 *
 * <ul>
 *   <li>业务流沿用客户端默认 {@code Kryo5Codec(registrationRequired=false)} —— 反序列化 RCE 面；
 *   <li>{@code RBatch.getScoredSortedSet(key)} 漏传 codec（书写为链式调用，肉眼/正则都容易漏）——写入的成员按 客户端 codec
 *       编码，而删除侧用 {@code StringCodec}，导致 ZREM 不匹配、事务回查条目永久残留 （由 {@code TransactionBinaryCodecIT} 在
 *       Kryo5 客户端下捕获）。
 * </ul>
 *
 * <p>本测试用静态扫描把这类缺陷挡在编译期之后、运行期之前：任何 <b>不带 codec</b> 的 Redis 结构访问都视为违规。 这是"架构约束可执行化"，而不是重复实现代码逻辑。
 */
@DisplayName("编解码显式性守卫")
class CodecExplicitnessTest {

    /** 参与值（反）序列化的 Redis 结构访问；名字型操作（getLock/getSemaphore/getScript/getKeys）不在此列。 */
    private static final Pattern RISKY_GETTER =
            Pattern.compile(
                    "[A-Za-z_][A-Za-z0-9_]*\\s*\\.\\s*(?:<[^>]*>\\s*)?"
                        + "(getStream|getMap|getScoredSortedSet|getSet|getBucket|getList|getQueue"
                        + "|getBlockingQueue|getBlockingDeque|getMapCache|getMultimap|getSortedSet)\\s*\\(");

    @Test
    @DisplayName("主源码中不存在未显式指定 codec 的 Redis 结构访问")
    void everyStructureAccessDeclaresItsCodec() {
        Path root = Path.of("src", "main", "java");
        assertThat(Files.isDirectory(root))
                .as("测试需在模块目录下运行（%s 不存在）", root.toAbsolutePath())
                .isTrue();

        List<String> violations = new ArrayList<>();
        try (Stream<Path> files = Files.walk(root)) {
            for (Path file : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                String text;
                try {
                    text = Files.readString(file, StandardCharsets.UTF_8);
                } catch (IOException ex) {
                    throw new UncheckedIOException(ex);
                }
                Matcher m = RISKY_GETTER.matcher(text);
                while (m.find()) {
                    int close = matchingCloseParen(text, m.end() - 1);
                    if (close < 0) {
                        continue;
                    }
                    String args = text.substring(m.end(), close);
                    if (args.contains("StringCodec")) {
                        continue;
                    }
                    long line =
                            text.substring(0, m.start()).chars().filter(c -> c == '\n').count() + 1;
                    violations.add(
                            file + ":" + line + "  →  " + (m.group(1) + "(" + args.strip() + ")"));
                }
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }

        assertThat(violations)
                .as(
                        "SDK 自有 Redis 键必须显式使用 StringCodec（勿依赖客户端全局 codec）；"
                                + "漏传会导致跨 codec 编码不一致（读写不匹配）并继承客户端默认 codec 的攻击面。违规：\n%s",
                        String.join("\n", violations))
                .isEmpty();
    }

    private static int matchingCloseParen(String text, int openParenIdx) {
        int depth = 0;
        for (int i = openParenIdx; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }
}
