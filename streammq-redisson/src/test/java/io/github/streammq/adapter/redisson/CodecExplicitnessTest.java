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
 *
 * <p><b>扫描范围：</b>仓库内所有模块的 {@code src/main/java}（不只本模块）——starter / diagnostics 等模块同样直接访问 Redis
 * 结构，历史缺陷正是"守卫只覆盖一个模块、其它模块漂移"。
 *
 * <p><b>防空转保护：</b>断言扫描到的受保护调用点数量不低于 {@link #MIN_EXPECTED_GUARDED_SITES}； 否则一次重构（改接收者名/新增未列出的
 * getter）会让扫描匹配为零而"绿着漏过"。
 *
 * <p><b>getter 清单口径（B-11）：</b>{@link #RISKY_GETTERS} 为 pinned Redisson 3.34.1 的 {@code
 * RedissonClient} 上"可显式传入 codec"的结构访问 getter 全集（即存在 {@code (String name, Codec codec)}
 * 形参的重载）。刻意<b>不</b>纳入以下两类：
 *
 * <ul>
 *   <li>无 codec 形参的 getter（{@code getAtomicLong} / {@code getAtomicDouble} / {@code getBitSet} /
 *       {@code getLexSortedSet} / {@code getCountDownLatch} / {@code getRateLimiter} 等）：其内部固定使用
 *       StringCodec/LongCodec，客户端全局 codec 无法渗入，纳入只会产生误报；
 *   <li>该版本不存在的历史名（{@code getBitmap} / {@code getCachedMap} / {@code getGeoSortedSet}）：现代等价名 {@code
 *       getBitSet} / {@code getLocalCachedMap} / {@code getGeo} 已覆盖。
 * </ul>
 */
@DisplayName("编解码显式性守卫")
class CodecExplicitnessTest {

    /**
     * 参与值（反）序列化的 Redis 结构访问；名字型操作（getLock/getSemaphore/getScript/getKeys）不在此列。
     *
     * <p>清单来源：{@code RedissonClient}（Redisson 3.34.1）中所有带 {@code Codec} 形参的结构 getter 与 {@code
     * RBatch}/{@code RedissonClient} 的结构访问同名重载。
     */
    private static final List<String> RISKY_GETTERS =
            List.of(
                    // Key-Value / 容器
                    "getBucket",
                    "getMap",
                    "getMapCache",
                    "getMapCacheNative",
                    "getJsonBucket",
                    "getLocalCachedMap",
                    "getSet",
                    "getSetCache",
                    "getList",
                    "getQueue",
                    "getBlockingQueue",
                    "getBlockingDeque",
                    "getBoundedBlockingQueue",
                    "getDeque",
                    "getPriorityQueue",
                    "getPriorityBlockingQueue",
                    "getPriorityDeque",
                    "getPriorityBlockingDeque",
                    "getSortedSet",
                    "getScoredSortedSet",
                    "getRingBuffer",
                    "getTransferQueue",
                    "getDelayedQueue",
                    // 多维 / 特殊结构
                    "getMultimap",
                    "getListMultimap",
                    "getListMultimapCache",
                    "getSetMultimap",
                    "getSetMultimapCache",
                    "getTimeSeries",
                    "getBloomFilter",
                    "getHyperLogLog",
                    "getGeo",
                    // Stream / 消息通道
                    "getStream",
                    "getReliableTopic",
                    "getShardedTopic");

    /** 结构访问 getter 的正则前缀（接收者任意，含泛型调用形式 {@code client.<K,V>getMap(...)}）。 */
    private static final String RISKY_GETTER_PREFIX =
            "[A-Za-z_][A-Za-z0-9_]*\\s*\\.\\s*(?:<[^()]*>\\s*)?(";

    /** 结构访问 getter 的正则：前缀 + 全部 getter 名（捕获组 1 为命中的 getter 名，供违规信息展示）。 */
    private static final Pattern RISKY_GETTER =
            structureAccessPattern(RISKY_GETTERS, RISKY_GETTER_PREFIX);

    private static Pattern structureAccessPattern(List<String> getters, String prefix) {
        return Pattern.compile(prefix + String.join("|", getters) + ")\\s*\\(");
    }

    /**
     * Redisson 发布/订阅主题访问：{@code getTopic} 与业务对象的 {@code message.getTopic()} 同名， 因此单独用接收者名 {@code
     * redisson} 限定，避免误报（本仓库所有 RTopic 访问均以该字段/变量发起）。
     */
    private static final Pattern RISKY_TOPIC_GETTER =
            Pattern.compile("\\bredisson\\s*\\.\\s*(getTopic|getReliableTopic)\\s*\\(");

    /** 扫描覆盖的模块目录前缀（排除 target 与示例工程）。 */
    private static final String MODULE_PREFIX = "streammq-";

    /** 示例/压测工程不属于 SDK 运行时，排除在守卫之外。 */
    private static final List<String> EXCLUDED_PATH_SEGMENTS =
            List.of("target", "streammq-samples", "streammq-benchmark");

    /**
     * 受保护调用点数量下限：低于该值说明扫描本身失效（正则失配 / 目录遍历失败），必须让测试失败而不是静默通过。 当前实测 98（B-11 补齐 getter 清单后），取保守下限 75。
     */
    private static final int MIN_EXPECTED_GUARDED_SITES = 75;

    @Test
    @DisplayName("全部模块主源码中不存在未显式指定 codec 的 Redis 结构访问")
    void everyStructureAccessDeclaresItsCodec() {
        Path repoRoot = Path.of("..").toAbsolutePath().normalize();
        assertThat(Files.isDirectory(repoRoot)).as("测试需在模块目录下运行（仓库根 %s 不存在）", repoRoot).isTrue();

        List<String> violations = new ArrayList<>();
        int guarded = 0;
        try (Stream<Path> roots = Files.list(repoRoot)) {
            for (Path module : roots.filter(Files::isDirectory).toList()) {
                String moduleName = module.getFileName().toString();
                if (!moduleName.startsWith(MODULE_PREFIX)) {
                    continue;
                }
                for (Path srcRoot : mainSourceRoots(module)) {
                    int[] scanned = scan(srcRoot, violations);
                    guarded += scanned[0];
                }
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }

        assertThat(guarded)
                .as(
                        "扫描到的受保护 Redis 结构访问点过少（%d < %d）：守卫可能已失效（正则失配或目录遍历失败）",
                        guarded, MIN_EXPECTED_GUARDED_SITES)
                .isGreaterThanOrEqualTo(MIN_EXPECTED_GUARDED_SITES);

        assertThat(violations)
                .as(
                        "SDK 自有 Redis 键必须显式使用 StringCodec（勿依赖客户端全局 codec）；"
                                + "漏传会导致跨 codec 编码不一致（读写不匹配）并继承客户端默认 codec 的攻击面。违规：\n%s",
                        String.join("\n", violations))
                .isEmpty();
    }

    /** 收集模块下的主源码根（含 samples 之外的嵌套模块）。 */
    private static List<Path> mainSourceRoots(Path module) throws IOException {
        Path direct = module.resolve(Path.of("src", "main", "java"));
        if (Files.isDirectory(direct) && !isExcluded(direct)) {
            return List.of(direct);
        }
        List<Path> nested = new ArrayList<>();
        // 父聚合模块（如 streammq-samples）：逐个子模块查找 src/main/java
        try (Stream<Path> children = Files.list(module)) {
            for (Path child : children.filter(Files::isDirectory).toList()) {
                Path candidate = child.resolve(Path.of("src", "main", "java"));
                if (Files.isDirectory(candidate) && !isExcluded(candidate)) {
                    nested.add(candidate);
                }
            }
        }
        return nested;
    }

    private static boolean isExcluded(Path path) {
        for (Path segment : path) {
            if (EXCLUDED_PATH_SEGMENTS.contains(segment.toString())) {
                return true;
            }
        }
        return false;
    }

    /** 扫描单个源码根；返回 {@code [guardedCount]}，并把违规追加到 {@code violations}。 */
    private static int[] scan(Path srcRoot, List<String> violations) throws IOException {
        int guarded = 0;
        try (Stream<Path> files = Files.walk(srcRoot)) {
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
                        guarded++;
                        continue;
                    }
                    long line =
                            text.substring(0, m.start()).chars().filter(c -> c == '\n').count() + 1;
                    violations.add(
                            file + ":" + line + "  →  " + (m.group(1) + "(" + args.strip() + ")"));
                }
                Matcher topicMatcher = RISKY_TOPIC_GETTER.matcher(text);
                while (topicMatcher.find()) {
                    int close = matchingCloseParen(text, topicMatcher.end() - 1);
                    if (close < 0) {
                        continue;
                    }
                    String args = text.substring(topicMatcher.end(), close);
                    if (args.contains("StringCodec")) {
                        guarded++;
                        continue;
                    }
                    long line =
                            text.substring(0, topicMatcher.start())
                                            .chars()
                                            .filter(c -> c == '\n')
                                            .count()
                                    + 1;
                    violations.add(
                            file
                                    + ":"
                                    + line
                                    + "  →  "
                                    + (topicMatcher.group(1) + "(" + args.strip() + ")"));
                }
            }
        }
        return new int[] {guarded};
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
