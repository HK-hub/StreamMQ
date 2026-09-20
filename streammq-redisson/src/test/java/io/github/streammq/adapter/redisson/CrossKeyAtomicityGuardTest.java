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
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * 跨 key 原子性守卫（架构级静态检查，R6-CLUSTER）。
 *
 * <p><b>背景（实测风险）：</b>延时入队/转投、重试与 DLQ 的调度/转投、事务 prepare 元数据都依赖 {@code REDIS_WRITE_ATOMIC} 原子批把多个
 * key「同生同死」写入。在 Redis Cluster 客户端下，Redisson 会把 这类批<b>按节点拆分提交</b>：两 key
 * 跨节点时双方各自提交成功、整体却不再原子（可能半写且不报错）， 同节点时又退化为 {@code CROSSSLOT}。同一份代码因 key 落在哪个节点而结果不同——属于静默降级。
 *
 * <p>因此每个这样的批调用点前都必须有 {@link
 * io.github.streammq.adapter.redisson.support.RedisClusterCompatibility#requireCrossKeyAtomicity}
 * 前置守卫（Cluster 配置下显式拒绝）。本测试把这条约束变成可执行检查：新增一个原子批而忘记加守卫， 或者把守卫挪到批之后，都会在这里变红。
 *
 * <p><b>防空转保护：</b>断言扫描到的原子批调用点数量不低于 {@link #MIN_EXPECTED_ATOMIC_BATCH_SITES}；
 * 否则一次重构（改常量书写形式/目录移动）会让扫描匹配为零而"绿着漏过"。
 *
 * <p><b>范围与口径：</b>扫描所有模块的 {@code src/main/java}（与 {@code CodecExplicitnessTest} 同一约定， 示例/压测工程不属于
 * SDK 运行时）；只认 {@code ExecutionMode.REDIS_WRITE_ATOMIC} 这一书写形式—— 默认模式（{@code
 * redisson.createBatch()}）不构成跨 key 事务语义，单 key 批在 Cluster 上是安全的。
 *
 * @author StreamMQ Contributors
 * @since 0.1.2
 */
@DisplayName("跨 key 原子性守卫（静态检查）")
class CrossKeyAtomicityGuardTest {

    /** 原子批标记：出现即代表该处依赖「多 key 同生同死」。 */
    private static final String ATOMIC_BATCH_MARKER = "ExecutionMode.REDIS_WRITE_ATOMIC";

    /** 守卫调用标记（含左括号，避免把 javadoc / import 里的类名算成调用）。 */
    private static final String GUARD_CALL_MARKER = "requireCrossKeyAtomicity(";

    /** 扫描覆盖的模块目录前缀（排除 target 与示例/压测工程）。 */
    private static final String MODULE_PREFIX = "streammq-";

    private static final List<String> EXCLUDED_PATH_SEGMENTS =
            List.of("target", "streammq-samples", "streammq-benchmark");

    /**
     * 原子批调用点数量下限：低于该值说明扫描本身失效（常量改名 / 目录遍历失败），必须让测试失败。 当前实测 6（延时入队 1、延时转投 1、重试/DLQ 转投 1、重试与 DLQ 调度
     * 2、事务 prepare 元数据 1）。
     */
    private static final int MIN_EXPECTED_ATOMIC_BATCH_SITES = 6;

    @Test
    @DisplayName("每个 REDIS_WRITE_ATOMIC 原子批之前都必须有 requireCrossKeyAtomicity 前置守卫")
    void everyAtomicBatchIsPrecededByGuard() {
        Path repoRoot = Path.of("..").toAbsolutePath().normalize();
        assertThat(Files.isDirectory(repoRoot)).as("测试需在模块目录下运行（仓库根 %s 不存在）", repoRoot).isTrue();

        List<String> violations = new ArrayList<>();
        int[] sites = {0};
        try (Stream<Path> modules = Files.list(repoRoot)) {
            for (Path module : modules.filter(Files::isDirectory).toList()) {
                if (!module.getFileName().toString().startsWith(MODULE_PREFIX)) {
                    continue;
                }
                Path srcRoot = module.resolve(Path.of("src", "main", "java"));
                if (!Files.isDirectory(srcRoot) || isExcluded(srcRoot)) {
                    continue;
                }
                try (Stream<Path> files = Files.walk(srcRoot)) {
                    for (Path file : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                        scan(file, violations, sites);
                    }
                }
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }

        assertThat(sites[0])
                .as(
                        "扫描到的跨 key 原子批调用点过少（%d < %d）：守卫可能已失效（常量改名或目录遍历失败）",
                        sites[0], MIN_EXPECTED_ATOMIC_BATCH_SITES)
                .isGreaterThanOrEqualTo(MIN_EXPECTED_ATOMIC_BATCH_SITES);

        assertThat(violations)
                .as(
                        "跨 key 原子批在 Cluster 上会被按节点拆分（静默失去原子性）或退化为 CROSSSLOT；每个 REDIS_WRITE_ATOMIC"
                            + " 调用点之前必须调用 RedisClusterCompatibility.requireCrossKeyAtomicity(...)"
                            + " 前置拒绝。违规：\n"
                            + "%s",
                        String.join("\n", violations))
                .isEmpty();
    }

    /** 扫描单个文件：第 i 个原子批之前必须出现第 i 次守卫调用（同一文件内按出现顺序配对）。 */
    private static void scan(Path file, List<String> violations, int[] sites) throws IOException {
        String text = Files.readString(file, StandardCharsets.UTF_8);
        List<Integer> batchLines = lineNumbers(text, ATOMIC_BATCH_MARKER);
        if (batchLines.isEmpty()) {
            return;
        }
        List<Integer> guardLines = lineNumbers(text, GUARD_CALL_MARKER);
        sites[0] += batchLines.size();
        for (int i = 0; i < batchLines.size(); i++) {
            int batchLine = batchLines.get(i);
            if (guardLines.size() <= i) {
                violations.add(
                        file + ":" + batchLine + "  →  原子批缺少配套的 requireCrossKeyAtomicity 前置守卫");
            } else if (guardLines.get(i) >= batchLine) {
                violations.add(
                        file
                                + ":"
                                + batchLine
                                + "  →  守卫出现在原子批之后（第 "
                                + guardLines.get(i)
                                + " 行），拒绝必须发生在创建批之前");
            }
        }
    }

    /** 统计 {@code marker} 出现在文本中的 1-based 行号（按出现顺序）。 */
    private static List<Integer> lineNumbers(String text, String marker) {
        List<Integer> lines = new ArrayList<>();
        int index = text.indexOf(marker);
        while (index >= 0) {
            lines.add((int) text.substring(0, index).chars().filter(c -> c == '\n').count() + 1);
            index = text.indexOf(marker, index + marker.length());
        }
        return lines;
    }

    private static boolean isExcluded(Path path) {
        for (Path segment : path) {
            if (EXCLUDED_PATH_SEGMENTS.contains(segment.toString())) {
                return true;
            }
        }
        return false;
    }
}
