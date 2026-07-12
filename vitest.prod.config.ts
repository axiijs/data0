import { defineConfig } from 'vitest/config'
import tsconfigPaths from 'vite-tsconfig-paths'

/**
 * 生产语义测试配置:与生产构建同款 `__DEV__: false` 编译所有源码后跑同一套测试。
 *
 * 动机(2026-H2 review):dev 构建的全局不变量断言/告警可能掩盖 prod-only 行为
 * (历史上方法 9 只做过一次性探针)。CI 上 dev/prod 双跑,语义漂移当场暴露。
 *
 * 排除的两个文件是 dev 特化测试:
 * - invariantAssertions.spec.ts 断言 __DEV__ 不变量断言会开火(prod 下被 DCE);
 * - coverage.spec.ts 是行覆盖补齐测试,直接断言 warn 等 dev-only 行为。
 */
export default defineConfig({
    define: {
        __DEV__: false
    },
    test: {
        include: ['__tests__/**/*.spec.ts'],
        exclude: ['**/node_modules/**', '__tests__/invariantAssertions.spec.ts', '__tests__/coverage.spec.ts'],
        setupFiles: ['./setupVitestEnv.ts'],
    },
    plugins: [tsconfigPaths()],
})
