import { resolve } from 'path'
import { defineConfig } from 'vite'
import dts from 'vite-plugin-dts'

// 双构建(2026-H3 round6 工程面):
// - 默认构建 = 生产语义(__DEV__:false):dist/data0.js + dist/data0.umd.cjs + 类型;
// - `vite build --mode dev` = 开发语义(__DEV__:true):dist/data0.dev.js +
//   dist/data0.dev.umd.cjs(不清空 outDir、不重复产出类型)。
// package.json 的 exports 用 `development` 条件把 dev 构建交给现代打包器的开发
// 模式(Vite ≥5.1 / webpack 5 / Node --conditions=development)——纯度探测、
// 销毁后变更警告、不变量断言从此触达 npm 消费者;无条件解析(生产构建、老工具)
// 仍拿 __DEV__:false 的产物,行为与旧单构建完全一致。
export default defineConfig(({mode}) => {
    const dev = mode === 'dev'
    return {
        define: {
            __DEV__: dev
        },
        build: {
            lib: {
                // Could also be a dictionary or array of multiple entry points
                entry: resolve(__dirname, 'src/index.ts'),
                name: 'data0',
                // the proper extensions will be added
                fileName: dev ? 'data0.dev' : 'data0',
            },
            sourcemap: true,
            emptyOutDir: !dev,
        },
        plugins: dev ? [] : [dts({
            tsconfigPath: resolve(__dirname, 'tsconfig.production.json'),
            rollupTypes: true
        })]
    }
})
