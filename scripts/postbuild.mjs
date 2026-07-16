// require 条件的类型入口:node16/nodenext 解析下 CJS 消费者需要 .d.cts,
// 否则 ESM 形状的 index.d.ts 会被 TS 判为"类型伪装成 ESM"(TS1479 类错误)。
// UMD 具名导出面与 ESM 一致,直接复用同一份声明。
import {copyFileSync} from 'node:fs'
import {fileURLToPath} from 'node:url'

const dts = fileURLToPath(new URL('../dist/index.d.ts', import.meta.url))
const dcts = fileURLToPath(new URL('../dist/index.d.cts', import.meta.url))
copyFileSync(dts, dcts)
console.log('[postbuild] dist/index.d.cts written (require-condition types)')
