import {readdirSync, readFileSync} from 'node:fs'
import {join} from 'node:path'
import {describe, expect, test} from 'vitest'

/**
 * 源码级不变量静态执法（2026-H3 round4 补法，「不变量升格」规则的机械载体）。
 *
 * 教训：三个 R4 缺陷的共同形态是"兄弟代码不一致"——不变量在某处被正确编码
 * （spliceMany 的存在动机、doSplice 的 session 顺序），别处绕过而无任何机制
 * 报警。凡能静态检查的不变量在此执法；不能静态检查的走 dev 断言或构造性原语
 * （dispatchStructuralThen 先例），登记见 AGENTS「不变量升格」。
 */

const SRC_DIR = join(__dirname, '..', 'src')

function srcFiles(): Array<{name: string, content: string}> {
    return readdirSync(SRC_DIR)
        .filter(f => f.endsWith('.ts'))
        .map(name => ({name, content: readFileSync(join(SRC_DIR, name), 'utf8')}))
}

/**
 * R4-3 等价类：不定长数组 spread 进函数调用（`.splice(...arr)` 等）会撞实参/
 * 栈上限（≈10^5 量级 RangeError）。批量传参一律走数组参数版（spliceArray/
 * spliceMany）。本审计扫出所有"spread 进 splice/push/unshift/apply 调用"的
 * 行，必须命中允许清单——清单里只允许两类：
 *   1. rest 参数再散开（spread 量 = 调用方自身实参量，上限在用户调用点）；
 *   2. 有显式上限守卫的内部实现（spliceMany 的 SPLICE_SPREAD_LIMIT）。
 * 新增合法用例必须在此登记并说明有界性；无理由的新增当场失败。
 */
const SPREAD_CALL_RE = /\.(?:splice|push|unshift|apply|call)\s*\(/

const SPREAD_ALLOWLIST: Array<{file: string, snippet: string, why: string}> = [
    {file: 'RxList.ts', snippet: 'return this.splice(this.data.length, 0, ...items)', why: 'push 的 rest 参数再散开：量 = 用户实参量'},
    {file: 'RxList.ts', snippet: 'return this.splice(0, 0, ...items)', why: 'unshift 的 rest 参数再散开：量 = 用户实参量'},
    {file: 'util.ts', snippet: 'return arr.splice(start, deleteCount, ...items!)', why: 'spliceMany 内部：上方 SPLICE_SPREAD_LIMIT 守卫保证 ≤8192'},
]

// 判定一行是否把数组 spread 进了函数调用实参：
// - 注释行跳过；
// - 对象/数组字面量内的 spread（{...info} / [...xs]）不是实参 spread，
//   以"... 前的首个非空白字符是 { [ , ("来区分——, 与 ( 后的 ... 才是实参位。
function lineHasArgSpread(line: string): boolean {
    const trimmed = line.trim()
    if (trimmed.startsWith('*') || trimmed.startsWith('//') || trimmed.startsWith('/*')) return false
    const callMatch = SPREAD_CALL_RE.exec(line)
    if (!callMatch) return false
    const args = line.slice(callMatch.index + callMatch[0].length)
    let searchFrom = 0
    while (true) {
        const spreadAt = args.indexOf('...', searchFrom)
        if (spreadAt === -1) return false
        let j = spreadAt - 1
        while (j >= 0 && /\s/.test(args[j])) j--
        const before = j >= 0 ? args[j] : '(' // 行首 = 实参起点
        if (before !== '{' && before !== '[') return true
        searchFrom = spreadAt + 3
    }
}

function findSpreadCallViolations(files: Array<{name: string, content: string}>) {
    const violations: string[] = []
    for (const {name, content} of files) {
        const lines = content.split('\n')
        lines.forEach((line, i) => {
            if (!lineHasArgSpread(line)) return
            const allowed = SPREAD_ALLOWLIST.some(
                entry => entry.file === name && line.includes(entry.snippet)
            )
            if (!allowed) violations.push(`${name}:${i + 1}: ${line.trim()}`)
        })
    }
    return violations
}

describe('源码不变量：spread 进函数调用必须有界', () => {
    test('src 内 spread-into-call 全部在允许清单（新增必须登记有界性理由）', () => {
        const violations = findSpreadCallViolations(srcFiles())
        expect(
            violations,
            `以下 spread 调用未登记（R4-3 等价类：不定长数组 spread 会在 10^5 量级 RangeError，` +
            `批量传参走 spliceArray/spliceMany）：\n${violations.join('\n')}`
        ).toEqual([])
    })

    test('允许清单条目全部真实存在（防清单腐化成幽灵豁免）', () => {
        const files = srcFiles()
        const stale = SPREAD_ALLOWLIST.filter(entry => {
            const file = files.find(f => f.name === entry.file)
            return !file || !file.content.includes(entry.snippet)
        })
        expect(
            stale.map(e => `${e.file}: ${e.snippet}`),
            '允许清单指向的代码已不存在，请同步清单'
        ).toEqual([])
    })

    test('扫描器自检：能检出违例、能放过合法形态（oracle 强度证明）', () => {
        const synthetic = [{
            name: 'RxList.ts',
            content: [
                'this.splice(start, 0, ...hugeItems)',        // 违例：实参 spread
                'arr.push(...unbounded)',                     // 违例：实参 spread
                'list.push({...info, source})',               // 合法：对象字面量 spread
                'frames.push([...effects])',                  // 合法：数组字面量 spread
                '// arr.splice(0, 1, ...comment)',            // 合法：注释
                ' * `arr.splice(start, deleteCount, ...items)`', // 合法：JSDoc
            ].join('\n'),
        }]
        const violations = findSpreadCallViolations(synthetic)
        expect(violations.length).toBe(2)
        expect(violations[0]).toContain('hugeItems')
        expect(violations[1]).toContain('unbounded')
    })
})
