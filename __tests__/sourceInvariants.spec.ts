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

/**
 * R7-3 等价类的静态执法（「不变量升格」登记项,2026-H3 round7）:
 * 重算生命周期内一切用户代码窗口必须处于统一错误恢复(handleRecomputeError)
 * 的覆盖之内。prepareRecompute() 是钩子窗口的唯一汇聚点(onRecompute/onCleanup
 * 回调、context.onCleanup 清理都在里面执行),且位于 setStatus(RECOMPUTING)/
 * inPatch=true 之后——裸调用时钩子抛错会把状态机永久卡死(同步:每次写入抛
 * 误导性断言;async patch:静默冻结)。本审计强制:computed.ts 中每个
 * `this.prepareRecompute()` 调用点的前一行非空实代码必须是 `try {`(即调用
 * 被 try/catch 包裹,catch 内走 handleRecomputeError——行为面由
 * deepReview2026H3Round7 的全形态横扫执法,这里执法结构形状,防未来新增
 * 调用点绕过保护)。
 */
function findUnguardedPrepareRecompute(content: string): string[] {
    const lines = content.split('\n')
    const violations: string[] = []
    lines.forEach((line, i) => {
        const trimmed = line.trim()
        if (trimmed.startsWith('//') || trimmed.startsWith('*')) return
        if (!trimmed.includes('this.prepareRecompute()')) return
        // 定义处(prepareRecompute() {)不是调用点
        if (/prepareRecompute\(\)\s*\{/.test(trimmed)) return
        // 向上找前一行非空、非注释的实代码
        let j = i - 1
        while (j >= 0) {
            const prev = lines[j].trim()
            if (prev !== '' && !prev.startsWith('//') && !prev.startsWith('*') && !prev.startsWith('/*')) break
            j--
        }
        const prevCode = j >= 0 ? lines[j].trim() : ''
        if (prevCode !== 'try {') {
            violations.push(`computed.ts:${i + 1}: prepareRecompute() 调用未被 try 直接包裹: ${trimmed}`)
        }
    })
    return violations
}

describe('源码不变量：用户钩子窗口必须在错误恢复保护内(R7-3)', () => {
    test('computed.ts 中每个 prepareRecompute() 调用点必须被 try 直接包裹', () => {
        const computedSrc = srcFiles().find(f => f.name === 'computed.ts')!
        const violations = findUnguardedPrepareRecompute(computedSrc.content)
        expect(
            violations,
            `以下 prepareRecompute() 调用点缺少错误恢复保护(钩子抛错会永久卡死状态机,` +
            `处置见 fullRecompute/patchRecompute 的同名 try/catch):\n${violations.join('\n')}`
        ).toEqual([])
        // 可达性自检:computed.ts 里确实存在被审计的调用点(防扫描器静默失效)
        const callSites = computedSrc.content.split('\n')
            .filter(l => l.includes('this.prepareRecompute()') && !/prepareRecompute\(\)\s*\{/.test(l))
        expect(callSites.length).toBeGreaterThanOrEqual(2)
    })

    test('扫描器自检：能检出裸调用、能放过 try 包裹与定义处', () => {
        const guarded = [
            '        try {',
            '            this.prepareRecompute()',
            '        } catch (err) {',
        ].join('\n')
        expect(findUnguardedPrepareRecompute(guarded)).toEqual([])

        const unguarded = [
            '        this.inPatch = true',
            '        this.prepareRecompute()',
        ].join('\n')
        expect(findUnguardedPrepareRecompute(unguarded).length).toBe(1)

        const definition = '    prepareRecompute() {'
        expect(findUnguardedPrepareRecompute(definition)).toEqual([])

        // 注释间隔不影响判定(向上跳过注释找实代码)
        const guardedWithComment = [
            '        try {',
            '            // 说明注释',
            '            this.prepareRecompute()',
        ].join('\n')
        expect(findUnguardedPrepareRecompute(guardedWithComment)).toEqual([])
    })
})

/**
 * 清理隔离的静态执法（「不变量升格」登记项）：`isolateTeardown()` 返回 true 时追踪已被暂停，
 * 调用方必须在 finally 里 `notifier.resetTracking()`。漏掉恢复（或恢复不在 finally 里、被用户
 * 清理的抛错跳过）会让之后整段执行都不追踪依赖——effect 静默失聪，且只在少数路径上有栈深断言。
 * 本审计强制形状：调用之后（中间只允许字面量初始化的 let/const）紧跟 `try {`，与它同缩进配对的
 * `} finally {` 的第一行就是 `if (isolated) notifier.resetTracking()`。
 */
const ISOLATE_CALL_RE = /^\s*const isolated = isolateTeardown\(\)\s*$/
const LITERAL_DECL_RE = /^\s*(?:let|const)\s+\w+\s*=\s*(?:\d+|true|false|undefined)\s*$/

function findUnrestoredIsolation(content: string, file: string): string[] {
    const lines = content.split('\n')
    const violations: string[] = []
    lines.forEach((line, i) => {
        if (!line.includes('isolateTeardown()')) return
        const trimmed = line.trim()
        if (trimmed.startsWith('//') || trimmed.startsWith('*')) return
        // 定义处不是调用点
        if (/function isolateTeardown\(/.test(trimmed)) return
        if (!ISOLATE_CALL_RE.test(line)) {
            violations.push(`${file}:${i + 1}: isolateTeardown() 的返回值必须赋给 const isolated: ${trimmed}`)
            return
        }
        let j = i + 1
        while (j < lines.length && LITERAL_DECL_RE.test(lines[j])) j++
        const tryLine = lines[j] ?? ''
        if (!/^\s*try\s*\{\s*$/.test(tryLine)) {
            violations.push(`${file}:${i + 1}: isolateTeardown() 之后没有紧跟 try {`)
            return
        }
        const indent = tryLine.match(/^\s*/)![0]
        for (let k = j + 1; k < lines.length; k++) {
            if (!lines[k].startsWith(indent + '}')) continue
            const closing = lines[k].trim()
            if (closing.startsWith('} catch')) continue
            if (closing === '} finally {' && /^\s*if \(isolated\) notifier\.resetTracking\(\)\s*$/.test(lines[k + 1] ?? '')) return
            violations.push(`${file}:${i + 1}: 与 isolateTeardown() 配对的 try 没有以 \`if (isolated) notifier.resetTracking()\` 开头的 finally`)
            return
        }
        violations.push(`${file}:${i + 1}: 找不到与 isolateTeardown() 配对的 try 的结尾`)
    })
    return violations
}

describe('源码不变量：清理隔离必须在 finally 里恢复追踪', () => {
    test('src 内每个 isolateTeardown() 调用点都由 try/finally 恢复', () => {
        const files = srcFiles()
        const violations = files.flatMap(f => findUnrestoredIsolation(f.content, f.name))
        expect(violations, violations.join('\n')).toEqual([])
        // 可达性自检：确实存在被审计的调用点（防扫描器静默失效）
        const callSites = files.flatMap(f => f.content.split('\n').filter(l => ISOLATE_CALL_RE.test(l)))
        expect(callSites.length).toBeGreaterThanOrEqual(5)
    })

    test('扫描器自检：能检出缺少恢复的形态、能放过合法形态', () => {
        const ok = [
            '        const isolated = isolateTeardown()',
            '        let i = 0',
            '        try {',
            '            run()',
            '        } catch (error) {',
            '            report(error)',
            '        } finally {',
            '            if (isolated) notifier.resetTracking()',
            '        }',
        ].join('\n')
        expect(findUnrestoredIsolation(ok, 'x.ts')).toEqual([])

        const noTry = [
            '        const isolated = isolateTeardown()',
            '        run()',
            '        if (isolated) notifier.resetTracking()',
        ].join('\n')
        expect(findUnrestoredIsolation(noTry, 'x.ts').length).toBe(1)

        const callBeforeTry = [
            '        const isolated = isolateTeardown()',
            '        let i = compute()',
            '        try {',
            '        } finally {',
            '            if (isolated) notifier.resetTracking()',
            '        }',
        ].join('\n')
        expect(findUnrestoredIsolation(callBeforeTry, 'x.ts').length).toBe(1)

        const resetOutsideFinally = [
            '        const isolated = isolateTeardown()',
            '        try {',
            '            run()',
            '        } catch (error) {',
            '            if (isolated) notifier.resetTracking()',
            '            throw error',
            '        }',
        ].join('\n')
        expect(findUnrestoredIsolation(resetOutsideFinally, 'x.ts').length).toBe(1)

        const discarded = '        isolateTeardown()'
        expect(findUnrestoredIsolation(discarded, 'x.ts').length).toBe(1)

        const definition = 'export function isolateTeardown(): boolean {'
        expect(findUnrestoredIsolation(definition, 'x.ts')).toEqual([])
    })
})
