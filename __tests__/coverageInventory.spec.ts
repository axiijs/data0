import {describe, expect, test} from 'vitest'
import {existsSync} from 'node:fs'
import {join} from 'node:path'
import {RxList} from '../src/RxList.js'
import {RxMap} from '../src/RxMap.js'
import {RxSet} from '../src/RxSet.js'

/**
 * 承诺面 × 对抗维度 的覆盖清单(常驻 conformance 资产)。
 *
 * 动机(2026-H2 review 教训):两个存活多轮的缺陷类都落在"攻击轴已存在、但没和
 * 该算子/值域相乘"的盲格上(重复值域 × selection、undefined 值域 × toSorted)。
 * 方法轮换有清单,覆盖矩阵却没有清单——盲格因此不可见。本文件把「每个公开派生
 * 算子 × 每个对抗维度 → 防线资产」显式化为数据,并机械强制三件事:
 *   1. 清单里引用的资产文件必须真实存在(资产改名/删除立刻失败);
 *   2. 清单键必须真实存在于原型上(算子改名立刻失败);
 *   3. 原型上每个公开成员必须被分类(派生/读取/变更/内部)——新增算子而不
 *      登记覆盖状况,本测试直接失败。
 *
 * 规则(AGENTS.md「覆盖清单纪律」):
 *   - UNCOVERED 是显式登记的已知盲格,是后续 review 轮的立项来源;禁止把
 *     无覆盖的格子谎报为有资产。
 *   - 'NA:<理由>' 表示该维度对该算子构造性不适用(如按 index 键的结构与
 *     重复值无关、Set 成员构造性唯一),理由必须站得住。
 *   - 新增对抗维度时:值域生成器加进 fuzzKit,本清单加一列,全部算子重新过账。
 */

// 对抗维度(列)。与 AGENTS.md 方法清单对应:
//   unique       — 唯一值域差分(方法 3 基线)
//   duplicates   — 重复值域差分(方法 3/10)
//   undefinedVal — undefined 作为合法元素值(方法 10)
//   sparseOOB    — 越界 set 产生的稀疏形态(方法 9)
//   batchReplay  — 多 info 单次 digest 重放(方法 7)
//   destroy      — 僵尸/泄漏对称性(方法 8)
//   weirdNum     — NaN/-0 元素值(Object.is 与 SameValueZero 分歧;2026-H2 裁定后新列)
const DIMENSIONS = ['unique', 'duplicates', 'undefinedVal', 'sparseOOB', 'batchReplay', 'destroy', 'weirdNum'] as const
type Dimension = typeof DIMENSIONS[number]

// 格子取值:资产 spec 文件名(可加注释) | 'UNCOVERED' | 'NA:<构造性理由>'
type Cell = string
type Row = Record<Dimension, Cell>

// weirdNum 列缺省 UNCOVERED:新维度先显后清,禁止默认谎报
const row = (unique: Cell, duplicates: Cell, undefinedVal: Cell, sparseOOB: Cell, batchReplay: Cell, destroy: Cell, weirdNum: Cell = 'UNCOVERED'): Row =>
    ({unique, duplicates, undefinedVal, sparseOOB, batchReplay, destroy, weirdNum})

const BROAD = 'broadOperatorsFuzz.spec.ts'
const DUP = 'duplicateValuesFuzz.spec.ts'
const H2 = 'deepReview2026H2Findings.spec.ts'
const SWEEP = 'sparseSetOperatorsSweep.spec.ts'
const BATCH = 'batchReplayFuzz.spec.ts'
const DESTROY = 'destroySemantics.spec.ts'
const MODEL = 'modelComparisonFuzz.spec.ts'
const WEIRD = 'weirdNumbersFuzz.spec.ts'

export const INVENTORY: Record<string, Row> = {
    // ---- RxList 派生 ----
    'RxList.map':            row(BROAD, DUP, H2, SWEEP, BATCH, DESTROY, WEIRD),
    'RxList.filter':         row(BROAD, DUP, H2, SWEEP, BATCH, DESTROY, WEIRD),
    'RxList.toSorted':       row(BROAD, DUP, H2, SWEEP, BATCH, DESTROY, `${WEIRD}(NaN-aware comparator;裸数值 comparator × NaN 属契约外,见 README)`),
    'RxList.slice':          row(BROAD, DUP, H2, SWEEP, BATCH, DESTROY, 'NA:按区间位置增量,不按值定位'),
    'RxList.concat':         row(BROAD, DUP, H2, `${SWEEP}+deepReview2026H3Round3.spec.ts(段长跳变回退)`, BATCH, DESTROY, 'NA:按段位置增量,不按值定位'),
    'RxList.groupBy':        row(BROAD, DUP, H2, SWEEP, BATCH, DESTROY, WEIRD),
    'RxList.toSet':          row(BROAD, DUP, H2, SWEEP, BATCH, DESTROY, WEIRD),
    'RxList.findIndex':      row(`${BROAD}(含响应式谓词)`, DUP, H2, SWEEP, BATCH, DESTROY, WEIRD),
    'RxList.find':           row(`via findIndex: ${BROAD}`, `via findIndex: ${DUP}`, `via findIndex: ${H2}`, SWEEP, `via findIndex: ${BATCH}`, DESTROY, `via findIndex: ${WEIRD}`),
    'RxList.some':           row(`via findIndex: ${BROAD}`, `via findIndex: ${DUP}`, `via findIndex: ${H2}`, `via findIndex: ${SWEEP}`, `via findIndex: ${BATCH}`, DESTROY, `via findIndex: ${WEIRD}`),
    'RxList.every':          row(`via findIndex: ${BROAD}`, `via findIndex: ${DUP}`, `via findIndex: ${H2}`, `via findIndex: ${SWEEP}`, `via findIndex: ${BATCH}`, DESTROY, `via findIndex: ${WEIRD}`),
    'RxList.indexBy':        row('rxList.spec.ts(示例级)', 'NA:key 唯一性契约(重复 key 断言拒绝)', 'collectionLedgerBurndown2.spec.ts(undefined 行跳过)', `${SWEEP}+collectionLedgerBurndown2.spec.ts(属性形式修复回归)`, 'collectionLedgerBurndown.spec.ts(删+插同 key)', DESTROY, 'collectionLedgerBurndown2.spec.ts(NaN key)'),
    'RxList.toMap':          row('rxList.spec.ts(示例级)', 'NA:key 唯一性契约(重复 key 断言拒绝)', 'collectionLedgerBurndown2.spec.ts(undefined value 元组)', 'collectionLedgerBurndown2.spec.ts(OOB set 修复回归)', 'collectionLedgerBurndown.spec.ts(删+插同 key)', 'collectionLedgerBurndown.spec.ts(僵尸检查)', 'collectionLedgerBurndown2.spec.ts(NaN key 元组)'),
    'RxList.reduce':         row('reduceOperator.spec.ts(示例级差分)', 'reduceOperator.spec.ts(示例级差分)', 'collectionLedgerBurndown2.spec.ts', 'collectionLedgerBurndown2.spec.ts', 'collectionLedgerBurndown.spec.ts(batch 尾追加/混合)+deepReview2026H3Round2.spec.ts(越界 clamp 尾插按操作时长度)', 'reduceOperator.spec.ts(僵尸检查)', 'collectionLedgerBurndown2.spec.ts(NaN/-0 透传)'),
    'RxList.reduceToAtom':   row('rxList.spec.ts(示例级)', 'collectionLedgerBurndown2.spec.ts', 'collectionLedgerBurndown2.spec.ts', SWEEP, 'collectionLedgerBurndown2.spec.ts(batch 混合回退)', DESTROY, 'collectionLedgerBurndown2.spec.ts'),
    'RxList.length':         row(BROAD, 'NA:值无关(只依赖结构长度)', 'NA:值无关(只依赖结构长度)', SWEEP, BATCH, DESTROY, 'NA:值无关(只依赖结构长度)'),
    'RxList.createSelection':         row('createSelection.spec.ts(示例级)', H2, 'collectionLedgerBurndown.spec.ts(undefined item)', SWEEP, MODEL, DESTROY, 'collectionLedgerBurndown.spec.ts(NaN item 含孪生行)'),
    'RxList.createSelections':        row('createSelection.spec.ts(示例级)', H2, 'collectionLedgerBurndown2.spec.ts(undefined item)', 'collectionLedgerBurndown2.spec.ts', 'collectionLedgerBurndown.spec.ts(batch splice+选中集)', 'collectionLedgerBurndown.spec.ts(含 autoReset 清理)', 'collectionLedgerBurndown2.spec.ts(NaN 孪生行)'),
    'RxList.createIndexKeySelection': row('createSelection.spec.ts(示例级)', 'NA:按 index 键(与元素值无关)', 'collectionLedgerBurndown.spec.ts(undefined 行内容)', `${SWEEP}+deepReview2026H3Round3.spec.ts(校正循环洞位)`, 'collectionLedgerBurndown.spec.ts(batch 不等长 splice)', DESTROY, 'collectionLedgerBurndown2.spec.ts(NaN/-0 行内容)'),

    // ---- RxMap 派生 ----
    'RxMap.keys':    row(BROAD, 'NA:Map key 构造性唯一', 'collectionLedgerBurndown.spec.ts(undefined value)', 'NA:非数组结构', MODEL, DESTROY, 'reproducedIssuesFixes.spec.ts(F10 delete(NaN),示例级)'),
    'RxMap.values':  row(BROAD, 'collectionLedgerBurndown.spec.ts(同 value 多 key)', 'collectionLedgerBurndown.spec.ts(undefined value)', 'NA:非数组结构', MODEL, DESTROY, 'collectionLedgerBurndown2.spec.ts(NaN key/value)'),
    'RxMap.entries': row(BROAD, 'collectionLedgerBurndown.spec.ts(同 value 多 key)', 'collectionLedgerBurndown.spec.ts(undefined value)', 'NA:非数组结构', MODEL, DESTROY, 'collectionLedgerBurndown2.spec.ts(NaN key/value)'),
    'RxMap.size':    row(BROAD, 'NA:值无关(只依赖成员数)', 'NA:值无关(只依赖成员数)', 'NA:非数组结构', MODEL, DESTROY, 'NA:值无关(只依赖成员数)'),

    // ---- RxSet 派生 ----
    'RxSet.difference':          row(`${BROAD}(replace 含重复入参)`, 'NA:Set 成员构造性唯一', 'collectionLedgerBurndown.spec.ts(undefined 成员)', 'NA:非数组结构', MODEL, DESTROY, 'lifecycleAndReplayFixes.spec.ts(replace 含 NaN,示例级)'),
    'RxSet.intersection':        row(`${BROAD}(replace 含重复入参)`, 'NA:Set 成员构造性唯一', 'collectionLedgerBurndown.spec.ts(undefined 成员)', 'NA:非数组结构', MODEL, DESTROY, 'collectionLedgerBurndown2.spec.ts'),
    'RxSet.symmetricDifference': row(`${BROAD}(replace 含重复入参)`, 'NA:Set 成员构造性唯一', 'collectionLedgerBurndown.spec.ts(undefined 成员)', 'NA:非数组结构', MODEL, DESTROY, 'collectionLedgerBurndown2.spec.ts'),
    'RxSet.union':               row(`${BROAD}(replace 含重复入参)`, 'NA:Set 成员构造性唯一', 'collectionLedgerBurndown.spec.ts(undefined 成员)', 'NA:非数组结构', MODEL, DESTROY, 'collectionLedgerBurndown2.spec.ts'),
    'RxSet.toList':              row(`${BROAD}(replace 含重复入参)`, 'NA:Set 成员构造性唯一', 'collectionLedgerBurndown.spec.ts(undefined 成员)', 'NA:非数组结构', MODEL, DESTROY, 'reproducedIssuesFixes.spec.ts(F10 delete(NaN),示例级)'),
    'RxSet.has':                 row('rxSet.spec.ts(示例级)', 'NA:Set 成员构造性唯一', 'collectionLedgerBurndown2.spec.ts(has(undefined))', 'NA:非数组结构', 'collectionLedgerBurndown2.spec.ts(batch 谓词)', 'collectionLedgerBurndown.spec.ts(随宿主销毁)', 'collectionLedgerBurndown2.spec.ts(has(NaN))'),
    'RxSet.size':                row(BROAD, 'NA:值无关(只依赖成员数)', 'NA:值无关(只依赖成员数)', 'NA:非数组结构', MODEL, 'collectionLedgerBurndown.spec.ts(随宿主销毁)', 'NA:值无关(只依赖成员数)'),
    'RxSet.isSubsetOf':          row('rxSet.spec.ts(示例级)', 'NA:Set 成员构造性唯一', 'collectionLedgerBurndown2.spec.ts', 'NA:非数组结构', 'collectionLedgerBurndown2.spec.ts(batch 谓词)', DESTROY, 'collectionLedgerBurndown2.spec.ts'),
    'RxSet.isSupersetOf':        row('via isSubsetOf: rxSet.spec.ts(示例级)', 'NA:Set 成员构造性唯一', 'collectionLedgerBurndown2.spec.ts', 'NA:非数组结构', 'via isSubsetOf: collectionLedgerBurndown2.spec.ts', 'collectionLedgerBurndown.spec.ts(宿主销毁后源变更安全)', 'via isSubsetOf: collectionLedgerBurndown2.spec.ts'),
    'RxSet.isDisjointFrom':      row('rxSet.spec.ts(示例级)', 'NA:Set 成员构造性唯一', 'collectionLedgerBurndown2.spec.ts', 'NA:非数组结构', 'collectionLedgerBurndown2.spec.ts(batch 谓词)', DESTROY, 'collectionLedgerBurndown2.spec.ts'),
}

// 原型上的非派生成员分类(新增公开方法必须归入某一类,否则测试失败)
const READS: Record<string, string[]> = {
    RxList: ['at', 'forEach', 'toArray'],
    RxMap: ['get', 'forEach'],
    RxSet: ['forEach', 'toArray'],
}
const MUTATIONS: Record<string, string[]> = {
    RxList: ['push', 'pop', 'shift', 'unshift', 'splice', 'spliceArray', 'set', 'clear', 'reorder', 'reposition', 'swap', 'sortSelf', 'replaceData'],
    RxMap: ['set', 'delete', 'clear'],
    RxSet: ['add', 'delete', 'clear', 'replace', 'replaceData'],
}
const INTERNAL: Record<string, string[]> = {
    RxList: ['constructor', 'doSplice', 'ensureAtomIndex', 'addAtomIndexesDep', 'removeAtomIndexesDep', 'pruneIndexKeyDeps', 'onUntrack', 'destroyResources', 'raw', 'indexKeyDeps'],
    RxMap: ['constructor', 'destroyResources'],
    RxSet: ['constructor', 'destroyResources'],
}

const CLASSES: Record<string, any> = {RxList: RxList.prototype, RxMap: RxMap.prototype, RxSet: RxSet.prototype}

describe('覆盖清单 conformance', () => {
    test('清单引用的资产文件全部真实存在', () => {
        const missing: string[] = []
        for (const [operator, cells] of Object.entries(INVENTORY)) {
            for (const dim of DIMENSIONS) {
                const cell = cells[dim]
                if (cell === 'UNCOVERED' || cell.startsWith('NA:')) continue
                // 允许 'via xxx: file.spec.ts(注)' 与 'file.spec.ts(注)' 形态
                const fileMatch = cell.match(/([A-Za-z0-9]+\.spec\.ts)/)
                if (!fileMatch) {
                    missing.push(`${operator} × ${dim}: 格式无法解析 "${cell}"`)
                    continue
                }
                if (!existsSync(join(__dirname, fileMatch[1]))) {
                    missing.push(`${operator} × ${dim}: 资产不存在 "${fileMatch[1]}"`)
                }
            }
        }
        expect(missing, missing.join('\n')).toEqual([])
    })

    test('清单键全部真实存在于原型(算子改名会在此失败)', () => {
        for (const key of Object.keys(INVENTORY)) {
            const [className, method] = key.split('.')
            const proto = CLASSES[className]
            expect(proto, `未知类 ${className}`).toBeTruthy()
            const descriptor = Object.getOwnPropertyDescriptor(proto, method)
            expect(descriptor, `${key} 不在 ${className}.prototype 上`).toBeTruthy()
        }
    })

    test('原型公开成员全部被分类(新增算子必须登记覆盖状况)', () => {
        const unclassified: string[] = []
        for (const [className, proto] of Object.entries(CLASSES)) {
            const inventoried = new Set(
                Object.keys(INVENTORY)
                    .filter(k => k.startsWith(className + '.'))
                    .map(k => k.split('.')[1])
            )
            const known = new Set([
                ...inventoried,
                ...(READS[className] ?? []),
                ...(MUTATIONS[className] ?? []),
                ...(INTERNAL[className] ?? []),
            ])
            for (const name of Object.getOwnPropertyNames(proto)) {
                if (!known.has(name)) {
                    unclassified.push(`${className}.${name}`)
                }
            }
        }
        expect(
            unclassified,
            `以下成员未分类——新增派生算子必须在 INVENTORY 登记每个对抗维度的覆盖资产(或显式 UNCOVERED/NA):\n${unclassified.join('\n')}`
        ).toEqual([])
    })

    test('盲格账本:UNCOVERED 汇总(review 轮立项来源)', () => {
        const uncovered: string[] = []
        for (const [operator, cells] of Object.entries(INVENTORY)) {
            for (const dim of DIMENSIONS) {
                if (cells[dim] === 'UNCOVERED') uncovered.push(`${operator} × ${dim}`)
            }
        }
        // 不 fail:盲格是显式登记的债务。这里输出总账,涨落都可见于 diff。
        console.info(`[coverageInventory] 当前显式盲格 ${uncovered.length} 个:\n  ${uncovered.join('\n  ')}`)
        expect(uncovered.length).toBeGreaterThanOrEqual(0)
    })
})
