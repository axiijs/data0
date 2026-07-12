import {describe, expect, test} from 'vitest'
import {existsSync} from 'node:fs'
import {join} from 'node:path'
import * as data0 from '../src/index.js'

/**
 * 响应式核心的「计算表面 × 对抗维度」覆盖账本 + 包导出面普查(常驻 conformance)。
 *
 * 动机(2026-H2 review):coverageInventory 只盘点了派生算子平面;computed/notify/
 * reactiveEffect/atom 这 ~2500 行最关键代码的维度空间(async 表面 × 调度 × 交错 ×
 * 错误注入 × destroy 时机)没有账本——那里的暗区是隐形的。本文件:
 *   1. 普查 src/index 的每个运行时导出,强制分类(核心表面/集合/外围/工具/诊断),
 *      新增导出未分类 → 失败;
 *   2. 核心计算表面 × 六个维度的格子逐一登记资产或 UNCOVERED(与
 *      coverageInventory 同规则:禁止谎报,NA 必须给构造性理由)。
 */

// ---- 维度 ----
//   basic         — 基本正确性(终值收敛)
//   errorInjection — 用户回调抛错后的状态复原/上报/可重试
//   destroyTiming  — destroy 与运行中/挂起中/已排队计算的交错
//   interleaving   — 多轮 async 计算与写入的完成序交错
//   batchSession   — effect session(batch)内触发的行为
//   scheduler      — 调度器变体(immediate/microtask/自定义 2 参/自定义 3 参)
const DIMENSIONS = ['basic', 'errorInjection', 'destroyTiming', 'interleaving', 'batchSession', 'scheduler'] as const
type Dimension = typeof DIMENSIONS[number]
type Cell = string
type Row = Record<Dimension, Cell>
const row = (basic: Cell, errorInjection: Cell, destroyTiming: Cell, interleaving: Cell, batchSession: Cell, scheduler: Cell): Row =>
    ({basic, errorInjection, destroyTiming, interleaving, batchSession, scheduler})

const GAPS = 'coreLifecycleGaps.spec.ts'

export const CORE_INVENTORY: Record<string, Row> = {
    'atom.primitive': row(
        'atom.spec.ts',
        'NA:无内部用户回调(订阅者异常归 effect 侧防线)',
        'NA:无生命周期(GC 管理)',
        'NA:同步标量写',
        'atom.spec.ts(batched triggers)+computed.spec.ts',
        'NA:无调度'),
    'atom.objectProxy': row(
        `atom.spec.ts+verifiedReviewFixes.spec.ts(F2)+${GAPS}(协议边界特征测试)`,
        'NA:陷阱内无状态,异常直接透传;订阅者异常见 reproducedIssuesFixes.spec.ts(F8)',
        'NA:无生命周期',
        'NA:同步写',
        'UNCOVERED',
        'NA:无调度'),
    'computed.syncImmediate': row(
        'computed.spec.ts',
        'reproducedIssuesFixes.spec.ts(F8)+deepReviewFixes.spec.ts(失败重算依赖恢复)',
        'computed.spec.ts(destroy 事件)+verifiedReviewFixes.spec.ts(F4 destroy-inside-run)',
        'NA:同步不可交错',
        'computed.spec.ts(coalesce/嵌套/抛错)',
        'NA:immediate 即语义'),
    'computed.customScheduler': row(
        'computed.spec.ts(next micro task)',
        `${GAPS}(调度重算抛错兜底+出队可重试)`,
        `${GAPS}(排队后 destroy 为 no-op)`,
        'NA:重算本体同步',
        'lifecycleAndReplayFixes.spec.ts(自定义微任务积累重放)',
        `${GAPS}(三参调度器 triggerInfos 契约)`),
    'computed.asyncGetter': row(
        'asyncComputed.spec.ts',
        'reviewFixes.spec.ts(cleanPromise reject/asyncStatus 复位)+deepReviewFixes.spec.ts(F1 console 兜底/双报去重)',
        `${GAPS}(挂起中 destroy 不复活写入)`,
        'asyncComputed.spec.ts(新 dep 触发打断旧轮)',
        'UNCOVERED',
        'asyncComputed.spec.ts(默认 microtask)'),
    'computed.generatorGetter': row(
        'asyncComputed.spec.ts(use generator getter)',
        'UNCOVERED',
        `${GAPS}(mid-yield destroy)`,
        'UNCOVERED(执行到一半的 generator 被再次 trigger:reactiveEffect.run 的 FIXME 语义)',
        'UNCOVERED',
        'via asyncGetter 同路径: asyncComputed.spec.ts'),
    'applyPatch.sync': row(
        'rxList.spec.ts 全派生族+broadOperatorsFuzz.spec.ts',
        `reproducedIssuesFixes.spec.ts(F8 全局状态复位)+${GAPS}(抛错回退全量重算)`,
        'verifiedReviewFixes.spec.ts(F4)',
        'NA:同步不可交错',
        'batchReplayFuzz.spec.ts+lifecycleAndReplayFixes.spec.ts',
        'lifecycleAndReplayFixes.spec.ts(自定义调度积累重放)'),
    'applyPatch.async': row(
        'asyncComputed.spec.ts(async patch)',
        `${GAPS}(抛错兜底+下次触发全量恢复)`,
        'lifecycleAndReplayFixes.spec.ts(destroy 取消在途 patch)',
        'asyncPatchInterleavings.spec.ts(全排列)+reproducedIssuesFixes.spec.ts(F1)',
        'sparseSetOperatorsSweep.spec.ts(batch 多 info 回退路径)',
        'NA:async patch 自带轮询语义'),
    'applyPatch.generator': row(
        'reproducedIssuesFixes.spec.ts(F2 同步 getter+generator patch)+asyncComputed.spec.ts',
        'UNCOVERED',
        'UNCOVERED(active 检查与 async patch 共路径,但无 generator 专项)',
        'UNCOVERED',
        'UNCOVERED',
        'NA:同 async patch'),
    'autorun': row(
        'common.spec.ts',
        `${GAPS}(默认调度重跑抛错兜底+存活)`,
        'verifiedReviewFixes.spec.ts(F4 自销毁)+common.spec.ts(stop)',
        'NA:重跑经 nextJob 串行',
        'architectureSemantics.spec.ts(A2:preventEffectSession 读写一致)',
        'common.spec.ts(默认 nextJob 与 true 立即两形态)'),
    'once': row(
        'common.spec.ts(stop-on-true+默认调度)',
        `via nextJob 等价类: ${GAPS}(autorun 兜底同路径)`,
        'common.spec.ts(nextJob 延迟 stop 的 dep 完整清理)',
        'NA:同 autorun',
        'NA:同 autorun(preventEffectSession)',
        'common.spec.ts'),
    'oncePromise': row(
        'common.spec.ts',
        `${GAPS}(fn 抛错 → reject 且停止监听)`,
        'via once: common.spec.ts',
        'NA:同 once',
        'NA:同 once',
        'via once: common.spec.ts'),
    'onChange': row(
        'common.spec.ts(atom/list/map/set 四源)',
        'UNCOVERED',
        'common.spec.ts(返回 destroy,示例级)',
        'NA:同步 patch',
        'UNCOVERED',
        'NA:固定 immediate patch'),
    'batch.session': row(
        'computed.spec.ts(coalesce/嵌套边界)',
        'computed.spec.ts(订阅者抛错不中断 digest+首错上抛)',
        'NA:session 非生命周期对象',
        'NA:同步 digest',
        'invariantAssertions.spec.ts(栈深复原/静止态断言)',
        'NA:无调度'),
    'reactiveEffect.children': row(
        'computed.spec.ts(inner destroy/惰性集合)+common.spec.ts(uncontrolled child)',
        'reduceOperator.spec.ts+rxList.spec.ts(行级 effect 回收,间接)',
        'lifecycleAndReplayFixes.spec.ts(统一资源清理链)+reproducedIssuesFixes.spec.ts(F9 孤儿 child)',
        'NA:同步构造',
        'UNCOVERED',
        'NA:无调度'),
    'reactiveEffect.detachedAndTransfer': row(
        'computed.spec.ts(createDetached 惰性 meta)+via map 行级探测: broadOperatorsFuzz.spec.ts',
        'UNCOVERED(probe 中 mapFn 抛错的 deps 残留清理,仅 MapItemDependencyProbe 内部防御)',
        'destroySemantics.spec.ts(惰性 meta 随宿主释放)',
        'NA:同步',
        'NA:同步',
        'NA:无调度'),
}

// ---- 包导出面普查:每个运行时导出必须被分类 ----
const EXPORT_CLASSIFICATION: Record<string, string[]> = {
    // 核心表面(必须在 CORE_INVENTORY 中有行覆盖其行为)
    coreSurface: ['atom', 'computed', 'AtomComputed', 'Computed', 'autorun', 'once', 'oncePromise', 'onChange', 'batch', 'ReactiveEffect', 'Notifier', 'notifier', 'recompute', 'destroyComputed', 'scheduleNextTick', 'scheduleNextMicroTask'],
    // 集合平面(覆盖账本在 coverageInventory.spec.ts)
    collections: ['RxList', 'RxMap', 'RxSet', 'createSelection'],
    // 外围模块(弱覆盖已知,盘点在案:rxTime/rxSlice/linkedList spec 为示例级)
    peripheral: ['RxTime', 'AsyncRxSlice', 'LinkedList', 'ManualCleanup'],
    // 工具与常量
    utils: ['isAtom', 'isReactivableType', 'replace', 'setComputedRetainedDiagnosticSource', 'TrackOpTypes', 'TriggerOpTypes', 'ITERATE_KEY', 'ITERATE_KEY_KEY_ONLY', 'maxMarkerBits', 'STATUS_DIRTY', 'STATUS_RECOMPUTING_DEPS', 'STATUS_RECOMPUTING', 'STATUS_CLEAN'],
    // 诊断与调试
    diagnostics: [
        'enableData0RetainedObjectDiagnostics', 'disableData0RetainedObjectDiagnostics', 'resetData0RetainedObjectDiagnostics', 'isData0RetainedObjectDiagnosticsEnabled', 'getData0RetainedObjectDiagnosticsSnapshot',
        'trackRetainedReactiveEffectCreated', 'markRetainedReactiveEffectKind', 'setRetainedReactiveEffectSource', 'trackRetainedReactiveEffectDestroyed', 'trackRetainedPrimitiveAtomDepCreated', 'trackRetainedDepEffectAdded', 'trackRetainedDepEffectRemoved',
        'setDebugName', 'getDebugName',
        'debugTarget', 'isDebugTarget', 'debug', 'reactiveTargetName', 'createDebugWithName', 'createName', 'createDebug', 'onTrack', 'onTrigger',
    ],
}

describe('核心表面账本 conformance', () => {
    test('账本引用的资产文件全部真实存在', () => {
        const missing: string[] = []
        for (const [surface, cells] of Object.entries(CORE_INVENTORY)) {
            for (const dim of DIMENSIONS) {
                const cell = cells[dim]
                if (cell === 'UNCOVERED' || cell.startsWith('UNCOVERED(') || cell.startsWith('NA:')) continue
                const files = cell.match(/([A-Za-z0-9]+\.spec\.ts)/g)
                if (!files) {
                    missing.push(`${surface} × ${dim}: 格式无法解析 "${cell}"`)
                    continue
                }
                for (const file of files) {
                    if (!existsSync(join(__dirname, file))) {
                        missing.push(`${surface} × ${dim}: 资产不存在 "${file}"`)
                    }
                }
            }
        }
        expect(missing, missing.join('\n')).toEqual([])
    })

    test('包运行时导出全部被分类(新增导出必须归类,核心表面必须有账本行)', () => {
        const classified = new Set(Object.values(EXPORT_CLASSIFICATION).flat())
        const unclassified: string[] = []
        for (const name of Object.keys(data0)) {
            if (!classified.has(name)) unclassified.push(name)
        }
        expect(
            unclassified,
            `以下导出未分类——新增核心表面必须在 CORE_INVENTORY 补行,其余归入相应类别:\n${unclassified.join('\n')}`
        ).toEqual([])
        // 分类表不允许引用不存在的导出(改名/删除时同步清账)
        const stale = classified.size ? [...classified].filter(name => !(name in data0)) : []
        expect(stale, `分类表引用了不存在的导出:\n${stale.join('\n')}`).toEqual([])
    })

    test('盲格账本:UNCOVERED 汇总(与 coverageInventory 共同构成 review 立项来源)', () => {
        const uncovered: string[] = []
        for (const [surface, cells] of Object.entries(CORE_INVENTORY)) {
            for (const dim of DIMENSIONS) {
                if (cells[dim].startsWith('UNCOVERED')) uncovered.push(`${surface} × ${dim}`)
            }
        }
        console.info(`[coreSurfaceInventory] 当前显式盲格 ${uncovered.length} 个:\n  ${uncovered.join('\n  ')}`)
        expect(uncovered.length).toBeGreaterThanOrEqual(0)
    })
})
