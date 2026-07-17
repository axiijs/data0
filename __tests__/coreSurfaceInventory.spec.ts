import {describe, expect, test} from 'vitest'
import {existsSync, readFileSync} from 'node:fs'
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
    // CAUTION 派发通道是独立表面(2026-H3 round9,方法 25 教训):atom 行的
    //  errorInjection 格曾以"订阅者异常归 effect 侧防线/见 F8"带过——F8 是
    //  单订阅者 × 结构通道 × 只观察抛错者与全局状态,受害者(兄弟订阅者)与
    //  内联通道两个维度都塌缩成了一个点,格子亮着但资产只覆盖格子语义空间的
    //  一角。通道语义(错误隔离/到达序/skip 门)现由下方 notify.* 两行承载。
    'atom.primitive': row(
        'atom.spec.ts',
        'deepReview2026H3Round9.spec.ts(F1:订阅者抛错 × 兄弟隔离,经 notify.inlineDispatch 行)',
        'NA:无生命周期(GC 管理)',
        'NA:同步标量写',
        'atom.spec.ts(batched triggers)+computed.spec.ts',
        'NA:无调度'),
    'atom.objectProxy': row(
        `atom.spec.ts+verifiedReviewFixes.spec.ts(F2)+${GAPS}(协议边界特征测试)`,
        'deepReview2026H3Round9.spec.ts(F1 object atom 隔离)+reproducedIssuesFixes.spec.ts(F8:全局追踪状态复原)',
        'NA:无生命周期',
        'NA:同步写',
        'coreLedgerBurndown.spec.ts',
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
        'coreLedgerBurndown.spec.ts',
        'asyncComputed.spec.ts(默认 microtask)'),
    'computed.generatorGetter': row(
        'asyncComputed.spec.ts(use generator getter)',
        'coreLedgerBurndown.spec.ts(yield 后抛错→兜底+重试)',
        `${GAPS}(mid-yield destroy)`,
        'coreLedgerBurndown.spec.ts(重入新轮,旧轮被代次丢弃)',
        'coreLedgerBurndown.spec.ts',
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
        'coreLedgerBurndown.spec.ts(抛错兜底+全量恢复)',
        'coreLedgerBurndown.spec.ts(mid-yield destroy)',
        'coreLedgerBurndown.spec.ts(挂起期间写入排队消化)',
        'coreLedgerBurndown.spec.ts(batch 多 info)',
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
        'coreLedgerBurndown.spec.ts(handler 抛错传播+错误恢复语义)',
        'common.spec.ts(返回 destroy,示例级)',
        'NA:同步 patch',
        'coreLedgerBurndown.spec.ts(batch 单次调用多 info)',
        'NA:固定 immediate patch'),
    'batch.session': row(
        'computed.spec.ts(coalesce/嵌套边界)',
        'computed.spec.ts(订阅者抛错不中断 digest+首错上抛)',
        'NA:session 非生命周期对象',
        'NA:同步 digest',
        'invariantAssertions.spec.ts(栈深复原/静止态断言)',
        'NA:无调度'),
    // 生命周期钩子是独立的错误注入表面(2026-H3 round7 R7-3 教训):onRecompute/
    // onCleanup/context.onCleanup 清理与 dirty/clean 监听者同样是库在重算周期内
    // 同步调用的用户代码,但此前按"观察设施"归类、从未进入注入表面的行集合——
    // 钩子窗口位于 setStatus(RECOMPUTING)/inPatch=true 之后,抛错曾永久卡死状态机
    // (同步:误导性断言;async patch:静默冻结)。注入用例必须附 recovery probe
    // (再触发一次并断言自愈)——冻结类缺陷在抛错当步不可见。
    'computed.lifecycleHooks(onRecompute/onCleanup/context.onCleanup/dirty/clean)': row(
        'computed.spec.ts(callbacks/onCleanup 语义)+coverage.spec.ts',
        'deepReview2026H3Round7.spec.ts(R7-3 全形态横扫:sync full/sync patch/async getter/async patch × 钩子抛错→自愈)',
        'destroySemantics.spec.ts(onDestroy 派发)+verifiedReviewFixes.spec.ts(F4)',
        'NA:钩子同步执行,交错由宿主计算形态承载',
        'deepReview2026H3Round7.spec.ts(dirty 监听者抛错×标脏先行)',
        'deepReview2026H3Round7.spec.ts(async 调度路径钩子抛错 console 兜底后自愈)'),
    'reactiveEffect.children': row(
        'computed.spec.ts(inner destroy/惰性集合)+common.spec.ts(uncontrolled child)',
        'reduceOperator.spec.ts+rxList.spec.ts(行级 effect 回收,间接)',
        'lifecycleAndReplayFixes.spec.ts(统一资源清理链)+reproducedIssuesFixes.spec.ts(F9 孤儿 child)',
        'NA:同步构造',
        'coreLedgerBurndown.spec.ts(batch 内宿主重算一次,子销毁重建恰一次)',
        'NA:无调度'),
    'reactiveEffect.detachedAndTransfer': row(
        'computed.spec.ts(createDetached 惰性 meta)+via map 行级探测: broadOperatorsFuzz.spec.ts',
        'coreLedgerBurndown.spec.ts(mapFn 抛错→传播+全量恢复+行级依赖无污染)',
        'destroySemantics.spec.ts(惰性 meta 随宿主释放)',
        'NA:同步',
        'NA:同步',
        'NA:无调度'),
    // ---- 派发通道表面(2026-H3 round9 新增维面) ----
    // 「向多个订阅者派发一次变更」这一语义有多个独立实现点(内联循环 ×4 /
    // session digest / 结构方法的恒 session),错误隔离、到达序、skip 门全部是
    // 通道条件语义——通道此前不是任何账本的行,三个 round9 缺陷全部生活在这里。
    // 同语义新增派发路径时必须入行,并与既有通道做兄弟差分(错误隔离/保序对齐)。
    'notify.inlineDispatch(非batch多订阅者内联派发:triggerEffects/trigger去重/PrimitiveAtom overflow/recursiveMarkDirty)': row(
        'deepReview2026H3Round9.spec.ts(F1 组:四循环受害者枚举)+architectureSemantics.spec.ts(A1 派发序)',
        'deepReview2026H3Round9.spec.ts(F1:首错抛写入方+兄弟 ≡ 全量+双抛错者 console+recovery probe)',
        'deepReview2026H3Round9.spec.ts(派发中 destroy 兄弟:快照迭代+active 门)',
        'deepReview2026H3Round9.spec.ts(F2:重入写到达序特征钉扎+终态对账 fuzz)',
        'deepReview2026H3Round9.spec.ts(通道对齐特征:同场景 内联 ≡ batch)',
        'deepReview2026H3Round9.spec.ts(recursiveMarkDirty 经自定义调度器 markDirty)'),
    'notify.structuralDispatch(dispatchStructuralThen/sendTriggerInfos,恒有 session)': row(
        'sourceInvariants.spec.ts(info 先于原子写的静态执法)+broadOperatorsFuzz.spec.ts(全算子差分)',
        'deepReview2026H3Round9.spec.ts(通道对齐:mapFn 抛错传播给变更调用方+兄弟派生仍更新)+reproducedIssuesFixes.spec.ts(F8)',
        'destroySemantics.spec.ts(变更中派生销毁)',
        'deepReview2026H3Round4.spec.ts(R4-1:行级触发序 × 结构 info 序)+deepReview2026H3Round9.spec.ts(RxSet 重入 ≡ 保序)',
        'batchReplayFuzz.spec.ts(多 info 单 digest 重放差分)',
        'NA:通道自身无调度形态(session 即语义)'),
    'computed.skipIndicator(触发派发的跳过门)': row(
        'deepReview2026H3Round9.spec.ts(F3 组:完全静默语义+dirty/调度器零派发)',
        'NA:门内无用户代码(skip 判断为纯字段读)',
        'UNCOVERED(destroy × skip 窗口的交错未单测;destroy 后变更本就 no-op)',
        'deepReview2026H3Round9.spec.ts(F3:skip 窗口丢 info → 解除后首次触发全量追平;含 batch 窗口)',
        'deepReview2026H3Round9.spec.ts(batch 内 skip 窗口)',
        'deepReview2026H3Round9.spec.ts(skip 期间调度器零调用+显式 recompute 逃生口)'),
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
    utils: ['isAtom', 'isReactivableType', 'replace', 'setComputedRetainedDiagnosticSource', 'TrackOpTypes', 'TriggerOpTypes', 'ITERATE_KEY', 'ITERATE_KEY_KEY_ONLY', 'METHOD_TRACK_KEY', 'EXPLICIT_KEY_CHANGE_TRACK_KEY', 'maxMarkerBits', 'STATUS_DIRTY', 'STATUS_RECOMPUTING_DEPS', 'STATUS_RECOMPUTING', 'STATUS_CLEAN'],
    // 诊断与调试
    diagnostics: [
        'enableData0RetainedObjectDiagnostics', 'disableData0RetainedObjectDiagnostics', 'resetData0RetainedObjectDiagnostics', 'isData0RetainedObjectDiagnosticsEnabled', 'getData0RetainedObjectDiagnosticsSnapshot',
        'trackRetainedReactiveEffectCreated', 'markRetainedReactiveEffectKind', 'setRetainedReactiveEffectSource', 'trackRetainedReactiveEffectDestroyed', 'trackRetainedPrimitiveAtomDepCreated', 'trackRetainedDepEffectAdded', 'trackRetainedDepEffectRemoved',
        'setDebugName', 'getDebugName',
        'debugTarget', 'isDebugTarget', 'debug', 'reactiveTargetName', 'createDebugWithName', 'createName', 'createDebug', 'onTrack', 'onTrigger',
    ],
}

// ---- 参数级公开面普查(2026-H3 round9 F3 教训) ----
// skipIndicator(computed 第 5 参)零文档、零测试、零账本登记地存活了全部 24 轮:
// 导出面普查按"运行时导出"分类,契约账本按 README 条款,覆盖账本按算子——
// **参数**落在三张网之外;mutation 审计每轮都在报它的幸存信号(skip 守卫删除类
// 变异无测试可杀),但无文档无契约的参数没有行为锚点,分类环节只能归档不能立案。
// 规则:核心工厂/构造器的每个参数必须登记 {README 逐字锚点 | UNDOCUMENTED 债务 |
// NA 构造性理由} × {行为资产 | UNCOVERED 债务 | NA}。runtimeArity 钉住 fn.length
// (新增无默认值参数当场红;带默认值的参数 fn.length 不可见,靠本表 + review 纪律)。
// UNDOCUMENTED/UNCOVERED 汇总入盲格账本,是 review 立项来源。
type ParamEntry = {
    name: string
    /** README 逐字锚点(必须命中)| `UNDOCUMENTED:<债务说明>` | `NA:<构造性理由>` */
    doc: string
    /** 行为资产 spec 文件(可加注)| 'UNCOVERED' | `NA:<理由>` */
    assets: string
}
const PARAMETER_SURFACES: Array<{surface: string, fn: Function, runtimeArity: number, params: ParamEntry[]}> = [
    {
        surface: 'atom()', fn: data0.atom as unknown as Function, runtimeArity: 3,
        params: [
            {name: 'initValue', doc: 'atom 的对象特性由创建时初始值的形态决定', assets: 'creationShapeMatrix.spec.ts(形态 × 能力矩阵)'},
            {name: 'interceptor', doc: 'UNDOCUMENTED:updater/handler 拦截器,README 无一字;半内部扩展点', assets: 'coverage.spec.ts(行覆盖级)'},
            {name: 'name', doc: 'UNDOCUMENTED:调试命名(atom.as.xxx 同源),README 无一字', assets: 'coverage.spec.ts(行覆盖级)'},
        ],
    },
    {
        surface: 'computed()', fn: data0.computed as unknown as Function, runtimeArity: 5,
        params: [
            {name: 'getter', doc: '派生值:默认急切重算', assets: 'computed.spec.ts'},
            {name: 'applyPatch', doc: '直接实现 `applyPatch` 的协议消费者拿到的是共享广播', assets: 'computed.spec.ts+digestReplay.spec.ts'},
            {name: 'dirtyCallback', doc: '自定义延迟调度器积累', assets: 'coreLifecycleGaps.spec.ts(三参调度器契约)+lifecycleAndReplayFixes.spec.ts'},
            {name: 'callbacks', doc: '`onRecompute`/`onCleanup`/`context.onCleanup` 注册的清理', assets: 'computed.spec.ts+deepReview2026H3Round7.spec.ts(R7-3 钩子抛错)'},
            {name: 'skipIndicator', doc: '`skipIndicator`(`{skip: boolean}`)', assets: 'deepReview2026H3Round9.spec.ts(F3 组)'},
        ],
    },
    {
        surface: 'autorun()', fn: data0.autorun as unknown as Function, runtimeArity: 1,
        params: [
            {name: 'fn', doc: '`autorun(fn)` 的重跑经 **microtask**', assets: 'common.spec.ts'},
            {name: 'scheduleRerun(默认 nextJob)', doc: '需要与写入同步一致时使用 `autorun(fn, true)`', assets: 'common.spec.ts(默认与 true 两形态)'},
        ],
    },
    {
        surface: 'once()', fn: data0.once as unknown as Function, runtimeArity: 1,
        params: [
            {name: 'fn', doc: 'UNDOCUMENTED:once 本体在 README 无一字(返回 true 停止监听的语义只在测试与源码注释)', assets: 'common.spec.ts(stop-on-true)'},
            {name: 'scheduleRerun(默认 nextJob)', doc: 'NA:与 autorun 同一参数语义(见 autorun 行)', assets: 'common.spec.ts'},
        ],
    },
    {
        surface: 'oncePromise()', fn: data0.oncePromise as unknown as Function, runtimeArity: 1,
        params: [
            {name: 'fn', doc: 'UNDOCUMENTED:与 once 同族,README 无一字', assets: 'common.spec.ts+coreLifecycleGaps.spec.ts(拒绝路径)'},
            {name: 'scheduleRerun(默认 nextJob)', doc: 'NA:与 autorun 同一参数语义', assets: 'common.spec.ts'},
        ],
    },
    {
        surface: 'onChange()', fn: data0.onChange as unknown as Function, runtimeArity: 2,
        params: [
            {name: 'source', doc: '观察出口(`onChange` 的 handler、自定义调度器的 infos 参数)收到的也是载荷副本', assets: 'common.spec.ts(atom/list/map/set 四源)'},
            {name: 'handler', doc: '观察出口(`onChange` 的 handler、自定义调度器的 infos 参数)收到的也是载荷副本', assets: 'deepReview2026H3Round8.spec.ts(敌意 handler 全协议形状)'},
        ],
    },
    {
        surface: 'new RxList()', fn: data0.RxList as unknown as Function, runtimeArity: 4,
        params: [
            {name: 'sourceOrGetter', doc: '`new RxList(arr)` 后 `list.data === arr`', assets: 'architectureSemantics.spec.ts(A3)+rxList.spec.ts'},
            {name: 'applyPatch', doc: 'NA:computed 模式构造与 `computed()` 同族(applyPatch/调度器/callbacks 语义同 §2/§5/§6),由全部派生算子资产覆盖', assets: 'broadOperatorsFuzz.spec.ts(全算子差分)'},
            {name: 'scheduleRecompute', doc: 'NA:同上(computed 同族)', assets: 'rxList.spec.ts(map options.scheduleRecompute)'},
            {name: 'callbacks', doc: 'NA:同上(computed 同族)', assets: 'destroySemantics.spec.ts(onDestroy)'},
        ],
    },
    {
        surface: 'new RxMap()', fn: data0.RxMap as unknown as Function, runtimeArity: 5,
        params: [
            {name: 'sourceOrGetter', doc: '构造与 `RxSet.replace` 直接采纳传入容器的引用', assets: 'rxMap.spec.ts+architectureSemantics.spec.ts(A3)'},
            {name: 'applyPatch', doc: 'NA:computed 同族(见 RxList 行)', assets: 'rxMap.spec.ts'},
            {name: 'scheduleRecompute', doc: 'NA:computed 同族', assets: 'NA:无独立行为(透传 base)'},
            {name: 'callbacks', doc: 'NA:computed 同族', assets: 'destroySemantics.spec.ts'},
            {name: 'skipIndicator', doc: '`RxMap` 构造器的第 5 参', assets: 'deepReview2026H3Round9.spec.ts(F3 入口等价类)'},
        ],
    },
    {
        surface: 'new RxSet()', fn: data0.RxSet as unknown as Function, runtimeArity: 4,
        params: [
            {name: 'sourceOrGetter', doc: '构造与 `RxSet.replace` 直接采纳传入容器的引用', assets: 'rxSet.spec.ts+architectureSemantics.spec.ts(A3)'},
            {name: 'applyPatch', doc: 'NA:computed 同族(见 RxList 行)', assets: 'rxSet.spec.ts(代数族即 patch 消费者)'},
            {name: 'scheduleRecompute', doc: 'NA:computed 同族', assets: 'NA:无独立行为(透传 base)'},
            {name: 'callbacks', doc: 'NA:computed 同族', assets: 'destroySemantics.spec.ts'},
        ],
    },
    {
        surface: 'new Computed()(公开类,autorun/once 的底座)', fn: data0.Computed as unknown as Function, runtimeArity: 6,
        params: [
            {name: 'getter', doc: 'NA:与 computed() 工厂同一语义(工厂行已过账)', assets: 'computed.spec.ts'},
            {name: 'applyPatch', doc: 'NA:同上', assets: 'computed.spec.ts'},
            {name: 'scheduleRecompute', doc: 'NA:同上', assets: 'coreLifecycleGaps.spec.ts'},
            {name: 'callbacks', doc: 'NA:同上', assets: 'computed.spec.ts'},
            {name: 'skipIndicator', doc: 'NA:同上(§3.2)', assets: 'deepReview2026H3Round9.spec.ts'},
            {name: 'preventEffectSession', doc: 'UNDOCUMENTED:autorun/once 的读写一致机制(AGENTS A2 提及,README 无一字);公开可构造', assets: 'architectureSemantics.spec.ts(A2:autorun 读写一致,间接)'},
        ],
    },
]

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

    test('参数级普查:runtimeArity 钉扎 + 文档锚点逐字命中 + 资产存在', () => {
        const README = readFileSync(join(__dirname, '..', 'README.md'), 'utf8')
        const problems: string[] = []
        for (const {surface, fn, runtimeArity, params} of PARAMETER_SURFACES) {
            if (fn.length !== runtimeArity) {
                problems.push(`${surface}: fn.length = ${fn.length} ≠ 登记的 ${runtimeArity}(新增/删除无默认值参数必须同步本表)`)
            }
            if (params.length < runtimeArity) {
                problems.push(`${surface}: 登记参数 ${params.length} 个 < runtimeArity ${runtimeArity}(有参数漏登记)`)
            }
            for (const p of params) {
                if (!p.doc.startsWith('UNDOCUMENTED:') && !p.doc.startsWith('NA:') && !README.includes(p.doc)) {
                    problems.push(`${surface}.${p.name}: 文档锚点未命中 README → "${p.doc}"`)
                }
                if (p.assets !== 'UNCOVERED' && !p.assets.startsWith('NA:')) {
                    const files = p.assets.match(/([A-Za-z0-9_]+\.spec\.ts)/g)
                    if (!files) {
                        problems.push(`${surface}.${p.name}: 资产格式无法解析 "${p.assets}"`)
                    } else {
                        for (const file of files) {
                            if (!existsSync(join(__dirname, file))) problems.push(`${surface}.${p.name}: 资产不存在 "${file}"`)
                        }
                    }
                }
            }
        }
        expect(problems, problems.join('\n')).toEqual([])
    })

    test('盲格账本:UNCOVERED 汇总(与 coverageInventory 共同构成 review 立项来源)', () => {
        const uncovered: string[] = []
        for (const [surface, cells] of Object.entries(CORE_INVENTORY)) {
            for (const dim of DIMENSIONS) {
                if (cells[dim].startsWith('UNCOVERED')) uncovered.push(`${surface} × ${dim}`)
            }
        }
        // 参数级债务(UNDOCUMENTED 文档面 / UNCOVERED 行为面)同入盲格汇总
        for (const {surface, params} of PARAMETER_SURFACES) {
            for (const p of params) {
                if (p.doc.startsWith('UNDOCUMENTED:')) uncovered.push(`${surface}.${p.name} × doc`)
                if (p.assets === 'UNCOVERED') uncovered.push(`${surface}.${p.name} × assets`)
            }
        }
        console.info(`[coreSurfaceInventory] 当前显式盲格 ${uncovered.length} 个:\n  ${uncovered.join('\n  ')}`)
        expect(uncovered.length).toBeGreaterThanOrEqual(0)
    })
})
