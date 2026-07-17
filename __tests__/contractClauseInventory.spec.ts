import {readFileSync, existsSync} from 'node:fs'
import {join} from 'node:path'
import {describe, expect, test} from 'vitest'
import {batch, destroyComputed, onChange, RxList, RxMap, RxSet, TriggerInfo} from '../src/index.js'

/**
 * 契约条款账本(2026-H3 round7 反思机械化,§3.3「契约条款纪律」的载体)。
 *
 * 教训:round7 六个缺陷全部落在「README 授予调用方的权利/做出的可靠性承诺」
 * 与实现之间的窗口差上——README 在体系里的角色一直是**裁决依据**(立案与否
 * 以它为准),从未被当作**攻击面清单**(每句承诺都是待执行的测试生成器)。
 * coverageInventory/coreSurfaceInventory 让"算子 × 维度"的盲格可见,但契约
 * 文本这个面没有账本:grant 类条款(「归调用方所有」「均可」「不产生」)授予了
 * 却从未被行使,六个缺陷因此存活(R7-1 载荷 pair、R7-3 钩子抛错、R7-4
 * unhandled、R7-5 destroyComputed×Rx……)。
 *
 * 规则(机械强制):
 *   1. 每条登记条款携带 README 的**逐字引文**——README 改动该条款时引文失配,
 *      当场红,强制账本与契约文本同步(条款漂移检测);
 *   2. 引用的资产文件必须存在;
 *   3. 「语义契约」章的每个 ### 小节必须至少有一条登记(新增小节不入账当场红);
 *   4. **grant 类条款(授予调用方权利/可靠性承诺)禁止 NA**——权利必须被行使过,
 *      要么指向执行它的资产,要么显式 UNCOVERED(可见债务,review 立项来源);
 *   5. UNCOVERED 汇总打印,与两本既有账本共同构成 review 立项来源。
 *
 * kind 语义:
 *   grant      — 授予调用方的权利/对调用方的可靠性承诺(必须被行使/验证);
 *   behavior   — 库的行为承诺(常规断言面);
 *   obligation — 调用方义务(违约属契约外;资产钉扎义务边界的两侧,或 NA);
 *   boundary   — 契约边界声明(architectureSemantics 式特征钉扎,或 NA:纯文档)。
 */

type Kind = 'grant' | 'behavior' | 'obligation' | 'boundary'
type Assets = string[] | 'UNCOVERED' | `NA:${string}`
type Clause = {
    /** 所属 README 小节(用小节标题的前缀匹配,如 '1.'/'3.1'/'矩阵'/'构建') */
    section: string
    /** README 原文逐字子串(漂移检测锚点;含 markdown 标记) */
    quote: string
    kind: Kind
    assets: Assets
}

const README = readFileSync(join(__dirname, '..', 'README.md'), 'utf8')

export const CONTRACT_CLAUSES: Record<string, Clause> = {
    // ── §1 变更边界与所有权 ──
    'C1.1 零拷贝采纳(所有权移交)': {
        section: '1.', kind: 'boundary',
        quote: '构造与 `RxSet.replace` 直接采纳传入容器的引用',
        assets: ['architectureSemantics.spec.ts'],
    },
    'C1.2 data/.raw 只读视图': {
        section: '1.', kind: 'obligation',
        quote: '是**只读视图**:可以读,不可以写',
        assets: 'NA:调用方义务——绕过方法直写属契约外(A3),特征由 architectureSemantics 钉扎',
    },
    // ── §2 传播模型 ──
    'C2.1 同步急切推播': {
        section: '2.', kind: 'behavior',
        quote: 'atom 写入后**同步**执行订阅者',
        assets: ['computed.spec.ts', 'atom.spec.ts'],
    },
    'C2.2 对象 atom 浅属性写触发/深路径不触发': {
        section: '2.', kind: 'behavior',
        quote: '对象 atom 的浅属性写入会触发',
        assets: ['creationShapeMatrix.spec.ts', 'coreLifecycleGaps.spec.ts'],
    },
    'C2.3 atom 形态由创建时初值定型': {
        section: '2.', kind: 'boundary',
        quote: 'atom 的对象特性由创建时初始值的形态决定',
        assets: ['creationShapeMatrix.spec.ts'],
    },
    'C2.4 菱形 glitch 终值收敛': {
        section: '2.', kind: 'boundary',
        quote: '菱形依赖存在 glitch',
        assets: ['architectureSemantics.spec.ts'],
    },
    'C2.5 同步重算环 loud-fail': {
        section: '2.', kind: 'behavior',
        quote: '同步重算环会抛错',
        assets: ['mutationKillersComputed.spec.ts', '_coreReview16ScheduleRecompute.spec.ts'],
    },
    // grant 行使空间(R8-2 规则):承诺方向 = 隔离对"其余订阅者"成立;行使维度 =
    // 通道(内联 primitive atom/object atom/多 dep 去重/recursiveMarkDirty × batch ×
    // 结构)× 受害者(单/双抛错者的兄弟、console 上报)× recovery probe。
    // 2026-H3 round9 F1:该承诺此前只在 batch 通道存在(限定词圈住了内联通道的洞)。
    'C2.6 订阅者错误隔离跨通道一致(非 batch 内联同样不阻断兄弟)': {
        section: '2.', kind: 'grant',
        quote: '订阅者抛错不会阻断同一次派发中的其余订阅者(batch 与非 batch 一致)',
        assets: ['deepReview2026H3Round9.spec.ts', 'knownIssuesReproductions.spec.ts'],
    },
    // grant + boundary(R9 F2):平衡回写受支持(grant:值链完成、订阅者不重入自身),
    // 到达序对后订阅者非因果序是显式契约边界(boundary:delta 消费者按终态对账或用 batch)。
    'C2.7 重入写受支持,到达序只对先订阅者保因果序(delta 消费者按终态对账)': {
        section: '2.', kind: 'boundary',
        quote: 'info 到达序只对"先订阅"的消费者保持因果序',
        assets: ['deepReview2026H3Round9.spec.ts'],
    },
    // ── §3 batch ──
    'C3.1 batch 内读 computed 旧值(A2)': {
        section: '3.', kind: 'boundary',
        quote: '读到的是进入 batch 前的旧值',
        assets: ['architectureSemantics.spec.ts'],
    },
    'C3.2 batch 退出后 ≡ 终态全量重算': {
        section: '3.', kind: 'behavior',
        quote: '所有派生结构必须等于从终态 source 全量重算的结果',
        assets: ['batchReplayFuzz.spec.ts', 'modelComparisonFuzz.spec.ts'],
    },
    'C3.3 订阅者错误隔离,第一个错误抛给调用方': {
        section: '3.', kind: 'grant',
        quote: 'batch 中某个订阅者抛错不会阻断其余订阅者',
        assets: ['deepReviewFixes.spec.ts'],
    },
    'C3.4 batch 体异常优先': {
        section: '3.', kind: 'grant',
        quote: '**batch 体自身抛错时体异常优先**',
        assets: ['deepReview2026H3Round6.spec.ts'],
    },
    // ── §3.1 autorun ──
    'C3.5 autorun 默认 microtask 重跑/true 立即': {
        section: '3.1', kind: 'behavior',
        quote: '重跑经 **microtask**',
        assets: ['computed.spec.ts'],
    },
    // ── §3.2 skipIndicator ──(2026-H3 round9 F3:此前该公开参数零文档零测试,
    // 参数级表面落在导出普查/条款账本/算子账本三张网之外;成文 + 参数级普查后入账)
    'C3.6 skip 期间完全静默': {
        section: '3.2', kind: 'behavior',
        quote: '期间该 computed 对一切触发**完全静默**',
        assets: ['deepReview2026H3Round9.spec.ts'],
    },
    // grant 行使空间:承诺 = skip 窗口丢弃 info 不造成增量分叉;行使维度 = 入口
    // (computed 工厂/RxMap 构造)× 源通道(结构 METHOD/atom ATOM)× batch × 调度器形态。
    'C3.7 skip 窗口不造成增量分叉(解除后首次触发全量追平)': {
        section: '3.2', kind: 'grant',
        quote: 'skip 期间的变更不会造成增量分叉',
        assets: ['deepReview2026H3Round9.spec.ts', 'knownIssuesReproductions.spec.ts'],
    },
    'C3.8 显式 recompute 不受 skip 拦截': {
        section: '3.2', kind: 'grant',
        quote: '显式 `recompute(computed, true)` 不受拦截',
        assets: ['deepReview2026H3Round9.spec.ts'],
    },
    // ── §4 RxList 参数契约 ──
    'C4.1 splice 参数按规范归一化': {
        section: '4.', kind: 'behavior',
        quote: '参数按 `Array.prototype.splice` 规范归一化',
        assets: ['reviewFixes.spec.ts', 'broadOperatorsFuzz.spec.ts'],
    },
    'C4.2 toSorted comparator 一致全序义务(边界两侧钉扎)': {
        section: '4.', kind: 'obligation',
        quote: 'comparator 必须对元素值域构成一致全序',
        assets: ['weirdNumbersFuzz.spec.ts', 'batchReplayFuzz.spec.ts'],
    },
    'C4.3 argv 透传原始参数': {
        section: '4.', kind: 'behavior',
        quote: '`triggerInfo.argv` 透传用户原始参数',
        assets: ['deepReviewFixes.spec.ts', 'deepReview2026H3Round5.spec.ts'],
    },
    'C4.4 返回数组归调用方(载荷所有权)': {
        section: '4.', kind: 'grant',
        quote: '变更方法返回的数组归调用方所有',
        assets: ['deepReview2026H3Round5.spec.ts', 'deepReview2026H3Round7.spec.ts', 'batchReplayFuzz.spec.ts'],
    },
    'C4.5 reorder 的 order 数组连同 Order 对归调用方': {
        section: '4.', kind: 'grant',
        quote: '连同其中的 `[from, to]` 对**同理,调用后仍归调用方',
        assets: ['deepReview2026H3Round7.spec.ts', 'batchReplayFuzz.spec.ts'],
    },
    'C4.6 applyPatch 协议消费者只读共享广播': {
        section: '4.', kind: 'obligation',
        quote: '`triggerInfo.argv`/`methodResult` 只读**',
        // CAUTION 本 NA 注记曾声称"onChange/调度器出口的副本由 C4.4/C4.5 资产钉扎"
        //  ——虚假的覆盖声明(2026-H3 round8 教训):C4.4/C4.5 的资产全部行使
        //  "调用方改原始数组"方向,handler 改自己副本的方向零覆盖,reorder pair
        //  共享因此存活。观察出口的 grant 现在是独立条款 C4.11。
        assets: 'NA:下游 applyPatch 消费者义务——协议只读性由本契约承载;观察出口的副本 grant 见 C4.11',
    },
    'C4.11 观察出口收到载荷副本,可自由处置': {
        section: '4.', kind: 'grant',
        // grant 的行使空间是 行使者 × 对象 × 方向 的叉乘:C4.4/C4.5 行使的是
        // "调用方改原始载荷",本条款行使"handler/调度器深改写**自己收到的副本**
        // (含 reorder pair 与 reorderInfo)不得毒化兄弟订阅者"(R8-2),以及
        // "副本中的用户值保引用身份"(R8-3)。
        quote: '收到的也是载荷副本,可自由处置',
        assets: ['deepReview2026H3Round8.spec.ts'],
    },
    'C4.7 getKey/comparator/reduceFn 纯度契约 + dev 探测': {
        section: '4.', kind: 'obligation',
        quote: '必须是纯的确定函数',
        assets: ['deepReview2026H3Round5.spec.ts'],
    },
    'C4.8 EKC 消费 info.newValue(下游契约)': {
        section: '4.', kind: 'obligation',
        quote: '必须消费 `info.newValue`',
        assets: ['deepReview2026H3Findings.spec.ts', 'consumerContractReplay.spec.ts'],
    },
    'C4.9 set 契约与规范下标字符串归一化': {
        section: '4.', kind: 'behavior',
        quote: '规范下标字符串会归一化为 number',
        assets: ['deepReview2026H3Round5.spec.ts', 'sparseOpsFuzz.spec.ts'],
    },
    'C4.10 at 负索引与细粒度依赖': {
        section: '4.', kind: 'behavior',
        quote: '`at(index)` 支持负索引',
        assets: ['rxList.spec.ts', 'mutationKillersRxList.spec.ts'],
    },
    // ── §5 async 契约 ──
    'C5.1 async getter 只追踪首个 await 前': {
        section: '5.', kind: 'boundary',
        quote: 'async getter 只追踪第一个 `await` 之前读取的依赖',
        assets: ['asyncComputed.spec.ts'],
    },
    'C5.2 async generator loud-fail': {
        section: '5.', kind: 'behavior',
        quote: '不支持,构造时报错',
        assets: ['deepReview2026H3Round6.spec.ts'],
    },
    'C5.3 转译降级属契约外': {
        section: '5.', kind: 'boundary',
        quote: '构建工具把 async 降级转译',
        assets: 'NA:构建工具行为,本仓无法执行;README「async 契约」文档性边界',
    },
    'C5.4 挂起期间源变更排队不丢失': {
        section: '5.', kind: 'behavior',
        quote: '挂起期间到达的源变更会排队',
        assets: ['asyncPatchInterleavings.spec.ts'],
    },
    'C5.5 destroy 取消在途 async patch': {
        section: '5.', kind: 'behavior',
        quote: '**destroy 取消在途 async patch**',
        assets: ['lifecycleAndReplayFixes.spec.ts', 'destroySemantics.spec.ts'],
    },
    'C5.6 不产生 unhandled rejection': {
        section: '5.', kind: 'grant',
        quote: '不产生 unhandled rejection',
        assets: ['deepReviewFixes.spec.ts', 'deepReview2026H3Round7.spec.ts'],
    },
    'C5.7 async 收尾错误 console 兜底 + cleanPromise 照常 settle': {
        section: '5.', kind: 'grant',
        quote: 'async 收尾阶段(向订阅者派发/回退全量重算)发生的错误同样 `console.error` 兜底',
        assets: ['deepReview2026H3Round7.spec.ts'],
    },
    // ── §6 生命周期 ──
    'C6.1 谁创建谁销毁': {
        section: '6.', kind: 'behavior',
        quote: '谁创建,谁销毁',
        assets: ['destroySemantics.spec.ts'],
    },
    'C6.2 源模式 destroy 一视同仁': {
        section: '6.', kind: 'behavior',
        quote: 'destroy 对源模式与计算模式一视同仁',
        assets: ['destroySemantics.spec.ts'],
    },
    'C6.3 destroy 后只读 + 变更 no-op': {
        section: '6.', kind: 'behavior',
        quote: '已销毁实例的变更方法(`splice`/`set`/`add`/`replace` 等)一律 no-op',
        assets: ['destroySemantics.spec.ts', 'mutationKillersRxList.spec.ts'],
    },
    'C6.4 destroy 后惰性 meta 快照且零残留': {
        section: '6.', kind: 'behavior',
        quote: '销毁后读取惰性 meta(`length`/`keys`/`size` 等)返回快照值',
        assets: ['deepReview2026H3Round7.spec.ts'],
    },
    'C6.5 destroyComputed 对派生结构均可': {
        section: '6.', kind: 'grant',
        quote: '需调用 `.destroy()`(或 `destroyComputed`)',
        assets: ['deepReview2026H3Round7.spec.ts'],
    },
    'C6.6 getter 内创建被收集为 child/createDetached 隔离': {
        section: '6.', kind: 'behavior',
        quote: '会被收集为 child',
        assets: ['deepReview2026H3Round6.spec.ts', 'reviewFixes.spec.ts'],
    },
    'C6.7 map context.onCleanup 随行移动各执行一次': {
        section: '6.', kind: 'grant',
        quote: '`map` 的 `context.onCleanup` 注册行级清理',
        assets: ['rxList.spec.ts', 'deepReview2026H3Round7.spec.ts'],
    },
    'C6.8 用户回调抛错不冻结状态机': {
        section: '6.', kind: 'grant',
        quote: '**用户回调抛错不冻结状态机**',
        assets: ['deepReview2026H3Round7.spec.ts'],
    },
    // ── 支持矩阵章 ──
    'CM.1 增量格子双向可执行(零回退/必回退)': {
        section: '矩阵', kind: 'behavior',
        quote: '矩阵中声明"增量"的格子必须有差分验证',
        assets: ['incrementalityWitness.spec.ts', 'broadOperatorsFuzz.spec.ts'],
    },
    'CM.2 回退是正确性措施,结果不变': {
        section: '矩阵', kind: 'behavior',
        quote: '回退是正确性措施,结果不变',
        assets: ['batchReplayFuzz.spec.ts', 'incrementalityWitness.spec.ts'],
    },
    // ── 构建产物章 ──
    'CB.1 dev/prod 双构建经 development 条件分发': {
        section: '构建', kind: 'behavior',
        quote: 'npm 包内含 **dev/prod 双构建**',
        assets: ['knownIssuesReproductions.spec.ts'],
    },
    'CB.2 双包危害属契约外(混用 import/require)': {
        section: '构建', kind: 'boundary',
        quote: '同一进程内不要混用 `import` 与 `require`',
        assets: 'NA:进程内双模块实例的行为不属承诺面,README 文档性警示',
    },
}

// ---------------------------------------------------------------------------
// 限定词审计(2026-H3 round9 F1 教训,§3.3「契约限定词纪律」)
//
// 「batch 中某个订阅者抛错不会阻断其余订阅者」——限定词"batch 中"恰好圈住了
// 实现的洞(内联通道无隔离,digest 修复的理由文本逐字适用却只落在 digest)。
// 契约按实现现状成文时,范围限定词把实现边界固化成契约形状;而逐字锚定的
// 账本对"诚实的窄承诺"永远不红。规则:登记条款的引文命中限定词 token
// (收窄承诺范围的措辞)时,必须在本表登记**对侧语义的着落**——对侧行为的
// 资产、对侧的显式契约外/边界声明,或指向承接对侧的兄弟条款。token 表宁窄
// 勿噪(不追求捕获一切自然语言限定),按事故形状扩充;新增带限定词的承诺句
// 时对照本表过账。
const SCOPE_QUALIFIER_TOKENS = ['batch 中', '只', '仅']
const QUALIFIER_AUDIT: Record<string, {qualifier: string, counterpart: string}> = {
    'C1.2 data/.raw 只读视图': {
        qualifier: '只(读)',
        counterpart: '对侧 = 直写行为:属契约外(A3 架构语义,architectureSemantics.spec.ts 特征钉扎"绕过方法直改不触发")',
    },
    'C2.7 重入写受支持,到达序只对先订阅者保因果序(delta 消费者按终态对账)': {
        qualifier: '只对先订阅者',
        counterpart: '对侧 = 后订阅者的嵌套优先序:deepReview2026H3Round9.spec.ts 到达序特征钉扎(两侧都有资产)+ 终态对账/batch 的规避指引成文',
    },
    'C3.3 订阅者错误隔离,第一个错误抛给调用方': {
        qualifier: 'batch 中',
        counterpart: '对侧 = 非 batch 内联通道:曾无隔离且零资产(round9 F1 的立法事故本尊);现由 C2.6 承诺跨通道一致并资产化(deepReview2026H3Round9.spec.ts)',
    },
    'C4.6 applyPatch 协议消费者只读共享广播': {
        qualifier: '只读',
        counterpart: '对侧 = 消费者改写共享广播:属契约违约,毒化后果与防御边界由载荷所有权资产钉扎(deepReview2026H3Round5.spec.ts R5-D1 组、deepReview2026H3Round8.spec.ts R8-2/R8-3 组)',
    },
    'C5.1 async getter 只追踪首个 await 前': {
        qualifier: '只追踪 await 前',
        counterpart: '对侧 = await 后读取不建立依赖:asyncComputed.spec.ts 断言跨 await 读不触发;替代形态(generator getter 逐段追踪)同句成文并有 C5 族资产',
    },
}

// ---------------------------------------------------------------------------

describe('契约条款账本 conformance', () => {
    test('引文漂移检测:每条登记条款的引文必须逐字存在于 README(改条款必须同步账本)', () => {
        const missing = Object.entries(CONTRACT_CLAUSES)
            .filter(([, clause]) => !README.includes(clause.quote))
            .map(([id, clause]) => `${id}: 引文未命中 README → "${clause.quote}"`)
        expect(missing, missing.join('\n')).toEqual([])
    })

    test('资产文件必须真实存在(防账本腐化成幽灵豁免)', () => {
        const missing: string[] = []
        for (const [id, clause] of Object.entries(CONTRACT_CLAUSES)) {
            if (!Array.isArray(clause.assets)) continue
            for (const asset of clause.assets) {
                if (!existsSync(join(__dirname, asset))) missing.push(`${id} → ${asset}`)
            }
        }
        expect(missing, missing.join('\n')).toEqual([])
    })

    test('限定词审计:引文含范围限定词的条款必须登记对侧语义的着落', () => {
        const violations: string[] = []
        for (const [id, clause] of Object.entries(CONTRACT_CLAUSES)) {
            const hit = SCOPE_QUALIFIER_TOKENS.find(token => clause.quote.includes(token))
            if (hit && !QUALIFIER_AUDIT[id]) {
                violations.push(`${id}: 引文含限定词「${hit}」但未登记对侧语义 → "${clause.quote}"`)
            }
        }
        expect(violations, violations.join('\n')).toEqual([])
        // 审计表自身不得腐化:登记键必须是真实条款,counterpart 必须给出实质内容
        for (const [id, entry] of Object.entries(QUALIFIER_AUDIT)) {
            expect(CONTRACT_CLAUSES[id], `QUALIFIER_AUDIT 登记了不存在的条款:${id}`).toBeTruthy()
            expect(entry.counterpart.length, `${id} 的 counterpart 为空`).toBeGreaterThan(10)
        }
    })

    test('小节全覆盖:「语义契约」章的每个 ### 小节必须至少有一条登记条款', () => {
        // 提取「## 语义契约」与下一个 ## 之间的所有 ### 标题
        const chapter = README.slice(README.indexOf('## 语义契约'), README.indexOf('## 派生结构'))
        const sections = [...chapter.matchAll(/^### (\S+)/gm)].map(m => m[1])
        expect(sections.length).toBeGreaterThanOrEqual(7) // 自检:解析器没有静默失效
        const registered = new Set(Object.values(CONTRACT_CLAUSES).map(c => c.section))
        const uncovered = sections.filter(s => !registered.has(s))
        expect(uncovered,
            `以下小节没有任何登记条款——新增契约小节必须入账:\n${uncovered.join('\n')}`,
        ).toEqual([])
        // 矩阵与构建产物两章按章级登记
        expect(registered.has('矩阵')).toBe(true)
        expect(registered.has('构建')).toBe(true)
    })

    test('grant 类条款(授予权利/可靠性承诺)禁止 NA:权利必须被行使', () => {
        const violations = Object.entries(CONTRACT_CLAUSES)
            .filter(([, clause]) => clause.kind === 'grant' && !Array.isArray(clause.assets) && clause.assets !== 'UNCOVERED')
            .map(([id]) => id)
        expect(violations,
            `grant 类条款不允许 NA(授予了就必须有行使它的资产,或显式 UNCOVERED):\n${violations.join('\n')}`,
        ).toEqual([])
    })

    test('盲格账本:UNCOVERED 汇总(review 轮立项来源)', () => {
        const uncovered = Object.entries(CONTRACT_CLAUSES)
            .filter(([, clause]) => clause.assets === 'UNCOVERED')
            .map(([id]) => id)
        console.log(`[contractClauseInventory] 当前显式盲格 ${uncovered.length} 个:\n  ${uncovered.join('\n  ')}`)
        expect(uncovered.length).toBeLessThanOrEqual(8) // 债务只允许有界存在
    })
})

// ---------------------------------------------------------------------------
// 兄弟契约实现点差分(2026-H3 round7 模式 7 的机械化):同一契约在多个实现点
// 落地时,实现点之间的不一致本身就是缺陷信号——R7-1 的可静态发现形态正是
// 「toProtocolPayload(trigger 出口,外层)与 copyTriggerInfoPayload(onChange
// 出口,外层+一层)对同一载荷的保护深度不同」。本组把已知的兄弟对钉成行为差分。

describe('兄弟契约实现点差分', () => {
    test('载荷隔离深度跨出口一致:调用方深改写 order 后,patch 消费者与 onChange 出口都不受影响', () => {
        const source = new RxList([10, 20, 30])
        const mapped = source.map(x => x * 2)          // patch 消费者出口
        const received: TriggerInfo[][] = []
        const stopWatch = onChange(source, (infos: TriggerInfo[]) => { received.push(infos) })  // onChange 出口

        const order: [number, number][] = [[0, 1], [1, 0]]
        batch(() => {
            source.reorder(order)
            // 行使 C4.5 授予的权利:order 数组连同 pair 归调用方
            order[0][0] = 9; order[0][1] = 9
            order[1][0] = 9; order[1][1] = 9
            order.length = 0
        })
        // 出口 1:patch 消费者(map)不被毒化
        expect(source.data).toEqual([20, 10, 30])
        expect(mapped.data).toEqual([40, 20, 60])
        // 出口 2:onChange handler 收到的 argv[0] 是完好的 Order 对副本
        const reorderInfo = received.flat().find(info => info.method === 'reorder')!
        expect(reorderInfo.argv![0]).toEqual([[0, 1], [1, 0]])

        stopWatch(); mapped.destroy(); source.destroy()
    })

    test('载荷隔离深度跨出口一致(handler 方向,R8-2):handler 深改写自己的副本后,patch 消费者与后注册 handler 都不受影响', () => {
        // C4.11 的行使方向补全:round7 只测了"调用方改原始数组"(上一个测试),
        // 本测试行使"先注册的 handler 改**自己收到的副本**里的 pair/reorderInfo"
        // ——两个方向共同覆盖 grant 的行使空间(行使者 × 对象 的叉乘)。
        const source = new RxList([10, 20, 30])
        const stopHostile = onChange(source, (infos: TriggerInfo[]) => {
            for (const info of infos) {
                if (info.method === 'reorder') {
                    for (const pair of info.argv![0] as [number, number][]) { pair[0] = 0; pair[1] = 0 }
                    const ri = info.reorderInfo as {oldIndexToNewIndex?: Map<number, number>} | undefined
                    ri?.oldIndexToNewIndex?.clear()
                }
            }
        })
        const mapped = source.map(x => x * 2)
        const received: TriggerInfo[][] = []
        const stopWatch = onChange(source, (infos: TriggerInfo[]) => { received.push(infos) })

        source.swap(0, 2)
        expect(source.data).toEqual([30, 20, 10])
        expect(mapped.data).toEqual([60, 40, 20])
        const reorderInfo = received.flat().find(info => info.method === 'reorder')!
        expect(reorderInfo.argv![0]).toEqual([[0, 2], [2, 0]])

        stopHostile(); stopWatch(); mapped.destroy(); source.destroy()
    })

    test('replace 回退语义跨兄弟一致:RxMap.keys × replace 与 RxSet.toList × replace 都全量重算', () => {
        const map = new RxMap<string, number>({a: 1, b: 2})
        const keys = map.keys()
        const set = new RxSet([1, 2])
        const list = set.toList()
        let keysFulls = 0
        let listFulls = 0
        keys.on('fullRecompute', () => keysFulls++)
        list.on('fullRecompute', () => listFulls++)

        map.replace({b: 2, a: 1})   // 仅重排 entry 序
        set.replace([2, 1])          // 仅重排成员序
        // 兄弟一致:两者都以全量重建对齐新容器的迭代序
        expect(keysFulls).toBeGreaterThan(0)
        expect(listFulls).toBeGreaterThan(0)
        expect(keys.data).toEqual([...map.data.keys()])
        expect([...list.data]).toEqual([...set.data])

        list.destroy(); set.destroy()
        destroyComputed(keys); map.destroy()
    })
})
