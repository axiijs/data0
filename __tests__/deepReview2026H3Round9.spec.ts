/**
 * 2026-H3 第九轮深度 review(方法 25:派发通道敌意差分)的等价类资产。
 *
 * 攻击轴:同一「多订阅者 × 敌意场景(订阅者抛错 / 派发中重入写)」在三条派发
 * 通道下执行并对照可观察语义——
 *   ① 非 batch atom 写的内联通道(triggerEffects/triggerPrimitiveAtomValue 循环);
 *   ② batch 的 session digest 通道;
 *   ③ Rx 结构变更方法的结构通道(dispatchStructuralThen/sendTriggerInfos,恒有 session)。
 * ②③ 对两个敌意场景都有防线(逐 effect try/catch;重入写 info 追加队尾保持因果序),
 * ① 曾双双缺失;skipIndicator(F3)是通道上的"门",其丢弃语义同样未与增量阶段对账。
 *
 * 本文件按 AGENTS §3.1 承载三个缺陷类的等价类横扫:
 *   F1 内联派发错误隔离 —— 四个多订阅者派发循环 × 受害者枚举(兄弟订阅者 ≡ 全量重算
 *      + 首错抛给写入方 + 其余 console.error + recovery probe);
 *   F2 selection 终态对账 —— 两模式(atom 单选/RxSet 多选)× 三通道 × 订阅顺序 ×
 *      重入链 × NaN 值域 + 到达序特征钉扎(README §2 边界)+ 固定 seed 差分 fuzz;
 *   F3 skipIndicator —— 全部入口(computed 工厂/RxMap 构造)× 源通道(结构/atom)×
 *      调度形态 × batch × 显式 recompute 逃生口(README §3.2)。
 */
import {describe, expect, test, vi} from 'vitest'
import {atom} from '../src/atom.js'
import {autorun, onChange} from '../src/common.js'
import {computed, Computed, destroyComputed, getComputedInternal, recompute} from '../src/computed.js'
import {batch, ITERATE_KEY, notifier, TriggerInfo} from '../src/notify.js'
import {TrackOpTypes, TriggerOpTypes} from '../src/operations.js'
import {createIndexKeySelection, createSelection, createSelections, RxList} from '../src/RxList.js'
import {RxMap} from '../src/RxMap.js'
import {RxSet} from '../src/RxSet.js'
import type {Atom} from '../src/atom.js'
import {installEquilibriumRewriter, mulberry32} from './fuzzKit.js'

// ============================================================
// F1:内联派发错误隔离(四个多订阅者派发循环 × 受害者枚举)
// ============================================================
describe('F1: 内联派发错误隔离 —— 兄弟订阅者 ≡ 全量重算', () => {
    test('primitive atom(overflow 循环):首订阅者抛错,兄弟仍执行;首错抛给写入方;recovery probe', () => {
        const a = atom(0)
        let boomOnce = true
        const c1 = computed(() => {
            const v = a() as number
            if (v === 1 && boomOnce) { boomOnce = false; throw new Error('first boom') }
            return v
        })
        const c2 = computed(() => (a() as number) * 2)
        const c3 = computed(() => (a() as number) + 100)
        try {
            expect(() => a(1)).toThrow('first boom')
            // 受害者枚举:抛错者之后的全部订阅者必须已执行(≡ 从终态全量重算)
            expect(c2.raw).toBe(2)
            expect(c3.raw).toBe(101)
            // recovery probe(round7 通则):再触发一次,全员追平(含抛错者自身)
            a(2)
            expect(c1.raw).toBe(2)
            expect(c2.raw).toBe(4)
            expect(c3.raw).toBe(102)
        } finally {
            destroyComputed(c1); destroyComputed(c2); destroyComputed(c3)
        }
    })

    test('primitive atom:两个订阅者都抛错 —— 首错抛给写入方,第二个 console.error 上报', () => {
        const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
        const a = atom(0)
        let boom1 = true
        let boom2 = true
        const c1 = computed(() => {
            const v = a() as number
            if (v === 1 && boom1) { boom1 = false; throw new Error('boom-1') }
            return v
        })
        const c2 = computed(() => {
            const v = a() as number
            if (v === 1 && boom2) { boom2 = false; throw new Error('boom-2') }
            return v
        })
        const c3 = computed(() => (a() as number) - 1)
        try {
            expect(() => a(1)).toThrow('boom-1')
            expect(c3.raw).toBe(0) // 第三个订阅者不被两个抛错者牵连
            expect(consoleError.mock.calls.some(args =>
                String(args[0]).includes('suppressed additional subscriber error') && String(args[1]?.message ?? args[1]).includes('boom-2')
            )).toBe(true)
        } finally {
            consoleError.mockRestore()
            destroyComputed(c1); destroyComputed(c2); destroyComputed(c3)
        }
    })

    test('object atom(triggerEffects 循环):浅属性写入的订阅者抛错同样隔离;双抛错者 console 上报', () => {
        const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
        const obj = atom<{n: number}>({n: 0})
        let boom1 = true
        let boom2 = true
        const c1 = computed(() => {
            const v = (obj as any).n as number
            if (v === 1 && boom1) { boom1 = false; throw new Error('proxy boom') }
            return v
        })
        const c2 = computed(() => {
            const v = (obj as any).n as number
            if (v === 1 && boom2) { boom2 = false; throw new Error('proxy boom 2') }
            return v
        })
        const c3 = computed(() => ((obj as any).n as number) * 2)
        try {
            expect(() => { (obj as any).n = 1 }).toThrow('proxy boom')
            expect(c3.raw).toBe(2)
            expect(consoleError.mock.calls.some(args =>
                String(args[0]).includes('suppressed additional subscriber error') && String(args[1]?.message ?? args[1]).includes('proxy boom 2')
            )).toBe(true)
        } finally {
            consoleError.mockRestore()
            destroyComputed(c1); destroyComputed(c2); destroyComputed(c3)
        }
    })

    test('多 dep 去重循环(手动 notifier.trigger 的自定义结构协议):key dep 订阅者抛错,ITERATE 订阅者仍执行', () => {
        // LinkedList 式用法:非 Rx 结构 + 手动 trigger(不经 session),SET 同时命中
        // key dep 与 ITERATE dep → 走 trigger() 的 dedupedEffects 内联循环
        const target = {}
        let counter = 0
        let boomOnce = true
        const byKey = computed(function (this: Computed) {
            ;(this as any).manualTrack(target, TrackOpTypes.GET, 'k')
            if (counter === 1 && boomOnce) { boomOnce = false; throw new Error('key-sub boom') }
            return counter
        })
        const byIterate = computed(function (this: Computed) {
            ;(this as any).manualTrack(target, TrackOpTypes.ITERATE, ITERATE_KEY)
            return counter
        })
        try {
            counter = 1
            expect(() => notifier.trigger(target, TriggerOpTypes.SET, {key: 'k'})).toThrow('key-sub boom')
            expect(byIterate.raw).toBe(1) // 兄弟订阅者(ITERATE dep)不被跳过
        } finally {
            destroyComputed(byKey); destroyComputed(byIterate)
        }
    })

    test('多 dep 去重循环:双抛错者 —— 首错抛出,第二个 console 上报,幸存者一致', () => {
        const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
        const target = {}
        let counter = 0
        let boom1 = true
        let boom2 = true
        const byKey = computed(function (this: Computed) {
            ;(this as any).manualTrack(target, TrackOpTypes.GET, 'k')
            if (counter === 1 && boom1) { boom1 = false; throw new Error('dedup boom 1') }
            return counter
        })
        const byIterate = computed(function (this: Computed) {
            ;(this as any).manualTrack(target, TrackOpTypes.ITERATE, ITERATE_KEY)
            if (counter === 1 && boom2) { boom2 = false; throw new Error('dedup boom 2') }
            return counter
        })
        const survivor = computed(function (this: Computed) {
            ;(this as any).manualTrack(target, TrackOpTypes.ITERATE, ITERATE_KEY)
            return counter * 10
        })
        try {
            counter = 1
            expect(() => notifier.trigger(target, TriggerOpTypes.SET, {key: 'k'})).toThrow('dedup boom 1')
            expect(survivor.raw).toBe(10)
            expect(consoleError.mock.calls.some(args =>
                String(args[0]).includes('suppressed additional subscriber error') && String(args[1]?.message ?? args[1]).includes('dedup boom 2')
            )).toBe(true)
        } finally {
            consoleError.mockRestore()
            destroyComputed(byKey); destroyComputed(byIterate); destroyComputed(survivor)
        }
    })

    test('recursiveMarkDirty 循环(自定义调度器的 markDirty):首个下游抛错,其余下游仍重跑', () => {
        const a = atom(0)
        const scheduled = computed(() => a(), undefined, (recomputeFn, markDirty) => {
            // 只标脏不重算:markDirty 广播给下游(recursiveMarkDirty 的公开消费形态)
            markDirty()
        })
        // 初始化窗口不抛错(computed 构造即运行),armed 之后的 markDirty 重跑才抛
        let armed = false
        const d1 = computed(() => {
            const v = scheduled() as number
            if (armed) { armed = false; throw new Error('markDirty boom') }
            return v
        })
        let d2Runs = 0
        const d2 = computed(() => { d2Runs++; return scheduled() })
        const d2RunsBefore = d2Runs
        try {
            armed = true
            expect(() => a(1)).toThrow('markDirty boom')
            expect(d2Runs).toBe(d2RunsBefore + 1) // 兄弟下游仍被标脏重跑
        } finally {
            destroyComputed(d1); destroyComputed(d2); destroyComputed(scheduled)
        }
    })

    test('recursiveMarkDirty 循环:双抛错者 —— 首错抛出,第二个 console 上报', () => {
        const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
        const a = atom(0)
        const scheduled = computed(() => a(), undefined, (recomputeFn, markDirty) => { markDirty() })
        let armed1 = false
        let armed2 = false
        const d1 = computed(() => {
            const v = scheduled() as number
            if (armed1) { armed1 = false; throw new Error('md boom 1') }
            return v
        })
        const d2 = computed(() => {
            const v = scheduled() as number
            if (armed2) { armed2 = false; throw new Error('md boom 2') }
            return v
        })
        let d3Runs = 0
        const d3 = computed(() => { d3Runs++; return scheduled() })
        const before = d3Runs
        try {
            armed1 = true; armed2 = true
            expect(() => a(1)).toThrow('md boom 1')
            expect(d3Runs).toBe(before + 1)
            expect(consoleError.mock.calls.some(args =>
                String(args[0]).includes('suppressed additional subscriber error') && String(args[1]?.message ?? args[1]).includes('md boom 2')
            )).toBe(true)
        } finally {
            consoleError.mockRestore()
            destroyComputed(d1); destroyComputed(d2); destroyComputed(d3); destroyComputed(scheduled)
        }
    })

    test('内联派发中 destroy 兄弟订阅者:快照迭代 + active 门,无崩溃、幸存者一致', () => {
        const a = atom(0)
        let victim: ReturnType<typeof computed> | undefined
        const killer = computed(() => {
            if ((a() as number) === 1 && victim) destroyComputed(victim)
            return a()
        })
        victim = computed(() => (a() as number) * 2)
        const survivor = computed(() => (a() as number) + 10)
        try {
            expect(() => a(1)).not.toThrow()
            expect(survivor.raw).toBe(11)
            expect(victim.raw).toBe(0) // 已销毁:停留销毁当刻快照
        } finally {
            destroyComputed(killer); destroyComputed(survivor)
        }
    })

    test('通道对齐特征:同一抛错场景在内联/batch/结构通道下兄弟结局一致', () => {
        const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
        try {
            const run = (channel: 'inline' | 'batch') => {
                const a = atom(0)
                let boomOnce = true
                const c1 = computed(() => {
                    const v = a() as number
                    if (v === 1 && boomOnce) { boomOnce = false; throw new Error(`${channel} boom`) }
                    return v
                })
                const c2 = computed(() => (a() as number) * 2)
                let caught: unknown
                try {
                    channel === 'inline' ? a(1) : batch(() => a(1))
                } catch (e) { caught = e }
                const result = {threw: (caught as Error)?.message, sibling: c2.raw}
                destroyComputed(c1); destroyComputed(c2)
                return result
            }
            expect(run('inline')).toEqual({threw: 'inline boom', sibling: 2})
            expect(run('batch')).toEqual({threw: 'batch boom', sibling: 2})

            // 结构通道(digest 隔离,既有语义):mapFn 抛错传播给 push 调用方,兄弟派生仍更新
            const list = new RxList<number>([1])
            const m1 = list.map((x) => { if (x === 99) throw new Error('structural boom'); return x * 10 })
            const m2 = list.map((x) => x + 1)
            expect(() => list.push(99)).toThrow('structural boom')
            expect(m2.data).toEqual([2, 100])
            m1.destroy(); m2.destroy(); list.destroy()
        } finally {
            consoleError.mockRestore()
        }
    })
})

// ============================================================
// F2:selection 终态对账(两模式 × 三通道 × 订阅顺序 × 重入链)
// ============================================================
describe('F2: selection 终态对账 —— indicator ≡ currentValues 终态成员关系', () => {
    const indicatorsOf = (sel: RxList<[unknown, ...Atom<boolean>[]]>, column = 0) =>
        sel.data.map((row) => (row as any[])[1 + column].raw as boolean)

    test('atom 单选 × 内联重入(rewriter 先订阅):终态对账,后续写入正常', () => {
        const list = new RxList<number>([0, 1, 2])
        const cur = atom<number | null>(null)
        const stop = installEquilibriumRewriter(cur, (v) => (v === 1 ? 2 : undefined))
        const selection = createSelection(list, cur)
        try {
            cur(1)
            expect(cur.raw).toBe(2)
            expect(indicatorsOf(selection)).toEqual([false, false, true])
            // 后续正常写入(修复前 item 1 永久卡 true)
            cur(0)
            expect(indicatorsOf(selection)).toEqual([true, false, false])
            cur(null)
            expect(indicatorsOf(selection)).toEqual([false, false, false])
            // 增量 ≡ 全量:force recompute 后不变
            recompute(selection, true)
            expect(indicatorsOf(selection)).toEqual([false, false, false])
        } finally {
            stop(); selection.destroy(); list.destroy()
        }
    })

    test('atom 单选 × 双 rewriter 重入链(1→2→3):loud 断言边界 + selection 终态仍一致', () => {
        // 链式重写需要两个独立订阅者(单 rewriter 的自写被 activeEffect 抑制):
        // A 把 1 改成 2,B 把 2 改成 3。B 的嵌套派发会命中**仍在运行中**的 A
        // ——README §2 的「同步重算环会抛错」loud 断言开火(契约边界:深度 ≥2 的
        // 同步重写链应改用调度回调),这里钉扎该边界下的两个行为:
        //   1) 断言照常抛给写入方(loud,不静默);
        //   2) F1 隔离 + F2 终态对账使 selection 不落入静默分叉:值链完成
        //      (cur.raw === 3),indicator ≡ 终态成员关系,后续写入正常。
        const list = new RxList<number>([1, 2, 3])
        const cur = atom<number | null>(null)
        const stopA = installEquilibriumRewriter(cur, (v) => (v === 1 ? 2 : undefined))
        const stopB = installEquilibriumRewriter(cur, (v) => (v === 2 ? 3 : undefined))
        const selection = createSelection(list, cur)
        try {
            expect(() => cur(1)).toThrow('detect recompute triggerred in sync recompute')
            expect(cur.raw).toBe(3) // 值链照常完成(断言不吞值写入)
            expect(indicatorsOf(selection)).toEqual([false, false, true])
            // recovery probe:同形态再来一遍(断言照旧,不累积损坏),
            // indicator 始终 ≡ 终态成员关系
            expect(() => cur(1 as any)).toThrow('detect recompute triggerred in sync recompute')
            expect(indicatorsOf(selection)).toEqual(list.data.map(item => item === cur.raw))
            // 无重入的正常写入完全恢复
            cur(null)
            expect(indicatorsOf(selection)).toEqual([false, false, false])
        } finally {
            stopA(); stopB(); selection.destroy(); list.destroy()
        }
    })

    test('atom 单选 × 重入到 null(非法值自动清选):无残留选中', () => {
        const list = new RxList<number>([0, 1])
        const cur = atom<number | null>(null)
        const stop = autorun(() => { if (cur() === 1) cur(null) }, true)
        const selection = createSelection(list, cur)
        try {
            cur(1)
            expect(cur.raw).toBe(null)
            expect(indicatorsOf(selection)).toEqual([false, false])
        } finally {
            stop(); selection.destroy(); list.destroy()
        }
    })

    test('订阅顺序反转(selection 先订阅,rewriter 后订阅):因果序 info,同样正确', () => {
        const list = new RxList<number>([0, 1, 2])
        const cur = atom<number | null>(null)
        const selection = createSelection(list, cur)
        const stop = installEquilibriumRewriter(cur, (v) => (v === 1 ? 2 : undefined))
        try {
            cur(1)
            expect(cur.raw).toBe(2)
            expect(indicatorsOf(selection)).toEqual([false, false, true])
        } finally {
            stop(); selection.destroy(); list.destroy()
        }
    })

    test('batch 通道 × 重入:session 保持因果序,终态一致(既有语义回归)', () => {
        const list = new RxList<number>([0, 1, 2])
        const cur = atom<number | null>(null)
        const stop = installEquilibriumRewriter(cur, (v) => (v === 1 ? 2 : undefined))
        const selection = createSelection(list, cur)
        try {
            batch(() => cur(1))
            expect(indicatorsOf(selection)).toEqual([false, false, true])
        } finally {
            stop(); selection.destroy(); list.destroy()
        }
    })

    test('NaN item × atom 单选(R6-1 SameValueZero 语义在对账下保持)', () => {
        const nan = Number.NaN
        const list = new RxList<number>([nan, 1])
        const cur = atom<number | null>(null)
        const selection = createSelection(list, cur)
        try {
            cur(nan)
            expect(indicatorsOf(selection)).toEqual([true, false])
            cur(1)
            expect(indicatorsOf(selection)).toEqual([false, true])
        } finally {
            selection.destroy(); list.destroy()
        }
    })

    test('createSelections 多列继承:重入只影响对应列,两列各自 ≡ 终态', () => {
        const list = new RxList<number>([0, 1, 2])
        const curA = atom<number | null>(null)
        const curB = atom<number | null>(null)
        const stop = installEquilibriumRewriter(curA, (v) => (v === 1 ? 2 : undefined))
        const selections = createSelections(list, [curA], [curB])
        try {
            curA(1)
            curB(0)
            expect(indicatorsOf(selections, 0)).toEqual([false, false, true])
            expect(indicatorsOf(selections, 1)).toEqual([true, false, false])
        } finally {
            stop(); selections.destroy(); list.destroy()
        }
    })

    test('createIndexKeySelection × atom 重入(index 1 → 2):终态对账', () => {
        const list = new RxList<string>(['a', 'b', 'c'])
        const cur = atom<null | number>(null)
        const stop = installEquilibriumRewriter(cur as Atom<number | null>, (v) => (v === 1 ? 2 : undefined))
        const sel = createIndexKeySelection(list, cur)
        try {
            ;(cur as any)(1)
            expect(cur.raw).toBe(2)
            expect(sel.data.map(([, ind]) => ind.raw)).toEqual([false, false, true])
        } finally {
            stop(); sel.destroy(); list.destroy()
        }
    })

    test('RxSet 多选 × 重入(结构通道,本就保序):对账改造不破坏既有正确性', () => {
        const list = new RxList<number>([0, 1, 2])
        const cur = new RxSet<number>([])
        // 「选中 1 时自动改选 2」:结构通道的平衡回写
        const stop = autorun(() => {
            const arr = cur.toArray()
            if (arr.includes(1)) { cur.delete(1); cur.add(2) }
        }, true)
        const selection = createSelection(list, cur)
        try {
            cur.add(1)
            expect([...cur.data]).toEqual([2])
            expect(indicatorsOf(selection)).toEqual([false, false, true])
            cur.add(0)
            expect(indicatorsOf(selection)).toEqual([true, false, true])
            cur.replace([1]) // replace 采纳 + 重写链
            expect([...cur.data].sort()).toEqual([2])
            expect(indicatorsOf(selection)).toEqual([false, false, true])
        } finally {
            stop(); selection.destroy(); list.destroy()
        }
    })

    test('到达序特征钉扎(README §2 边界):先订阅者见因果序,后订阅者见嵌套优先序', () => {
        const cur = atom<number | null>(null)
        const seenBefore: Array<[unknown, unknown]> = []
        const stopBefore = onChange(cur, (infos: TriggerInfo[]) => {
            for (const info of infos) seenBefore.push([info.oldValue, info.newValue])
        })
        const stop = installEquilibriumRewriter(cur, (v) => (v === 1 ? 2 : undefined))
        const seenAfter: Array<[unknown, unknown]> = []
        const stopAfter = onChange(cur, (infos: TriggerInfo[]) => {
            for (const info of infos) seenAfter.push([info.oldValue, info.newValue])
        })
        try {
            cur(1)
            // 先订阅(在 rewriter 之前)的消费者按因果序收到
            expect(seenBefore).toEqual([[null, 1], [1, 2]])
            // 后订阅(在 rewriter 之后)的消费者先收到嵌套写(契约边界:README §2;
            // delta 消费者应按终态对账或改用 batch)
            expect(seenAfter).toEqual([[1, 2], [null, 1]])
        } finally {
            stopBefore(); stop(); stopAfter()
        }
    })

    test('固定 seed 差分 fuzz:随机写(含重入重写/batch 混排)后 indicator ≡ 终态成员关系', () => {
        for (const seed of [901, 902, 903]) {
            const rand = mulberry32(seed)
            const history: string[] = []
            // atom 单选列:奇数非法,重写为 v+1
            const items = [0, 1, 2, 3, 4]
            const list = new RxList<number>(items.slice())
            const cur = atom<number | null>(null)
            const stop = installEquilibriumRewriter(cur, (v) =>
                (typeof v === 'number' && v % 2 === 1) ? v + 1 : undefined)
            const selection = createSelection(list, cur)
            // RxSet 多选列:奇数非法,delete+add(v+1)
            const curSet = new RxSet<number>([])
            const stopSet = autorun(() => {
                const arr = curSet.toArray()
                for (const v of arr) {
                    if (v % 2 === 1) { curSet.delete(v); curSet.add(v + 1) }
                }
            }, true)
            const selectionSet = createSelection(list, curSet)
            try {
                for (let step = 0; step < 120; step++) {
                    const inBatch = rand() < 0.33
                    const op = () => {
                        if (rand() < 0.6) {
                            const v = rand() < 0.15 ? null : Math.floor(rand() * 6) - 1 // -1..4 ∪ null
                            history.push(`cur(${v})`)
                            cur(v)
                        } else {
                            const v = Math.floor(rand() * 6) - 1
                            if (rand() < 0.5) { history.push(`set.add(${v})`); curSet.add(v) }
                            else { history.push(`set.delete(${v})`); curSet.delete(v) }
                        }
                    }
                    inBatch ? batch(op) : op()

                    const failMsg = () => `seed=${seed} step=${step}\n操作史:\n  ${history.slice(-12).join('\n  ')}`
                    // oracle:indicator ≡ 终态成员关系(全可观察状态,含两列)
                    selection.data.forEach(([item, ind]) => {
                        const expected = Object.is(item, cur.raw) || (item === cur.raw)
                        expect(ind.raw, failMsg()).toBe(expected)
                    })
                    selectionSet.data.forEach(([item, ind]) => {
                        expect(ind.raw, failMsg()).toBe(curSet.data.has(item as number))
                    })
                }
            } finally {
                stop(); stopSet()
                selection.destroy(); selectionSet.destroy(); list.destroy()
            }
        }
    })
})

// ============================================================
// F3:skipIndicator(全部入口 × 源通道 × 调度形态)
// ============================================================
describe('F3: skipIndicator —— skip 窗口丢 info 后回退全量,解除后首次触发追平', () => {
    function makeStructuralMirror(source: RxList<number>, skip: {skip: boolean}) {
        return computed<number[]>(
            function computation(this: Computed) {
                ;(this as any).manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                return source.data.slice()
            },
            function applyPatch(this: Computed, data: any, infos: TriggerInfo[]) {
                const arr = (data.raw as number[]).slice()
                for (const info of infos) {
                    if (info.method !== 'splice') return false
                    arr.splice(info.argv![0] as number, (info.methodResult as unknown[]).length, ...(info.argv!.slice(2) as number[]))
                }
                data(arr)
            },
            true, undefined, skip,
        )
    }

    test('patch computed × 结构源:skip 窗口变更在解除后的首次触发被全量追平', () => {
        const source = new RxList<number>([1, 2, 3])
        const skip = {skip: false}
        const mirror = makeStructuralMirror(source, skip)
        try {
            skip.skip = true
            source.push(4)
            source.splice(0, 1)
            expect(mirror.raw).toEqual([1, 2, 3]) // skip 期间完全静默
            skip.skip = false
            expect(mirror.raw).toEqual([1, 2, 3]) // 解除本身不触发(库不观察 flip)
            source.push(5)
            expect(mirror.raw).toEqual(source.data) // 首次触发全量追平(含窗口内变更)
            source.push(6) // 追平后恢复增量
            expect(mirror.raw).toEqual(source.data)
        } finally {
            destroyComputed(mirror); source.destroy()
        }
    })

    test('patch computed × atom 源(ATOM info 通道):同一等价类', () => {
        const src = atom(1)
        const skip = {skip: false}
        // 历史累加器:增量丢失可观察(last-value 型消费者会把分叉洗白)
        const historyMirror = computed<number[]>(
            function computation(this: Computed) {
                ;(this as any).manualTrack(src, TrackOpTypes.ATOM, 'value')
                return [src.raw as number]
            },
            function applyPatch(this: Computed, data: any, infos: TriggerInfo[]) {
                const arr = (data.raw as number[]).slice()
                for (const info of infos) arr.push(info.newValue as number)
                data(arr)
            },
            true, undefined, skip,
        )
        try {
            src(2)
            expect(historyMirror.raw).toEqual([1, 2])
            skip.skip = true
            src(3) // 丢弃
            skip.skip = false
            src(4)
            // 全量重算语义 = [终态](修复前:[1,2,4] 的静默分叉形态)
            expect(historyMirror.raw).toEqual([4])
        } finally {
            destroyComputed(historyMirror)
        }
    })

    test('patch computed × object atom 源(非 session 的 keyed 派发入口 runFromTrigger):skip 门同样生效', () => {
        // 入口等价类第三面:run(digest 交付)/runFromAtomTrigger(primitive atom)之外,
        // object atom 的浅属性写走 notifier.trigger → 内联 triggerEffect →
        // runFromTrigger——skip 门在该入口的可达性此前无测试(mutation 复审的
        // (e) 类幸存:`if (skip) → if (false)` 于 runFromTrigger 无 killer)。
        const src = atom<{n: number}>({n: 1})
        const skip = {skip: false}
        const history = computed<number[]>(
            function computation(this: Computed) {
                ;(this as any).manualTrack(src, TrackOpTypes.ATOM, 'value')
                return [(src.raw as {n: number}).n]
            },
            function applyPatch(this: Computed, data: any, infos: TriggerInfo[]) {
                const arr = (data.raw as number[]).slice()
                for (const info of infos) arr.push((info.newValue as {n: number}).n)
                data(arr)
            },
            true, undefined, skip,
        )
        try {
            ;(src as any).n = 2
            expect(history.raw).toEqual([1, 2])
            skip.skip = true
            ;(src as any).n = 3 // skip 门必须拦下 runFromTrigger(否则此处 push 3)
            expect(history.raw).toEqual([1, 2])
            skip.skip = false
            ;(src as any).n = 4
            expect(history.raw).toEqual([4]) // 全量追平(≡ 终态全量重算)
        } finally {
            destroyComputed(history)
        }
    })

    test('RxMap 构造入口(第 5 参):skip 窗口 set 丢弃后全量追平(入口等价类)', () => {
        const src = new RxMap<string, number>({a: 1})
        const skip = {skip: false}
        const mirror = new RxMap<string, number>(
            function computation(this: RxMap<string, number>) {
                this.manualTrack(src, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                return new Map(src.data)
            },
            function applyPatch(this: RxMap<string, number>, _data, infos: TriggerInfo[]) {
                for (const info of infos) {
                    if (info.method !== 'set') return false
                    this.set(info.argv![0] as string, info.argv![1] as number)
                }
            },
            undefined, undefined, skip,
        )
        try {
            skip.skip = true
            src.set('b', 2) // 丢弃
            skip.skip = false
            src.set('c', 3) // 首次触发全量追平
            expect([...mirror.data.entries()].sort()).toEqual([...src.data.entries()].sort())
        } finally {
            mirror.destroy(); src.destroy()
        }
    })

    test('非 patch computed:skip 期间静默陈旧,解除后首次触发自愈(既有语义钉扎)', () => {
        const source = new RxList<number>([1, 2, 3])
        const skip = {skip: false}
        const sum = computed<number>(
            function computation(this: Computed) {
                ;(this as any).manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                return source.data.reduce((a, b) => a + b, 0)
            },
            undefined, true, undefined, skip,
        )
        try {
            skip.skip = true
            source.push(4)
            expect(sum.raw).toBe(6) // 静默陈旧(skip 的语义)
            skip.skip = false
            source.push(5)
            expect(sum.raw).toBe(15)
        } finally {
            destroyComputed(sum); source.destroy()
        }
    })

    test('skip 期间显式 recompute(force) 是强制同步的逃生口', () => {
        const source = new RxList<number>([1, 2, 3])
        const skip = {skip: false}
        const mirror = makeStructuralMirror(source, skip)
        try {
            skip.skip = true
            source.push(4)
            recompute(mirror, true)
            expect(mirror.raw).toEqual([1, 2, 3, 4]) // skip 拦截触发派发,不拦截显式 recompute
        } finally {
            destroyComputed(mirror); source.destroy()
        }
    })

    test('batch 内的 skip 窗口:digest 交付被丢弃,解除后同样全量追平', () => {
        const source = new RxList<number>([1])
        const skip = {skip: false}
        const mirror = makeStructuralMirror(source, skip)
        try {
            skip.skip = true
            batch(() => { source.push(2); source.push(3) })
            expect(mirror.raw).toEqual([1])
            skip.skip = false
            source.push(4)
            expect(mirror.raw).toEqual([1, 2, 3, 4])
        } finally {
            destroyComputed(mirror); source.destroy()
        }
    })

    test('skip 期间不派发 dirty、不调用调度器(完全静默)', () => {
        const source = new RxList<number>([1])
        const skip = {skip: false}
        const schedulerCalls: number[] = []
        const mirror = computed<number[]>(
            function computation(this: Computed) {
                ;(this as any).manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                return source.data.slice()
            },
            function applyPatch(this: Computed, data: any, infos: TriggerInfo[]) {
                const arr = (data.raw as number[]).slice()
                for (const info of infos) {
                    if (info.method !== 'splice') return false
                    arr.splice(info.argv![0] as number, (info.methodResult as unknown[]).length, ...(info.argv!.slice(2) as number[]))
                }
                data(arr)
            },
            (recomputeFn) => { schedulerCalls.push(1); recomputeFn() },
            undefined, skip,
        )
        const internal = getComputedInternal(mirror)!
        const dirtySpy = vi.fn()
        internal.on('dirty', dirtySpy)
        try {
            skip.skip = true
            source.push(2)
            expect(dirtySpy).not.toHaveBeenCalled()
            expect(schedulerCalls.length).toBe(0)
            skip.skip = false
            source.push(3)
            expect(dirtySpy).toHaveBeenCalled()
            expect(schedulerCalls.length).toBe(1)
            expect(mirror.raw).toEqual(source.data)
        } finally {
            destroyComputed(mirror); source.destroy()
        }
    })
})