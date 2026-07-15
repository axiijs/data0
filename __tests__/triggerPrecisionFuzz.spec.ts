/**
 * 触发精确度差分 fuzz(2026-H3 round3 根因 3 的机械化闭合)。
 *
 * 背景:全部既有差分 fuzz 只对比终值——"结果正确但多做了工作"类缺陷(值未变的
 * 幽灵触发、无关源的误触发、一次 digest 多轮重算)对值 oracle 天然不可见。
 * 方法 15 曾为 at(index) 手写过一个精确触发穷举并写下"差分 fuzz 从不检查
 * 谁被通知了",但该观察维度此前没有常驻的 fuzz 级执法者。RxMap.replace 的
 * 幽灵 SET(值未变触发全部 get(key) 订阅者)因此存活。
 *
 * 断言列(在架构语义 A1 的边界内,全部为契约内承诺):
 *   P1 digest 有界性:一次源操作(或一个 batch)= 一次 digest,每个订阅的派生
 *      至多 1 轮重算 + 至多 1 轮全量回退(patch return false);
 *   P2 无关源隔离:对源 A 的操作不得触发源 B 的任何派生(计数恒零);
 *   P3 判等零触发:Object.is 相同的值写入(atom 写、RxMap.set、RxMap.replace
 *      的未变 key)零轮重算。
 *
 * 计数器挂载用 fuzzKit.attachRecomputeCounter('recompute'/'fullRecompute' 事件,
 * 无监听者时派发零分配,挂载本身不改变被测行为)。
 */
import {describe, expect, test} from 'vitest'
import {RxList} from '../src/RxList.js'
import {RxMap} from '../src/RxMap.js'
import {atom} from '../src/atom.js'
import {computed, getComputedInternal, Computed} from '../src/computed.js'
import {batch} from '../src/notify.js'
import {autorun} from '../src/common.js'
import {attachRecomputeCounter, mulberry32, uniqueInts, type RecomputeCounter} from './fuzzKit.js'

function buildCountedFamily(source: RxList<number>) {
    const derived = {
        mapped: source.map(x => x * 2),
        sorted: source.toSorted((a, b) => a - b),
        sliced: source.slice(0, 4),
        grouped: source.groupBy(x => x % 3),
        asSet: source.toSet(),
        fi: source.findIndex(x => x % 7 === 0),
        len: source.length,
    }
    const counters: Record<string, RecomputeCounter> = {
        mapped: attachRecomputeCounter(derived.mapped),
        sorted: attachRecomputeCounter(derived.sorted),
        sliced: attachRecomputeCounter(derived.sliced),
        grouped: attachRecomputeCounter(derived.grouped),
        asSet: attachRecomputeCounter(derived.asSet),
        fi: attachRecomputeCounter(getComputedInternal(derived.fi) as Computed),
        len: attachRecomputeCounter(getComputedInternal(derived.len) as Computed),
    }
    const destroy = () => {
        Object.values(counters).forEach(c => c.detach())
        derived.mapped.destroy(); derived.sorted.destroy(); derived.sliced.destroy()
        derived.grouped.destroy(); derived.asSet.destroy()
        ;(getComputedInternal(derived.fi) as Computed).destroy()
    }
    return {derived, counters, destroy}
}

const resetAll = (cs: Record<string, RecomputeCounter>) => Object.values(cs).forEach(c => c.reset())

function assertDigestBounds(cs: Record<string, RecomputeCounter>, ctx: string) {
    for (const [name, c] of Object.entries(cs)) {
        // P1:至多 1 轮 patch + 1 轮全量回退 = rounds ≤ 2 且 fulls ≤ 1。
        // (rounds 为 0 是合法的:该派生未订阅本次变更触及的 key。)
        expect(c.rounds(), `${name} rounds ${ctx}`).toBeLessThanOrEqual(2)
        expect(c.fulls(), `${name} fulls ${ctx}`).toBeLessThanOrEqual(1)
        if (c.rounds() === 2) {
            expect(c.fulls(), `${name} 两轮重算的第二轮必须是全量回退 ${ctx}`).toBe(1)
        }
    }
}

function assertAllZero(cs: Record<string, RecomputeCounter>, ctx: string) {
    for (const [name, c] of Object.entries(cs)) {
        expect(c.rounds(), `${name} 必须零触发 ${ctx}`).toBe(0)
    }
}

describe('P1+P2:digest 有界性 × 无关源隔离(随机操作序列)', () => {
    const SEEDS = 12
    const STEPS = 30
    for (let seed = 1; seed <= SEEDS; seed++) {
        test(`seed=${seed}`, () => {
            const rand = mulberry32(seed * 104729)
            const nextValue = uniqueInts(seed * 1000)
            const a = new RxList<number>([nextValue(), nextValue(), nextValue(), nextValue()])
            const b = new RxList<number>([nextValue(), nextValue(), nextValue()])
            const famA = buildCountedFamily(a)
            const famB = buildCountedFamily(b)
            const history: string[] = []
            try {
                for (let step = 0; step < STEPS; step++) {
                    resetAll(famA.counters); resetAll(famB.counters)
                    const r = rand()
                    // 契约内操作域(精确度承诺面);形态操作的精确度由 sparseOpsFuzz 的 T1-T3 兜底
                    if (r < 0.30) {
                        const v = nextValue()
                        a.push(v); history.push(`a.push(${v})`)
                    } else if (r < 0.45 && a.data.length) {
                        const i = Math.floor(rand() * a.data.length)
                        const v = nextValue()
                        a.set(i, v); history.push(`a.set(${i},${v})`)
                    } else if (r < 0.60 && a.data.length) {
                        const start = Math.floor(rand() * a.data.length)
                        a.splice(start, 1 + Math.floor(rand() * 2)); history.push(`a.splice(${start},..)`)
                    } else if (r < 0.72 && a.data.length >= 2) {
                        const i = Math.floor(rand() * (a.data.length - 1))
                        a.swap(i, i + 1); history.push(`a.swap(${i},${i + 1})`)
                    } else {
                        const k = 2 + Math.floor(rand() * 2)
                        const ops: string[] = []
                        batch(() => {
                            for (let j = 0; j < k; j++) {
                                const v = nextValue()
                                if (rand() < 0.5) { a.push(v); ops.push(`push(${v})`) }
                                else if (a.data.length) {
                                    const i = Math.floor(rand() * a.data.length)
                                    a.set(i, v); ops.push(`set(${i},${v})`)
                                } else { a.unshift(v); ops.push(`unshift(${v})`) }
                            }
                        })
                        history.push(`batch{${ops.join(';')}}`)
                    }
                    assertDigestBounds(famA.counters, `step=${step} seed=${seed}`)
                    // P2:B 家族与 a 无任何依赖关系,一次都不许动
                    assertAllZero(famB.counters, `(isolation) step=${step} seed=${seed}`)
                }
            } catch (e) {
                throw new Error(`triggerPrecisionFuzz seed=${seed} 失败\n操作史:\n  ${history.join('\n  ')}\n${(e as Error).stack ?? e}`)
            } finally {
                famA.destroy(); famB.destroy()
                a.destroy(); b.destroy()
            }
        })
    }
})

describe('P3:判等门零触发(同一变更语义的全部入口)', () => {
    test('atom:Object.is 相同写入零重跑', () => {
        const v = atom(1)
        let runs = 0
        const stop = autorun(() => { runs++; v() }, true)
        expect(runs).toBe(1)
        v(1)
        expect(runs).toBe(1)
        v(NaN); v(NaN)
        expect(runs).toBe(2) // NaN→NaN 只算一次变化
        stop()
    })
    test('RxMap.set:未变 key 零触发;RxMap.replace:未变 key 零触发(入口等价)', () => {
        const m = new RxMap<string, number>([['a', 1], ['b', 2]])
        let aRuns = 0
        const stop = autorun(() => { aRuns++; m.get('a') }, true)
        expect(aRuns).toBe(1)
        m.set('a', 1)                    // 入口 1:set,值未变
        expect(aRuns).toBe(1)
        m.replace([['a', 1], ['b', 9]])  // 入口 2:replace,a 未变 b 变
        expect(aRuns).toBe(1)
        m.set('a', 5)
        expect(aRuns).toBe(2)
        stop(); m.destroy()
    })
    test('computed 判等:结果未变时下游零重跑(AtomComputed 的 atom 门)', () => {
        const src = atom(2)
        const parity = computed(() => src() % 2)
        let runs = 0
        const stop = autorun(() => { runs++; parity() }, true)
        expect(runs).toBe(1)
        src(4) // parity 重算但结果仍 0 → 下游 atom 判等挡住
        expect(runs).toBe(1)
        src(5)
        expect(runs).toBe(2)
        stop()
    })
    test('RxList.set 特征钉扎:同值写入当前仍触发(EKC 协议,判等语义变更须与 axii/axle 同步)', () => {
        // 见 entryPointSemanticsInventory 的入口分类:RxList.set 是 equalityExempt。
        // 下游可能依赖 set(i, sameItem) 强制重建行;单方面加判等门属协议变更。
        const list = new RxList<number>([7])
        const infos: unknown[] = []
        const capture = new Computed(function (this: Computed) {
            this.manualTrack(list, 'explicit_key_change' as any, 'explicit_key_change' as any)
        }, function (_d: unknown, ts: unknown[]) { infos.push(...ts) })
        capture.run([], true)
        list.set(0, 7) // Object.is 相同
        expect(infos.length).toBe(1) // 现状:仍触发(特征,非承诺)
        capture.destroy(); list.destroy()
    })
})

describe('P1 定向:batch 多 op 单 digest 恰一轮', () => {
    test('batch 内 5 次 push,map/toSorted/groupBy 各至多 1+1 轮', () => {
        const source = new RxList<number>([1, 2, 3])
        const fam = buildCountedFamily(source)
        try {
            resetAll(fam.counters)
            batch(() => { for (let i = 0; i < 5; i++) source.push(100 + i) })
            assertDigestBounds(fam.counters, '(batch 5 push)')
            // map 无 index:多 info 保持增量,恰 1 轮 patch 零回退
            expect(fam.counters.mapped.rounds()).toBe(1)
            expect(fam.counters.mapped.fulls()).toBe(0)
        } finally {
            fam.destroy(); source.destroy()
        }
    })
})
