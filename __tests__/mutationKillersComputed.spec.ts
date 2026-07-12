import {afterEach, describe, expect, test, vi} from 'vitest'
import {atom} from '../src/atom.js'
import {
    Computed,
    computed,
    destroyComputed,
    getComputedInternal,
    GetterContext,
    STATUS_CLEAN,
} from '../src/computed.js'
import {onChange} from '../src/common.js'
import {RxList} from '../src/RxList.js'
import {TrackOpTypes, TriggerOpTypes} from '../src/operations.js'

/**
 * 幸存 mutant 语料驱动的杀手资产（方法 15，computed.ts 侧）。
 *
 * 2026-H2 mutation 债务清仓轮：computed.ts 重跑审计（146 survived / 21 no-cov）。
 * 分类后确认的真实盲区——这些语义此前只有"结果正确"级别的覆盖，
 * 状态机的**时机与转换序**从未被钉住：
 *   1. async computed × dirtyCallback===true 的立即语义（写后同步启动新一轮）；
 *   2. 重算被打断时 asyncStatus 的完整转换序（true→false→true→false），
 *      以及过期轮完成时不得写 asyncStatus / 不得落地结果；
 *   3. generator getter 过期轮的段级丢弃（beforeRun 代次守卫）；
 *   4. patch computed 的 recompute(force) 必须真正全量重跑（phase 越过 PATCH）；
 *   5. 自定义调度器拿到的 recompute 回调默认不带 force（增量性不被调度路径破坏）
 *      与 markDirty 的传播语义；
 *   6. updatedAt 惰性 atom 的同步与响应性；
 *   7. 同步重算环断言的可达性与消息。
 */

const tick = () => Promise.resolve()
const wait = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

afterEach(() => {
    vi.useRealTimers()
})

describe('async computed 的调度与打断语义', () => {
    test('dirtyCallback===true：依赖写入后同步启动新一轮（asyncStatus 立即为 true）', async () => {
        const dep = atom(1)
        const resolvers: Array<(v: number) => void> = []
        const c = computed(async function (this: Computed) {
            const v = dep()
            const extra = await new Promise<number>(r => resolvers.push(r))
            return v * 100 + extra
        } as any, undefined, true)
        const internal = getComputedInternal(c)!
        try {
            expect(internal.asyncStatus!.raw).toBe(true)   // 构造即启动
            resolvers[0]!(7)
            await wait(5)
            expect(c.raw).toBe(107)
            expect(internal.asyncStatus!.raw).toBe(false)

            dep(2)
            // 立即语义：不等 microtask，新一轮已同步启动
            expect(internal.asyncStatus!.raw).toBe(true)
            resolvers[1]!(9)
            await wait(5)
            expect(c.raw).toBe(209)
        } finally {
            destroyComputed(c)
        }
    })

    test('打断时 asyncStatus 的转换序恰为 true→false→true→false，过期轮不落地', async () => {
        const dep = atom(1)
        const resolvers: Array<(v: number) => void> = []
        const c = computed(async function (this: Computed) {
            const v = dep()
            const extra = await new Promise<number>(r => resolvers.push(r))
            return v * 100 + extra
        } as any, undefined, true)
        const internal = getComputedInternal(c)!
        const transitions: Array<boolean | string | null> = []
        const stop = onChange(internal.asyncStatus!, () => transitions.push(internal.asyncStatus!.raw))
        try {
            expect(internal.asyncStatus!.raw).toBe(true)   // 第一轮在途
            dep(2)                                          // 打断:false(旧轮标记中止)→true(新轮)
            expect(transitions).toEqual([false, true])
            expect(resolvers.length).toBe(2)

            resolvers[0]!(7)                                // 过期轮完成
            await wait(5)
            expect(c.raw).toBe(null)                        // 结果不落地
            expect(internal.asyncStatus!.raw).toBe(true)    // 状态不被过期轮改写
            expect(transitions).toEqual([false, true])

            resolvers[1]!(9)
            await wait(5)
            expect(c.raw).toBe(209)
            expect(transitions).toEqual([false, true, false])
        } finally {
            stop()
            destroyComputed(c)
        }
    })

    test('generator getter 过期轮在 yield 恢复点被段级丢弃（不再执行后续段）', async () => {
        const dep = atom(1)
        const resolvers: Array<(v: number) => void> = []
        const segmentLog: string[] = []
        let round = 0
        const c = computed(function* (this: Computed): Generator<any, number, number> {
            const myRound = ++round
            const v = dep()
            segmentLog.push(`r${myRound}s1`)
            const extra: number = yield new Promise<number>(r => resolvers.push(r))
            segmentLog.push(`r${myRound}s2`)
            return v * 100 + extra
        } as any)
        try {
            await tick()
            dep(2)
            await tick()
            expect(segmentLog).toEqual(['r1s1', 'r2s1'])

            resolvers[0]!(7)     // 过期轮恢复：beforeRun 代次守卫必须拦截其后续段
            await wait(5)
            expect(segmentLog).toEqual(['r1s1', 'r2s1'])
            expect(c.raw).toBe(null)

            resolvers[1]!(9)
            await wait(5)
            expect(segmentLog).toEqual(['r1s1', 'r2s1', 'r2s2'])
            expect(c.raw).toBe(209)
        } finally {
            destroyComputed(c)
        }
    })
})

describe('recompute(force) 与调度回调的增量性', () => {
    test('patch computed 处于 PATCH 阶段时 force 必须全量重跑，默认调用不重跑', () => {
        const source = new RxList([1, 2])
        const mapped = source.map(x => x * 2)
        let fulls = 0
        mapped.on('fullRecompute', () => fulls++)
        try {
            source.push(3)                    // 进入 PATCH 阶段的增量路径
            expect(fulls).toBe(0)
            expect(mapped.data).toEqual([2, 4, 6])

            mapped.recompute(true)            // 强制全量（RxList 自身就是 internal）
            expect(fulls).toBe(1)
            expect(mapped.data).toEqual([2, 4, 6])

            mapped.recompute()                // 默认不强制:CLEAN 状态下是 no-op
            expect(fulls).toBe(1)
        } finally {
            mapped.destroy()
            source.destroy()
        }
    })

    test('自定义调度器收到的 recompute 默认非 force：flush 走增量而非全量', () => {
        const source = new RxList([1])
        let queued: ((force?: boolean) => void) | undefined
        const mapped = source.map(x => x + 1, {
            scheduleRecompute(recomputeFn) {
                queued = recomputeFn
            }
        })
        let fulls = 0
        mapped.on('fullRecompute', () => fulls++)
        try {
            source.push(2)
            expect(mapped.data).toEqual([2])  // 尚未应用
            queued!()
            expect(mapped.data).toEqual([2, 3])
            expect(fulls).toBe(0)             // 增量应用,不是全量
        } finally {
            mapped.destroy()
            source.destroy()
        }
    })

    test('markDirty 沿依赖图强制下游重跑', () => {
        const dep = atom(1)
        let queued: ((force?: boolean) => void) | undefined
        let markDirty: (() => void) | undefined
        const base = computed(function (this: Computed) {
            this.manualTrack(dep, TrackOpTypes.ATOM, 'value')
            return dep.raw * 10
        }, undefined, (recomputeFn, markDirtyFn) => {
            queued = recomputeFn
            markDirty = markDirtyFn
        })
        let downstreamRuns = 0
        const downstream = computed(() => {
            downstreamRuns++
            return base() + 1
        })
        try {
            expect(downstream()).toBe(11)
            dep(2)
            expect(markDirty).toBeTypeOf('function')
            const before = downstreamRuns
            markDirty!()                       // 不重算 base,只驱动下游重跑
            expect(downstreamRuns).toBe(before + 1)
            expect(base.raw).toBe(10)          // base 自身仍未重算
            queued!()
            expect(base.raw).toBe(20)
            expect(downstream()).toBe(21)
        } finally {
            destroyComputed(downstream)
            destroyComputed(base)
        }
    })
})

describe('updatedAt 惰性 atom', () => {
    test('惰性创建时同步既有时间戳，此后随重算响应式更新', () => {
        vi.useFakeTimers({now: 100_000})
        const dep = atom(1)
        const c = computed(() => dep())
        const internal = getComputedInternal(c)!
        try {
            // 惰性创建：atom 初值取创建前最后一次重算的时间戳
            expect(internal.updatedAt.raw).toBe(100_000)

            const observed: Array<number | undefined> = []
            const stop = onChange(internal.updatedAt, () => observed.push(internal.updatedAt.raw))
            vi.setSystemTime(100_500)
            dep(2)
            expect(internal.updatedAt.raw).toBe(100_500)
            expect(observed).toEqual([100_500])
            stop()
        } finally {
            destroyComputed(c)
        }
    })

    test('未读过 updatedAt 时时间戳仍在内部字段上演进（读取即同步）', () => {
        vi.useFakeTimers({now: 200_000})
        const dep = atom(1)
        const c = computed(() => dep())
        const internal = getComputedInternal(c)!
        try {
            vi.setSystemTime(201_000)
            dep(2)
            // 第一次读取发生在两次重算之后：必须反映最近一次
            expect(internal.updatedAt.raw).toBe(201_000)
        } finally {
            destroyComputed(c)
        }
    })
})

describe('状态机与守卫', () => {
    test('源模式结构构造后 status 为 CLEAN', () => {
        const source = new RxList([1])
        try {
            expect(source._status).toBe(STATUS_CLEAN)
            expect(source.status.raw).toBe(STATUS_CLEAN)
        } finally {
            source.destroy()
        }
    })

    test('同步重算环被断言拦截（消息与可达性）', () => {
        const a = atom(1)
        const b = atom(10)
        let enable = false
        const c1 = computed(() => {
            const av = a()
            if (enable) b(av + 100)
            return av
        })
        const c2 = computed(() => {
            const bv = b()
            if (enable && bv > 100) a(bv)
            return bv
        })
        try {
            enable = true
            expect(() => a(2)).toThrow('detect recompute triggerred in sync recompute')
        } finally {
            enable = false
            destroyComputed(c2)
            destroyComputed(c1)
        }
    })

    test('getterContext 的 pauseCollectChild/resumeCollectChild 真实生效', () => {
        let detached: Computed | undefined
        let collected: Computed | undefined
        const host = new Computed(function (this: Computed, context: GetterContext) {
            context.pauseCollectChild()
            detached = new Computed(function (this: Computed) { return 1 })
            context.resumeCollectChild()
            collected = new Computed(function (this: Computed) { return 2 })
            return 0
        })
        host.run([], true)
        try {
            expect(host.children.includes(detached!)).toBe(false)
            expect(host.children.includes(collected!)).toBe(true)
            host.destroy()
            expect(collected!.active).toBe(false)  // 随宿主销毁
            expect(detached!.active).toBe(true)    // 游离于宿主生命周期
        } finally {
            detached?.destroy()
            if (host.active) host.destroy()
        }
    })

    test('onChange 对 RxList 同时追踪 METHOD 与 EXPLICIT_KEY_CHANGE 两个面', () => {
        const list = new RxList([1, 2])
        const methods: (string | undefined)[] = []
        const stop = onChange(list, (infos: any[]) => {
            for (const info of infos) methods.push(info.method ?? String(info.type))
        })
        try {
            list.push(3)
            list.set(0, 9)
            expect(methods).toContain('splice')
            expect(methods).toContain(String(TriggerOpTypes.EXPLICIT_KEY_CHANGE))
        } finally {
            stop()
            list.destroy()
        }
    })
})
