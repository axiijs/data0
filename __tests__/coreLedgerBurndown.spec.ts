import {describe, expect, test, vi} from 'vitest'
import {atom} from '../src/atom.js'
import {computed, Computed, destroyComputed} from '../src/computed.js'
import {autorun, onChange} from '../src/common.js'
import {batch} from '../src/notify.js'
import {RxList} from '../src/RxList.js'
import {TrackOpTypes, TriggerOpTypes} from '../src/operations.js'

/**
 * 核心表面账本(coreSurfaceInventory)2026-H2 燃尽轮:逐格闭合 13 个 UNCOVERED。
 * 每个 describe 标注对应的「表面 × 维度」格子。
 */

const tick = () => new Promise<void>(r => setTimeout(r, 0))
const wait = (ms: number) => new Promise<void>(r => setTimeout(r, ms))

describe('atom.objectProxy × batchSession', () => {
    test('batch 内多次浅属性写合并为一次订阅者执行,退出后读写一致', () => {
        const obj = atom<{x: number}>({x: 0})
        let runs = 0
        const stop = autorun(() => { obj.x; runs++ }, true)
        expect(runs).toBe(1)
        batch(() => {
            obj.x = 1
            obj.x = 2
        })
        expect(runs).toBe(2) // 两次写一次执行
        expect(obj.raw.x).toBe(2)
        stop()
    })
})

describe('computed.asyncGetter × batchSession', () => {
    test('batch 内多次写依赖,退出后单轮 async 重算收敛到终值', async () => {
        const dep = atom(1)
        let getterRuns = 0
        const c = computed(async () => {
            getterRuns++
            const v = dep()
            await tick()
            return v * 10
        })
        await wait(10)
        expect(c.raw).toBe(10)
        const runsBefore = getterRuns
        batch(() => {
            dep(2)
            dep(3)
        })
        await wait(10)
        expect(c.raw).toBe(30)
        expect(getterRuns - runsBefore).toBe(1) // batch 合并为一轮
        destroyComputed(c)
    })
})

describe('computed.generatorGetter × errorInjection', () => {
    test('yield 后抛错:状态回 DIRTY、可观测上报,下次触发重试成功', async () => {
        const dep = atom(1)
        let shouldThrow = false
        const c = computed(function* (): Generator<any, number, number> {
            const v = dep()
            const extra: number = yield Promise.resolve(1)
            if (shouldThrow) throw new Error('gen getter boom')
            return v + extra
        } as any)
        await wait(10)
        expect(c.raw).toBe(2)

        const errSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined)
        try {
            shouldThrow = true
            dep(5)
            await wait(10)
            expect(errSpy).toHaveBeenCalled() // 无观测方 → console 兜底
            expect(c.raw).toBe(2) // 保持旧值

            shouldThrow = false
            dep(7)
            await wait(10)
            expect(c.raw).toBe(8) // 重试成功
        } finally {
            errSpy.mockRestore()
            destroyComputed(c)
        }
    })
})

describe('computed.generatorGetter × interleaving(重入启动新一轮,旧轮被代次丢弃)', () => {
    test('挂起在 yield 时依赖再变:旧轮结果不落地,终值为新轮', async () => {
        const dep = atom(1)
        const resolvers: Array<(v: number) => void> = []
        const c = computed(function* (): Generator<any, number, number> {
            const v = dep()
            const extra: number = yield new Promise<number>(r => { resolvers.push(r) })
            return v * 100 + extra
        } as any)
        await tick() // 第一轮启动,挂起(resolvers[0])
        dep(2)      // 触发第二轮(runtEffectId 推进)
        await tick() // 第二轮启动,挂起(resolvers[1])
        expect(resolvers.length).toBe(2)

        // 旧轮先完成:结果必须被丢弃
        resolvers[0](7)
        await wait(10)
        expect(c.raw).toBe(null) // 尚无任何轮完成落地(初始 null)

        resolvers[1](9)
        await wait(10)
        expect(c.raw).toBe(209) // 新轮:2*100+9
        destroyComputed(c)
    })
})

describe('computed.generatorGetter × batchSession', () => {
    test('batch 内多次写依赖,退出后单轮 generator 重算', async () => {
        const dep = atom(1)
        let rounds = 0
        const c = computed(function* (): Generator<any, number, number> {
            rounds++
            const v = dep()
            yield Promise.resolve(0)
            return v * 10
        } as any)
        await wait(10)
        const roundsBefore = rounds
        batch(() => {
            dep(2)
            dep(3)
        })
        await wait(10)
        expect(c.raw).toBe(30)
        expect(rounds - roundsBefore).toBe(1)
        destroyComputed(c)
    })
})

// generator applyPatch 工厂:x*10 的派生列表,支持注入抛错
function createGeneratorPatchList(source: RxList<number>,控制?: {throwOnce?: boolean}) {
    const state = {throwNext: false}
    const derived = new RxList<number>(
        function computation(this: RxList<number>) {
            this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
            return source.data.map(x => x * 10)
        },
        function* applyPatch(this: RxList<number>, _data: any, triggerInfos: any[]): Generator<any, void, any> {
            yield Promise.resolve()
            if (state.throwNext) {
                state.throwNext = false
                throw new Error('gen patch boom')
            }
            for (const info of triggerInfos) {
                this.spliceArray(info.argv![0] as number, info.argv![1] as number, (info.argv!.slice(2) as number[]).map(x => x * 10))
            }
        } as any,
    )
    return {derived, state}
}

describe('applyPatch.generator × errorInjection', () => {
    test('generator patch 抛错:console 兜底,下次触发全量恢复一致', async () => {
        const source = new RxList<number>([1])
        const {derived, state} = createGeneratorPatchList(source)
        await wait(10)
        expect(derived.data).toEqual([10])

        const errSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined)
        try {
            state.throwNext = true
            source.push(2)
            await wait(10)
            expect(errSpy).toHaveBeenCalled()

            source.push(3) // phase 已回退:全量重算恢复
            await wait(10)
            expect(derived.data).toEqual(source.data.map(x => x * 10))
        } finally {
            errSpy.mockRestore()
            derived.destroy()
            source.destroy()
        }
    })
})

describe('applyPatch.generator × destroyTiming', () => {
    test('mid-yield destroy:恢复后不再写入,数据保持销毁快照', async () => {
        const source = new RxList<number>([1])
        let release!: () => void
        const gate = new Promise<void>(r => { release = r })
        const derived = new RxList<number>(
            function computation(this: RxList<number>) {
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                return source.data.map(x => x * 10)
            },
            function* applyPatch(this: RxList<number>, _data: any, triggerInfos: any[]): Generator<any, void, any> {
                yield gate
                for (const info of triggerInfos) {
                    this.spliceArray(info.argv![0] as number, info.argv![1] as number, (info.argv!.slice(2) as number[]).map(x => x * 10))
                }
            } as any,
        )
        expect(derived.data).toEqual([10])
        source.push(2) // patch 启动并挂起在 yield
        await tick()
        const snapshot = [...derived.data]
        derived.destroy()
        release() // 恢复:必须是 no-op
        await wait(10)
        expect(derived.data).toEqual(snapshot)
        source.destroy()
    })
})

describe('applyPatch.generator × interleaving', () => {
    test('挂起期间的源写入排队,由后续轮次消化,终值 ≡ 全量重算', async () => {
        const source = new RxList<number>([1])
        const {derived} = createGeneratorPatchList(source)
        await wait(10)

        source.push(2) // 第一轮 patch 启动(挂起在 yield Promise.resolve)
        source.push(3) // 挂起期间到达:排队
        source.push(4)
        await wait(20) // 轮询消化全部
        expect(derived.data).toEqual(source.data.map(x => x * 10))
        derived.destroy()
        source.destroy()
    })
})

describe('applyPatch.generator × batchSession', () => {
    test('batch 多操作单次 digest:generator patch 逐条消化,终值 ≡ 全量重算', async () => {
        const source = new RxList<number>([1, 2, 3])
        const {derived} = createGeneratorPatchList(source)
        await wait(10)
        batch(() => {
            source.push(4)
            source.splice(0, 1)
        })
        await wait(20)
        expect(derived.data).toEqual(source.data.map(x => x * 10))
        derived.destroy()
        source.destroy()
    })
})

describe('onChange × errorInjection', () => {
    test('handler 抛错同步传播;错误后第一次变更走全量恢复(无通知),之后恢复通知', () => {
        const list = new RxList<number>([1])
        const seen: number[] = []
        let shouldThrow = false
        const stop = onChange(list, (infos: any[]) => {
            if (shouldThrow) { shouldThrow = false; throw new Error('handler boom') }
            seen.push(infos.length)
        })
        try {
            shouldThrow = true
            expect(() => list.push(2)).toThrow('handler boom')
            // phase 已回退 FULL_RECOMPUTE_PHASE(handleRecomputeError 的错误恢复语义):
            // 下一次变更触发全量重算(重新 track,不产生 patch 通知),这是"抛错轮
            // info 已丢失、增量态不可信"下保证派生一致性的统一处置;再下一次恢复增量通知。
            list.push(3)
            expect(seen).toEqual([])
            list.push(4)
            expect(seen).toEqual([1])
        } finally {
            stop()
            list.destroy()
        }
    })
})

describe('onChange × batchSession', () => {
    test('batch 多操作 → handler 单次调用收到全部 info', () => {
        const list = new RxList<number>([1])
        const calls: number[] = []
        const stop = onChange(list, (infos: any[]) => { calls.push(infos.length) })
        try {
            batch(() => {
                list.push(2)
                list.push(3)
            })
            expect(calls).toEqual([2]) // 一次调用,两条 info
        } finally {
            stop()
            list.destroy()
        }
    })
})

describe('reactiveEffect.children × batchSession', () => {
    test('batch 内多次触发宿主:退出后重算一次,子 effect 销毁重建恰一次', () => {
        const dep = atom(0)
        let childCreations = 0
        let childDestroys = 0
        const host = new Computed(function (this: Computed) {
            dep()
            const child = computed(() => 1, undefined, undefined, {
                onDestroy: () => { childDestroys++ },
            })
            childCreations++
            return child
        })
        host.run([], true)
        expect(childCreations).toBe(1)
        batch(() => {
            dep(1)
            dep(2)
        })
        expect(childCreations).toBe(2) // 只重算一次
        expect(childDestroys).toBe(1)  // 旧子恰好销毁一次
        host.destroy()
        expect(childDestroys).toBe(2)
    })
})

describe('reactiveEffect.detachedAndTransfer × errorInjection(行级探测 probe)', () => {
    test('mapFn 在 patch 新行抛错:异常传播,phase 回退,下次触发全量恢复(probe deps 无残留污染)', () => {
        const source = new RxList<number>([1])
        const rowDep = atom(1)
        let shouldThrow = false
        const mapped = source.map(x => {
            if (shouldThrow) { shouldThrow = false; throw new Error('mapFn boom') }
            return x * rowDep()
        })
        try {
            expect(mapped.data).toEqual([1])
            shouldThrow = true
            expect(() => source.push(2)).toThrow('mapFn boom')
            // 错误恢复:下次触发全量重算(mapFn 不再抛)
            source.push(3)
            expect(mapped.data).toEqual(source.data.map(x => x * rowDep.raw))
            // 行级响应依赖仍然工作(probe 捕获转移没有被上次异常污染)
            rowDep(2)
            expect(mapped.data).toEqual(source.data.map(x => x * 2))
        } finally {
            mapped.destroy()
            source.destroy()
        }
    })
})
