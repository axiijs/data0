/**
 * 深度评估（2026-07）发现问题的回归测试。
 *
 * 致命类：
 *  F1. async computed 的 getter 在依赖触发的重算中 reject，且无人 await cleanPromise 时，
 *      不能变成 unhandled rejection（Node >= 15 默认崩溃进程）；无任何观测方时用
 *      console.error 兜底，有 await 方时错误正常送达（不重复上报）。
 *  F2. batch 中积累的大批量 triggerInfos 传给 Computed.run 时不能用 spread push
 *      （超过引擎实参上限直接 RangeError）。
 *  F3. splice 的负/越界/小数 start 必须在 data0 内部归一化后消费：
 *      - 带 index 的 map 派生列表不能拿到负下标的 index atom（旧实现直接 TypeError）；
 *      - at(index) 的订阅者必须收到 SET 触发（旧实现算错受影响区间，静默保持旧值）；
 *      - triggerInfo.argv 仍按契约透传用户原始参数（axii/axle 锁定该行为）。
 *  F4. 内置调度器（scheduleNextMicroTask/scheduleNextTick/autorun 的 nextJob）中
 *      recompute 抛错后：computed 不能永久卡死（queuedRecomputes 泄漏），错误
 *      console.error 上报而不是变成 microtask 的 uncaught exception。
 *  F5. batch digest 中单个 effect 抛错不能丢弃队列中其余 effect（否则它们连脏标记
 *      都没有，读到的是静默的陈旧值）；第一个错误抛给 batch 调用方，其余上报。
 *
 * 缺陷类：
 *  I1. getter context 的 onCleanup 只注册一次时，cleanup 只能被调用一次（旧实现
 *      不复位 lastCleanupFn，条件注册场景下 stale cleanup 每轮重算都被重复调用）。
 *  I2. filter 的结果保持与源列表相同的相对顺序（中段 toggle 成匹配、中段纯插入）。
 *  I3. recursiveMarkDirty 不再把订阅方记进只增不减的集合（旧 dirtyFromDeps/
 *      markedDirtyEffects 无任何读取方且对已销毁 effect 保持强引用）。
 *  I4. RxList.at 支持负索引（与 Array.prototype.at 一致），并正确响应结构变更。
 *  I6. TrackOpTypes/TriggerOpTypes 是运行时常量对象（不再是 const enum），
 *      isolatedModules 的下游可以直接引用成员。
 */
import {afterEach, describe, expect, test, vi} from "vitest";
import {
    atom,
    autorun,
    batch,
    computed,
    Computed,
    RxList,
    scheduleNextMicroTask,
    TrackOpTypes,
    TriggerInfo,
    TriggerOpTypes
} from "../src/index.js";
import {getComputedInternal, recompute, STATUS_CLEAN} from "../src/computed.js";

// 捕获 axii（RxListHost）同款订阅形态收到的 triggerInfos
function captureTriggerInfos(source: RxList<any>) {
    const captured: TriggerInfo[] = []
    const c = new Computed(
        function computation(this: Computed) {
            this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
            this.manualTrack(source, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE)
            return null
        },
        function applyPatch(this: Computed, _, triggerInfos) {
            triggerInfos.forEach(info => captured.push(info))
        },
        true
    )
    c.run([], true)
    return {captured, destroy: () => c.destroy()}
}

const macroTask = (ms = 0) => new Promise(resolve => setTimeout(resolve, ms))

describe('F1: async computed errors never become unhandled rejections', () => {
    afterEach(() => {
        vi.restoreAllMocks()
    })

    test('dep-triggered async rejection with no awaiter: no unhandled rejection, console.error fallback', async () => {
        const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
        const unhandled = vi.fn()
        process.on('unhandledRejection', unhandled)
        try {
            const a = atom(1)
            const c = computed(async () => {
                const v = a()
                if (v === 2) throw new Error('async boom')
                return v * 10
            })
            await macroTask(10)
            expect(c.raw).toBe(10)

            a(2) // 触发 async 重算 → reject，无人 await
            await macroTask(20)

            expect(unhandled).not.toHaveBeenCalled()
            // 无任何观测方（cleanPromise/error 监听）时必须 console.error 兜底，不能完全静默
            expect(consoleError).toHaveBeenCalledTimes(1)
            expect(String(consoleError.mock.calls[0][1])).toContain('async boom')

            // 错误后可恢复
            a(3)
            await getComputedInternal(c)!.cleanPromise
            expect(c.raw).toBe(30)
        } finally {
            process.off('unhandledRejection', unhandled)
        }
    })

    test('awaiting cleanPromise still receives the rejection, without console.error double-report', async () => {
        const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
        const unhandled = vi.fn()
        process.on('unhandledRejection', unhandled)
        try {
            const a = atom(1)
            const c = computed(async () => {
                const v = a()
                if (v === 2) throw new Error('await boom')
                return v
            })
            const internal = getComputedInternal(c)!
            await internal.cleanPromise

            a(2)
            await expect(internal.cleanPromise).rejects.toThrow('await boom')
            await macroTask(10)
            expect(unhandled).not.toHaveBeenCalled()
            expect(consoleError).not.toHaveBeenCalled()
        } finally {
            process.off('unhandledRejection', unhandled)
        }
    })

    test('error listener receives the rejection and suppresses the console fallback', async () => {
        const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
        const onError = vi.fn()
        const a = atom(1)
        const c = computed(async () => {
            const v = a()
            if (v === 2) throw new Error('listener boom')
            return v
        }, undefined, undefined, {onDestroy() {}})
        getComputedInternal(c)!.on('error', onError)
        await macroTask(10)

        a(2)
        await macroTask(20)
        expect(onError).toHaveBeenCalledTimes(1)
        expect(String(onError.mock.calls[0][0])).toContain('listener boom')
        expect(consoleError).not.toHaveBeenCalled()
    })

    test('exported recompute(item, force) returns an awaitable cleanPromise for async computed', async () => {
        let runs = 0
        const c = computed(async () => {
            runs++
            return runs
        })
        await macroTask(10)
        expect(c.raw).toBe(1)
        await recompute(c, true)
        expect(c.raw).toBe(2)
    })
})

describe('F2: huge batches do not blow the argument limit', () => {
    test('150k pushes in one batch flow through a patchable computed without RangeError', () => {
        const source = new RxList<number>([])
        const derived = new RxList(
            function computation(this: RxList<number>) {
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                return source.data.slice()
            },
            function applyPatch(this: RxList<number>, _data, triggerInfos) {
                for (const info of triggerInfos) {
                    if (info.method === 'splice') {
                        this.spliceArray(info.argv![0], info.argv![1], info.argv!.slice(2))
                    }
                }
            }
        )
        const N = 150000
        batch(() => {
            for (let i = 0; i < N; i++) source.push(i)
        })
        expect(derived.data.length).toBe(N)
        expect(derived.data[N - 1]).toBe(N - 1)
    })
})

describe('F3: negative/out-of-range splice start is normalized for internal consumers', () => {
    test('map with index survives negative-start splice (used to crash on undefined index atom)', () => {
        const source = new RxList([1, 2, 3, 4])
        const mapped = source.map((x, index) => `${x}@${index!.raw}`)
        expect(mapped.data).toEqual(['1@0', '2@1', '3@2', '4@3'])

        source.splice(-1, 1, 9) // 等价 splice(3, 1, 9)
        expect(source.data).toEqual([1, 2, 3, 9])
        expect(mapped.data).toEqual(['1@0', '2@1', '3@2', '9@3'])

        source.splice(-10, 0, 0) // 越界负数：等价头插
        expect(source.data).toEqual([0, 1, 2, 3, 9])
        expect(mapped.data.length).toBe(5)
        expect(mapped.data[0]).toBe('0@0')
    })

    test('at(index) subscribers receive SET triggers on negative-start splice (used to go silently stale)', () => {
        const list = new RxList([1, 2, 3, 4])
        const last = computed(() => list.at(3))
        expect(last()).toBe(4)
        list.splice(-1, 1, 9)
        expect(last.raw).toBe(9)
    })

    test('metadata fast path: out-of-range deleteCount clear still works', () => {
        const list = new RxList([1, 2, 3])
        const mapped = list.map(x => x * 2)
        list.splice(-3, Infinity) // 等价 clear
        expect(list.data).toEqual([])
        expect(mapped.data).toEqual([])
    })

    test('fractional and NaN starts follow Array#splice ToIntegerOrInfinity semantics', () => {
        const source = new RxList(['a', 'b', 'c'])
        const mapped = source.map(x => x.toUpperCase())
        source.splice(1.5 as any, 1, 'X') // 截断为 1
        expect(source.data).toEqual(['a', 'X', 'c'])
        expect(mapped.data).toEqual(['A', 'X', 'C'])
        source.splice(NaN as any, 1, 'Y') // NaN 归 0
        expect(source.data).toEqual(['Y', 'X', 'c'])
        expect(mapped.data).toEqual(['Y', 'X', 'C'])
    })

    test('contract preserved: triggerInfo.argv still carries the raw user arguments', () => {
        const list = new RxList(['a', 'b', 'c'])
        const sub = captureTriggerInfos(list)
        list.splice(-1, 1)
        expect(sub.captured[0].argv).toEqual([-1, 1])
        expect(sub.captured[0].methodResult).toEqual(['c'])
        sub.captured.length = 0

        list.splice(-10, 0, 'x')
        expect(sub.captured[0].argv).toEqual([-10, 0, 'x'])
        expect(list.data).toEqual(['x', 'a', 'b'])
        sub.destroy()
    })

    test('findIndex handles negative-start splice patches', () => {
        const list = new RxList([10, 20, 30])
        const idx = list.findIndex(v => v === 99)
        expect(idx()).toBe(-1)
        list.splice(-1, 1, 99) // 末尾替换出一个新匹配
        expect(idx()).toBe(2)
        list.splice(-3, 1) // 删除头部，匹配位置前移
        expect(idx()).toBe(1)
    })

    test('concat handles negative-start splice from a non-first source', () => {
        const a = new RxList([1, 2])
        const b = new RxList([3, 4])
        const merged = a.concat(b)
        expect(merged.data).toEqual([1, 2, 3, 4])
        b.splice(-1, 0, 5) // b 变为 [3, 5, 4]
        expect(merged.data).toEqual([1, 2, 3, 5, 4])
    })

    test('groupBy treats out-of-range negative start as head insert', () => {
        const list = new RxList([1, 2])
        const groups = list.groupBy(() => 'all')
        expect(groups.data.get('all')!.data).toEqual([1, 2])
        list.splice(-10, 0, 3, 4) // 等价头插 [3, 4]
        expect(list.data).toEqual([3, 4, 1, 2])
        expect(groups.data.get('all')!.data).toEqual([3, 4, 1, 2])
    })

    test('createIndexKeySelection normalizes negative start and overshooting deleteCount', () => {
        const currentValue = atom<null | number>(2)
        const list = new RxList(['a', 'b', 'c', 'd'])
        const selection = list.createIndexKeySelection(currentValue)
        expect(selection.data.map(([, indicator]) => indicator.raw)).toEqual([false, false, true, false])
        list.splice(-4, 100, 'x') // 等价 splice(0, 4, 'x')：全量替换
        expect(selection.data.length).toBe(1)
        expect(selection.data[0][0]).toBe('x')
    })
})

describe('F4: throwing recompute inside built-in schedulers', () => {
    afterEach(() => {
        vi.restoreAllMocks()
    })

    test('scheduleNextMicroTask: computed recovers after a throwing recompute (no permanent wedge)', async () => {
        const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
        const a = atom(0)
        // CAUTION getter 必须先读依赖再抛错：依赖是每轮运行重新收集的，读之前就抛
        //  会（与 Vue 一致地）丢掉订阅，那是另一种性质的问题。这里专门验证调度器
        //  的 queuedRecomputes 不再因异常永久卡死。
        const c = computed(() => {
            const v = a()
            if (v === 1) throw new Error('boom-once')
            return v
        }, undefined, scheduleNextMicroTask)
        expect(c.raw).toBe(0)

        a(1)
        await macroTask(10)
        // 错误被上报而不是变成 microtask 的 uncaught exception
        expect(consoleError).toHaveBeenCalledTimes(1)
        expect(String(consoleError.mock.calls[0][1])).toContain('boom-once')

        // 旧实现：queuedRecomputes 永久卡死，这里将保持 0 不再更新
        a(2)
        await macroTask(10)
        expect(c.raw).toBe(2)
    })

    test('autorun default scheduler: throwing rerun is reported, autorun keeps working', async () => {
        const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
        const unhandled = vi.fn()
        process.on('unhandledRejection', unhandled)
        try {
            const a = atom(0)
            const seen: number[] = []
            let shouldThrow = false
            const stop = autorun(() => {
                const v = a()
                if (shouldThrow && v === 1) throw new Error('rerun boom')
                seen.push(v)
            })
            expect(seen).toEqual([0])

            shouldThrow = true
            a(1)
            await macroTask(10)
            expect(unhandled).not.toHaveBeenCalled()
            expect(consoleError).toHaveBeenCalledTimes(1)

            shouldThrow = false
            a(2)
            await macroTask(10)
            expect(seen).toEqual([0, 2])
            stop()
        } finally {
            process.off('unhandledRejection', unhandled)
        }
    })
})

describe('F5: one throwing effect in a batch no longer starves the rest', () => {
    afterEach(() => {
        vi.restoreAllMocks()
    })

    test('sibling computeds still update; the first error propagates to the batch caller', () => {
        const a = atom(0)
        computed(() => {
            if (a() === 1) throw new Error('boom')
            return a()
        })
        const c2 = computed(() => a() * 100)
        expect(c2()).toBe(0)

        expect(() => batch(() => a(1))).toThrow('boom')
        // 旧实现：c2 连脏标记都没有，永久停留在陈旧的 0
        expect(c2.raw).toBe(100)
        expect(getComputedInternal(c2)!._status).toBe(STATUS_CLEAN)
    })

    test('additional errors are reported via console.error, not silently swallowed', () => {
        const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
        const a = atom(0)
        computed(() => {
            if (a() === 1) throw new Error('first boom')
            return a()
        })
        computed(() => {
            if (a() === 1) throw new Error('second boom')
            return a()
        })
        const c3 = computed(() => a() + 1)

        expect(() => batch(() => a(1))).toThrow('first boom')
        expect(consoleError).toHaveBeenCalledTimes(1)
        expect(String(consoleError.mock.calls[0][1])).toContain('second boom')
        expect(c3.raw).toBe(2)
    })
})

describe('I1: context onCleanup runs exactly once', () => {
    test('stale cleanup is not re-invoked when the next run does not register one', () => {
        const a = atom(0)
        const calls: string[] = []
        computed(({onCleanup}) => {
            const v = a()
            if (v === 0) {
                onCleanup(() => calls.push('cleanup-of-run0'))
            }
            return v
        })
        expect(calls).toEqual([])
        a(1) // run1 清理 run0 注册的 cleanup
        expect(calls).toEqual(['cleanup-of-run0'])
        a(2) // run1 没有注册新 cleanup，不能再次调用 run0 的
        a(3)
        expect(calls).toEqual(['cleanup-of-run0'])
    })

    test('re-registered cleanups run once per recompute (existing semantics preserved)', () => {
        const a = atom(0)
        const calls: number[] = []
        computed(({onCleanup}) => {
            const v = a()
            onCleanup(() => calls.push(v))
            return v
        })
        a(1)
        a(2)
        expect(calls).toEqual([0, 1])
    })
})

describe('I2: filter preserves source order', () => {
    test('middle item toggling to match is inserted at its source position', () => {
        const flag = atom(false)
        const list = new RxList([{id: 1}, {id: 2}, {id: 3}])
        const filtered = list.filter(item => item.id !== 2 || flag())
        expect(filtered.data.map(i => i.id)).toEqual([1, 3])
        flag(true)
        expect(filtered.data.map(i => i.id)).toEqual([1, 2, 3])
        flag(false)
        expect(filtered.data.map(i => i.id)).toEqual([1, 3])
    })

    test('pure middle insert lands at its source position', () => {
        const list = new RxList([1, 3, 5])
        const odd = list.filter(x => x % 2 === 1)
        expect(odd.data).toEqual([1, 3, 5])
        list.splice(1, 0, 7, 2) // 源变为 [1, 7, 2, 3, 5]
        expect(odd.data).toEqual([1, 7, 3, 5])
    })

    test('head/tail toggles stay ordered', () => {
        const threshold = atom(10)
        const list = new RxList([1, 2, 3])
        const filtered = list.filter(x => x >= threshold())
        expect(filtered.data).toEqual([])
        threshold(3)
        expect(filtered.data).toEqual([3])
        threshold(1)
        expect(filtered.data).toEqual([1, 2, 3])
        threshold(2)
        expect(filtered.data).toEqual([2, 3])
    })

    test('unshift + middle set keep order (regression for legacy head special-case)', () => {
        const list = new RxList<{id: number, score: number}>([
            {id: 1, score: 5},
            {id: 2, score: 1},
            {id: 3, score: 5},
        ])
        const filtered = list.filter(i => i.score > 2)
        expect(filtered.data.map(i => i.id)).toEqual([1, 3])
        list.set(1, {id: 2, score: 9}) // 中段替换成匹配项
        expect(filtered.data.map(i => i.id)).toEqual([1, 2, 3])
        list.unshift({id: 0, score: 8})
        expect(filtered.data.map(i => i.id)).toEqual([0, 1, 2, 3])
    })
})

describe('I3: recursiveMarkDirty no longer retains subscribers in bookkeeping sets', () => {
    test('the leaky sets are gone and dep effects still rerun', () => {
        const source = atom(1)
        const parent = new Computed(function (this: Computed) {
            this.manualTrack(source, TrackOpTypes.ATOM, 'value')
            return source.raw
        }, undefined, true)
        parent.trackClassInstance = true
        parent.run([], true)

        let childRuns = 0
        const child = new Computed(function (this: Computed) {
            childRuns++
            this.manualTrack(parent, TrackOpTypes.ATOM, 'value')
        }, undefined, true)
        child.run([], true)

        const before = childRuns
        parent.recursiveMarkDirty()
        expect(childRuns).toBe(before + 1)
        // 泄漏源已移除：不再有对订阅方的记账强引用
        expect((parent as any).markedDirtyEffects).toBeUndefined()
        expect((child as any).dirtyFromDeps).toBeUndefined()

        child.destroy()
        parent.destroy()
    })
})

describe('I4: RxList.at supports negative indexes like Array.prototype.at', () => {
    test('reads from the end and reacts to structural changes', () => {
        const list = new RxList([1, 2, 3])
        expect(list.at(-1)).toBe(3)
        expect(list.at(-3)).toBe(1)
        expect(list.at(-4)).toBeUndefined()

        const last = computed(() => list.at(-1))
        expect(last()).toBe(3)
        list.push(4)
        expect(last.raw).toBe(4)
        list.splice(0, 1)
        expect(last.raw).toBe(4)
        list.pop()
        expect(last.raw).toBe(3)
        list.set(list.data.length - 1, 9)
        expect(last.raw).toBe(9)
    })

    test('empty list returns undefined', () => {
        const list = new RxList<number>([])
        expect(list.at(-1)).toBeUndefined()
    })
})

describe('I6: op types are runtime constants (isolatedModules-safe), not const enums', () => {
    test('members are plain string values usable from value positions', () => {
        expect(TrackOpTypes.METHOD).toBe('method')
        expect(TrackOpTypes.EXPLICIT_KEY_CHANGE).toBe('explicit_key_change')
        expect(TriggerOpTypes.ATOM).toBe('atom')
        expect(TriggerOpTypes.METHOD).toBe('method')
        // 运行时对象存在（const enum 会被完全擦除，无法在 isolatedModules 下引用）
        expect(Object.values(TriggerOpTypes)).toContain('explicit_key_change')
    })
})
