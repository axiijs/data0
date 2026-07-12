import {describe, expect, test, vi} from 'vitest'
import {atom} from '../src/atom.js'
import {AtomComputed, computed, Computed, destroyComputed, getComputedInternal} from '../src/computed.js'
import {autorun} from '../src/common.js'
import {RxList} from '../src/RxList.js'
import {TrackOpTypes, TriggerOpTypes} from '../src/operations.js'

/**
 * 响应式核心表面账本(coreSurfaceInventory)中高危 UNCOVERED 格子的闭合资产
 * (2026-H2)。每个 describe 对应一个此前无任何测试的「表面 × 维度」格子。
 */

const tick = () => new Promise<void>(r => setTimeout(r, 0))
const wait = (ms: number) => new Promise<void>(r => setTimeout(r, ms))

describe('async getter × destroy 中途(在途全量重算不得复活写入)', () => {
    test('挂起中 destroy:恢复后结果不应用、无 unhandled rejection、可重复销毁', async () => {
        const dep = atom(1)
        let resolveGetter!: (v: number) => void
        const c = computed(async function (this: Computed) {
            const base = dep()
            const extra = await new Promise<number>(r => { resolveGetter = r })
            return base + extra
        })
        const internal = getComputedInternal(c)!
        await tick()
        // 第一轮完成,进入稳态
        resolveGetter(10)
        await tick()
        expect(c.raw).toBe(11)

        // 触发第二轮,挂起中销毁
        dep(2)
        await tick() // microtask 调度启动重算,getter 挂起在 promise 上
        destroyComputed(c)
        expect(internal.active).toBe(false)

        // 在途 getter 恢复:结果不得写入已销毁实例
        resolveGetter(100)
        await wait(10)
        expect(c.raw).toBe(11)
        // cleanPromise 已被 destroy settle,不悬挂
        await internal.cleanPromise
        // 幂等销毁安全
        destroyComputed(c)
    })
})

describe('generator getter × destroy 中途(mid-yield)', () => {
    test('yield 挂起中 destroy:后续段不再应用结果', async () => {
        const dep = atom(1)
        let resolveStep!: (v: number) => void
        const c = computed(function* (this: Computed): Generator<any, number, number> {
            const base = dep()
            const extra: number = yield new Promise<number>(r => { resolveStep = r })
            return base + extra
        } as any)
        const internal = getComputedInternal(c)!
        await tick()
        resolveStep(10)
        await wait(10)
        expect(c.raw).toBe(11)

        dep(5)
        await tick() // 重算启动,挂起在 yield
        destroyComputed(c)
        resolveStep(1000)
        await wait(10)
        expect(c.raw).toBe(11)
        expect(internal.active).toBe(false)
    })
})

describe('自定义调度器 × destroy(排队后销毁)与三参 infos 契约', () => {
    test('已排队的 recompute 在 destroy 后执行是 no-op', async () => {
        const dep = atom(0)
        let queued: ((force?: boolean) => void) | undefined
        const c = computed(() => dep() * 2, undefined, (recompute) => { queued = recompute })
        expect(c.raw).toBe(0)
        dep(1) // 标脏并进入自定义调度器
        expect(queued).toBeTypeOf('function')
        destroyComputed(c)
        // 调度器晚到的执行:不得抛错、不得写入
        expect(() => queued!(true)).not.toThrow()
        expect(c.raw).toBe(0)
    })

    test('声明三参的调度器收到该次触发积累的 triggerInfos 拷贝', () => {
        const source = new RxList<number>([1])
        const received: any[][] = []
        const c = new Computed(
            function (this: Computed) {
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                return source.data.length
            },
            undefined,
            // 三个形参 → scheduleNeedsInfos 生效
            (recompute, _markDirty, infos) => {
                received.push(infos ?? [])
                recompute(true)
            },
        )
        c.run([], true)
        try {
            source.push(2)
            expect(received.length).toBe(1)
            expect(received[0].length).toBe(1)
            expect(received[0][0].method).toBe('splice')
            expect(received[0][0].source).toBe(source)

            source.push(3)
            expect(received.length).toBe(2)
            expect(received[1][0].argv![0]).toBe(2) // push(3) 的操作时位置
        } finally {
            c.destroy()
            source.destroy()
        }
    })
})

describe('调度上下文的错误兜底(无同步调用方可传播)', () => {
    test('scheduleNextMicroTask:getter 抛错被 console.error 兜底,队列出队后可重试', async () => {
        const dep = atom(1)
        let shouldThrow = false
        const c = computed(async () => {
            const v = dep()
            if (shouldThrow) throw new Error('scheduled boom')
            return v * 2
        })
        await tick()
        expect(c.raw).toBe(2)

        const errSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined)
        try {
            shouldThrow = true
            dep(2)
            await wait(10)
            // 错误被兜底上报(不是 unhandled rejection),值保持旧值
            expect(errSpy).toHaveBeenCalled()
            expect(c.raw).toBe(2)

            // queuedRecomputes 已出队:下次触发能重新调度并成功
            shouldThrow = false
            dep(3)
            await wait(10)
            expect(c.raw).toBe(6)
        } finally {
            errSpy.mockRestore()
            destroyComputed(c)
        }
    })

    test('autorun 默认调度(nextJob):重跑抛错被兜底,系统存活且继续重跑', async () => {
        const dep = atom(0)
        const seen: number[] = []
        const errSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined)
        const stop = autorun(() => {
            const v = dep()
            if (v === 1) throw new Error('rerun boom')
            seen.push(v)
        })
        try {
            expect(seen).toEqual([0])
            dep(1) // 重跑抛错 → console.error 兜底
            await wait(10)
            expect(errSpy).toHaveBeenCalled()

            dep(2) // 仍然存活,继续重跑
            await wait(10)
            expect(seen).toEqual([0, 2])
        } finally {
            errSpy.mockRestore()
            stop()
        }
    })
})

describe('patch 抛错后回退全量重算(等价类:错误恢复 × 增量阶段)', () => {
    // 缺陷(2026-H2 动态复现):抛错轮的 triggerInfos 已被消费、patch 可能已部分
    // 应用,但 phase 停留在 PATCH_PHASE——下次触发只增量重放新 info,抛错轮的
    // 变更永久缺失(静默违反"派生 ≡ 全量重算")。修复:handleRecomputeError 统一
    // 回退 FULL_RECOMPUTE_PHASE 并清空残留 info。
    test('sync applyPatch 抛错一次后,下次触发全量恢复一致', () => {
        const source = new RxList<number>([1, 2, 3])
        let shouldThrow = false
        const derived = new RxList<number>(
            function computation(this: RxList<number>) {
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                return source.data.map(x => x * 2)
            },
            function applyPatch(this: RxList<number>, _data, triggerInfos) {
                if (shouldThrow) {
                    shouldThrow = false
                    throw new Error('patch boom')
                }
                for (const info of triggerInfos) {
                    this.spliceArray(info.argv![0] as number, info.argv![1] as number, (info.argv!.slice(2) as number[]).map(x => x * 2))
                }
            }
        )
        try {
            shouldThrow = true
            expect(() => source.push(4)).toThrow('patch boom')
            source.push(5)
            expect(derived.data).toEqual(source.data.map(x => x * 2))
        } finally {
            derived.destroy()
            source.destroy()
        }
    })

    test('async applyPatch 抛错一次后,下次触发全量恢复一致', async () => {
        const source = new RxList<number>([1])
        let shouldThrow = false
        const derived = new RxList<number>(
            function computation(this: RxList<number>) {
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                return source.data.map(x => x * 10)
            },
            async function applyPatch(this: RxList<number>, _data, triggerInfos) {
                await tick()
                if (shouldThrow) {
                    shouldThrow = false
                    throw new Error('async patch boom')
                }
                for (const info of triggerInfos) {
                    this.spliceArray(info.argv![0] as number, info.argv![1] as number, (info.argv!.slice(2) as number[]).map(x => x * 10))
                }
            }
        )
        const internal = getComputedInternal(derived.data) ?? derived
        const errSpy = vi.spyOn(console, 'error').mockImplementation(() => undefined)
        try {
            expect(derived.data).toEqual([10])
            shouldThrow = true
            source.push(2) // 该轮 async patch 将抛错
            await wait(10)
            // 错误无观测方 → console.error 兜底(deepReviewFixes F1 同款契约)
            expect(errSpy).toHaveBeenCalled()

            source.push(3) // 下次触发回退全量,结果与终态一致
            await wait(10)
            expect(derived.data).toEqual(source.data.map(x => x * 10))
        } finally {
            errSpy.mockRestore()
            void internal
            derived.destroy()
            source.destroy()
        }
    })
})

describe('oncePromise 拒绝路径', () => {
    test('fn 抛错 → promise reject 且监听停止', async () => {
        const dep = atom(0)
        const {oncePromise} = await import('../src/common.js')
        let runs = 0
        const p = oncePromise(() => {
            runs++
            if (dep() === 1) throw new Error('once boom')
            return dep() === 2
        })
        dep(1)
        await expect(p).rejects.toThrow('once boom')
        const runsAtReject = runs
        // reject 后监听已停止(stop 经 nextJob,等一拍再写)
        await wait(10)
        dep(2)
        await wait(10)
        expect(runs).toBe(runsAtReject)
    })
})

describe('对象 atom 的 Proxy 协议边界(特征测试:固定当前语义防漂移)', () => {
    // 对象 atom 只实现 get/set/getPrototypeOf 陷阱。以下操作**当前**不代理到内部值:
    // Object.keys/in/delete/spread 作用在 updater 函数对象上。这是既定语义
    // (README「浅属性写入」只承诺属性读写),特征测试防止无意漂移;
    // 若未来补 ownKeys/has/deleteProperty 陷阱,本测试须随语义决策一起更新。
    test('Object.keys / in / delete / spread 的当前行为', () => {
        const obj = atom<{a?: number, b: number}>({a: 1, b: 2})
        expect(Object.keys(obj)).toEqual([])
        expect('a' in obj).toBe(false)
        // @ts-ignore 契约外操作
        delete obj.a
        expect(obj.raw).toEqual({a: 1, b: 2}) // delete 不作用于内部值
        // spread 只拿到 updater 函数对象上的自有可枚举键(Symbol.toPrimitive 挂载),
        // 拿不到内部值的任何数据属性
        expect(Object.keys({...obj})).toEqual([])
        // 属性读写(承诺面)不受影响
        expect(obj.a).toBe(1)
        obj.b = 3
        expect(obj.raw.b).toBe(3)
    })
})

describe('AtomComputed 源模式占位(无 getter)生命周期', () => {
    test('占位实例可安全销毁且 destroy 事件派发', () => {
        const placeholder = new AtomComputed()
        let destroyed = 0
        placeholder.on('destroy', () => destroyed++)
        placeholder.destroy()
        expect(destroyed).toBe(1)
        placeholder.destroy() // 幂等
        expect(destroyed).toBe(1)
    })
})
