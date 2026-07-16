/**
 * 2026-H3 第六轮深度 review(方法 22)的等价类资产。
 *
 * 四个动态复现缺陷类的实例回归在 knownIssuesReproductions.spec.ts(按纪律从
 * test.fails 翻转);本文件承担各缺陷类的**等价类横扫**(AGENTS §3.1 第 2/3 问):
 *
 * - R6-1 判等语义 × atom 单选 selection 的全部入口:createNewIndicator(全量
 *   重建 + argv 插入)、updateIndicatorsFromCurrentValueChange(toggle)、
 *   autoReset 回收(splice 删除 + EKC 替换 + 孪生行存活)统一 SameValueZero,
 *   与记账 Map/RxSet 多选路径对齐;-0/0 域行为不变的钉扎。
 * - R6-2 实例缓存惰性结构 × 创建作用域生命周期:所有「实例字段缓存 + 惰性
 *   创建」的内部结构(RxList.length、RxMap.keys/values/entries/size、
 *   RxSet.size、AsyncRxSlice.autoFetchPromise、RxTime.resolve 的 autorun)在
 *   宿主 effect 重算后必须继续工作(createDetached 隔离,生命周期归实例)。
 * - R6-3 batch 错误组合矩阵:body 异常 × 订阅者异常的四象限——body 异常恒
 *   优先传播,digest 恒执行(订阅者不被牵连),被抑制的订阅者错误 console 上报。
 * - R6-4 isAtom ⇒ AtomBase 契约:atom.fixed/atom.lazy 的 `.raw` 可读且语义
 *   与两种 atom 形态一致(读值不追踪);computed 解包路径拿到真实值。
 */
import {describe, expect, test, vi} from 'vitest'
import {atom, isAtom} from '../src/atom.js'
import {autorun} from '../src/common.js'
import {computed, destroyComputed} from '../src/computed.js'
import {batch} from '../src/notify.js'
import {createSelection, createSelections, RxList} from '../src/RxList.js'
import {RxMap} from '../src/RxMap.js'
import {RxSet} from '../src/RxSet.js'
import {RxTime} from '../src/RxTime.js'
import {AsyncRxSlice} from '../src/AsyncRxSlice.js'
import type {Atom} from '../src/atom.js'

describe('R6-1 判等语义 × atom 单选 selection 全入口(SameValueZero)', () => {
    test('argv 插入通道:NaN 选中时新插入的 NaN 行 indicator 为 true(≡ 全量重建)', () => {
        const source = new RxList<number>([1])
        const selected = atom<number | null>(NaN)
        const incremental = createSelection(source, selected as any)
        let full: ReturnType<typeof createSelection<number>> | undefined
        try {
            source.push(NaN) // splice 插入侧走 createNewIndicator
            full = createSelection(source, selected as any) // 全新构建 = 全量语义
            expect(incremental.data.map(([, ind]) => ind.raw))
                .toEqual(full.data.map(([, ind]) => ind.raw))
            expect(incremental.data[1][1].raw).toBe(true)
        } finally {
            full?.destroy()
            incremental.destroy()
            source.destroy()
        }
    })

    test('toggle 通道:selected(NaN) 置位、selected(其他) 反选(oldValue NaN 经记账 Map 命中)', () => {
        const source = new RxList<number>([NaN, 1])
        const selected = atom<number | null>(null)
        const selection = createSelection(source, selected as any)
        try {
            selected(NaN)
            expect(selection.data.map(([, ind]) => ind.raw)).toEqual([true, false])
            selected(1)
            expect(selection.data.map(([, ind]) => ind.raw)).toEqual([false, true])
        } finally {
            selection.destroy()
            source.destroy()
        }
    })

    test('autoReset × 孪生 NaN 行:存活时不回收,删净后回收', () => {
        const source = new RxList<number>([NaN, NaN, 1])
        const selected = atom<number | null>(NaN)
        const selection = createSelection(source, selected as any, true)
        try {
            source.splice(0, 1)
            expect(Number.isNaN(selected.raw)).toBe(true) // 孪生行存活,不回收
            source.splice(0, 1)
            expect(selected.raw).toBeNull() // 最后一行删除,回收
        } finally {
            selection.destroy()
            source.destroy()
        }
    })

    test('autoReset × EKC 替换通道:set 替换掉唯一 NaN 行后回收选中值', () => {
        const source = new RxList<number>([NaN, 1])
        const selected = atom<number | null>(NaN)
        const selection = createSelection(source, selected as any, true)
        try {
            source.set(0, 5)
            expect(selected.raw).toBeNull()
        } finally {
            selection.destroy()
            source.destroy()
        }
    })

    test('-0/0 域:SameValueZero 与旧 === 行为一致(0 选中命中 -0 行,零漂移钉扎)', () => {
        const source = new RxList<number>([-0, 1])
        const selected = atom<number | null>(0)
        const selection = createSelection(source, selected as any)
        try {
            // Map/Set 键语义(SameValueZero)下 0 与 -0 同键;=== 也判真,行为不变
            expect(selection.data[0][1].raw).toBe(true)
        } finally {
            selection.destroy()
            source.destroy()
        }
    })

    test('createSelections 多列继承同一修复(atom 列 + RxSet 列并行)', () => {
        const source = new RxList<number>([NaN, 1])
        const single = atom<number | null>(null)
        const multi = new RxSet<number>([])
        const selection = createSelections(source, [single as any], [multi])
        try {
            single(NaN)
            multi.add(NaN)
            expect(selection.data.map(([, a, b]) => [a.raw, b.raw]))
                .toEqual([[true, true], [false, false]])
        } finally {
            selection.destroy()
            multi.destroy()
            source.destroy()
        }
    })
})

describe('R6-2 实例缓存惰性结构 × 宿主重算横扫(createDetached 等价类)', () => {
    test('length/keys/values/entries/size 家族在宿主重算后仍随源更新', () => {
        const rerun = atom(0)
        const list = new RxList<number>([1])
        const map = new RxMap<string, number>([['a', 1]])
        const set = new RxSet<number>([1])
        let len!: Atom<number>
        let keys!: RxList<string>
        let values!: RxList<number>
        let entries!: RxList<[string, number]>
        let mapSize!: Atom<number>
        let setSize!: Atom<number>
        const stop = autorun(() => {
            rerun()
            len = list.length
            keys = map.keys()
            values = map.values()
            entries = map.entries()
            mapSize = map.size
            setSize = set.size
        }, true)
        try {
            rerun(1) // 宿主重算:未 detached 的惰性结构会被 destroyChildren 销毁
            list.push(2)
            map.set('b', 2)
            set.add(2)
            expect(len.raw).toBe(2)
            expect(keys.data).toEqual(['a', 'b'])
            expect(values.data).toEqual([1, 2])
            expect(entries.data).toEqual([['a', 1], ['b', 2]])
            expect(mapSize.raw).toBe(2)
            expect(setSize.raw).toBe(2)
        } finally {
            stop()
            list.destroy()
            map.destroy()
            set.destroy()
        }
    })

    test('AsyncRxSlice.autoFetchPromise 在宿主重算后保持活跃(实例 destroy 仍可清理)', async () => {
        let n = 0
        const slice = new AsyncRxSlice<number>([], async () => [++n])
        const rerun = atom(0)
        const stop = autorun(() => {
            rerun()
            slice.fetch()
        }, true)
        try {
            await new Promise(resolve => setTimeout(resolve, 5))
            rerun(1)
            await new Promise(resolve => setTimeout(resolve, 5))
            const {getComputedInternal} = await import('../src/computed.js')
            expect(getComputedInternal(slice.autoFetchPromise!)?.active).toBe(true)
        } finally {
            stop()
            slice.destroy()
        }
        const {getComputedInternal} = await import('../src/computed.js')
        expect(getComputedInternal(slice.autoFetchPromise!)?.active).toBe(false) // 生命周期归实例
    })

    test('RxTime.resolve 的内部 autorun 在宿主重算后仍响应参数变化并重排定时器', () => {
        vi.useFakeTimers()
        try {
            vi.setSystemTime(1_000_000)
            const deadline = atom(1_000_500)
            const rerun = atom(0)
            let isAfter!: Atom<boolean>
            let t!: RxTime
            const host = computed(() => {
                rerun()
                if (!t) {
                    t = new RxTime()
                    isAfter = t.gt(deadline)
                }
                return null
            })
            try {
                expect(isAfter.raw).toBe(false)
                rerun(1) // 宿主重算:未 detached 时内部 autorun 被销毁
                deadline(1_000_100) // 参数变化 → autorun 重算并按新 deadline 重排定时器
                vi.advanceTimersByTime(150) // 越过新 deadline(+2ms skew 后于 102ms 唤醒)
                expect(isAfter.raw).toBe(true)
            } finally {
                destroyComputed(host)
                t.destroy()
            }
        } finally {
            vi.useRealTimers()
        }
    })
})

describe('R6-3 batch 错误组合矩阵(body 异常恒优先,digest 恒执行)', () => {
    test('body 抛错 × 订阅者正常:body 异常传播,排队订阅者照常执行', () => {
        const source = atom(1)
        let observed = 0
        const stop = autorun(() => { observed = source() }, true)
        let caught: unknown
        try {
            batch(() => {
                source(2)
                throw new Error('BODY_ERROR')
            })
        } catch (error) {
            caught = error
        } finally {
            stop()
        }
        expect((caught as Error).message).toBe('BODY_ERROR')
        expect(observed).toBe(2)
    })

    test('body 抛错 × 订阅者抛错:body 异常传播,订阅者错误 console 上报,兄弟订阅者不受牵连', () => {
        const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
        const source = atom(1)
        let sibling = 0
        const stopThrowing = autorun(() => {
            if (source() > 1) throw new Error('SUBSCRIBER_ERROR')
        }, true)
        const stopSibling = autorun(() => { sibling = source() }, true)
        let caught: unknown
        try {
            batch(() => {
                source(2)
                throw new Error('BODY_ERROR')
            })
        } catch (error) {
            caught = error
        } finally {
            stopThrowing()
            stopSibling()
        }
        const reported = consoleSpy.mock.calls.some(args =>
            args.some(arg => String((arg as Error)?.message ?? arg).includes('SUBSCRIBER_ERROR')))
        consoleSpy.mockRestore()
        expect((caught as Error).message).toBe('BODY_ERROR')
        expect(sibling).toBe(2)
        expect(reported).toBe(true)
    })

    test('body 正常 × 订阅者抛错:第一个订阅者错误抛给调用方(README §3 原契约不变)', () => {
        const source = atom(1)
        const stop = autorun(() => {
            if (source() > 1) throw new Error('SUBSCRIBER_ERROR')
        }, true)
        let caught: unknown
        try {
            batch(() => source(2))
        } catch (error) {
            caught = error
        } finally {
            stop()
        }
        expect((caught as Error).message).toBe('SUBSCRIBER_ERROR')
    })

    test('嵌套 batch:内层 body 异常穿透到外层调用方,外层 digest 照常执行', () => {
        const source = atom(1)
        let observed = 0
        const stop = autorun(() => { observed = source() }, true)
        let caught: unknown
        try {
            batch(() => {
                batch(() => {
                    source(2)
                    throw new Error('INNER_BODY_ERROR')
                })
            })
        } catch (error) {
            caught = error
        } finally {
            stop()
        }
        expect((caught as Error).message).toBe('INNER_BODY_ERROR')
        expect(observed).toBe(2)
    })
})

describe('工程面: async generator getter/patch 必须 loud-fail(静默错误形态清偿)', () => {
    // isAsync/isGenerator 都不命中 AsyncGeneratorFunction——曾被当同步 getter,
    // computed 的值静默变成一个从未被推进的 AsyncGenerator 对象;patch 同形
    // (返回的 generator 对象被当作 patch 成功)。现在构造时报错(dev/prod 一致)。
    test('async generator getter 构造时抛错', () => {
        expect(() => computed(async function* () {
            yield 1
            return 2
        } as any)).toThrow('async generator getter is not supported')
    })

    test('async generator applyPatch 构造时抛错', () => {
        expect(() => computed(
            function (this: any) { return 1 },
            async function* () { yield; return 'ok' } as any,
        )).toThrow('async generator applyPatch is not supported')
    })

    test('同步 generator getter 与 async getter 不受影响(守卫只挡 async generator)', async () => {
        const dep = atom(1)
        const gen = computed(function* (this: any) {
            const v: number = yield dep()
            return v * 2
        } as any)
        const asy = computed(async function (this: any) {
            return dep() + 10
        } as any)
        await new Promise(resolve => setTimeout(resolve, 5))
        try {
            expect((gen as any).raw).toBe(2)
            expect((asy as any).raw).toBe(11)
        } finally {
            destroyComputed(gen)
            destroyComputed(asy)
        }
    })
})

describe('R6-4 isAtom ⇒ AtomBase 契约(atom.fixed/atom.lazy 的 raw)', () => {
    test('atom.fixed:isAtom、raw、调用读值三面一致', () => {
        const fixed = atom.fixed(42)
        expect(isAtom(fixed)).toBe(true)
        expect(fixed.raw).toBe(42)
        expect(fixed()).toBe(42)
    })

    test('atom.lazy:raw 委托 getter,computed 解包拿到真实值', () => {
        const lazy = atom.lazy(() => 7)
        expect(isAtom(lazy)).toBe(true)
        expect(lazy.raw).toBe(7)
        const unwrapped = computed(() => lazy)
        try {
            expect(unwrapped.raw).toBe(7)
        } finally {
            destroyComputed(unwrapped)
        }
    })

    test('atom.lazy 的 raw 语义与 atom 一致:读值不建立依赖', () => {
        const dep = atom(1)
        const lazy = atom.lazy(() => dep() * 2)
        let runs = 0
        const stop = autorun(() => {
            runs++
            void lazy.raw
        }, true)
        try {
            expect(runs).toBe(1)
            dep(2)
            expect(runs).toBe(1) // raw 读不追踪,不触发重跑
            expect(lazy.raw).toBe(4)
        } finally {
            stop()
        }
    })
})
