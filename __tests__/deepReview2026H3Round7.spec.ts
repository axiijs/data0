/**
 * 2026-H3 第七轮深度 review(方法 23:契约条款对抗执行——把 README/接口签名授予
 * 调用方的自由在最敌意的时序/形态下逐条执行)的缺陷类资产。
 *
 * 五个动态复现的缺陷类(修复前均可复现,详见各 describe 头注):
 *   R7-1 reorder 协议载荷的 Order 对与调用方共享(载荷所有权 D1 的内层漏网)
 *   R7-2 RxSet.toList × replace:增量顺序 ≠ 全量重算顺序(「增量 ≡ 全量」含序)
 *   R7-3 prepareRecompute 用户钩子抛错 × 状态机卡死(RECOMPUTING/inPatch 永久滞留)
 *   R7-4 async 收尾阶段(订阅者派发/回退重算)异常 → unhandled rejection(README §5 反例)
 *   R7-5 destroyComputed/recompute × Rx 类结构:注册表未命中 → cryptic TypeError
 * 以及一个改进项:
 *   R7-6 destroy 后惰性 meta 首读重建常驻活 effect(现:创建即随葬,快照语义)
 */
import {afterEach, beforeEach, describe, expect, test, vi} from "vitest";
import {
    atom,
    autorun,
    batch,
    computed,
    destroyComputed,
    recompute,
    RxList,
    RxMap,
    RxSet,
    TriggerInfo,
} from "../src/index.js";
import {getComputedInternal, isComputed} from "../src/computed.js";

const macroTask = (ms = 0) => new Promise(r => setTimeout(r, ms))

/**
 * R7-1 载荷所有权的内层漏网:README「参数契约」授予「reorder 传入的 order 数组
 * 调用后仍归调用方」。round5 D1 的 toProtocolPayload 只做外层 slice,内层
 * [from,to] 对仍与调用方共享——batch/async patch 等延迟消费窗口内调用方复用/
 * 改写 pair(对象池、原地更新是原生预期)会静默毒化全部 patch 消费者:
 * 修复前本组第一个用例 mapped 变成 [20,40,60](双重搬移的 silent 乱序)。
 * 修复:trigger 侧 argv[0] 深拷到 Order 对一层(与 onChange 出口的
 * copyTriggerInfoPayload「外层 + 一层嵌套」对齐)。
 */
describe('R7-1 reorder 协议载荷:Order 对一层的所有权隔离', () => {
    test('batch 窗口内改写 Order 对:map 派生不被毒化(修复前 silent 乱序)', () => {
        const source = new RxList([10, 20, 30])
        const mapped = source.map(x => x * 2)
        const order: [number, number][] = [[0, 1], [1, 0]]
        batch(() => {
            source.reorder(order)
            // README 授予的自由:order 数组(连同其中的 pair)调用后归调用方
            order[0][0] = 2; order[0][1] = 2
            order[1][0] = 2; order[1][1] = 2
        })
        expect(source.data).toEqual([20, 10, 30])
        expect(mapped.data).toEqual([40, 20, 60])
        mapped.destroy(); source.destroy()
    })

    test('batch 多 info(reorder + push)× 改写 pair:map(无 index)与 groupBy 收敛', () => {
        const source = new RxList([1, 2, 3, 4])
        const mapped = source.map(x => x * 10)
        const groups = source.groupBy(x => x % 2)
        const order: [number, number][] = [[0, 3], [3, 0]]
        batch(() => {
            source.reorder(order)     // [4,2,3,1]
            source.push(5)
            order[0][0] = 1; order[0][1] = 1; order[1][0] = 1; order[1][1] = 1
        })
        expect(source.data).toEqual([4, 2, 3, 1, 5])
        expect(mapped.data).toEqual(source.data.map(x => x * 10))
        const expected = new Map<number, number[]>()
        for (const x of source.data) {
            const k = x % 2
            if (!expected.has(k)) expected.set(k, [])
            expected.get(k)!.push(x)
        }
        const actual = new Map<number, number[]>()
        groups.data.forEach((g, k) => actual.set(k, [...g.data]))
        expect(actual).toEqual(expected)
        mapped.destroy(); groups.destroy(); source.destroy()
    })

    test('selection 家族(行随源重排)× batch 内改写 pair:行序与 indicator 不漂移', () => {
        const source = new RxList(['a', 'b', 'c'])
        const selected = new RxSet<string>(['b'])
        const selection = source.createSelection(selected)
        const order: [number, number][] = [[0, 2], [2, 0]]
        batch(() => {
            source.reorder(order)
            order[0][0] = 0; order[0][1] = 0; order[1][0] = 0; order[1][1] = 0
        })
        expect(source.data).toEqual(['c', 'b', 'a'])
        expect(selection.data.map(([item]) => item)).toEqual(['c', 'b', 'a'])
        expect(selection.data.map(([, ind]) => ind.raw)).toEqual([false, true, false])
        selection.destroy(); selected.destroy(); source.destroy()
    })

    test('非 batch 同步路径(对照组):消费先于改写,天然安全', () => {
        const source = new RxList([10, 20, 30])
        const mapped = source.map(x => x * 2)
        const order: [number, number][] = [[0, 1], [1, 0]]
        source.reorder(order)
        order[0][0] = 2
        expect(mapped.data).toEqual([40, 20, 60])
        mapped.destroy(); source.destroy()
    })
})

/**
 * R7-2 「增量 ≡ 全量重算」含可观察顺序:RxSet.replace 采纳的新 Set 决定全部成员
 * 的迭代序(含存活成员的相对顺序),按 [newItems, deletedItems] 增量维护只能改
 * 成员不能改序。修复前 replace([2,1]) on {1,2} 的增量 toList 停在 [1,2],
 * 全量重算(错误恢复/force recompute)得 [2,1]——同一集合状态因到达历史/重建
 * 时机呈现不同行序。修复:toList × replace 回退全量(RxMap.keys × replace 先例,
 * 兄弟实现点一致);add/delete 与 Set 插入序天然对齐,保持增量。
 */
describe('R7-2 RxSet.toList 顺序:增量 ≡ 全量重算(含 replace 重排)', () => {
    test('replace 仅重排成员:toList 行序 ≡ [...set.data]', () => {
        const s = new RxSet([1, 2])
        const l = s.toList()
        s.replace([2, 1])
        expect([...l.data]).toEqual([...s.data])   // [2,1]
        const incremental = [...l.data]
        l.recompute(true)
        expect([...l.data]).toEqual(incremental)   // 全量重建不改变已收敛的序
        l.destroy(); s.destroy()
    })

    test('replace 增删混合:存活成员的相对顺序随新容器', () => {
        const s = new RxSet([1, 2, 3])
        const l = s.toList()
        s.replace([3, 4, 1])
        expect([...l.data]).toEqual([3, 4, 1])
        l.recompute(true)
        expect([...l.data]).toEqual([3, 4, 1])
        l.destroy(); s.destroy()
    })

    test('add/delete 保持增量且序 ≡ Set 插入序;batch 内 add+replace 混排收敛', () => {
        const s = new RxSet([1, 2, 3])
        const l = s.toList()
        let fullRecomputes = 0
        l.on('fullRecompute', () => fullRecomputes++)
        s.delete(2)
        s.add(2)
        expect([...l.data]).toEqual([1, 3, 2])
        expect(fullRecomputes).toBe(0)             // add/delete 零回退(增量格子)
        batch(() => {
            s.add(9)
            s.replace([9, 1])
        })
        expect([...l.data]).toEqual([...s.data])   // [9,1]
        expect(fullRecomputes).toBeGreaterThan(0)  // replace 回退发生(重算格子)
        l.destroy(); s.destroy()
    })
})

/**
 * R7-3 prepareRecompute 的用户钩子窗口(onRecompute/onCleanup 回调、context.onCleanup
 * 注册的清理)位于 setStatus(RECOMPUTING)/inPatch=true 之后、getter/patch 的错误
 * 恢复保护之外。修复前钩子抛错一次:
 *   - 同步结构:status 永久卡 RECOMPUTING——此后每次上游写入都对 mutator 抛
 *     误导性的「detect recompute triggerred in sync recompute」断言,数据永久陈旧;
 *   - async patch 结构:inPatch 永久卡 true——此后所有触发只排队 info 永不消化,
 *     **静默**永久冻结(axii FunctionHost 把 context.onCleanup 直接暴露给用户渲染
 *     函数,清理抛错是现实输入)。
 * 修复:prepareRecompute 与 getter 同一处置(handleRecomputeError 复位
 * DIRTY/inPatch/FULL_RECOMPUTE_PHASE 后 rethrow);dirty 派发改为状态写入先行。
 */
describe('R7-3 用户钩子抛错 × 状态机可恢复性(全形态横扫)', () => {
    test('同步 computed × onRecompute 瞬时抛错:错误抛给 mutator,下次写入自愈', () => {
        const a = atom(1)
        const c = computed(() => a() * 10)
        expect(c()).toBe(10)
        let boomOnce = true
        getComputedInternal(c)!.on('recompute', () => {
            if (boomOnce) { boomOnce = false; throw new Error('hook boom') }
        })
        expect(() => a(2)).toThrow('hook boom')
        // 修复前:这里抛「detect recompute triggerred in sync recompute」且 c 永久停 10
        a(3)
        expect(c.raw).toBe(30)
        destroyComputed(c)
    })

    test('同步 computed × context.onCleanup 清理抛错:同一窗口同一处置', () => {
        const a = atom(1)
        let boom = false
        const c = computed(({onCleanup}) => {
            onCleanup(() => {
                if (boom) { boom = false; throw new Error('cleanup boom') }
            })
            return a() * 10
        })
        expect(c()).toBe(10)
        boom = true
        expect(() => a(2)).toThrow('cleanup boom')
        a(3)
        expect(c.raw).toBe(30)
        destroyComputed(c)
    })

    test('同步 patch 结构(RxList.map)× 钩子抛错:错误恢复后增量结构整体重建', () => {
        const source = new RxList([1, 2])
        const mapped = source.map(x => x * 2)
        let boomOnce = true
        mapped.on('recompute', () => {
            if (boomOnce) { boomOnce = false; throw new Error('row hook boom') }
        })
        expect(() => source.push(3)).toThrow('row hook boom')
        source.push(4)
        expect(mapped.data).toEqual(source.data.map(x => x * 2))
        mapped.destroy(); source.destroy()
    })

    test('async patch 结构 × 钩子抛错:不再静默冻结(修复前 inPatch 永久卡 true)', async () => {
        const source = new RxList([1, 2])
        const derived = new RxList(
            function computation(this: RxList<number>) {
                this.manualTrack(source, 'method' as any, 'method' as any)
                return source.data.map(x => x * 2)
            },
            async function applyPatch(this: RxList<number>, _d, infos: TriggerInfo[]) {
                for (const info of infos) {
                    if (info.method === 'splice') {
                        this.spliceArray(this.data.length, 0, (info.argv!.slice(2) as number[]).map(x => x * 2))
                    }
                }
            }
        )
        let boomOnce = true
        derived.on('recompute', () => {
            if (boomOnce) { boomOnce = false; throw new Error('hook boom once') }
        })
        expect(() => source.push(3)).toThrow('hook boom once')
        await macroTask(10)
        source.push(4)
        await macroTask(20)
        expect(derived.data).toEqual(source.data.map(x => x * 2))
        derived.destroy(); source.destroy()
    })

    test('async getter computed × 钩子抛错:调度路径 console 兜底后自愈', async () => {
        const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
        try {
            const a = atom(1)
            const c = computed(async () => a() * 10)
            await macroTask(10)
            expect(c.raw).toBe(10)
            let boomOnce = true
            getComputedInternal(c)!.on('recompute', () => {
                if (boomOnce) { boomOnce = false; throw new Error('async hook boom') }
            })
            a(2)
            await macroTask(10)
            a(3)
            await macroTask(20)
            expect(c.raw).toBe(30)
            destroyComputed(c)
        } finally {
            consoleError.mockRestore()
        }
    })

    test('持续抛错的 dirty 监听者:标脏先行,每两次写必然追平(修复前永久冻结在 CLEAN)', () => {
        const a = atom(1)
        const c = computed(() => a() * 10)
        expect(c()).toBe(10)
        getComputedInternal(c)!.on('dirty', () => {
            throw new Error('dirty boom')
        })
        expect(() => a(2)).toThrow('dirty boom')
        a(3)                       // status 已是 DIRTY:不再派发 dirty,正常重算追平
        expect(c.raw).toBe(30)
        expect(() => a(4)).toThrow('dirty boom')
        a(5)
        expect(c.raw).toBe(50)
        destroyComputed(c)
    })
})

/**
 * R7-4 async 收尾阶段的异常没有同步调用方:finishFullRecompute 的 digest /
 * finishPatchRecompute 的 sendTriggerInfos / patch 回退触发的全量重算,发生在
 * .then 回调里——修复前直接变成 unhandled rejection(Node 默认崩进程),违反
 * README §5「不产生 unhandled rejection」;且 settleCleanPromise 被跳过,
 * await recompute() 的调用方永久挂起。修复:settle 进 finally + 链尾 console 兜底。
 */
describe('R7-4 async 收尾异常:console 兜底,无 unhandled rejection,cleanPromise 照常 settle', () => {
    let unhandled: any[] = []
    const onUnhandled = (reason: any) => { unhandled.push(reason) }
    beforeEach(() => {
        unhandled = []
        process.on('unhandledRejection', onUnhandled)
    })
    afterEach(() => {
        process.off('unhandledRejection', onUnhandled)
    })

    test('async getter × 下游订阅者抛错(digest 重抛):console 上报,无 unhandled', async () => {
        const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
        try {
            const a = atom(1)
            const c = computed(async () => a() * 10)
            await macroTask(10)
            expect(c.raw).toBe(10)
            const stop = autorun(() => {
                if ((c() as unknown as number) === 20) throw new Error('subscriber boom')
            }, true)
            a(2)
            await macroTask(30)
            expect(unhandled).toEqual([])
            expect(consoleError.mock.calls.some(call => String(call[1]).includes('subscriber boom'))).toBe(true)
            expect(c.raw).toBe(20)      // 本 computed 的数据/状态不受下游错误影响
            a(3)
            await macroTask(20)
            expect(c.raw).toBe(30)
            stop()
            destroyComputed(c)
        } finally {
            consoleError.mockRestore()
        }
    })

    test('async patch return false → 全量回退的同步 getter 抛错:无 unhandled,状态可恢复', async () => {
        const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
        try {
            const source = new RxList([1])
            let boomOnRebuild = true
            const derived = new RxList(
                function computation(this: RxList<number>) {
                    this.manualTrack(source, 'method' as any, 'method' as any)
                    if (boomOnRebuild && this.data?.length) throw new Error('getter boom on rebuild')
                    return source.data.map(x => x * 2)
                },
                async function applyPatch(this: RxList<number>, _d, _infos: TriggerInfo[]) {
                    return false as any
                }
            )
            expect(derived.data).toEqual([2])
            source.push(3)
            await macroTask(30)
            expect(unhandled).toEqual([])
            expect(consoleError.mock.calls.some(call => String(call[1]).includes('getter boom on rebuild'))).toBe(true)
            // 错误恢复:回到 DIRTY,下次触发重建成功
            boomOnRebuild = false
            source.push(5)
            await macroTask(30)
            expect(derived.data).toEqual(source.data.map(x => x * 2))
            derived.destroy(); source.destroy()
        } finally {
            consoleError.mockRestore()
        }
    })

    test('await recompute() 的调用方在下游订阅者抛错时照常 settle(不挂起)', async () => {
        const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
        try {
            const a = atom(1)
            const c = computed(async () => a() * 10)
            await macroTask(10)
            const stop = autorun(() => {
                if ((c() as unknown as number) === 20) throw new Error('await-path subscriber boom')
            }, true)
            a(2)
            const settled = await Promise.race([
                (recompute(c) ?? Promise.resolve()).then(() => 'settled', () => 'settled'),
                macroTask(300).then(() => 'timeout'),
            ])
            expect(settled).toBe('settled')
            expect(unhandled).toEqual([])
            stop()
            destroyComputed(c)
        } finally {
            consoleError.mockRestore()
        }
    })
})

/**
 * R7-5 README §6 承诺「.destroy() 或 destroyComputed 均可」,但 Rx 类结构不经
 * computed() 工厂注册进 computedToInternal——destroyComputed/recompute 对任何
 * RxList/RxMap/RxSet(含派生)都是 cryptic 的 `undefined.destroy` TypeError。
 * 修复:注册表未命中时回退到 Computed 实例本身。
 */
describe('R7-5 destroyComputed/recompute/getComputedInternal × Rx 类结构', () => {
    test('destroyComputed(RxList/RxMap/RxSet):等价 .destroy()', () => {
        const list = new RxList([1, 2])
        const map = new RxMap<string, number>({a: 1})
        const set = new RxSet([1])
        expect(() => destroyComputed(list)).not.toThrow()
        expect(() => destroyComputed(map)).not.toThrow()
        expect(() => destroyComputed(set)).not.toThrow()
        expect(list.active).toBe(false)
        expect(map.active).toBe(false)
        expect(set.active).toBe(false)
    })

    test('recompute(派生 RxList, force):走全量重算而不是 TypeError', () => {
        const s = new RxSet([1, 2])
        const l = s.toList()
        expect(() => recompute(l, true)).not.toThrow()
        expect([...l.data]).toEqual([...s.data])
        l.destroy(); s.destroy()
    })

    test('getComputedInternal/isComputed 对 Rx 结构返回实例本身', () => {
        const list = new RxList([1])
        expect(getComputedInternal(list)).toBe(list)
        expect(isComputed(list)).toBe(true)
        list.destroy()
        // 工厂注册的 AtomComputed 语义不变
        const c = computed(() => 1)
        expect(isComputed(c)).toBe(true)
        expect(getComputedInternal(c)).not.toBe(undefined)
        destroyComputed(c)
    })
})

/**
 * R7-6 destroy 后惰性 meta 首读:destroyResources 只能清理当时已存在的 meta,
 * 销毁后首读曾重建一个**活的** computed 订阅已销毁的 source(值是正确快照,但
 * effect 常驻、逃过一切销毁路径)。现:创建后立即随葬——快照值保留,零残留订阅。
 */
describe('R7-6 destroy 后惰性 meta 首读:快照语义,零残留活 effect', () => {
    test('RxList.length:快照值 + inactive', () => {
        const list = new RxList([1, 2, 3])
        list.destroy()
        const len = list.length
        expect(len()).toBe(3)
        expect(getComputedInternal(len)!.active).toBe(false)
        // 幂等:再次读取仍是同一实例
        expect(list.length).toBe(len)
    })

    test('RxMap.keys/values/entries/size:快照 + 整链 inactive', () => {
        const m = new RxMap<string, number>([['a', 1], ['b', 2]])
        m.destroy()
        const keys = m.keys()
        const values = m.values()
        const entries = m.entries()
        const size = m.size
        expect(keys.data).toEqual(['a', 'b'])
        expect(values.data).toEqual([1, 2])
        expect(entries.data).toEqual([['a', 1], ['b', 2]])
        expect(size()).toBe(2)
        expect(keys.active).toBe(false)
        expect(values.active).toBe(false)
        expect(entries.active).toBe(false)
        expect(getComputedInternal(size)!.active).toBe(false)
    })

    test('RxSet.size:快照 + inactive', () => {
        const s = new RxSet([1, 2])
        s.destroy()
        const size = s.size
        expect(size()).toBe(2)
        expect(getComputedInternal(size)!.active).toBe(false)
    })

    test('对照组:销毁前创建的 meta 随结构销毁,销毁后读取仍为快照(既有语义不变)', () => {
        const list = new RxList([1, 2])
        const len = list.length
        expect(len()).toBe(2)
        list.destroy()
        expect(getComputedInternal(len)!.active).toBe(false)
        expect(len()).toBe(2)
    })

    test('meta getter 幂等性:重复读取返回同一实例(缓存门是承载语义的,mutation 复审补杀)', () => {
        // 缓存门变异成恒 miss 时,每次读取都会新建派生结构:订阅翻倍、
        // destroyResources 只销毁最后一个(先建的全部泄漏)。五个惰性 meta 全员断言。
        const list = new RxList([1, 2])
        expect(list.length).toBe(list.length)
        const m = new RxMap<string, number>({a: 1})
        expect(m.keys()).toBe(m.keys())
        expect(m.values()).toBe(m.values())
        expect(m.entries()).toBe(m.entries())
        expect(m.size).toBe(m.size)
        const s = new RxSet([1])
        expect(s.size).toBe(s.size)
        list.destroy(); m.destroy(); s.destroy()
    })

    test('destroy 后行级 effect 僵尸检查:行内 atom 变化不再驱动 mapFn(行为钉扎)', () => {
        // 注:destroyResources 的 frames 销毁循环变异(清空循环体)不被本测试检出——
        // rowComputed 同时是 mapped 的 child,destroyChildren 兜底销毁(构造性遮蔽,
        // mutation 幸存分类 (c) 防御分支,与 dep.ts 的 reattach 分支同类接受)。
        // 本测试钉的是可观察行为本身:readsItemAtom 行 × destroy 的僵尸不可达。
        const rowAtom = atom(1)
        const source = new RxList([{v: rowAtom}])
        let mapRuns = 0
        const mapped = source.map(item => {
            mapRuns++
            return item.v()
        })
        expect(mapped.data).toEqual([1])
        expect(mapRuns).toBe(1)
        mapped.destroy()
        rowAtom(2)
        expect(mapRuns).toBe(1)   // 行级 effect 已随 mapped 销毁,mapFn 不得重跑
        source.destroy()
    })

    test('RxMap.keys × delete 的 SameValueZero 子句:NaN key 在场时删除其他 key 不误删 NaN(mutation 复审补杀)', () => {
        // 变异 `(key !== key && deletedKey !== deletedKey)` → `(key !== key && true)` 时,
        // 谓词对任何 NaN key 恒真:NaN 排在前面时 delete('a') 会从 keys 列表误删 NaN。
        const m = new RxMap<any, number>([[NaN, 1], ['a', 2], ['b', 3]])
        const keys = m.keys()
        expect(keys.data).toEqual([NaN, 'a', 'b'])
        m.delete('a')
        expect(keys.data).toEqual([NaN, 'b'])
        m.delete(NaN)
        expect(keys.data).toEqual(['b'])
        destroyComputed(keys); m.destroy()
    })
})
