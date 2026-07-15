/**
 * 2026-H3 第三轮深度 review(方法 19:创建时形态假设 × 运行时形态迁移)的
 * 实例回归 + 特征钉扎资产。
 *
 * 方法说明:多个结构的运行时"形态"在创建时一次性定型——atom 的 primitive/proxy
 * 形态由 initValue 决定、派生列表的段偏移/校正区间算术假设源是稠密数组、
 * RxTime 的定时器排定假设线性表达式非退化。攻击轴 = 让值/形态在运行时迁移穿过
 * 这些创建时假设:primitive atom 写入对象、稠密列表经 OOB set 变稀疏后走
 * 校正/分段路径、系数相消。既有 sweep 只覆盖了"稀疏 × 纯尾插"(尾插不进任何
 * 校正循环),盲格在"稀疏 × 不等长 splice / reorder / EKC"。
 *
 * 动态复现并修复的缺陷类:
 *   R3-1 createIndexKeySelection 的两条 index 校正循环(不等长 splice 平移区、
 *        reorder affectedRange)对洞位行直接 `[1]` 解引用 → TypeError 抛给
 *        list.splice/swap 调用方,违反"OOB set × 派生算子不崩溃且可恢复"等价类
 *        (sparseSetOperatorsSweep 资产)。修复:洞位行 ?. 跳过(与 map/filter 的
 *        行级守卫一致)。
 *   R3-2 concat 的 EKC 分支对越界 key(段长跳变)按段内偏移直写,把"段内替换"
 *        变成跨段覆盖——B 段元素整体错位(结构性错乱,不只是洞物化差异)。
 *        修复:key ≥ 旧段长(= 本列表长 − 其他源现长)时回退全量重算。
 *   R3-3 RxMap.replace 的 SET 触发缺 Object.is 判等门与 oldValue 字段——
 *        2026-H3 round2 修复了 set() 的同一等价类("判等门必须覆盖同一变更
 *        语义的所有入口"),replace 作为 set 的批量形态漏网:整表 replace 时
 *        值未变的 key 幽灵触发全部 get(key) 订阅者。修复:同门判等 + oldValue。
 *   R3-4 RxTime 系数相消(t1.gt(t2) 类 RxTime 差,coefficient=0)时
 *        -constant/0 = ±Infinity,setTimeout(Infinity) 在 Node 打
 *        TimeoutOverflowWarning 并 clamp 成 1ms 虚假唤醒。修复:isFinite 守卫。
 *
 * 特征钉扎(既定行为边界,非缺陷;README「传播模型」已同步):
 *   R3-C1 atom 的对象特性(属性读转发/浅属性写触发)由创建时 initValue 的形态
 *        决定且不随值迁移:atom(null) 起手的 primitive 形态写入对象后,属性
 *        读写落在函数对象上(不写 value、不触发)。整值替换(a(newObj))是
 *        该形态下的正确用法(axii 的 atom(null) 惯用法即整值替换)。
 *   R3-C2 class 实例 atom 的读写不对称:set 陷阱放行(写实例 + 触发),
 *        get 不转发(落函数对象)。属性级用法仅对 plain object 承诺。
 */
import {describe, expect, test, vi, afterEach} from 'vitest'
import {atom} from '../src/atom.js'
import {RxList, createIndexKeySelection, createSelection} from '../src/RxList.js'
import {RxMap} from '../src/RxMap.js'
import {RxSet} from '../src/RxSet.js'
import {RxTime} from '../src/RxTime.js'
import {autorun} from '../src/common.js'
import {Computed} from '../src/computed.js'
import {TrackOpTypes, TriggerOpTypes} from '../src/operations.js'
import type {TriggerInfo} from '../src/notify.js'

describe('R3-1 createIndexKeySelection × 稀疏洞 × 校正循环(修复回归)', () => {
    test('不等长 splice 的平移校正跳过洞位行', () => {
        const list = new RxList<number>([1, 2, 3])
        const selected = new RxSet<number>([1])
        const sel = createIndexKeySelection(list, selected)
        list.set(5, 99) // 洞在 3、4
        expect(() => list.splice(0, 1)).not.toThrow()
        // 存活行的指示器仍按 index 语义校正:选中 index 1 → 现在的行 3
        expect(sel.data[0][1].raw).toBe(false)
        expect(sel.data[1][1].raw).toBe(true)
        expect(sel.data[4][1].raw).toBe(false)
        // 可恢复:后续契约内操作继续工作
        expect(() => list.push(7)).not.toThrow()
        sel.destroy(); list.destroy(); selected.destroy()
    })
    test('reorder(swap)的 affectedRange 校正跳过洞位行', () => {
        const list = new RxList<number>([1, 2, 3])
        const selected = new RxSet<number>([0])
        const sel = createIndexKeySelection(list, selected)
        list.set(5, 99)
        expect(() => list.swap(0, 5)).not.toThrow()
        // index 选中语义:选中位置不随 item 移动,swap 后 index 0 仍选中
        expect(sel.data[0][1].raw).toBe(true)
        sel.destroy(); list.destroy(); selected.destroy()
    })
    test('atom currentValues 形态同样不崩溃', () => {
        const list = new RxList<number>([1, 2, 3])
        const selected = atom<number|null>(1)
        const sel = createIndexKeySelection(list, selected)
        list.set(5, 99)
        expect(() => list.splice(0, 1)).not.toThrow()
        expect(sel.data[0][1].raw).toBe(false)
        expect(sel.data[1][1].raw).toBe(true)
        sel.destroy(); list.destroy()
    })
})

describe('R3-2 concat × OOB set 段长跳变(修复回归)', () => {
    function fullConcat(sources: RxList<number>[]): number[] {
        const merged: number[] = []
        sources.forEach(src => { for (const item of src.data) merged.push(item) })
        return merged
    }
    test('前段 OOB set 后增量结果 ≡ 全量重算(后续段不错位)', () => {
        const a = new RxList<number>([1, 2, 3])
        const b = new RxList<number>([4, 5])
        const merged = a.concat(b)
        a.set(10, 99) // A 段长 3 → 11
        expect([...merged.data]).toEqual(fullConcat([a, b]))
        // B 段元素仍在尾部(修复前被留在错误位置)
        expect(merged.data.at(-2)).toBe(4)
        expect(merged.data.at(-1)).toBe(5)
        merged.destroy(); a.destroy(); b.destroy()
    })
    test('段内(非越界)set 保持增量直写', () => {
        const a = new RxList<number>([1, 2, 3])
        const b = new RxList<number>([4, 5])
        const merged = a.concat(b)
        b.set(1, 50)
        expect([...merged.data]).toEqual([1, 2, 3, 4, 50])
        a.set(0, 10)
        expect([...merged.data]).toEqual([10, 2, 3, 4, 50])
        merged.destroy(); a.destroy(); b.destroy()
    })
    test('后段 OOB set 同样回退且一致', () => {
        const a = new RxList<number>([1, 2, 3])
        const b = new RxList<number>([4, 5])
        const merged = a.concat(b)
        b.set(7, 99)
        expect([...merged.data]).toEqual(fullConcat([a, b]))
        merged.destroy(); a.destroy(); b.destroy()
    })
})

describe('R3-3 RxMap.replace 的判等门与 SET 协议形状(修复回归)', () => {
    test('值未变的 key 不触发 SET(get(key) 订阅者不被幽灵触发)', () => {
        const m = new RxMap<string, number>([['a', 1], ['b', 2]])
        const seen: number[] = []
        const stop = autorun(() => { seen.push(m.get('a')!) }, true)
        expect(seen).toEqual([1])
        m.replace([['a', 1], ['b', 3]]) // a 未变,b 变
        expect(seen).toEqual([1])       // a 的订阅者不重跑
        m.replace([['a', 9], ['b', 3]])
        expect(seen).toEqual([1, 9])    // a 变化正常触发
        stop(); m.destroy()
    })
    test('Object.is 语义与 set() 一致:0→-0 触发、NaN→NaN 不触发', () => {
        const m = new RxMap<string, number>([['z', 0], ['n', NaN]])
        const zSeen: number[] = []
        const stop = autorun(() => { zSeen.push(m.get('z')!) }, true)
        let nRuns = 0
        const stopN = autorun(() => { nRuns++; m.get('n') }, true)
        expect(zSeen).toEqual([0])
        expect(nRuns).toBe(1)
        m.replace([['z', -0], ['n', NaN]])
        expect(zSeen.length).toBe(2)             // 0 → -0 是可观察变化
        expect(Object.is(zSeen[1], -0)).toBe(true)
        expect(nRuns).toBe(1)                    // NaN → NaN 不触发
        stop(); stopN(); m.destroy()
    })
    test('replace 的 SET info 带 oldValue(与 set() 协议形状一致)', () => {
        const m = new RxMap<string, number>([['a', 1]])
        const infos: TriggerInfo[] = []
        const capture = new Computed(function (this: Computed) {
            this.manualTrack(m, TrackOpTypes.GET, 'a')
        }, function (_d: any, ts: TriggerInfo[]) {
            infos.push(...ts)
        })
        capture.run([], true)
        m.replace([['a', 5]])
        const setInfo = infos.find(i => i.type === TriggerOpTypes.SET)
        expect(setInfo).toBeTruthy()
        expect(setInfo!.oldValue).toBe(1)
        expect(setInfo!.newValue).toBe(5)
        capture.destroy(); m.destroy()
    })
})

describe('R3-4 RxTime 系数相消不排定时器(修复回归)', () => {
    afterEach(() => { vi.useRealTimers() })
    test('coefficient=0 时不 setTimeout(Infinity)', () => {
        vi.useFakeTimers()
        const t1 = new RxTime()
        const t2 = new RxTime()
        const result = t1.gt(t2.add(1000)) // 系数 1-1=0,恒 false
        expect(result.raw).toBe(false)
        expect((t1 as any).timeoutId).toBe(null) // 修复前是 Infinity 定时器
        vi.advanceTimersByTime(10_000)
        expect(result.raw).toBe(false)
        t1.destroy(); t2.destroy()
    })
})

describe('R3-C1 特征:atom 形态由创建时 initValue 定型(primitive→object 迁移)', () => {
    test('atom(null) 写入对象后:整值读写工作,属性读写落在函数对象上', () => {
        const a = atom<any>(null)
        a({x: 1})
        // 整值路径(axii 惯用法)完全正常
        let runs = 0
        const stop = autorun(() => { runs++; a() }, true)
        expect(a().x).toBe(1)
        expect(a.raw.x).toBe(1)
        a({x: 5})
        expect(runs).toBe(2)
        // 属性级路径不可用:写落在函数对象上,不写 value、不触发
        ;(a as any).x = 9
        expect(runs).toBe(2)          // 不触发
        expect(a.raw.x).toBe(5)       // value 未被改写
        expect((a as any).x).toBe(9)  // 落在函数对象自有属性上
        stop()
    })
    test('对照:atom({...}) 起手的 proxy 形态,属性写触发且写进 value', () => {
        const a = atom<any>({x: 1})
        let runs = 0
        const stop = autorun(() => { runs++; a.x }, true)
        a.x = 2
        expect(runs).toBe(2)
        expect(a.raw.x).toBe(2)
        stop()
    })
})

describe('R3-C2 特征:class 实例 atom 的属性读写不对称', () => {
    class Point { x = 1 }
    test('属性写:写实例 + 触发;属性读:不转发(undefined)', () => {
        const p = new Point()
        const a = atom<any>(p)
        let runs = 0
        const stop = autorun(() => { runs++; a() }, true)
        a.x = 2
        expect(p.x).toBe(2)          // 写穿到实例
        expect(runs).toBe(2)         // 且触发
        expect(a.x).toBeUndefined()  // 读不转发(仅 plain object 承诺属性读)
        expect(a().x).toBe(2)        // 整值读路径正常
        stop()
    })
})

describe('R3-5 幽灵 EKC:非稠密 key(负/小数)的 set 是属性赋值,派生结构不得物化', () => {
    test('filter × set(-1):不得插入幽灵行(sparseOpsFuzz seed=11 命中的最小复现)', () => {
        const list = new RxList<number>([1, 2, 3])
        const even = list.filter(x => typeof x === 'number' && x % 2 === 0)
        expect([...even.data]).toEqual([2])
        list.set(-1, 100) // 数组属性赋值,元素不变;修复前 100(偶数)被物化成真实行插到最前
        expect([...even.data]).toEqual([2])
        expect(list.data.length).toBe(3)
        even.destroy(); list.destroy()
    })
    test('slice × set(1.5):区间内小数 key 不得替换真实行', () => {
        const list = new RxList<number>([10, 20, 30, 40])
        const sliced = list.slice(0, 3)
        expect([...sliced.data]).toEqual([10, 20, 30])
        list.set(1.5 as any, 99) // 修复前:1.5 落在区间内,经 splice 归一化替换掉行 1
        expect([...sliced.data]).toEqual([10, 20, 30])
        sliced.destroy(); list.destroy()
    })
    test('groupBy/toSet/map/selection × 负 key set:成员与行均不变', () => {
        const list = new RxList<number>([1, 2])
        const grouped = list.groupBy(x => x % 2)
        const asSet = list.toSet()
        const mapped = list.map(x => x * 2)
        const sel = new RxSet<number>([1])
        const selection = createSelection(list, sel)
        list.set(-2, 77)
        expect([...asSet.data].sort()).toEqual([1, 2])
        expect([...grouped.data.keys()].sort()).toEqual([0, 1])
        expect(densifyRows(mapped.data)).toEqual([2, 4])
        expect(selection.data.map(r => r[0])).toEqual([1, 2])
        selection.destroy(); mapped.destroy(); asSet.destroy(); grouped.destroy(); list.destroy(); sel.destroy()
    })
})

function densifyRows<T>(a: readonly T[]) { return Array.from(a) }

describe('R3-6 filter × mapList 全量重算必须整体重建 filtered', () => {
    test('filterFn 抛错一次(错误恢复走全量)后,filtered ≡ 终态全量过滤', () => {
        const list = new RxList<number>([1, 2, 3, 4])
        let bomb = false
        const even = list.filter(x => {
            if (bomb) throw new Error('boom')
            return typeof x === 'number' && x % 2 === 0
        })
        expect([...even.data]).toEqual([2, 4])
        bomb = true
        expect(() => list.push(6)).toThrow('boom') // patch 抛错上抛给 mutator(错误恢复语义)
        bomb = false
        list.push(8) // mapList phase 已回退 FULL → 全量重建
        // 修复前:filtered 停留在 [2,4](6/8 丢失;或按旧行为翻倍计数)
        expect([...even.data]).toEqual(list.data.filter(x => x % 2 === 0))
        even.destroy(); list.destroy()
    })
})

describe('R3-7 groupBy × reorder × 稀疏洞:组内容按全下标扫描(与全量物化洞一致)', () => {
    test('已物化的 undefined 组经 reorder 不得被 filter 跳洞清空', () => {
        const list = new RxList<number>([1, 2, 3])
        const grouped = list.groupBy(x => (x === undefined ? 'hole' : x % 2))
        list.set(5, 99)          // 洞在 3、4(增量路径看不见洞——洞不产生事件)
        grouped.recompute(true)  // 全量物化:hole 组 = [undefined, undefined]
        expect(densifyRows(grouped.data.get('hole')!.data)).toEqual([undefined, undefined])
        list.swap(0, 5)          // 修复前:reorder 分支的 Array#filter 跳洞 → hole 组被清空
        // 参考:全量 computation 语义(按 [0,length) 读取,洞位读出 undefined)
        const model = new Map<any, any[]>()
        for (let i = 0; i < list.data.length; i++) {
            const k = list.data[i] === undefined ? 'hole' : (list.data[i] as number) % 2
            if (!model.has(k)) model.set(k, [])
            model.get(k)!.push(list.data[i])
        }
        for (const [k, expected] of model) {
            expect(densifyRows(grouped.data.get(k)!.data), `group[${String(k)}]`).toEqual(densifyRows(expected))
        }
        grouped.destroy(); list.destroy()
    })
})

describe('R3-8 toSorted × reorder:tie 稳定序跟随源序,必须回退全量(矩阵格子实为「重算」)', () => {
    test('reorder 后 tie 组顺序与全量稳定排序一致(回退发生)', () => {
        const source = new RxList<number>([0, -0, 1])
        const sorted = source.toSorted((a, b) => (a as number) - (b as number))
        let fulls = 0
        sorted.on('fullRecompute', () => fulls++)
        expect(Object.is(sorted.data[0], 0)).toBe(true)
        expect(Object.is(sorted.data[1], -0)).toBe(true)
        source.swap(0, 1) // 源序 [-0, 0, 1]:tie 组的稳定序必须翻转
        expect(fulls).toBe(1) // 修复守卫的等价类:reorder 不得被"非稠密 key 忽略"分支吞掉
        expect(Object.is(sorted.data[0], -0)).toBe(true)
        expect(Object.is(sorted.data[1], 0)).toBe(true)
        sorted.destroy(); source.destroy()
    })
})

describe('R3-9 selection itemToIndicators:全量重建清空记账(有界性回归)', () => {
    // 泄漏本体(旧 indicator 不可回收)由 scripts/audit-reachability.mjs 的 WeakRef
    // 检查钉住(修复前 FAIL);本测试钉 CI 可断言的行为面:重复 item × 反复全量
    // 重建后,记账仍精确驱动当前行(重建若不清账,死 indicator 累积在 Set 里,
    // 行级 deleteIndicator 的降级/删除路径会在污染的 Set 上漂移)。
    test('重复 item × force recompute churn × 行删除:存活行 indicator 仍精确', () => {
        const list = new RxList<number>([7, 7])
        const sel = new RxSet<number>([])
        const selection = createSelection(list, sel)
        for (let i = 0; i < 3; i++) selection.recompute(true)
        sel.add(7)
        expect(selection.data.map(r => r[1].raw)).toEqual([true, true])
        list.splice(0, 1) // 删一行,孪生行存活
        expect(selection.data.length).toBe(1)
        expect(selection.data[0][1].raw).toBe(true)
        sel.delete(7)
        expect(selection.data[0][1].raw).toBe(false)
        selection.destroy(); list.destroy(); sel.destroy()
    })
})

describe('R3-M notify.ts mutation 审计盲区补杀', () => {
    test('shouldTrigger=false 对 primitive atom 写路径同样生效(triggerPrimitiveAtomValue 的暂停门)', async () => {
        // 既有 shouldTrigger 覆盖只走 keyed trigger(notifier.trigger);primitive atom
        // 的特化写路径(triggerPrimitiveAtomValue)有独立的暂停门,变异该门无测试检出。
        const {Notifier} = await import('../src/notify.js')
        const a = atom(1)
        const seen: number[] = []
        const stop = autorun(() => { seen.push(a()) }, true)
        expect(seen).toEqual([1])
        Notifier.instance.shouldTrigger = false
        try {
            a(2) // 值已写入,但订阅者不触发
        } finally {
            Notifier.instance.shouldTrigger = true
        }
        expect(a.raw).toBe(2)
        expect(seen).toEqual([1])
        a(3)
        expect(seen).toEqual([1, 3])
        stop()
    })

    test('batch 内 primitive atom 写路径的 session info 形状(scheduleAtomEffect)', async () => {
        // scheduleAtomEffect 是 batch 内 atom 写的特化排队路径:info 必须逐次
        // 累积且形状为 {type:'atom', key:'value', newValue, oldValue}。变异
        // _sessionInfos 初值(塞垃圾元素)或 needsTriggerInfo 门无测试检出——
        // 既有 batch × atom 覆盖都用不消费 info 的 effect。
        const {batch} = await import('../src/notify.js')
        const a = atom(1)
        const batches: TriggerInfo[][] = []
        const capture = new Computed(function (this: Computed) {
            this.manualTrack(a, TrackOpTypes.ATOM, 'value')
        }, function (_d: any, ts: TriggerInfo[]) {
            batches.push([...ts])
        })
        capture.run([], true)
        batch(() => { a(2); a(3) })
        expect(batches.length).toBe(1)
        expect(batches[0].map(i => ({type: i.type, key: i.key, newValue: i.newValue, oldValue: i.oldValue}))).toEqual([
            {type: TriggerOpTypes.ATOM, key: 'value', newValue: 2, oldValue: 1},
            {type: TriggerOpTypes.ATOM, key: 'value', newValue: 3, oldValue: 2},
        ])
        capture.destroy()
    })

    test('同一 effect 同时订阅 key dep 与 ITERATE dep:DELETE 触发经去重路径恰跑一次', () => {
        // trigger 的多 dep 去重路径(dedupedEffects)在 deps 数组含空槽
        // (ITERATE_KEY_KEY_ONLY 无人订阅 → depsMap.get 为 undefined)时依赖
        // `if (!dep) continue` 跳空。既有测试从未同时铺满 key+ITERATE 两个 dep,
        // 变异该守卫/去重结构无检出。
        const m = new RxMap<string, number>([['a', 1], ['b', 2]])
        let runs = 0
        const stop = autorun(() => {
            runs++
            m.get('a')       // key dep
            m.forEach(() => {}) // ITERATE dep
        }, true)
        expect(runs).toBe(1)
        m.delete('a')        // DELETE:key dep + ITERATE + ITERATE_KEY_KEY_ONLY(空)
        expect(runs).toBe(2) // 去重:恰好一次,不因双 dep 跑两次
        stop(); m.destroy()
    })
})
