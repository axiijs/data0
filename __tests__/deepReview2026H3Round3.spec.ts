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
import {RxList, createIndexKeySelection} from '../src/RxList.js'
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
