import {describe, expect, test} from 'vitest'
import {atom, type Atom} from '../src/atom.js'
import {computed, destroyComputed} from '../src/computed.js'
import {batch} from '../src/notify.js'
import {RxList} from '../src/RxList.js'
import {RxMap} from '../src/RxMap.js'
import {RxSet} from '../src/RxSet.js'

/**
 * 架构语义特征测试（characterization tests）。
 *
 * 断言固定的是 AGENTS.md「架构决策与已知语义边界」一节描述的既定行为（A1–A3）：
 * 它们经过深度 review 的动态复现与修复评估，结论是与架构绑定、明确不修。
 * 这些测试不是缺陷复现，不得据此立案或提交"修复"。
 *
 * 若某条测试失败，先判断：
 * - 无意的行为漂移 → 修复代码，恢复本测试；
 * - 有意的架构变更 → 与维护者确认方向后，同步更新 AGENTS.md 对应条目与本测试。
 */

describe('A1: 急切推模式允许菱形依赖 glitch', () => {
    test('菱形依赖下游先观察到"新 a + 旧 b"的中间值,随后收敛到终值', () => {
        const a = atom(1)
        // 构造确定的订阅顺序:c 先订阅 a,b 后订阅 a(dep 内按插入序传播)
        let b: Atom<number> | undefined
        const c = computed(() => a() + (b ? b() : 0))
        b = computed(() => a() * 2)
        a(2) // 让 c 重新收集依赖(此后 c 同时依赖 a 与 b)
        expect(c.raw).toBe(6)

        const observed: number[] = []
        const watcher = computed(() => { observed.push(c()) })
        observed.length = 0
        try {
            a(3)
            // 特征断言:c 先以旧 b 算出 7(系统从未处于的中间态,被下游观察到),
            // b 更新后 c 二次重算收敛到 9。中间值与重复重算是急切推模式的既定语义。
            expect(observed).toEqual([7, 9])
            // 边界(A1 中"仍属缺陷"的部分):终值必须收敛正确
            expect(c.raw).toBe(9)
        } finally {
            destroyComputed(watcher)
            destroyComputed(c)
            destroyComputed(b)
        }
    })
})

describe('A2: batch() 内读 computed 返回进入 batch 前的值', () => {
    test('batch 内先写后读得旧值;atom 本身立即生效;batch 结束后一致', () => {
        const a = atom(1)
        const double = computed(() => a() * 2)
        try {
            expect(double()).toBe(2)
            let atomInBatch: number | undefined
            let computedInBatch: number | undefined
            batch(() => {
                a(10)
                atomInBatch = a.raw
                computedInBatch = double()
            })
            // atom 写入立即生效,session 推迟的只是订阅者
            expect(atomInBatch).toBe(10)
            // 特征断言:computed 的标脏随 run 推迟,读路径无"脏则重算"的拉取 → 旧值
            expect(computedInBatch).toBe(2)
            // 边界(A2 中"仍属缺陷"的部分):batch 结束后必须一致
            expect(double()).toBe(20)
        } finally {
            destroyComputed(double)
        }
    })
})

describe('A3: 构造与 replace 采纳外部容器引用(所有权移交)', () => {
    test('RxList 构造零拷贝采纳数组;绕过方法直改不触发通知', () => {
        const raw = [1, 2, 3]
        const list = new RxList(raw)
        const mapped = list.map(x => x * 2)
        try {
            // 特征断言:零拷贝,所有权移交
            expect(list.data).toBe(raw)
            // 契约外用法:绕过方法直改原数组,派生结构无感(静默失联是契约内行为)
            raw.push(4)
            expect(mapped.data).toEqual([2, 4, 6])
        } finally {
            mapped.destroy()
            list.destroy()
        }
    })

    test('RxSet 构造与 replace 采纳传入 Set;RxMap 构造采纳传入 Map', () => {
        const initSet = new Set([1])
        const rxSet = new RxSet<number>(initSet as any)
        const replacement = new Set([2])
        try {
            expect(rxSet.data).toBe(initSet)
            rxSet.replace(replacement)
            expect(rxSet.data).toBe(replacement)
        } finally {
            rxSet.destroy()
        }

        const rawMap = new Map<number, string>([[1, 'a']])
        const rxMap = new RxMap<number, string>(rawMap as any)
        try {
            expect(rxMap.data).toBe(rawMap)
        } finally {
            rxMap.destroy()
        }
    })

    test('边界:经 Rx 方法的修改必须正常触发派生结构', () => {
        const list = new RxList([1, 2, 3])
        const mapped = list.map(x => x * 2)
        try {
            list.push(4)
            expect(mapped.data).toEqual([2, 4, 6, 8])
        } finally {
            mapped.destroy()
            list.destroy()
        }
    })
})
