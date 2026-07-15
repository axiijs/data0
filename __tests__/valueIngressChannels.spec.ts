import {describe, expect, test} from 'vitest'
import {createSelection, RxList} from '../src/RxList.js'
import {RxSet} from '../src/RxSet.js'
import {batch} from '../src/notify.js'

/**
 * 值域 × 进入通道横扫（2026-H3 round4 补法，R4-2 等价类的常驻防线）。
 *
 * 教训：coverageInventory 的格子粒度是"算子 × 值域"，indexBy × undefined 格子
 * 亮着，但资产只覆盖了值从**初始数据/删除侧**进入的路径——从 **argv 插入**与
 * **EKC newValue** 通道进入的 null/undefined 一个测试都没有，patch 插入侧的
 * 属性读/解构当场 TypeError。值域必须叉乘进入通道：
 *
 *   initialData        构造时已在源里（全量 computation 消费）
 *   argvInsert         push/splice 新增项（patch 插入侧从 argv 消费）
 *   ekcNewValue        set(i, v) 的新值（patch EKC 侧从 info.newValue 消费）
 *   ekcOldValue        set(i, x) 替换掉的旧值（patch EKC 侧从 info.oldValue 消费）
 *   methodResultDelete splice 删除项（patch 删除侧从 methodResult 消费）
 *
 * 每个算子对五通道逐一喂 undefined（及 null，若该算子值域含 null），每步与
 * 全量重算模型差分。indexBy/toMap 的对应横扫在 deepReview2026H3Round4.spec.ts
 * （修复轮资产）；reduce 的 argv 通道在 collectionLedgerBurndown2.spec.ts。
 * 账本：coverageInventory 的 VALUE_INGRESS_INVENTORY。
 */

type V = number | undefined | null

// 对每个派生算子跑同一套五通道操作序列，callers 提供构建/断言/销毁。
function runChannels<D>(
    build: (source: RxList<V>) => D,
    assertEq: (derived: D, src: V[], ctx: string) => void,
    destroy: (derived: D) => void,
    hostileValue: V,
) {
    // initialData 通道：构造时已含敌意值
    const source = new RxList<V>([1, hostileValue, 2, 3])
    const derived = build(source)
    try {
        assertEq(derived, source.data, 'initialData')

        // argvInsert 通道：push 与中段 splice 插入
        expect(() => source.push(hostileValue), 'argvInsert push').not.toThrow()
        assertEq(derived, source.data, 'argvInsert push')
        expect(() => source.splice(1, 0, hostileValue), 'argvInsert splice').not.toThrow()
        assertEq(derived, source.data, 'argvInsert splice')

        // ekcNewValue 通道：实体行被敌意值替换
        expect(() => source.set(0, hostileValue), 'ekcNewValue').not.toThrow()
        assertEq(derived, source.data, 'ekcNewValue')

        // ekcOldValue 通道：敌意值行被实体值替换
        expect(() => source.set(0, 9), 'ekcOldValue').not.toThrow()
        assertEq(derived, source.data, 'ekcOldValue')

        // methodResultDelete 通道：删除敌意值行
        const holeIndex = source.data.findIndex(x => x === hostileValue)
        expect(holeIndex, 'precondition: hostile value present').toBeGreaterThanOrEqual(0)
        expect(() => source.splice(holeIndex, 1), 'methodResultDelete').not.toThrow()
        assertEq(derived, source.data, 'methodResultDelete')

        // 组合：一次 batch 内多通道混排（多 info 重放/回退路径）
        batch(() => {
            source.push(hostileValue)
            source.set(1, hostileValue)
            source.splice(0, 1)
        })
        assertEq(derived, source.data, 'batch mixed')
    } finally {
        destroy(derived)
        source.destroy()
    }
}

describe('undefined × 五通道（全部值消费型派生算子）', () => {
    const fmt = (x: V) => (x === undefined ? 'U' : x === null ? 'N' : String((x as number) * 2))

    test('map', () => {
        runChannels(
            (s) => s.map(fmt),
            (d, src, ctx) => expect(d.data, ctx).toEqual(src.map(fmt)),
            (d) => d.destroy(),
            undefined,
        )
    })

    test('filter', () => {
        const pred = (x: V) => typeof x === 'number' && x % 2 === 1
        runChannels(
            (s) => s.filter(pred),
            (d, src, ctx) => expect(d.data, ctx).toEqual(src.filter(pred)),
            (d) => d.destroy(),
            undefined,
        )
    })

    test('toSorted', () => {
        // README 契约：undefined 元素由引擎语义排尾（comparator 不会收到 undefined）；
        // null 参与 comparator 属契约外（一致全序要求），本列只喂 undefined。
        const cmp = (a: V, b: V) => (a as number) - (b as number)
        runChannels(
            (s) => s.toSorted(cmp),
            (d, src, ctx) => expect(d.data, ctx).toEqual([...src].sort(cmp as (a: V, b: V) => number)),
            (d) => d.destroy(),
            undefined,
        )
    })

    test('groupBy', () => {
        const key = (x: V) => (x === undefined ? 'U' : x === null ? 'N' : (x as number) % 2)
        type K = ReturnType<typeof key>
        runChannels(
            (s) => s.groupBy(key),
            (d, src, ctx) => {
                const model = new Map<K, V[]>()
                for (const x of src) {
                    const k = key(x)
                    if (!model.has(k)) model.set(k, [])
                    model.get(k)!.push(x)
                }
                expect(new Set(d.data.keys()), `${ctx} 键集`).toEqual(new Set(model.keys()))
                for (const [k, items] of model) {
                    expect(d.data.get(k)?.data, `${ctx} 组 ${String(k)}`).toEqual(items)
                }
            },
            (d) => {
                for (const g of d.data.values()) g.destroy()
                d.destroy()
            },
            undefined,
        )
    })

    test('toSet', () => {
        runChannels(
            (s) => s.toSet(),
            (d, src, ctx) => expect([...d.data].sort(), ctx).toEqual([...new Set(src)].sort()),
            (d) => d.destroy(),
            undefined,
        )
    })

    test('createSelection（行元组镜像 + 指示器可用）', () => {
        const currentValues = new RxSet<V | number>([])
        runChannels(
            (s) => createSelection(s, currentValues),
            (d, src, ctx) => {
                expect(d.data.map(row => row[0]), ctx).toEqual(src)
                // 指示器仍可驱动（undefined 行的记账未被污染）
                expect(d.data.every(row => typeof row[1].raw === 'boolean'), `${ctx} indicator`).toBe(true)
            },
            (d) => d.destroy(),
            undefined,
        )
        currentValues.destroy()
    })
})

describe('null × 五通道（值域含 null 的算子）', () => {
    const fmt = (x: V) => (x === undefined ? 'U' : x === null ? 'N' : String((x as number) * 2))

    test('map', () => {
        runChannels(
            (s) => s.map(fmt),
            (d, src, ctx) => expect(d.data, ctx).toEqual(src.map(fmt)),
            (d) => d.destroy(),
            null,
        )
    })

    test('filter', () => {
        const pred = (x: V) => typeof x === 'number' && x % 2 === 1
        runChannels(
            (s) => s.filter(pred),
            (d, src, ctx) => expect(d.data, ctx).toEqual(src.filter(pred)),
            (d) => d.destroy(),
            null,
        )
    })

    test('groupBy', () => {
        const key = (x: V) => (x === undefined ? 'U' : x === null ? 'N' : (x as number) % 2)
        type K = ReturnType<typeof key>
        runChannels(
            (s) => s.groupBy(key),
            (d, src, ctx) => {
                const model = new Map<K, V[]>()
                for (const x of src) {
                    const k = key(x)
                    if (!model.has(k)) model.set(k, [])
                    model.get(k)!.push(x)
                }
                expect(new Set(d.data.keys()), `${ctx} 键集`).toEqual(new Set(model.keys()))
                for (const [k, items] of model) {
                    expect(d.data.get(k)?.data, `${ctx} 组 ${String(k)}`).toEqual(items)
                }
            },
            (d) => {
                for (const g of d.data.values()) g.destroy()
                d.destroy()
            },
            null,
        )
    })

    test('toSet', () => {
        runChannels(
            (s) => s.toSet(),
            (d, src, ctx) => expect(new Set(d.data), ctx).toEqual(new Set(src)),
            (d) => d.destroy(),
            null,
        )
    })

    test('createSelection（null item 行元组镜像 + 指示器可用）', () => {
        const currentValues = new RxSet<V | number>([])
        runChannels(
            (s) => createSelection(s, currentValues),
            (d, src, ctx) => {
                expect(d.data.map(row => row[0]), ctx).toEqual(src)
                expect(d.data.every(row => typeof row[1].raw === 'boolean'), `${ctx} indicator`).toBe(true)
            },
            (d) => d.destroy(),
            null,
        )
        currentValues.destroy()
    })
})
