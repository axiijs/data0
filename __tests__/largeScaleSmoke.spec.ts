import {describe, expect, test} from 'vitest'
import {createSelection, RxList} from '../src/RxList.js'
import {RxMap} from '../src/RxMap.js'
import {RxSet} from '../src/RxSet.js'
import {batch} from '../src/notify.js'

/**
 * 大 N 冒烟层（2026-H3 round4 补法，R4-3 等价类的常驻防线）。
 *
 * 教训：全部正确性测试 N ≤ 数千；"十万行 replaceData"是明文性能契约
 * （spliceMany 的存在动机），却没有任何**正确性**测试在该量级跑过——
 * slice patch 的中间段 spread 在 150k 插入下 RangeError 潜伏至 round4。
 * 本层把该量级升格为正确性契约：每个算子族一发关键操作，断言与全量模型
 * 一致。目标是抓 spread/实参上限、栈深、平方级放大等规模缺陷类，
 * 不是性能基准（计时归 bench）。
 *
 * 规模债务清偿记录（2026-H3 round4）：groupBy 与 toSorted 的批量增量曾是
 * O(插入数 × 源长)（逐项前缀计数 / 逐项有序插入），10^5 级批量走分钟级，
 * 本层首版被迫绕开。现 groupBy 走单遍分桶批量路径（O(n+k)，每组 ≤2 次
 * splice）、toSorted 批量超阈值回退全量（README 脚注），10^5 批量格子已
 * 点亮（实测 replaceData(100k)：groupBy 6.6s→11ms，toSorted 2.0s→14ms）。
 */

const N = 100_000
const BIG = 150_000 // 超 spread/实参上限的单次插入量（R4-3 触发量级）

function range(n: number, offset = 0): number[] {
    const out = new Array<number>(n)
    for (let i = 0; i < n; i++) out[i] = i + offset
    return out
}

// 大数组比较不走 toEqual（深比较慢且失败信息巨大）：长度 + 逐位快扫 + 定位首个分歧
function expectSameArray(actual: readonly unknown[], expected: readonly unknown[], ctx: string) {
    expect(actual.length, `${ctx} length`).toBe(expected.length)
    for (let i = 0; i < expected.length; i++) {
        if (!Object.is(actual[i], expected[i])) {
            expect.fail(`${ctx}: first divergence at [${i}]: actual=${String(actual[i])} expected=${String(expected[i])}`)
        }
    }
}

describe('10^5 量级冒烟（正确性，非性能）', () => {
    test('replaceData 0→N→N + map/filter/length 派生', () => {
        const source = new RxList<number>([])
        const mapped = source.map(x => x * 2)
        const filtered = source.filter(x => x % 10 === 0)
        const len = source.length
        try {
            source.replaceData(range(N))
            expectSameArray(mapped.data, source.data.map(x => x * 2), 'map after 0→N')
            expectSameArray(filtered.data, source.data.filter(x => x % 10 === 0), 'filter after 0→N')
            expect(len.raw).toBe(N)

            source.replaceData(range(N, 7))
            expectSameArray(mapped.data, source.data.map(x => x * 2), 'map after N→N')
            expectSameArray(filtered.data, source.data.filter(x => x % 10 === 0), 'filter after N→N')
            expect(len.raw).toBe(N)
        } finally {
            mapped.destroy(); filtered.destroy(); source.destroy()
        }
    })

    test('map × BIG 中段插入（spread 上限量级经 patch 插入路径）', () => {
        const source = new RxList<number>(range(N))
        const mapped = source.map(x => x + 1)
        try {
            expect(() => source.spliceArray(10, 5, range(BIG, 1_000_000))).not.toThrow()
            expectSameArray(mapped.data, source.data.map(x => x + 1), 'map after BIG mid-splice')
        } finally {
            mapped.destroy(); source.destroy()
        }
    })

    test('indexBy/toMap/toSet × N 构建 + 批量 replace', () => {
        const source = new RxList<{id: number}>(range(N).map(id => ({id})))
        const byId = source.indexBy('id')
        try {
            expect(byId.data.size).toBe(N)
            source.replaceData(range(N, N).map(id => ({id})))
            expect(byId.data.size).toBe(N)
            expect(byId.data.has(N)).toBe(true)
            expect(byId.data.has(0)).toBe(false)
        } finally {
            byId.destroy(); source.destroy()
        }

        const entries = new RxList<[number, number]>(range(N).map(i => [i, i * 2]))
        const asMap = entries.toMap()
        const asSet = entries.toSet()
        try {
            expect(asMap.data.size).toBe(N)
            expect(asSet.data.size).toBe(N)
            entries.spliceArray(0, 1000, range(1000, 2_000_000).map(i => [i, i] as [number, number]))
            expect(asMap.data.size).toBe(N)
            expect(asMap.data.get(2_000_000)).toBe(2_000_000)
        } finally {
            asMap.destroy(); asSet.destroy(); entries.destroy()
        }
    })

    test('groupBy/toSorted × N 构建 + 小步增量', () => {
        const source = new RxList<number>(range(N))
        const grouped = source.groupBy(x => x % 7)
        const sorted = source.toSorted((a, b) => b - a)
        try {
            expect(grouped.data.size).toBe(7)
            expect(sorted.data[0]).toBe(N - 1)
            source.push(N)
            source.splice(0, 1)
            source.set(0, -5)
            expect(sorted.data[sorted.data.length - 1]).toBe(-5)
            let total = 0
            for (const g of grouped.data.values()) total += g.data.length
            expect(total).toBe(source.data.length)
        } finally {
            for (const g of grouped.data.values()) g.destroy()
            grouped.destroy(); sorted.destroy(); source.destroy()
        }
    })

    test('groupBy × N 批量：replaceData/大段替换（单遍批量路径）+ 幸存组引用稳定', () => {
        const source = new RxList<number>(range(N))
        const grouped = source.groupBy(x => x % 7)
        try {
            const groupRef = grouped.data.get(3)
            // 大段替换（两相都是批量路径；所有组都幸存 → 引用必须稳定）
            source.spliceArray(1000, 50_000, range(80_000, 1_000_000))
            expect(grouped.data.get(3)).toBe(groupRef)
            let total = 0
            for (const g of grouped.data.values()) {
                total += g.data.length
                // 组内序抽查：与模型首元素一致
                expect(g.data.length).toBeGreaterThan(0)
            }
            expect(total).toBe(source.data.length)
            // 全量换血（组会经历清空→重建）
            source.replaceData(range(N, 5_000_000))
            total = 0
            for (const [k, g] of grouped.data) {
                total += g.data.length
                expect(g.data[0] % 7).toBe(k)
            }
            expect(total).toBe(N)
            // reorder × 大 N（单遍分桶路径）
            source.sortSelf((a, b) => b - a)
            expect(grouped.data.get(grouped.data.keys().next().value!)!.data.length).toBeGreaterThan(0)
        } finally {
            for (const g of grouped.data.values()) g.destroy()
            grouped.destroy(); source.destroy()
        }
    })

    test('toSorted × N 批量：replaceData 经阈值回退，结果 ≡ 全量 sort', () => {
        const source = new RxList<number>(range(N).map(i => (i * 7919) % (N * 10)))
        const sorted = source.toSorted((a, b) => a - b)
        try {
            const next = range(N, 3).map(i => (i * 6271) % (N * 10))
            source.replaceData(next)
            const model = next.slice().sort((a, b) => a - b)
            expectSameArray(sorted.data, model, 'toSorted after bulk replaceData')
        } finally {
            sorted.destroy(); source.destroy()
        }
    })

    test('concat × 双 N 源 + 尾部 BIG 追加', () => {
        const a = new RxList<number>(range(N))
        const b = new RxList<number>(range(N, N))
        const combined = a.concat(b)
        try {
            expect(combined.data.length).toBe(2 * N)
            expect(() => b.spliceArray(b.data.length, 0, range(BIG, 3_000_000))).not.toThrow()
            expect(combined.data.length).toBe(2 * N + BIG)
            expect(combined.data[2 * N]).toBe(3_000_000)
            expect(combined.data[N]).toBe(N)
        } finally {
            combined.destroy(); a.destroy(); b.destroy()
        }
    })

    test('createSelection × N + 批量 replace', () => {
        const source = new RxList<number>(range(N))
        const selected = new RxSet<number>([0, N - 1])
        const selection = createSelection(source, selected)
        try {
            expect(selection.data.length).toBe(N)
            expect(selection.data[0][1].raw).toBe(true)
            source.replaceData(range(N, 1))
            expect(selection.data.length).toBe(N)
            expect(selection.data.map(r => r[0]).slice(0, 3)).toEqual([1, 2, 3])
        } finally {
            selection.destroy(); selected.destroy(); source.destroy()
        }
    })

    test('RxMap.replace × N entries + keys/values/size', () => {
        const map = new RxMap<number, number>(range(N).map(i => [i, i] as [number, number]))
        const keys = map.keys()
        const values = map.values()
        const size = map.size
        try {
            expect(keys.data.length).toBe(N)
            expect(size.raw).toBe(N)
            map.replace(range(N, 5).map(i => [i, i * 3] as [number, number]))
            expect(keys.data.length).toBe(N)
            expect(size.raw).toBe(N)
            expect(values.data[0]).toBe(map.data.get(keys.data[0]))
        } finally {
            map.destroy()
        }
    })

    test('RxSet.replace × N + toList', () => {
        const set = new RxSet<number>(range(N))
        const asList = set.toList()
        try {
            expect(asList.data.length).toBe(N)
            set.replace(range(N, 3))
            expect(asList.data.length).toBe(N)
            expect(set.data.has(2)).toBe(false)
            expect(set.data.has(N + 2)).toBe(true)
        } finally {
            asList.destroy(); set.destroy()
        }
    })

    test('batch 多 info × N（digestReplay 重建在大源上不崩溃且收敛）', () => {
        const source = new RxList<number>(range(N))
        const sliced = source.slice(100, 200)
        const found = source.findIndex(x => x === N - 1)
        try {
            batch(() => {
                source.spliceArray(50, 10, range(20, 5_000_000))
                source.set(0, -1)
                source.push(9_999_999)
            })
            expectSameArray(sliced.data, source.data.slice(100, 200), 'slice after batch')
            expect(found.raw).toBe(source.data.findIndex(x => x === N - 1))
        } finally {
            sliced.destroy(); source.destroy()
        }
    })
})
