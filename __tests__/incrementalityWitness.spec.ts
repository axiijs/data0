import {describe, expect, test} from 'vitest'
import {batch} from '../src/notify.js'
import {Computed, getComputedInternal} from '../src/computed.js'
import {createSelection, RxList} from '../src/RxList.js'
import {RxMap} from '../src/RxMap.js'
import {RxSet} from '../src/RxSet.js'
import {atom} from '../src/atom.js'

/**
 * 增量性见证(2026-H2 mutation 审计的直接产物)。
 *
 * RxList.ts 490 个幸存 mutant 的最大等价类:把增量 patch 的条件/区间算术变异成
 * "回退全量重算"——结果仍与全量一致,差分 fuzz 全部杀不死(差分只证结果,不证
 * 增量性)。本资产监听 Computed 的 `fullRecompute` 事件,对 README 支持矩阵中
 * 声明「增量」的格子断言:**契约内单 info 操作不触发全量重算**;对声明「重算」
 * 的格子反向断言回退确实发生(防双向漂移)。
 *
 * 新增算子或修改 patch 回退条件时,必须同步本资产与 README 矩阵。
 */

function witness(target: any): () => number {
    const internal: Computed = target instanceof Computed ? target : getComputedInternal(target)!
    let count = 0
    internal.on('fullRecompute', () => count++)
    return () => count
}

describe('README 矩阵「增量」格子:契约内操作不触发全量重算', () => {
    test('map(含 index):splice/set/reorder 全增量(行级重算不计全量)', () => {
        const source = new RxList<number>([1, 2, 3, 4])
        const mapped = source.map((x, i) => ({x, idx: i}))
        const fulls = witness(mapped)
        source.push(5)
        source.splice(1, 1, 9, 9)
        source.set(0, 7)
        source.reposition(0, 2)
        source.swap(0, 1)
        source.sortSelf((a, b) => a - b)
        expect(fulls()).toBe(0)
        expect(mapped.data.map(e => e.x)).toEqual(source.data)
        mapped.data.forEach((e, i) => expect(e.idx.raw).toBe(i))
        mapped.destroy(); source.destroy()
    })

    test('filter:splice/set/响应式 toggle 增量;reorder 走指示器重建(非全量)', () => {
        const flag = atom(1)
        const source = new RxList<number>([1, 2, 3, 4])
        const filtered = source.filter(x => x % 2 === flag.raw % 2)
        const fulls = witness(filtered)
        source.push(5)
        source.splice(0, 1)
        source.set(0, 8)
        source.sortSelf((a, b) => a - b)
        expect(fulls()).toBe(0)
        expect(filtered.data).toEqual(source.data.filter(x => x % 2 === 1))
        filtered.destroy(); source.destroy()
    })

    test('toSorted:非 tie splice/set 增量', () => {
        const source = new RxList<number>([10, 30, 20])
        const sorted = source.toSorted((a, b) => a - b)
        const fulls = witness(sorted)
        source.push(40)          // 无 tie 插入
        source.splice(0, 1)      // 删除(tie 组无可区分成员)
        source.set(0, 15)        // 替换,无 tie
        expect(fulls()).toBe(0)
        expect(sorted.data).toEqual(source.data.slice().sort((a, b) => a - b))
        sorted.destroy(); source.destroy()
    })

    test('slice(非负边界):单 splice/set 增量', () => {
        const source = new RxList<number>([1, 2, 3, 4, 5])
        const sliced = source.slice(1, 4)
        const fulls = witness(sliced)
        source.push(6)
        source.splice(2, 1, 9)
        source.set(1, 7)
        expect(fulls()).toBe(0)
        expect(sliced.data).toEqual(source.data.slice(1, 4))
        sliced.destroy(); source.destroy()
    })

    test('concat:单源单 splice/set 增量', () => {
        const a = new RxList<number>([1])
        const b = new RxList<number>([2])
        const cat = a.concat(b)
        const fulls = witness(cat)
        a.push(3)
        b.push(4)
        a.set(0, 9)
        expect(fulls()).toBe(0)
        expect(cat.data).toEqual([...a.data, ...b.data])
        cat.destroy(); a.destroy(); b.destroy()
    })

    test('groupBy/indexBy/toMap/toSet/length:单 info 增量', () => {
        const source = new RxList<number>([1, 2, 3])
        const grouped = source.groupBy(x => x % 2)
        const byKey = source.indexBy(x => x)
        const asSet = source.toSet()
        const len = source.length
        const tupleSource = new RxList<[string, number]>([['a', 1]])
        const asMap = tupleSource.toMap()
        const w = [witness(grouped), witness(byKey), witness(asSet), witness(len), witness(asMap)]
        source.push(4)
        source.splice(0, 1)
        source.set(0, 9)
        tupleSource.push(['b', 2])
        tupleSource.set(0, ['c', 3])
        expect(w.map(f => f())).toEqual([0, 0, 0, 0, 0])
        for (const g of grouped.data.values()) g.destroy()
        grouped.destroy(); byKey.destroy(); asSet.destroy(); asMap.destroy()
        source.destroy(); tupleSource.destroy()
    })

    test('findIndex(无响应式谓词)/reduce 尾部追加:增量', () => {
        const source = new RxList<number>([1, 2, 3])
        const found = source.findIndex(x => x === 3)
        const doubled = source.reduce<RxList<number>>((last, item) => last.push(item * 2))
        const wf = witness(found)
        const wr = witness(doubled)
        source.push(4)
        source.push(5)
        expect(wr()).toBe(0)  // 尾部追加增量
        expect(wf()).toBe(0)
        expect(found.raw).toBe(2)
        doubled.destroy()
        ;(getComputedInternal(found) as Computed).destroy()
        source.destroy()
    })

    test('createSelection:splice/set/reorder/选中集变化全增量', () => {
        const source = new RxList<number>([1, 2, 3])
        const current = new RxSet<number>([])
        const sel = createSelection(source, current)
        const fulls = witness(sel)
        source.push(4)
        source.set(0, 9)
        source.sortSelf((a, b) => a - b)
        current.add(4)
        current.delete(4)
        current.replace([2])
        expect(fulls()).toBe(0)
        sel.destroy(); source.destroy(); current.destroy()
    })

    test('RxSet 代数/RxMap keys/size:单 info 增量', () => {
        const a = new RxSet<number>([1, 2])
        const b = new RxSet<number>([2])
        const uni = a.union(b)
        const map = new RxMap<string, number>({x: 1})
        const keys = map.keys()
        const size = map.size
        const w = [witness(uni), witness(keys), witness(size)]
        a.add(3); a.delete(1); a.replace([5])
        b.add(9)
        map.set('y', 2); map.delete('x'); map.set('y', 3)
        expect(w.map(f => f())).toEqual([0, 0, 0])
        uni.destroy(); map.destroy()
        a.destroy(); b.destroy()
    })
})

describe('README 矩阵「重算」格子:回退确实发生(防双向漂移)', () => {
    test('toSorted:tie 插入回退', () => {
        const source = new RxList<number>([10, 20])
        const sorted = source.toSorted((a, b) => a - b)
        const fulls = witness(sorted)
        source.push(10) // 与既有 10 tie
        expect(fulls()).toBe(1)
        sorted.destroy(); source.destroy()
    })

    test('slice:reorder 回退;负边界 splice 回退', () => {
        const source = new RxList<number>([1, 2, 3, 4])
        const sliced = source.slice(1, 3)
        const negSliced = source.slice(-3)
        const w1 = witness(sliced)
        const w2 = witness(negSliced)
        source.sortSelf((a, b) => b - a)
        expect(w1()).toBe(1)
        source.push(5)
        expect(w2()).toBeGreaterThanOrEqual(1)
        sliced.destroy(); negSliced.destroy(); source.destroy()
    })

    test('map(index):batch 多 info 回退;groupBy/slice/findIndex 经 digestReplay 内核不再回退', () => {
        const source = new RxList<number>([1, 2, 3, 4])
        const grouped = source.groupBy(x => x % 2)
        const sliced = source.slice(1, 3)
        const mappedIdx = source.map((x, i) => x + i.raw)
        const found = source.findIndex(x => x === 3)
        const wIncremental = [witness(grouped), witness(sliced), witness(found)]
        const wMapIdx = witness(mappedIdx)
        batch(() => {
            source.push(5)
            source.splice(0, 1)
        })
        // digestReplay 内核重建操作时源状态:可重建的多 info 保持增量
        expect(wIncremental.map(f => f())).toEqual([0, 0, 0])
        // map(index) 的行级 index atom 记账仍回退
        expect(wMapIdx()).toBeGreaterThanOrEqual(1)
        // 增量结果 ≡ 全量重算
        expect([...grouped.data.keys()].sort()).toEqual([...new Set(source.data.map(x => x % 2))].sort())
        for (const [k, g] of grouped.data) {
            expect(g.data).toEqual(source.data.filter(x => x % 2 === k))
        }
        expect(sliced.data).toEqual(source.data.slice(1, 3))
        expect(found.raw).toBe(source.data.findIndex(x => x === 3))
        for (const g of grouped.data.values()) g.destroy()
        grouped.destroy(); sliced.destroy(); mappedIdx.destroy()
        ;(getComputedInternal(found) as Computed).destroy()
        source.destroy()
    })

    test('groupBy/slice/findIndex:多 info 含歧义 EKC(旧值 undefined)时回退全量', () => {
        const source = new RxList<number | undefined>([undefined, 2, 3, 4])
        const grouped = source.groupBy(x => (x ?? 0) % 2)
        const sliced = source.slice(1, 3)
        const found = source.findIndex(x => x === 3)
        const w = [witness(grouped), witness(sliced), witness(found)]
        batch(() => {
            source.splice(2, 1)     // 让 set 不是首条,凑成多 info
            source.set(0, 9)        // oldValue === undefined → digestReplay 判不可重建
        })
        expect(w.map(f => f()).every(c => c >= 1)).toBe(true)
        // 回退是正确性措施:终值仍 ≡ 全量重算
        expect(sliced.data).toEqual(source.data.slice(1, 3))
        expect(found.raw).toBe(source.data.findIndex(x => x === 3))
        for (const [k, g] of grouped.data) {
            expect(g.data).toEqual(source.data.filter(x => (x ?? 0) % 2 === k))
        }
        for (const g of grouped.data.values()) g.destroy()
        grouped.destroy(); sliced.destroy()
        ;(getComputedInternal(found) as Computed).destroy()
        source.destroy()
    })

    test('reduce 非尾部回退;RxMap keys 的 clear/replace 回退', () => {
        const source = new RxList<number>([1, 2])
        const doubled = source.reduce<RxList<number>>((last, item) => last.push(item * 2))
        const wr = witness(doubled)
        source.unshift(0)
        expect(wr()).toBe(1)

        const map = new RxMap<string, number>({a: 1})
        const keys = map.keys()
        const wk = witness(keys)
        map.clear()
        expect(wk()).toBe(1)
        map.replace({b: 2})
        expect(wk()).toBe(2)
        doubled.destroy(); source.destroy(); map.destroy()
    })

    test('findIndex 响应式谓词:元素依赖变化回退', () => {
        const source = new RxList<{score: ReturnType<typeof atom<number>>}>([{score: atom(1)}, {score: atom(5)}])
        const found = source.findIndex(item => (item.score() ?? 0) >= 4)
        const wf = witness(found)
        source.data[0].score(9)
        expect(wf()).toBeGreaterThanOrEqual(1)
        expect(found.raw).toBe(0)
        ;(getComputedInternal(found) as Computed).destroy()
        source.destroy()
    })
})
