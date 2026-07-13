import {describe, expect, test} from 'vitest'
import {batch} from '../src/notify.js'
import {createSelection, RxList} from '../src/RxList.js'
import {RxMap} from '../src/RxMap.js'
import {RxSet} from '../src/RxSet.js'
import {duplicateInts, mulberry32} from './fuzzKit.js'
import {expectGroupByEqualsModel} from './stateOracle.js'

/**
 * 方法 11:模型比对(model comparison)。
 *
 * 与逐算子差分 fuzz 的区别:这里把「多个源 × 多种派生 × 链式管道 × selection ×
 * RxSet/RxMap 派生」组装成一张系统级管道网,对源施加随机操作序列(约三分之一
 * 包在 batch 里形成多 info 单次 digest 重放),每步之后断言**网上每个节点**都
 * 等于用朴素 JS(无增量、无调度)从终态源全量重算的参考模型。
 *
 * 该资产同时是覆盖清单中 RxSet 运算族 / RxMap 派生 / createSelection 的
 * batchReplay 列的对账资产(此前为 UNCOVERED),以及链式深层管道(派生的派生)
 * 的常驻防线(此前只在一次性探测中验证过)。
 */
describe('model comparison: 系统级管道网 ≡ 朴素参考模型', () => {
    for (const seed of [201, 202, 203, 204]) {
        test(`seed=${seed}`, () => {
            const rand = mulberry32(seed)
            const val = duplicateInts(rand, 6)
            const key = () => 'k' + Math.floor(rand() * 5)

            // ---- 源 ----
            const list = new RxList<number>([val(), val(), val(), val()])
            const setA = new RxSet<number>([val(), val()])
            const setB = new RxSet<number>([val(), val()])
            const rxmap = new RxMap<string, number>({k0: val(), k1: val()})
            const selected = new RxSet<number>([])

            // ---- 一级派生 ----
            const mapped = list.map(x => x * 3)
            const filtered = list.filter(x => x % 2 === 0)
            const sorted = list.toSorted((a, b) => a - b)
            const grouped = list.groupBy(x => x % 2)
            const selection = createSelection(list, selected)
            const diff = setA.difference(setB)
            const uni = setA.union(setB)
            const inter = setA.intersection(setB)
            const sym = setA.symmetricDifference(setB)
            const aAsList = setA.toList()
            const setASize = setA.size
            const keys = rxmap.keys()
            const values = rxmap.values()
            const entries = rxmap.entries()
            const mapSize = rxmap.size

            // ---- 链式(派生的派生) ----
            const chainedA = mapped.filter(x => x % 2 === 0)          // map -> filter
            const chainedB = filtered.toSorted((a, b) => b - a)       // filter -> toSorted
            const chainedC = sorted.slice(0, 3)                       // toSorted -> slice
            const chainedD = aAsList.toSorted((a, b) => a - b)        // RxSet.toList -> toSorted

            // ---- 朴素参考模型(从终态源全量重算) ----
            const model = {
                mapped: () => list.data.map(x => x * 3),
                filtered: () => list.data.filter(x => x % 2 === 0),
                sorted: () => list.data.slice().sort((a, b) => a - b),
                group: (k: number) => list.data.filter(x => x % 2 === k),
                diff: () => [...setA.data].filter(x => !setB.data.has(x)),
                uni: () => [...new Set([...setA.data, ...setB.data])],
                inter: () => [...setA.data].filter(x => setB.data.has(x)),
                sym: () => [...[...setA.data].filter(x => !setB.data.has(x)), ...[...setB.data].filter(x => !setA.data.has(x))],
                aAsList: () => [...setA.data],
                keys: () => [...rxmap.data.keys()],
                values: () => [...rxmap.data.values()],
                entries: () => [...rxmap.data.entries()],
                chainedA: () => list.data.map(x => x * 3).filter(x => x % 2 === 0),
                chainedB: () => list.data.filter(x => x % 2 === 0).sort((a, b) => b - a),
                chainedC: () => list.data.slice().sort((a, b) => a - b).slice(0, 3),
                chainedD: () => [...setA.data].sort((a, b) => a - b),
            }
            const sortNum = (xs: number[]) => xs.slice().sort((a, b) => a - b)

            // ---- 随机操作(单发或 batch 打包) ----
            const ops: Array<() => void> = [
                () => { const len = list.data.length; list.splice(Math.floor(rand() * (len + 1)), Math.floor(rand() * 2), ...Array.from({length: Math.floor(rand() * 3)}, val)) },
                () => { const len = list.data.length; if (len) list.set(Math.floor(rand() * len), val()) },
                () => { list.sortSelf((a, b) => a - b) },
                () => { const len = list.data.length; if (len >= 2) { const a = Math.floor(rand() * len); const b = Math.floor(rand() * len); if (a !== b) list.reposition(a, b, 1) } },
                () => { setA.add(val()) },
                () => { const arr = [...setA.data]; if (arr.length) setA.delete(arr[Math.floor(rand() * arr.length)]) },
                () => { setA.replace(Array.from({length: Math.floor(rand() * 4)}, val)) },
                () => { setB.add(val()) },
                () => { const arr = [...setB.data]; if (arr.length) setB.delete(arr[Math.floor(rand() * arr.length)]) },
                () => { rxmap.set(key(), val()) },
                () => { rxmap.delete(key()) },
                () => { const obj: Record<string, number> = {}; for (let i = 0; i < Math.floor(rand() * 4); i++) obj[key()] = val(); rxmap.replace(obj) },
                () => { selected.add(val()) },
                () => { const arr = [...selected.data]; if (arr.length) selected.delete(arr[Math.floor(rand() * arr.length)]) },
            ]
            const doRandomOp = () => ops[Math.floor(rand() * ops.length)]()

            try {
                for (let step = 0; step < 100; step++) {
                    const useBatch = rand() < 0.35
                    if (useBatch) {
                        const n = 2 + Math.floor(rand() * 2)
                        batch(() => { for (let i = 0; i < n; i++) doRandomOp() })
                    } else {
                        doRandomOp()
                    }

                    const ctx = `seed=${seed} step=${step} batch=${useBatch} src=${JSON.stringify(list.data)} A=${JSON.stringify([...setA.data])} B=${JSON.stringify([...setB.data])} map=${JSON.stringify([...rxmap.data.entries()])} sel=${JSON.stringify([...selected.data])}`
                    expect(mapped.data, `mapped ${ctx}`).toEqual(model.mapped())
                    expect(filtered.data, `filtered ${ctx}`).toEqual(model.filtered())
                    expect(sorted.data, `sorted ${ctx}`).toEqual(model.sorted())
                    expectGroupByEqualsModel(grouped, list.data, x => x % 2, ctx)
                    expect(selection.data.length, `selection.len ${ctx}`).toBe(list.data.length)
                    selection.data.forEach(([item, indicator], i) => {
                        expect(item, `selection.item[${i}] ${ctx}`).toBe(list.data[i])
                        expect(indicator.raw, `selection.ind[${i}] ${ctx}`).toBe(selected.data.has(item))
                    })
                    expect(sortNum([...diff.data]), `diff ${ctx}`).toEqual(sortNum(model.diff()))
                    expect(sortNum([...uni.data]), `union ${ctx}`).toEqual(sortNum(model.uni()))
                    expect(sortNum([...inter.data]), `inter ${ctx}`).toEqual(sortNum(model.inter()))
                    expect(sortNum([...sym.data]), `sym ${ctx}`).toEqual(sortNum(model.sym()))
                    expect(sortNum([...aAsList.data]), `toList ${ctx}`).toEqual(sortNum(model.aAsList()))
                    expect(setASize.raw, `setA.size ${ctx}`).toBe(setA.data.size)
                    expect(keys.data, `keys ${ctx}`).toEqual(model.keys())
                    expect(values.data, `values ${ctx}`).toEqual(model.values())
                    expect(entries.data, `entries ${ctx}`).toEqual(model.entries())
                    expect(mapSize.raw, `map.size ${ctx}`).toBe(rxmap.data.size)
                    expect(chainedA.data, `chainedA ${ctx}`).toEqual(model.chainedA())
                    expect(chainedB.data, `chainedB ${ctx}`).toEqual(model.chainedB())
                    expect(chainedC.data, `chainedC ${ctx}`).toEqual(model.chainedC())
                    expect(sortNum(chainedD.data), `chainedD ${ctx}`).toEqual(sortNum(model.chainedD()))
                }
            } finally {
                chainedA.destroy(); chainedB.destroy(); chainedC.destroy(); chainedD.destroy()
                mapped.destroy(); filtered.destroy(); sorted.destroy()
                for (const g of grouped.data.values()) g.destroy()
                grouped.destroy()
                selection.destroy()
                diff.destroy(); uni.destroy(); inter.destroy(); sym.destroy(); aAsList.destroy()
                keys.destroy() // values/entries 派生自 keys,随 rxmap.destroyResources 释放
                list.destroy(); setA.destroy(); setB.destroy(); rxmap.destroy(); selected.destroy()
            }
        })
    }
})
