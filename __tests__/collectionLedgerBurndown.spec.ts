import {describe, expect, test} from 'vitest'
import {atom} from '../src/atom.js'
import {batch} from '../src/notify.js'
import {createSelection, createSelections, createIndexKeySelection, RxList} from '../src/RxList.js'
import {RxMap} from '../src/RxMap.js'
import {RxSet} from '../src/RxSet.js'

/**
 * 集合账本(coverageInventory)2026-H2 燃尽轮:闭合 undefined 值域 × RxMap/RxSet/
 * selection、weirdNum × selection、batchReplay × reduce/indexBy/toMap/selection、
 * destroy × toMap/createSelections/RxSet.has/size/isSupersetOf 等剩余格子。
 */

describe('undefined 值域 × RxMap 派生(undefined 是合法 Map value)', () => {
    test('set(k, undefined) 后 keys/values/entries 与终态 Map 一致', () => {
        const map = new RxMap<string, number | undefined>({a: 1})
        const keys = map.keys()
        const values = map.values()
        const entries = map.entries()
        try {
            map.set('b', undefined) // 新 key,value undefined
            expect(keys.data).toEqual([...map.data.keys()])
            expect(values.data).toEqual([...map.data.values()])
            expect(entries.data).toEqual([...map.data.entries()])

            map.set('b', 2)         // undefined → 有值
            map.set('a', undefined) // 有值 → undefined
            expect(values.data).toEqual([...map.data.values()])
            expect(entries.data).toEqual([...map.data.entries()])

            map.delete('a') // 删除 value 为 undefined 的 entry
            expect(keys.data).toEqual([...map.data.keys()])
            expect(values.data).toEqual([...map.data.values()])
        } finally {
            map.destroy()
        }
    })

    test('values/entries × 重复 value(不同 key 同 value)', () => {
        const map = new RxMap<string, number>({a: 1, b: 1})
        const values = map.values()
        const entries = map.entries()
        try {
            map.set('c', 1)
            expect(values.data).toEqual([1, 1, 1])
            map.delete('a') // 删除其中一个同 value entry,不得误删其它
            expect(values.data).toEqual([...map.data.values()])
            expect(entries.data).toEqual([...map.data.entries()])
        } finally {
            map.destroy()
        }
    })
})

describe('undefined 值域 × RxSet 运算族(undefined 是合法 Set 成员)', () => {
    test('add/delete/replace undefined 成员,派生运算与终态一致', () => {
        const a = new RxSet<number | undefined>([1, undefined])
        const b = new RxSet<number | undefined>([undefined, 2])
        const diff = a.difference(b)
        const uni = a.union(b)
        const inter = a.intersection(b)
        const sym = a.symmetricDifference(b)
        const asList = a.toList()
        const assertAll = () => {
            const A = [...a.data], B = [...b.data]
            const key = (x: number | undefined) => (x === undefined ? 'U' : String(x))
            const sort = (xs: (number | undefined)[]) => xs.map(key).sort()
            expect(sort([...diff.data])).toEqual(sort(A.filter(x => !b.data.has(x))))
            expect(sort([...uni.data])).toEqual(sort([...new Set([...A, ...B])]))
            expect(sort([...inter.data])).toEqual(sort(A.filter(x => b.data.has(x))))
            expect(sort([...sym.data])).toEqual(sort([...A.filter(x => !b.data.has(x)), ...B.filter(x => !a.data.has(x))]))
            expect(sort([...asList.data])).toEqual(sort(A))
        }
        try {
            assertAll()
            b.delete(undefined); assertAll()
            a.delete(undefined); assertAll()
            a.add(undefined); assertAll()
            a.replace([undefined, 3]); assertAll()
            b.replace([3, undefined]); assertAll()
        } finally {
            diff.destroy(); uni.destroy(); inter.destroy(); sym.destroy(); asList.destroy()
            a.destroy(); b.destroy()
        }
    })
})

describe('undefined/NaN 值域 × selection 家族', () => {
    test('undefined item 的选中/反选(记账 Map 以 undefined 为 key)', () => {
        const list = new RxList<number | undefined>([undefined, 1])
        const current = new RxSet<number | undefined>([])
        const sel = createSelection(list, current)
        try {
            current.add(undefined)
            expect(sel.data.map(([, ind]) => ind.raw)).toEqual([true, false])
            current.delete(undefined)
            expect(sel.data.map(([, ind]) => ind.raw)).toEqual([false, false])
        } finally {
            sel.destroy(); list.destroy(); current.destroy()
        }
    })

    test('NaN item 的选中/反选(SameValueZero 记账,含重复 NaN 行)', () => {
        const list = new RxList<number>([NaN, NaN, 1])
        const current = new RxSet<number>([])
        const sel = createSelection(list, current)
        try {
            current.add(NaN)
            expect(sel.data.map(([, ind]) => ind.raw)).toEqual([true, true, false])
            list.splice(0, 1) // 删一行 NaN,孪生行记账不受损
            current.delete(NaN)
            expect(sel.data.map(([, ind]) => ind.raw)).toEqual([false, false])
        } finally {
            sel.destroy(); list.destroy(); current.destroy()
        }
    })

    test('createIndexKeySelection × undefined item(按 index 键,值无关但行内容含 undefined)', () => {
        const list = new RxList<number | undefined>([undefined, 5])
        const current = atom<number | null>(0)
        const sel = createIndexKeySelection(list, current)
        try {
            expect(sel.data.map(([, ind]) => ind.raw)).toEqual([true, false])
            list.set(0, 7) // 替换 undefined 行,index 选中保持
            expect(sel.data.map(([item, ind]) => [item, ind.raw])).toEqual([[7, true], [5, false]])
        } finally {
            sel.destroy(); list.destroy()
        }
    })
})

describe('batchReplay × reduce/indexBy/toMap/selection', () => {
    test('reduce:batch 多操作(尾部追加×2/混合)后 ≡ 全量重算', () => {
        const source = new RxList<number>([1, 2])
        const doubled = source.reduce<RxList<number>>((last, item) => last.push(item * 2))
        try {
            batch(() => { source.push(3); source.push(4) })
            expect(doubled.data).toEqual(source.data.map(x => x * 2))
            batch(() => { source.unshift(0); source.push(5) })
            expect(doubled.data).toEqual(source.data.map(x => x * 2))
        } finally {
            doubled.destroy(); source.destroy()
        }
    })

    test('indexBy:batch 内删含 key 行 + 插同 key 新行,不触发重复 key 断言且终态一致', () => {
        const source = new RxList<{id: number, v: number}>([{id: 1, v: 10}, {id: 2, v: 20}])
        const byId = source.indexBy(item => item.id)
        try {
            batch(() => {
                source.splice(0, 1)          // 删 id:1
                source.push({id: 1, v: 99})  // 再插 id:1
            })
            expect([...byId.data.keys()].sort()).toEqual([1, 2])
            expect(byId.data.get(1)!.v).toBe(99)
        } finally {
            byId.destroy(); source.destroy()
        }
    })

    test('toMap:batch 内删+插同 key 元组,终态一致', () => {
        const source = new RxList<[string, number]>([['a', 1], ['b', 2]])
        const asMap = source.toMap()
        try {
            batch(() => {
                source.splice(0, 1)
                source.push(['a', 9])
            })
            expect([...asMap.data.entries()].sort()).toEqual([['a', 9], ['b', 2]])
        } finally {
            asMap.destroy(); source.destroy()
        }
    })

    test('createSelections:batch 内 splice + currentValues 变化,行与 indicator 全对齐', () => {
        const list = new RxList<number>([1, 2, 3])
        const cur1 = new RxSet<number>([])
        const sel = createSelections(list, [cur1])
        try {
            batch(() => {
                list.splice(0, 1, 7)
                cur1.add(7)
                cur1.add(3)
            })
            expect(sel.data.length).toBe(list.data.length)
            sel.data.forEach(([item, ind], i) => {
                expect(item).toBe(list.data[i])
                expect(ind.raw).toBe(cur1.data.has(item))
            })
        } finally {
            sel.destroy(); list.destroy(); cur1.destroy()
        }
    })

    test('createIndexKeySelection:batch 内不等长 splice 后按 index 校正', () => {
        const list = new RxList<number>([10, 20, 30])
        const current = new RxSet<number>([1])
        const sel = createIndexKeySelection(list, current)
        try {
            batch(() => {
                list.splice(0, 1)   // [20,30]:原选中 index 1 的行(20)移到 0
                list.push(40)       // [20,30,40]
            })
            const selectedIndexes = new Set(current.data)
            sel.data.forEach(([, ind], i) => {
                expect(ind.raw, `row ${i}`).toBe(selectedIndexes.has(i))
            })
        } finally {
            sel.destroy(); list.destroy(); current.destroy()
        }
    })
})

describe('destroy × 账本缺口(僵尸检查:destroy 后不再接收更新)', () => {
    test('toMap', () => {
        const source = new RxList<[string, number]>([['a', 1]])
        const asMap = source.toMap()
        const snapshot = [...asMap.data.entries()]
        asMap.destroy()
        source.push(['b', 2])
        expect([...asMap.data.entries()]).toEqual(snapshot)
        source.destroy()
    })

    test('createSelections(含 stopAutoResetValue 的清理)', () => {
        const list = new RxList<number>([1, 2])
        const cur = new RxSet<number>([1])
        const sel = createSelections(list, [cur, true])
        const snapshot = sel.data.map(([item, ind]) => [item, ind.raw])
        sel.destroy()
        list.push(3)
        cur.add(2)
        expect(sel.data.map(([item, ind]) => [item, ind.raw])).toEqual(snapshot)
        // autoReset effect 已销毁:删除行不再回收选中值
        list.splice(0, 1)
        expect(cur.data.has(1)).toBe(true)
        list.destroy(); cur.destroy()
    })

    test('RxSet.has / size / isSupersetOf', () => {
        const a = new RxSet<number>([1, 2])
        const b = new RxSet<number>([1])
        const has1 = a.has(1)
        const size = a.size
        const superset = a.isSupersetOf(b)
        expect(has1.raw).toBe(true)
        expect(size.raw).toBe(2)
        expect(superset.raw).toBe(true)

        a.destroy() // has/size 随宿主销毁;isSupersetOf 内部 intersection 由其 onDestroy 链清理
        const hasSnapshot = has1.raw
        const sizeSnapshot = size.raw
        b.add(9) // b 变化不得再驱动 superset 的内部派生已销毁部分崩溃
        expect(has1.raw).toBe(hasSnapshot)
        expect(size.raw).toBe(sizeSnapshot)
        b.destroy()
    })
})
