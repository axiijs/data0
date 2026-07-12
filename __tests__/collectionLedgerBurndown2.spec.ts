import {describe, expect, test} from 'vitest'
import {atom} from '../src/atom.js'
import {batch} from '../src/notify.js'
import {createIndexKeySelection, createSelections, RxList} from '../src/RxList.js'
import {RxMap} from '../src/RxMap.js'
import {RxSet} from '../src/RxSet.js'
import {destroyComputed} from '../src/computed.js'
import {duplicateInts, mulberry32, withWeirdNumbers} from './fuzzKit.js'

/**
 * 集合账本 2026-H2 第二轮燃尽:weirdNum × RxSet 代数/RxMap values/entries/
 * indexBy/toMap/reduce/selections、undefined × indexBy/toMap/reduce/
 * createSelections/RxSet 谓词、sparseOOB × toMap/reduce/createSelections、
 * batchReplay × RxSet 谓词、reduceToAtom 剩余维度。
 */

describe('weirdNum × RxSet 代数族差分(NaN/-0 成员,SameValueZero 语义)', () => {
    for (const seed of [401, 402]) {
        test(`seed=${seed}`, () => {
            const rand = mulberry32(seed)
            const val = withWeirdNumbers(rand, duplicateInts(rand, 4), 0.35)
            const a = new RxSet<number>([val(), val()])
            const b = new RxSet<number>([val(), val()])
            const diff = a.difference(b)
            const uni = a.union(b)
            const inter = a.intersection(b)
            const sym = a.symmetricDifference(b)
            const asList = a.toList()
            const norm = (xs: number[]) => xs.map(x => (Number.isNaN(x) ? 'NaN' : String(x))).sort()
            const assertAll = (ctx: string) => {
                const A = [...a.data], B = [...b.data]
                expect(norm([...diff.data]), `diff ${ctx}`).toEqual(norm(A.filter(x => !b.data.has(x))))
                expect(norm([...uni.data]), `uni ${ctx}`).toEqual(norm([...new Set([...A, ...B])]))
                expect(norm([...inter.data]), `inter ${ctx}`).toEqual(norm(A.filter(x => b.data.has(x))))
                expect(norm([...sym.data]), `sym ${ctx}`).toEqual(norm([...A.filter(x => !b.data.has(x)), ...B.filter(x => !a.data.has(x))]))
                expect(norm([...asList.data]), `toList ${ctx}`).toEqual(norm(A))
            }
            try {
                for (let step = 0; step < 100; step++) {
                    const target = rand() < 0.5 ? a : b
                    const r = rand()
                    if (r < 0.4) target.add(val())
                    else if (r < 0.75) {
                        const arr = [...target.data]
                        if (arr.length) target.delete(arr[Math.floor(rand() * arr.length)])
                    } else {
                        target.replace(Array.from({length: Math.floor(rand() * 4)}, val))
                    }
                    assertAll(`seed=${seed} step=${step}`)
                }
            } finally {
                diff.destroy(); uni.destroy(); inter.destroy(); sym.destroy(); asList.destroy()
                a.destroy(); b.destroy()
            }
        })
    }
})

describe('weirdNum/duplicates × RxMap values/entries', () => {
    test('NaN key 与 NaN/-0 value 的 set/delete/replace', () => {
        const map = new RxMap<number, number>(new Map<number, number>([[1, NaN]]) as any)
        const values = map.values()
        const entries = map.entries()
        const assertAll = () => {
            expect(values.data.map((x: number) => (Number.isNaN(x) ? 'NaN' : x))).toEqual([...map.data.values()].map((x: any) => (Number.isNaN(x) ? 'NaN' : x)))
            expect(entries.data.length).toBe(map.data.size)
        }
        try {
            assertAll()
            map.set(NaN, -0) // NaN key 合法(SameValueZero)
            assertAll()
            map.set(NaN, 5)  // 更新 NaN key
            assertAll()
            map.delete(NaN)
            assertAll()
        } finally {
            map.destroy()
        }
    })
})

describe('weirdNum/undefined × indexBy/toMap/reduce/reduceToAtom', () => {
    test('indexBy:NaN key 唯一时正常工作(SameValueZero 记账)', () => {
        const source = new RxList<{k: number}>([{k: NaN}, {k: 1}])
        const byKey = source.indexBy(item => item.k)
        try {
            expect(byKey.data.size).toBe(2)
            source.splice(0, 1) // 删 NaN key 行
            expect([...byKey.data.keys()]).toEqual([1])
            source.push({k: NaN})
            expect(byKey.data.size).toBe(2)
        } finally {
            byKey.destroy(); source.destroy()
        }
    })

    test('toMap:NaN key 元组与 undefined value 元组', () => {
        const source = new RxList<[number, number | undefined]>([[NaN, 1], [2, undefined]])
        const asMap = source.toMap()
        try {
            expect(asMap.data.size).toBe(2)
            source.splice(0, 1) // 删 NaN key 元组
            expect(asMap.data.size).toBe(1)
            expect(asMap.data.get(2)).toBe(undefined)
            expect(asMap.data.has(2)).toBe(true)
            source.set(0, [3, 9]) // 替换 [2, undefined]
            expect([...asMap.data.entries()]).toEqual([[3, 9]])
        } finally {
            asMap.destroy(); source.destroy()
        }
    })

    test('reduce/reduceToAtom:undefined 与 NaN/-0 元素经回调透传,尾部追加增量一致', () => {
        const source = new RxList<number | undefined>([1, undefined, NaN])
        const model = (src: (number | undefined)[]) => src.map(x => (x === undefined ? 'U' : Number.isNaN(x) ? 'NaN' : x * 1))
        const collected = source.reduce<RxList<any>>((last, item) => last.push(item === undefined ? 'U' : Number.isNaN(item as number) ? 'NaN' : item))
        const count = source.reduceToAtom((acc: number, item) => acc + (item === undefined ? 0 : 1), 0)
        try {
            expect(collected.data).toEqual(model(source.data))
            source.push(undefined) // 尾部追加增量
            source.push(-0)
            expect(collected.data).toEqual(model(source.data))
            expect(count.raw).toBe(source.data.filter(x => x !== undefined).length)
            source.unshift(5) // 非尾部:回退全量
            expect(collected.data).toEqual(model(source.data))
            expect(count.raw).toBe(source.data.filter(x => x !== undefined).length)
        } finally {
            collected.destroy()
            destroyComputed(count)
            source.destroy()
        }
    })

    test('reduceToAtom:重复值域 + batch 多操作(混合回退)', () => {
        const source = new RxList<number>([2, 2, 3])
        const sum = source.reduceToAtom((acc: number, item) => acc + item, 0)
        try {
            expect(sum.raw).toBe(7)
            batch(() => { source.push(2); source.push(2) })
            expect(sum.raw).toBe(source.data.reduce((a, b) => a + b, 0))
            batch(() => { source.unshift(1); source.push(4) })
            expect(sum.raw).toBe(source.data.reduce((a, b) => a + b, 0))
        } finally {
            destroyComputed(sum)
            source.destroy()
        }
    })
})

describe('sparseOOB × toMap/reduce/createSelections(不崩溃且可恢复)', () => {
    test('toMap(修复回归:OOB set 的 undefined oldValue 不再解构崩溃)', () => {
        const l = new RxList<[string, number]>([['a', 1]])
        const m = l.toMap()
        expect(() => l.set(6, ['z', 9])).not.toThrow()
        expect(() => l.push(['w', 7])).not.toThrow()
        expect(m.data.get('z')).toBe(9)
        expect(m.data.get('w')).toBe(7)
        m.destroy(); l.destroy()
    })

    test('indexBy 属性形式(修复回归:洞位行跳过)', () => {
        const l = new RxList<{id: number}>([{id: 1}])
        const m = l.indexBy('id')
        expect(() => l.set(6, {id: 9})).not.toThrow()
        expect(() => l.push({id: 7})).not.toThrow()
        expect([...m.data.keys()].sort()).toEqual([1, 7, 9])
        m.destroy(); l.destroy()
    })

    test('reduce', () => {
        const l = new RxList<number>([1, 2])
        const doubled = l.reduce<RxList<number>>((last, item) => last.push((item ?? 0) * 2))
        expect(() => l.set(6, 9)).not.toThrow()
        expect(() => l.push(7)).not.toThrow()
        expect(doubled.data[doubled.data.length - 1]).toBe(14)
        doubled.destroy(); l.destroy()
    })

    test('createSelections', () => {
        const l = new RxList<number>([1, 2])
        const cur = new RxSet<number>([])
        const sel = createSelections(l, [cur])
        expect(() => l.set(6, 9)).not.toThrow()
        expect(() => l.push(7)).not.toThrow()
        expect(() => cur.add(7)).not.toThrow()
        sel.destroy(); l.destroy(); cur.destroy()
    })
})

describe('undefined/weirdNum × createSelections/createIndexKeySelection', () => {
    test('createSelections × undefined item 与 NaN item(含孪生行)', () => {
        const list = new RxList<number | undefined>([undefined, NaN, NaN])
        const cur = new RxSet<number | undefined>([])
        const sel = createSelections(list, [cur])
        try {
            cur.add(undefined)
            expect(sel.data.map(([, ind]) => ind.raw)).toEqual([true, false, false])
            cur.add(NaN)
            expect(sel.data.map(([, ind]) => ind.raw)).toEqual([true, true, true])
            cur.replace([])
            expect(sel.data.map(([, ind]) => ind.raw)).toEqual([false, false, false])
        } finally {
            sel.destroy(); list.destroy(); cur.destroy()
        }
    })

    test('createIndexKeySelection × NaN/-0 行内容(index 键,值无关性验证)', () => {
        const list = new RxList<number>([NaN, -0, 1])
        const cur = atom<number | null>(1)
        const sel = createIndexKeySelection(list, cur)
        try {
            expect(sel.data.map(([, ind]) => ind.raw)).toEqual([false, true, false])
            list.set(1, NaN) // 行内容变化不影响 index 选中
            expect(sel.data.map(([, ind]) => ind.raw)).toEqual([false, true, false])
        } finally {
            sel.destroy(); list.destroy()
        }
    })
})

describe('undefined/weirdNum/batchReplay × RxSet 谓词(has/isSubsetOf/isSupersetOf/isDisjointFrom)', () => {
    test('has(undefined)/has(NaN) 响应增删', () => {
        const s = new RxSet<number | undefined>([1])
        const hasU = s.has(undefined)
        const hasNaN = s.has(NaN as number)
        try {
            expect(hasU.raw).toBe(false)
            s.add(undefined)
            expect(hasU.raw).toBe(true)
            s.add(NaN as number)
            expect(hasNaN.raw).toBe(true)
            s.delete(NaN as number)
            expect(hasNaN.raw).toBe(false)
        } finally {
            destroyComputed(hasU); destroyComputed(hasNaN)
            s.destroy()
        }
    })

    test('subset/superset/disjoint × undefined/NaN 成员', () => {
        const a = new RxSet<number | undefined>([undefined])
        const b = new RxSet<number | undefined>([undefined, NaN as number])
        const sub = a.isSubsetOf(b as any)
        const sup = (b as any).isSupersetOf(a)
        const dis = a.isDisjointFrom(b as any)
        try {
            expect(sub.raw).toBe(true)
            expect(sup.raw).toBe(true)
            expect(dis.raw).toBe(false)
            a.delete(undefined)
            a.add(5)
            expect(sub.raw).toBe(false)
            expect(dis.raw).toBe(true)
        } finally {
            destroyComputed(sub); destroyComputed(sup); destroyComputed(dis)
            a.destroy(); b.destroy()
        }
    })

    test('batch 内多操作后谓词与终态一致', () => {
        const a = new RxSet<number>([1, 2])
        const b = new RxSet<number>([1, 2, 3])
        const has2 = a.has(2)
        const sub = a.isSubsetOf(b)
        const dis = a.isDisjointFrom(b)
        try {
            batch(() => {
                a.delete(2)
                a.add(9)
                b.delete(1)
            })
            expect(has2.raw).toBe(a.data.has(2))
            const A = [...a.data]
            expect(sub.raw).toBe(A.every(x => b.data.has(x)))
            expect(dis.raw).toBe(A.every(x => !b.data.has(x)))
        } finally {
            destroyComputed(has2); destroyComputed(sub); destroyComputed(dis)
            a.destroy(); b.destroy()
        }
    })
})
