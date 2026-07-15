import {describe, expect, test} from 'vitest'
import {atom} from '../src/atom.js'
import {batch} from '../src/notify.js'
import {RxList} from '../src/RxList.js'
import {RxMap} from '../src/RxMap.js'
import {RxSet} from '../src/RxSet.js'
import {
    adversarialSpliceStart,
    adversarialSpliceDeleteCount,
    atomRowFactory,
    indexReadingMapFn,
    indexReadingModel,
    itemAtomReadingMapFn,
    itemAtomReadingModel,
    mulberry32,
    uniqueInts,
} from './fuzzKit.js'
import {expectGroupByEqualsModel} from './stateOracle.js'

type Op =
    | { kind: 'splice', start: number, deleteCount: number, items: number[] }
    | { kind: 'set', index: number, value: number }
    | { kind: 'sortSelf' }
    | { kind: 'reposition', start: number, newStart: number, limit: number }
    | { kind: 'swap', a: number, b: number }
    | { kind: 'push', items: number[] }
    | { kind: 'pop' }
    | { kind: 'shift' }
    | { kind: 'unshift', items: number[] }

function randomOp(rand: () => number, len: number, nextVal: () => number): Op {
    const r = rand()
    if (r < 0.35) {
        // 对抗参数域(负数/越界/小数/NaN;deleteCount 的负/NaN/Infinity/小数)统一由 fuzzKit 提供
        const start = adversarialSpliceStart(rand, len)
        const deleteCount = rand() < 0.5
            ? adversarialSpliceDeleteCount(rand, len)
            : (rand() < 0.2 ? Math.floor(rand() * 3) + len : Math.floor(rand() * 4))
        const items = Array.from({length: Math.floor(rand() * 4)}, nextVal)
        return {kind: 'splice', start, deleteCount, items}
    }
    if (r < 0.5 && len > 0) return {kind: 'set', index: Math.floor(rand() * len), value: nextVal()}
    if (r < 0.6) return {kind: 'sortSelf'}
    if (r < 0.7 && len >= 2) {
        const limit = 1 + Math.floor(rand() * Math.min(2, len - 1))
        const start = Math.floor(rand() * (len - limit + 1))
        const newStart = Math.floor(rand() * (len - limit + 1))
        return {kind: 'reposition', start, newStart, limit}
    }
    if (r < 0.8 && len >= 2) {
        const a = Math.floor(rand() * len)
        let b = Math.floor(rand() * len)
        if (a === b) b = (b + 1) % len
        return {kind: 'swap', a: Math.min(a, b), b: Math.max(a, b)}
    }
    if (r < 0.9) return {kind: 'push', items: Array.from({length: 1 + Math.floor(rand() * 3)}, nextVal)}
    if (r < 0.93 && len > 0) return {kind: 'pop'}
    if (r < 0.96 && len > 0) return {kind: 'shift'}
    return {kind: 'unshift', items: Array.from({length: 1 + Math.floor(rand() * 2)}, nextVal)}
}

function applyOp(list: RxList<number>, op: Op) {
    switch (op.kind) {
        case 'splice': return list.splice(op.start, op.deleteCount, ...op.items)
        case 'set': return list.set(op.index, op.value)
        case 'sortSelf': return list.sortSelf((a, b) => a - b)
        case 'reposition': return list.reposition(op.start, op.newStart, op.limit)
        case 'swap': {
            if (op.b - op.a < 1) return
            return list.swap(op.a, op.b, 1)
        }
        case 'push': return list.push(...op.items)
        case 'pop': return list.pop()
        case 'shift': return list.shift()
        case 'unshift': return list.unshift(...op.items)
    }
}

describe('broad fuzz: unique values, all operators', () => {
    for (const seed of [1, 2, 3, 4, 5, 42, 1337, 20260711]) {
        test(`seed=${seed}`, () => {
            const rand = mulberry32(seed)
            const nextVal = uniqueInts(100)
            const source = new RxList<number>([1, 2, 3, 4, 5])

            const mapped = source.map(x => x * 2)
            const filtered = source.filter(x => x % 2 === 0)
            const sorted = source.toSorted((a, b) => a - b)
            const sliced = source.slice(1, 4)
            const other = new RxList<number>([1000, 1001])
            const concated = source.concat(other)
            const asSet = source.toSet()
            const grouped = source.groupBy(x => x % 3)
            const found = source.findIndex(x => x % 5 === 0)
            const len = source.length
            const history: Op[] = []
            try {
                for (let step = 0; step < 150; step++) {
                    const op = randomOp(rand, source.data.length, nextVal)
                    history.push(op)
                    applyOp(source, op)

                    const src = source.data
                    const ctx = `seed=${seed} step=${step} op=${JSON.stringify(op)} src=${JSON.stringify(src)} recent=${JSON.stringify(history.slice(-6))}`
                    expect(mapped.data, `map ${ctx}`).toEqual(src.map(x => x * 2))
                    expect(filtered.data, `filter ${ctx}`).toEqual(src.filter(x => x % 2 === 0))
                    expect(sorted.data, `toSorted ${ctx}`).toEqual(src.slice().sort((a, b) => a - b))
                    expect(sliced.data, `slice ${ctx}`).toEqual(src.slice(1, 4))
                    expect(concated.data, `concat ${ctx}`).toEqual([...src, ...other.data])
                    expect([...asSet.data].sort((a, b) => a - b), `toSet ${ctx}`).toEqual([...new Set(src)].sort((a, b) => a - b))
                    expectGroupByEqualsModel(grouped, src, x => x % 3, ctx)
                    expect(found.raw, `findIndex ${ctx}`).toBe(src.findIndex(x => x % 5 === 0))
                    expect(len.raw, `length ${ctx}`).toBe(src.length)
                }
            } finally {
                mapped.destroy(); filtered.destroy(); sorted.destroy(); sliced.destroy()
                concated.destroy(); asSet.destroy()
                for (const g of grouped.data.values()) g.destroy()
                grouped.destroy(); source.destroy(); other.destroy()
            }
        })
    }
})

describe('broad fuzz: map(item, index) atomIndexes consistency', () => {
    // 行形态维度(fuzzKit):storesValue 与 readsIndex 两列并行差分。
    // R4-1 教训:旧版只有 storesValue 列(存 atom 不读值),行从不升级为带 index
    // 依赖的 rowComputed,reorder 的"结构搬移 × 行级重算"触发序路径从未被进入。
    for (const seed of [21, 22, 23]) {
        test(`seed=${seed}`, () => {
            const rand = mulberry32(seed)
            let counter = 0
            const source = new RxList<number>([counter++, counter++, counter++])
            const mapped = source.map((item, index) => ({item, index}))
            const mappedReadsIndex = source.map(indexReadingMapFn)
            try {
                for (let step = 0; step < 100; step++) {
                    const r = rand()
                    const len = source.data.length
                    if (r < 0.4) {
                        const start = Math.floor(rand() * (len + 1))
                        const dc = Math.floor(rand() * 3)
                        const items = Array.from({length: Math.floor(rand() * 3)}, () => counter++)
                        source.splice(start, dc, ...items)
                    } else if (r < 0.6) {
                        source.sortSelf((a, b) => b - a)
                    } else if (r < 0.8 && len >= 2) {
                        source.reposition(Math.floor(rand() * len), Math.floor(rand() * len), 1)
                    } else {
                        source.push(counter++)
                    }
                    expect(mapped.data.map(e => e.item), `seed=${seed} step=${step}`).toEqual(source.data)
                    mapped.data.forEach((entry, i) => {
                        expect(entry.index.raw, `seed=${seed} step=${step} row=${i}`).toBe(i)
                    })
                    expect(mappedReadsIndex.data, `readsIndex seed=${seed} step=${step}`)
                        .toEqual(indexReadingModel(source.data))
                }
            } finally {
                mapped.destroy(); mappedReadsIndex.destroy(); source.destroy()
            }
        })
    }

    // readsItemAtom 形态:行升级为带 item 依赖的 rowComputed;结构操作与
    // 行内 atom 写混排(含 batch 内"先写行依赖再结构操作"的 frame 定位分支)。
    for (const seed of [26, 27]) {
        test(`readsItemAtom seed=${seed}`, () => {
            const rand = mulberry32(seed)
            const mkRow = atomRowFactory()
            let n = 0
            const source = new RxList([mkRow('a' + n++), mkRow('a' + n++), mkRow('a' + n++)])
            const mapped = source.map(itemAtomReadingMapFn)
            try {
                for (let step = 0; step < 100; step++) {
                    const r = rand()
                    const len = source.data.length
                    if (r < 0.25 && len > 0) {
                        source.data[Math.floor(rand() * len)].label('w' + n++)
                    } else if (r < 0.45) {
                        const start = Math.floor(rand() * (len + 1))
                        const dc = Math.floor(rand() * 2)
                        source.splice(start, dc, mkRow('s' + n++))
                    } else if (r < 0.6 && len >= 2) {
                        source.swap(0, len - 1)
                    } else if (r < 0.75 && len > 0) {
                        source.set(Math.floor(rand() * len), mkRow('t' + n++))
                    } else if (r < 0.9 && len > 0) {
                        // batch 内先写行依赖、再结构操作:frame 定位分支的常驻覆盖
                        batch(() => {
                            source.data[Math.floor(rand() * source.data.length)].label('b' + n++)
                            if (rand() < 0.5 && source.data.length > 1) {
                                source.splice(Math.floor(rand() * source.data.length), 1)
                            } else {
                                source.splice(Math.floor(rand() * (source.data.length + 1)), 0, mkRow('b' + n++))
                            }
                        })
                    } else {
                        source.push(mkRow('p' + n++))
                    }
                    expect(mapped.data, `seed=${seed} step=${step}`).toEqual(itemAtomReadingModel(source.data))
                }
            } finally {
                mapped.destroy(); source.destroy()
            }
        })
    }
})

describe('broad fuzz: findIndex with reactive predicates', () => {
    for (const seed of [31, 32, 33, 34]) {
        test(`seed=${seed}`, () => {
            const rand = mulberry32(seed)
            let id = 0
            const mk = (score: number) => ({id: id++, score: atom(score)})
            type Item = ReturnType<typeof mk>
            const source = new RxList<Item>([mk(1), mk(5), mk(2)])
            const found = source.findIndex(item => item.score() >= 4)
            try {
                for (let step = 0; step < 120; step++) {
                    const r = rand()
                    const len = source.data.length
                    if (r < 0.35 && len > 0) {
                        source.data[Math.floor(rand() * len)].score(Math.floor(rand() * 8))
                    } else if (r < 0.6) {
                        const start = Math.floor(rand() * (len + 1))
                        const dc = Math.floor(rand() * 2)
                        const items = Array.from({length: Math.floor(rand() * 2)}, () => mk(Math.floor(rand() * 8)))
                        source.splice(start, dc, ...items)
                    } else if (r < 0.75 && len > 0) {
                        source.set(Math.floor(rand() * len), mk(Math.floor(rand() * 8)))
                    } else if (r < 0.85) {
                        source.sortSelf((a, b) => a.score.raw - b.score.raw)
                    } else {
                        source.push(mk(Math.floor(rand() * 8)))
                    }
                    expect(found(), `seed=${seed} step=${step}`).toBe(source.data.findIndex(item => item.score.raw >= 4))
                }
            } finally {
                source.destroy()
            }
        })
    }
})

describe('broad fuzz: RxSet operations and RxMap derivations', () => {
    for (const seed of [41, 42, 43]) {
        test(`rxset seed=${seed}`, () => {
            const rand = mulberry32(seed)
            const val = () => Math.floor(rand() * 10)
            const a = new RxSet<number>([val(), val(), val()])
            const b = new RxSet<number>([val(), val(), val()])
            const diff = a.difference(b)
            const inter = a.intersection(b)
            const sym = a.symmetricDifference(b)
            const uni = a.union(b)
            // toList 是 methodResult（replace 的 [newItems, deletedItems]）的消费方：
            // replace 收到含重复值的数组时 newItems 必须已按 Set 语义去重
            const asList = a.toList()
            try {
                for (let step = 0; step < 150; step++) {
                    const target = rand() < 0.5 ? a : b
                    const r = rand()
                    if (r < 0.4) target.add(val())
                    else if (r < 0.75) {
                        const arr = [...target.data]
                        if (arr.length) target.delete(arr[Math.floor(rand() * arr.length)])
                    } else {
                        // 值域窄（0..9）+ 长度 4：replace 数组高频包含重复值
                        target.replace(Array.from({length: Math.floor(rand() * 4)}, val))
                    }
                    const A = [...a.data], B = [...b.data]
                    const sortNum = (x: number[]) => x.slice().sort((m, n) => m - n)
                    const ctx = `seed=${seed} step=${step}`
                    expect(sortNum([...diff.data]), ctx).toEqual(sortNum(A.filter(x => !B.includes(x))))
                    expect(sortNum([...inter.data]), ctx).toEqual(sortNum(A.filter(x => B.includes(x))))
                    expect(sortNum([...sym.data]), ctx).toEqual(sortNum([...A.filter(x => !B.includes(x)), ...B.filter(x => !A.includes(x))]))
                    expect(sortNum([...uni.data]), ctx).toEqual(sortNum([...new Set([...A, ...B])]))
                    expect(sortNum([...asList.data]), `toList ${ctx}`).toEqual(sortNum(A))
                }
            } finally {
                diff.destroy(); inter.destroy(); sym.destroy(); uni.destroy()
                asList.destroy()
                a.destroy(); b.destroy()
            }
        })
    }

    for (const seed of [51, 52]) {
        test(`rxmap seed=${seed}`, () => {
            const rand = mulberry32(seed)
            const key = () => 'k' + Math.floor(rand() * 8)
            const map = new RxMap<string, number>({k0: 0, k1: 1})
            const keys = map.keys()
            const values = map.values()
            const entries = map.entries()
            const size = map.size
            try {
                for (let step = 0; step < 150; step++) {
                    const r = rand()
                    if (r < 0.45) map.set(key(), Math.floor(rand() * 100))
                    else if (r < 0.75) map.delete(key())
                    else if (r < 0.9) {
                        const obj: Record<string, number> = {}
                        for (let i = 0; i < Math.floor(rand() * 4); i++) obj[key()] = Math.floor(rand() * 100)
                        map.replace(obj)
                    } else map.clear()

                    const ctx = `seed=${seed} step=${step}`
                    expect(keys.data, ctx).toEqual([...map.data.keys()])
                    expect(values.data, ctx).toEqual([...map.data.values()])
                    expect(entries.data, ctx).toEqual([...map.data.entries()])
                    expect(size.raw, ctx).toBe(map.data.size)
                }
            } finally {
                map.destroy()
            }
        })
    }
})
