import {afterEach, describe, expect, test, vi} from 'vitest'
import {atom, Atom, batch, computed, onChange, RxList, RxSet} from '../src/index'
import {mulberry32} from './fuzzKit'
import {countPrimitiveRawReads} from './listWorkCounters'

/**
 * Regression tests for the branches touched by the audit fixes (the minimal
 * reproductions themselves live in audit-list.spec.ts).
 */

const expectIndexAtomsAligned = (list: RxList<any>) => {
    const atoms = list.atomIndexes
    if (!atoms) return
    expect(atoms.length).toBeLessThanOrEqual(list.data.length)
    atoms.forEach((indexAtom, i) => { expect(indexAtom.raw).toBe(i) })
}

describe('map: an operation-time index beyond the final source never reaches source.atomIndexes', () => {
    test('the replay falls back exactly once and the rebuilt rows track their real positions', () => {
        const live = atom(7)
        const source = new RxList<number | 'live'>([1, 2])
        const calls: any[] = []
        const mapped = source.map(x => { calls.push(x); return x === 'live' ? live() : x })
        let fullRecomputes = 0
        mapped.on('fullRecompute', () => fullRecomputes++)
        batch(() => {
            source.push('live')     // operation-time index 2
            source.splice(0, 2)     // final: ['live'] at index 0
        })
        expect(fullRecomputes).toBe(1)
        expect(mapped.data).toEqual([7])
        expectIndexAtomsAligned(source)
        // the surviving row is the rebuilt one: it follows its real position
        source.unshift(1)
        expect(mapped.data).toEqual([1, 7])
        live(8)
        expect(mapped.data).toEqual([1, 8])
        source.swap(0, 1)
        live(9)
        expect(mapped.data).toEqual([9, 1])
        expectIndexAtomsAligned(source)
        mapped.destroy()
        expect(source.atomIndexes).toBeUndefined()
    })
    test('the row built out of range is inert: its dependency never re-runs mapFn with a stale position', () => {
        // deferred map: the patch runs outside any digest, so a write made by a later
        // mapFn call triggers the earlier (out-of-range) row inline, in the middle of the replay
        const pending: (() => void)[] = []
        const flush = () => pending.splice(0).forEach(run => run())
        const dep = atom(0)
        const source = new RxList<string>([])
        const calls: any[] = []
        const mapped = source.map(x => {
            calls.push(x)
            if (x === 'reader') return `reader:${dep()}`
            if (x === 'writer') dep(dep.raw + 1)
            return x
        }, {scheduleRecompute: (run: any) => { pending.push(run) }})
        source.push('reader')       // operation-time index 0
        source.push('writer')       // its mapFn writes `dep` while the replay is still running
        source.splice(0, 2)         // final length 0
        flush()
        expect(calls).toEqual(['reader', 'writer'])     // never called with undefined / a foreign row
        expect(mapped.data).toEqual([])
        expectIndexAtomsAligned(source)
        dep(10)
        expect(calls).toEqual(['reader', 'writer'])     // the dead row does not come back
        expect(() => source.push('x')).not.toThrow()
        flush()
        expect(mapped.data).toEqual(['x'])
    })
    test('the fallback is aborted by a throwing recompute hook: the out-of-range row stays inert until the retry rebuilds it', () => {
        const live = atom(7)
        const source = new RxList<number | 'live'>([1, 2])
        const calls: any[] = []
        const mapped = source.map(x => { calls.push(x); return x === 'live' ? live() : x })
        // 'recompute' is dispatched once for the patch round and once more for the full recompute it falls back to
        let dispatches = 0
        let armed = false
        mapped.on('recompute', () => { if (armed && ++dispatches === 2) { armed = false; throw new Error('hook boom') } })
        armed = true
        expect(() => batch(() => {
            source.push('live')     // operation-time index 2
            source.splice(0, 2)     // final: ['live'] at index 0 -> the row built at index 2 survives the replay
        })).toThrow('hook boom')
        expectIndexAtomsAligned(source)
        expect(mapped.data).toEqual([7])                // the replay itself was applied; only the rebuild is missing
        calls.length = 0
        // the half-replayed row is still alive (the rebuild never ran) but holds no index:
        // its dependency must not write row 2 of a one-row list / call mapFn with source.data[2]
        live(8)
        expect(calls).toEqual([])
        expect(mapped.data).toEqual([7])
        // the next trigger rebuilds from the final source
        source.push(3)
        expect(mapped.data).toEqual([8, 3])
        live(9)
        expect(mapped.data).toEqual([9, 3])
        expectIndexAtomsAligned(source)
    })
    test('index-param map behind a multi-step upstream (slice window) keeps aligned atoms', () => {
        const live = atom(1)
        const source = new RxList<number>([1, 2, 3, 4, 5])
        const windowed = source.slice(1, 3)
        const mapped = windowed.map(x => x % 2 ? x : x * live())
        batch(() => { source.push(6); source.splice(0, 5) })
        expect(mapped.data).toEqual(windowed.data.map(x => x % 2 ? x : x * live.raw))
        expectIndexAtomsAligned(windowed)
        source.unshift(8, 10, 12)
        live(3)
        expect(mapped.data).toEqual(windowed.data.map(x => x % 2 ? x : x * 3))
        expectIndexAtomsAligned(windowed)
    })
})

describe('map options.onCleanup: exactly once per value that leaves the list, on every path', () => {
    type Token = { id: number, item: string }
    const setup = (useContext = false) => {
        const theme = atom('light')
        let nextId = 1
        const created: Token[] = []
        const cleaned: Token[] = []
        const source = new RxList<string>(['a', 'b', 'c'])
        const make = (item: string): Token => { theme(); const token = {id: nextId++, item}; created.push(token); return token }
        const mapped = useContext
            ? source.map((item, _index, _ctx) => make(item), {onCleanup: (token: Token) => cleaned.push(token)})
            : source.map(item => make(item), {onCleanup: (token: Token) => cleaned.push(token)})
        const check = () => {
            // no double cleanup
            expect(new Set(cleaned).size).toBe(cleaned.length)
            // never for a value still in the list
            for (const token of mapped.data) expect(cleaned.includes(token)).toBe(false)
            // everything that left the list was cleaned
            const inList = new Set(mapped.data)
            expect(created.filter(token => !inList.has(token) && !cleaned.includes(token))).toEqual([])
        }
        return {theme, source, mapped, created, cleaned, check}
    }
    for (const useContext of [false, true]) {
        test(`splice / set / row recompute / reorder / batch / full rebuild / destroy (context=${useContext})`, () => {
            const {theme, source, mapped, created, cleaned, check} = setup(useContext)
            check()
            source.splice(1, 1); check()                         // splice
            source.set(0, 'A'); check()                          // set
            theme('dark'); check()                               // row recompute (every row)
            expect(cleaned.length).toBe(1 + 1 + 2)
            source.swap(0, 1); theme('blue'); check()            // reorder, then row recompute at the new positions
            batch(() => { theme('red'); source.unshift('z'); source.pop() }); check()   // row recompute while structural infos are pending
            batch(() => { source.push('y'); theme('green') }); check()
            mapped.recompute(true); check()                      // full rebuild
            source.clear(); check()
            source.push('k', 'l'); theme('last'); check()
            mapped.destroy()
            expect(cleaned.length).toBe(created.length)          // destroy: the rest, once each
            expect(new Set(cleaned).size).toBe(created.length)
        })
    }
    test('a row that recomputes to the very same value is not cleaned while it is still in the list', () => {
        const tick = atom(0)
        const widgets = new Map<string, object>()
        const widgetOf = (item: string) => { if (!widgets.has(item)) widgets.set(item, {item}); return widgets.get(item)! }
        const cleaned: object[] = []
        const source = new RxList<string>(['a', 'b'])
        const mapped = source.map(item => { tick(); return widgetOf(item) }, {onCleanup: widget => cleaned.push(widget)})
        tick(1); tick(2)
        expect(mapped.data).toEqual([widgetOf('a'), widgetOf('b')])
        expect(cleaned).toEqual([])
        source.shift()
        expect(cleaned).toEqual([widgetOf('a')])
        mapped.destroy()
        expect(cleaned).toEqual([widgetOf('a'), widgetOf('b')])
    })
    test('the callback runs isolated: what it reads does not become a dependency of the row', () => {
        const theme = atom('light')
        const audit = atom(0)
        let runs = 0
        const source = new RxList<number>([1])
        const mapped = source.map(item => { runs++; return `${item}-${theme()}` }, {onCleanup: () => { audit() }})
        theme('dark')
        expect(runs).toBe(2)
        audit(1); audit(2)
        expect(runs).toBe(2)
        expect(mapped.data).toEqual(['1-dark'])
        mapped.destroy()
    })
})

describe('map options.onCleanup on a row recompute: the replaced value is cleaned whatever happens around the write', () => {
    afterEach(() => { vi.restoreAllMocks() })
    const teardownReports = (spy: ReturnType<typeof vi.spyOn>) => spy.mock.calls
        .filter(call => call[0] === '[data0] uncaught error in teardown:')
        .map(call => (call[1] as Error).message)
    // fresh token per mapFn call: every token that is not in the list must have been cleaned exactly once
    const setup = (onCleanupThrowsFor?: string) => {
        const n = atom(1)
        const source = new RxList(['x', 'y'])
        const live = new Set<string>()
        let cleanedTwice = 0
        const mapped = source.map(s => { const v = `${s}${n()}`; live.add(v); return v }, {
            onCleanup: v => {
                if (!live.delete(v)) cleanedTwice++
                if (v === onCleanupThrowsFor) throw new Error(`cleanup ${v}`)
            },
        })
        const leaked = () => [...live].filter(v => !mapped.data.includes(v))
        return {n, source, mapped, live, leaked, cleanedTwice: () => cleanedTwice}
    }
    test('a downstream subscriber that throws during the write (non-batch write: the row writes inline)', () => {
        const reported = vi.spyOn(console, 'error').mockImplementation(() => undefined)
        const {n, mapped, live, leaked, cleanedTwice} = setup()
        let armed = true
        const stop = onChange(mapped, () => { if (armed) { armed = false; throw new Error('downstream') } })
        expect(() => n(2)).toThrow('downstream')
        stop()
        expect(mapped.data).toEqual(['x2', 'y2'])
        expect(leaked()).toEqual([])
        expect(teardownReports(reported)).toEqual([])
        mapped.destroy()
        expect([...live]).toEqual([])
        expect(cleanedTwice()).toBe(0)
    })
    test('the write and the cleanup both throw: the write error is thrown, the cleanup error reported', () => {
        const reported = vi.spyOn(console, 'error').mockImplementation(() => undefined)
        const {n, mapped, leaked} = setup('x1')
        let armed = true
        const stop = onChange(mapped, () => { if (armed) { armed = false; throw new Error('downstream') } })
        expect(() => n(2)).toThrow('downstream')
        stop()
        expect(leaked()).toEqual([])
        expect(teardownReports(reported)).toEqual(['cleanup x1'])
        mapped.destroy()
    })
    test('the map is destroyed while a row re-runs (from that row\'s context cleanup): no cleanup with a value mapFn never produced', () => {
        const n = atom(0)
        const source = new RxList([1, 2])
        const received: unknown[] = []
        let mapped!: RxList<{x: number}>
        mapped = source.map((x, _i, {onCleanup}) => {
            const v = n()
            onCleanup(() => { if (x === 1 && v === 0) mapped.destroy() })
            return {x}
        }, {onCleanup: value => { received.push(value); void value.x }})   // dereferences, like a real disposer
        expect(() => n(1)).not.toThrow()
        expect(mapped.active).toBe(false)
        expect(received.includes(undefined)).toBe(false)
        expect(received.map(value => (value as {x: number}).x)).toEqual([1, 2])   // the destroy cleaned both rows once
    })
    test('an explicit undefined that leaves the list is still cleaned (undefined is a legal mapped value)', () => {
        const n = atom(0)
        const source = new RxList([1])
        const cleaned: unknown[] = []
        const mapped = source.map(x => (n() === 0 ? undefined : x), {onCleanup: value => { cleaned.push(value) }})
        n(1)
        expect(mapped.data).toEqual([1])
        expect(cleaned).toEqual([undefined])
        mapped.destroy()
        expect(cleaned).toEqual([undefined, 1])
    })
})

describe('sortSelf: undefined elements follow Array#sort (never passed to the comparator, always last)', () => {
    test('differential against Array#sort: random lists with undefined and duplicates, comparator never sees undefined', () => {
        const rand = mulberry32(20261123)
        for (let round = 0; round < 300; round++) {
            const length = Math.floor(rand() * 9)
            const data = Array.from({length}, () => rand() < 0.3 ? undefined : Math.floor(rand() * 5))
            const list = new RxList<number | undefined>(data.slice())
            let sawUndefined = false
            const cmp = (a: number | undefined, b: number | undefined) => {
                if (a === undefined || b === undefined) sawUndefined = true
                return (a as number) - (b as number)
            }
            list.sortSelf(cmp)
            expect(sawUndefined, `round ${round} [${data}]`).toBe(false)
            expect(list.data, `round ${round} [${data}]`).toEqual(data.slice().sort((a, b) => (a as number) - (b as number)))
            list.destroy()
        }
    })
    test('stable for ties, and a list without undefined sorts exactly as before', () => {
        const rows = [{k: 2, id: 'a'}, {k: 1, id: 'b'}, {k: 2, id: 'c'}, {k: 1, id: 'd'}, {k: 0, id: 'e'}]
        const list = new RxList(rows.slice())
        list.sortSelf((x, y) => x.k - y.k)
        expect(list.data.map(r => r.id)).toEqual(rows.slice().sort((x, y) => x.k - y.k).map(r => r.id))
    })
    test('holes sort after explicit undefined (same value order as Array#sort); reorder pairs stay a full permutation', () => {
        // eslint-disable-next-line no-sparse-arrays
        const data: (number | undefined)[] = [3, , 1, undefined, 2]
        const native = data.slice().sort((a, b) => (a as number) - (b as number))
        const list = new RxList<number | undefined>(data.slice())
        const orders: [number, number][][] = []
        const stop = onChange(list, (infos: any[]) => infos.forEach(info => { if (info.method === 'reorder') orders.push(info.argv[0]) }))
        list.sortSelf((a, b) => (a as number) - (b as number))
        expect(Array.from(list.data)).toEqual(Array.from(native))            // [1, 2, 3, undefined, undefined(hole)]
        expect(orders.length).toBe(1)
        const order = orders[0]
        expect(order.map(pair => pair[1])).toEqual([0, 1, 2, 3, 4])
        expect(order.map(pair => pair[0]).slice().sort()).toEqual([0, 1, 2, 3, 4])
        expect(order.find(pair => pair[0] === 3)![1]).toBe(3)                 // the explicit undefined
        expect(order.find(pair => pair[0] === 1)![1]).toBe(4)                 // the hole goes last
        stop()
    })
    test('derived structures follow the emitted reorder (undefined rows included)', () => {
        const rand = mulberry32(7)
        for (let round = 0; round < 60; round++) {
            const data = Array.from({length: 2 + Math.floor(rand() * 7)}, () => rand() < 0.3 ? undefined : Math.floor(rand() * 6))
            const source = new RxList<number | undefined>(data.slice())
            const selected = new RxSet<number | undefined>([undefined, 2])
            const cmp = (a: number | undefined, b: number | undefined) => (b as number) - (a as number)
            const total = (a: number | undefined, b: number | undefined) => a === undefined ? (b === undefined ? 0 : 1) : b === undefined ? -1 : a - b
            const mapped = source.map((x, i) => `${x}@${i()}`)
            const filtered = source.filter(x => x === undefined || x % 2 === 0)
            const sorted = source.toSorted(total)
            const sliced = source.slice(1, 4)
            const grouped = source.groupBy(x => x === undefined ? 'u' : x % 2)
            const selection = source.createSelection(selected as any)
            const indexSelection = source.createIndexKeySelection(atom<number | null>(1))
            const firstUndefined = source.findIndex(x => x === undefined)
            const last = computed(() => source.at(-1))
            const second = computed(() => source.at(1))
            source.sortSelf(cmp)
            const S = source.data
            const ctx = `round ${round} [${data}]`
            expect(S, ctx).toEqual(data.slice().sort(cmp))
            expect(mapped.data, ctx).toEqual(S.map((x, i) => `${x}@${i}`))
            expect(filtered.data, ctx).toEqual(S.filter(x => x === undefined || x % 2 === 0))
            expect(sorted.data, ctx).toEqual(S.slice().sort(total))
            expect(sliced.data, ctx).toEqual(S.slice(1, 4))
            for (const key of ['u', 0, 1] as const) {
                expect(grouped.data.get(key)?.data ?? [], `${ctx} group ${key}`).toEqual(S.filter(x => (x === undefined ? 'u' : x % 2) === key))
            }
            expect(selection.data.map(row => [row[0], row[1].raw]), ctx).toEqual(S.map(x => [x, x === undefined || x === 2]))
            expect(indexSelection.data.map(row => [row[0], row[1].raw]), ctx).toEqual(S.map((x, i) => [x, i === 1]))
            expect(firstUndefined.raw, ctx).toBe(S.findIndex(x => x === undefined))
            expect(last.raw, ctx).toBe(S.at(-1))
            expect(second.raw, ctx).toBe(S.at(1))
            expect(source.atomIndexes!.map(indexAtom => indexAtom.raw), ctx).toEqual(S.map((_, i) => i))
        }
    })
    test('characterization: a comparator-less call (outside the TS signature) still fails before anything is moved', () => {
        const list = new RxList([3, 1, 2])
        expect(() => (list as any).sortSelf()).toThrow(TypeError)
        expect(list.data).toEqual([3, 1, 2])
    })
})

describe('filter: tail operations cost O(suffix); the mid-list path is unchanged', () => {
    // rows scanned (indicator `.raw` reads) by `repeat` operations on an n-row source
    const scanned = (n: number, op: (source: RxList<number>, i: number) => void, repeat: number) => {
        const source = new RxList<number>(Array.from({length: n}, (_, i) => i))
        const filtered = source.filter(x => x % 2 === 0)
        const reads = countPrimitiveRawReads(() => { for (let i = 0; i < repeat; i++) op(source, i) })
        expect(filtered.data).toEqual(source.data.filter(x => x % 2 === 0))
        filtered.destroy(); source.destroy()
        return reads
    }
    test('rows scanned by push / pop / tail replace do not depend on the list length (2k vs 32k rows)', () => {
        const cases: [string, (s: RxList<number>, i: number) => void][] = [
            ['push', (s, i) => s.push(i)],
            ['pop', s => s.pop()],
            ['splice(len-2, 2, a, b)', (s, i) => s.splice(s.data.length - 2, 2, i, i + 1)],
            ['splice(-3, Infinity)', s => { s.splice(-3, Infinity); s.push(1, 2, 3) }],
        ]
        for (const [name, op] of cases) {
            // the old prefix scan reads every row per operation: 300 x 30000 more on the larger list
            expect(scanned(32000, op, 300), name).toBe(scanned(2000, op, 300))
        }
    })
    test('tail shapes: truncation, tail replace with mixed matches, clear, replace-all, batched tail ops, reactive predicate', () => {
        const flip = atom(0)
        const source = new RxList<number>([1, 2, 3, 4, 5, 6, 7, 8])
        const filtered = source.filter(x => (x + flip()) % 2 === 0)
        const mirror = filtered.map(x => x)                       // a downstream consumer of the emitted splices
        const check = () => {
            const model = source.data.filter(x => (x + flip.raw) % 2 === 0)
            expect(filtered.data).toEqual(model)
            expect(mirror.data).toEqual(model)
        }
        check()
        source.splice(5, Infinity); check()                        // truncate: deletes matched and unmatched rows
        source.splice(3, 2, 10, 11, 12); check()                   // tail replace, inserts mixed
        source.push(13, 14, 15, 16); check()
        source.pop(); check()
        batch(() => { source.push(20); source.push(21); source.pop(); source.push(22, 24) }); check()
        batch(() => { source.pop(); flip(1); source.push(30, 31) }); check()   // predicate dep changes in the same batch
        batch(() => { source.unshift(40); source.push(41); source.splice(source.data.length - 1, 1, 42, 43) }); check()
        source.splice(0, source.data.length, 50, 51, 52); check() // replace-all (range reaches the end from 0)
        source.clear(); check()
        source.push(60); source.push(61); check()
        source.splice(1, 0); check()                               // no-op tail splice
        source.splice(99, 5, 62); check()                          // out-of-range start clamps to the tail
        source.splice(-1, 1); check()                              // negative start
    })
    test('mid-list path: differential against Array#filter for random interior splices and sets', () => {
        const rand = mulberry32(42)
        const source = new RxList<number>(Array.from({length: 12}, (_, i) => i))
        const filtered = source.filter(x => x % 3 !== 0)
        for (let step = 0; step < 400; step++) {
            const len = source.data.length
            if (len < 6) { source.push(step, step + 1, step + 2); continue }
            const start = 1 + Math.floor(rand() * (len - 3))
            if (rand() < 0.3) source.set(start, Math.floor(rand() * 50))
            else source.splice(start, Math.floor(rand() * Math.min(3, len - start - 1)), ...Array.from({length: Math.floor(rand() * 3)}, () => Math.floor(rand() * 50)))
            expect(filtered.data, `step ${step}`).toEqual(source.data.filter(x => x % 3 !== 0))
        }
    })
})

describe('spliceArray: items may be the list\'s own data array', () => {
    for (const size of [3, 9000]) {   // 9000 > the native-splice spread limit: exercises the manual-move path too
        test(`self append / prepend / interior insert / replace-with-self (size ${size})`, () => {
            const base = Array.from({length: size}, (_, i) => i)
            const run = (apply: (list: RxList<number>) => void, model: (arr: number[]) => number[]) => {
                const list = new RxList<number>(base.slice())
                const mapped = list.map(x => x * 10)
                const indexed = list.map((x, i) => x - i())
                const evens = list.filter(x => x % 2 === 0)
                const len = list.length
                apply(list)
                const expected = model(base.slice())
                expect(list.data).toEqual(expected)
                expect(mapped.data).toEqual(expected.map(x => x * 10))
                expect(indexed.data).toEqual(expected.map((x, i) => x - i))
                expect(evens.data).toEqual(expected.filter(x => x % 2 === 0))
                expect(len.raw).toBe(expected.length)
            }
            run(list => list.spliceArray(list.data.length, 0, list.data), arr => arr.concat(arr))
            run(list => list.spliceArray(0, 0, list.data), arr => arr.concat(arr))
            run(list => list.spliceArray(1, 1, list.data), arr => [arr[0], ...arr, ...arr.slice(2)])
            run(list => list.spliceArray(0, list.data.length, list.data), arr => arr)
            run(list => list.replaceData(list.data), arr => arr)
        })
    }
    test('the returned deleted items and the payload are independent of the list afterwards', () => {
        const list = new RxList<number>([1, 2, 3])
        const seen: any[][] = []
        const stop = onChange(list, (infos: any[]) => infos.forEach(info => seen.push(info.argv.slice(2))))
        const removed = list.spliceArray(1, 2, list.data)
        expect(removed).toEqual([2, 3])
        expect(list.data).toEqual([1, 1, 2, 3])
        expect(seen).toEqual([[1, 2, 3]])
        stop()
    })
})

describe('at(): a non-integer index is a plain property key, symmetric with set (README §4)', () => {
    // set(1.5, v) is a plain array property write that triggers key 1.5 (README §4, key passed through);
    // at(1.5) must read that same property and subscribe that same key -- not truncate to row 1.
    test('at(1.5) reads what set(1.5, v) wrote, and a computed reading it re-runs', () => {
        const list = new RxList<any>([10, 20, 30])
        const fractional = computed(() => list.at(1.5))
        expect(fractional.raw).toBe(undefined)
        expect(list.at(1.5)).toBe(undefined)
        list.set(1.5, 'x')
        expect(list.at(1.5)).toBe('x')
        expect(fractional.raw).toBe('x')
        list.set(1, 'y')                        // row 1 is a different key
        expect(fractional.raw).toBe('x')
        expect(list.at(1)).toBe('y')
        list.set(1.5, 'z')
        expect(fractional.raw).toBe('z')
    })
    test('NaN behaves the same way (property "NaN"), integer and negative indexes are unchanged', () => {
        const list = new RxList<any>([10, 20, 30])
        const viaNaN = computed(() => list.at(NaN))
        expect(viaNaN.raw).toBe(undefined)
        list.set(NaN, 'n')
        expect(viaNaN.raw).toBe('n')
        expect([list.at(0), list.at(2), list.at(-1), list.at(-3), list.at(3)]).toEqual([10, 30, 30, 10, undefined])
    })
})

describe('map: a row cleanup that throws does not keep the other rows from being cleaned', () => {
    afterEach(() => { vi.restoreAllMocks() })
    // rows 1..4; the context cleanup of rows 1 and 3 throws, options.onCleanup throws for the value of row 2
    const setup = () => {
        const dep = atom(0)
        const source = new RxList<number>([1, 2, 3, 4])
        const contextCleaned: number[] = []
        const optionCleaned: number[] = []
        const armed = {value: true}
        const mapped = source.map((item, _index, {onCleanup}) => {
            dep()
            onCleanup(() => {
                contextCleaned.push(item)
                if (armed.value && (item === 1 || item === 3)) throw new Error(`context cleanup ${item}`)
            })
            return item * 10
        }, {
            onCleanup: (value: number) => {
                optionCleaned.push(value)
                if (armed.value && value === 20) throw new Error(`option cleanup ${value}`)
            },
        })
        const reported = vi.spyOn(console, 'error').mockImplementation(() => undefined)
        const reportedMessages = () => reported.mock.calls
            .filter(call => call[0] === '[data0] uncaught error in teardown:')
            .map(call => (call[1] as Error).message)
        return {dep, source, mapped, contextCleaned, optionCleaned, armed, reportedMessages}
    }
    test('destroy: every row is cleaned, the first error is thrown, the later ones are reported', () => {
        const {mapped, contextCleaned, optionCleaned, reportedMessages} = setup()
        expect(() => mapped.destroy()).toThrow('context cleanup 1')
        expect(contextCleaned).toEqual([1, 2, 3, 4])
        expect(optionCleaned).toEqual([10, 20, 30, 40])
        expect(reportedMessages()).toEqual(['context cleanup 3', 'option cleanup 20'])
        expect(mapped.active).toBe(false)
        mapped.destroy()                                   // idempotent: nothing runs twice
        expect(contextCleaned).toEqual([1, 2, 3, 4])
        expect(optionCleaned).toEqual([10, 20, 30, 40])
    })
    test('full rebuild: every old row is cleaned exactly once, the first error is thrown, and the list recovers', () => {
        const {source, mapped, contextCleaned, optionCleaned, armed, reportedMessages} = setup()
        expect(() => mapped.recompute(true)).toThrow('context cleanup 1')
        expect(contextCleaned).toEqual([1, 2, 3, 4])
        expect(optionCleaned).toEqual([10, 20, 30, 40])
        expect(reportedMessages()).toEqual(['context cleanup 3', 'option cleanup 20'])
        // the next trigger rebuilds from the final source and does not clean the old rows a second time
        source.push(5)
        expect(mapped.data).toEqual([10, 20, 30, 40, 50])
        expect(contextCleaned).toEqual([1, 2, 3, 4])
        expect(optionCleaned).toEqual([10, 20, 30, 40])
        // the rebuilt rows are live and get their own single cleanup
        armed.value = false
        mapped.destroy()
        expect(contextCleaned).toEqual([1, 2, 3, 4, 1, 2, 3, 4, 5])
        expect(optionCleaned).toEqual([10, 20, 30, 40, 10, 20, 30, 40, 50])
    })
    test('full rebuild that fails, then destroy: the already-cleaned rows are not cleaned again', () => {
        const {mapped, contextCleaned, optionCleaned} = setup()
        expect(() => mapped.recompute(true)).toThrow('context cleanup 1')
        mapped.destroy()
        expect(contextCleaned).toEqual([1, 2, 3, 4])
        expect(optionCleaned).toEqual([10, 20, 30, 40])
    })
    test('splice of several rows: every removed row is cleaned, the first error reaches the mutation caller', () => {
        const {source, mapped, contextCleaned, optionCleaned, armed, reportedMessages} = setup()
        expect(() => source.splice(0, 3)).toThrow('context cleanup 1')
        expect(contextCleaned).toEqual([1, 2, 3])
        expect(optionCleaned).toEqual([10, 20, 30])
        expect(reportedMessages()).toEqual(['context cleanup 3', 'option cleanup 20'])
        // error recovery: the next trigger rebuilds; the removed rows are not cleaned again
        armed.value = false
        source.push(5)
        expect(mapped.data).toEqual([40, 50])
        expect(contextCleaned).toEqual([1, 2, 3, 4])       // row 4 was torn down by the rebuild, once
        expect(optionCleaned).toEqual([10, 20, 30, 40])
        mapped.destroy()
        expect(contextCleaned).toEqual([1, 2, 3, 4, 4, 5])
        expect(optionCleaned).toEqual([10, 20, 30, 40, 40, 50])
    })
    test('set (single row): the option cleanup still runs after the context cleanup threw, and the new row is in place', () => {
        const {dep, source, mapped, contextCleaned, optionCleaned, armed, reportedMessages} = setup()
        expect(() => source.set(0, 7)).toThrow('context cleanup 1')
        expect(contextCleaned).toEqual([1])
        expect(optionCleaned).toEqual([10])
        expect(mapped.data).toEqual([70, 20, 30, 40])
        expect(reportedMessages()).toEqual([])
        // error recovery: the next trigger rebuilds; the replaced row is not cleaned again
        armed.value = false
        source.set(1, 8)
        expect(mapped.data).toEqual([70, 80, 30, 40])
        expect(contextCleaned).toEqual([1, 7, 2, 3, 4])   // rows 7, 2, 3, 4 torn down by the rebuild, once each
        expect(optionCleaned).toEqual([10, 70, 20, 30, 40])
        dep(1)
        expect(mapped.data).toEqual([70, 80, 30, 40])
    })
    test('set: both cleanups of the replaced row throw -> first thrown, second reported', () => {
        const source = new RxList<number>([1, 2])
        const order: string[] = []
        const mapped = source.map((item, _index, {onCleanup}) => {
            onCleanup(() => { order.push(`context ${item}`); if (item === 1) throw new Error('context boom') })
            return item * 10
        }, {onCleanup: (value: number) => { order.push(`option ${value}`); if (value === 10) throw new Error('option boom') }})
        const reported = vi.spyOn(console, 'error').mockImplementation(() => undefined)
        expect(() => source.set(0, 3)).toThrow('context boom')
        expect(order).toEqual(['context 1', 'option 10'])
        expect(reported.mock.calls.filter(call => call[0] === '[data0] uncaught error in teardown:').map(call => (call[1] as Error).message)).toEqual(['option boom'])
        expect(mapped.data).toEqual([30, 20])
        source.push(4)                                     // recovers; rows 3 and 2 are torn down by the rebuild exactly once
        expect(mapped.data).toEqual([30, 20, 40])
        expect(order).toEqual(['context 1', 'option 10', 'context 3', 'context 2', 'option 30', 'option 20'])
    })
    test('row effects: a child effect whose cleanup throws does not stop the teardown of the other removed rows', () => {
        const source = new RxList<number>([1, 2, 3])
        const childCleaned: number[] = []
        const contextCleaned: number[] = []
        const optionCleaned: number[] = []
        const mapped = source.map((item, _index, {onCleanup}) => {
            const child = computed(({onCleanup: onChildCleanup}) => {
                onChildCleanup(() => { childCleaned.push(item); if (item === 1) throw new Error('child cleanup boom') })
                return item
            })
            onCleanup(() => contextCleaned.push(item))
            return child
        }, {onCleanup: child => optionCleaned.push(child.raw)})
        vi.spyOn(console, 'error').mockImplementation(() => undefined)
        expect(() => source.splice(0, 2)).toThrow('child cleanup boom')
        expect(childCleaned).toEqual([1, 2])
        expect(contextCleaned).toEqual([1, 2])
        expect(optionCleaned).toEqual([1, 2])
        expect(mapped.data.length).toBe(1)
    })
    test('set (single row): a child effect of the replaced row whose cleanup throws does not skip the row\'s other cleanups', () => {
        const source = new RxList<number>([1, 2])
        const contextCleaned: number[] = []
        const optionCleaned: number[] = []
        const mapped = source.map((item, _index, {onCleanup}) => {
            computed(({onCleanup: onChildCleanup}) => {
                onChildCleanup(() => { if (item === 1) throw new Error('child cleanup boom') })
                return item
            })
            onCleanup(() => contextCleaned.push(item))
            return item * 10
        }, {onCleanup: value => optionCleaned.push(value)})
        vi.spyOn(console, 'error').mockImplementation(() => undefined)
        expect(() => source.set(0, 7)).toThrow('child cleanup boom')
        expect(contextCleaned).toEqual([1])
        expect(optionCleaned).toEqual([10])
        expect(mapped.data).toEqual([70, 20])
        mapped.destroy()
        expect(contextCleaned).toEqual([1, 7, 2])
        expect(optionCleaned).toEqual([10, 70, 20])
    })
})

describe('filter: indicator is committed before the filtered list changes (reactions during the dispatch see a consistent state)', () => {
    type Row = { id: number, on: Atom<boolean> }
    test('differential: a subscriber of the filtered list toggles other rows / mutates the source while a toggle is being dispatched', () => {
        for (let seed = 1; seed <= 150; seed++) {
            const rand = mulberry32(seed)
            let nextId = 1
            const pool: Row[] = []
            const mk = (): Row => { const row = {id: nextId++, on: atom(rand() < 0.5)}; pool.push(row); return row }
            const source = new RxList<Row>(Array.from({length: 6}, mk))
            const filtered = source.filter(row => row.on())
            const mirror = filtered.map(row => row.id)
            const history: string[] = []
            let budget = 0
            // rows whose indicator may be mid-recompute right now: writing their atom again from a
            // reaction is the documented synchronous recompute loop (README §2), not what is tested here
            const busy = new Set<Row>()
            const react = () => {
                if (budget <= 0) return
                budget--
                const r = rand()
                const len = source.data.length
                if (r < 0.5) {
                    const row = pool[Math.floor(rand() * pool.length)]
                    if (busy.has(row)) return
                    busy.add(row)
                    history.push(`  nested flip #${row.id}`); row.on(!row.on.raw)
                }
                else if (r < 0.65) { const row = mk(); history.push(`  nested push #${row.id}`); source.push(row) }
                else if (r < 0.8 && len) { const i = Math.floor(rand() * len); history.push(`  nested splice(${i},1)`); source.splice(i, 1) }
                else if (r < 0.9 && len >= 2) { history.push('  nested swap(0,last)'); source.swap(0, len - 1) }
                else if (len) { const i = Math.floor(rand() * len); const row = mk(); history.push(`  nested set(${i},#${row.id})`); source.set(i, row) }
            }
            const stop = onChange(filtered, react)
            for (let step = 0; step < 60; step++) {
                budget = Math.floor(rand() * 3)
                busy.clear()
                const row = pool[Math.floor(rand() * pool.length)]
                const r = rand()
                if (r < 0.7) { history.push(`flip #${row.id}`); busy.add(row); row.on(!row.on.raw) }
                else if (r < 0.85) { history.push('batch flips'); batch(() => { for (let i = 0; i < 3; i++) { const x = pool[Math.floor(rand() * pool.length)]; busy.add(x); x.on(!x.on.raw) } }) }
                else { const fresh = mk(); history.push(`unshift #${fresh.id}`); source.unshift(fresh) }
                const model = source.data.filter(x => x.on.raw).map(x => x.id)
                const ctx = `seed ${seed} step ${step}\n${history.join('\n')}`
                expect(filtered.data.map(x => x.id), ctx).toEqual(model)
                expect(mirror.data, ctx).toEqual(model)
            }
            stop(); mirror.destroy(); filtered.destroy(); source.destroy()
        }
    })
    test('a subscriber that throws during the dispatch leaves indicator and filtered list in step', () => {
        const a = {id: 'a', on: atom(false)}, b = {id: 'b', on: atom(true)}
        const source = new RxList([a, b])
        const filtered = source.filter(row => row.on())
        let boom = true
        const stop = onChange(filtered, () => { if (boom) { boom = false; throw new Error('subscriber boom') } })
        expect(() => a.on(true)).toThrow('subscriber boom')
        expect(filtered.data.map(row => row.id)).toEqual(['a', 'b'])
        // later structural changes still line up (the prefix count uses the committed indicator)
        source.shift()
        expect(filtered.data.map(row => row.id)).toEqual(['b'])
        source.unshift(a)
        a.on(false)
        expect(filtered.data.map(row => row.id)).toEqual(['b'])
        stop()
    })
})

describe('filter: one shared predicate dependency flipping many rows', () => {
    // rows scanned (indicator `.raw` reads) when one shared dependency flips every row off, on, then half off
    const flipAll = (n: number, shuffled: boolean) => {
        const threshold = atom(-1)
        const source = new RxList<number>(Array.from({length: n}, (_, i) => i))
        const filtered = source.filter(x => x > threshold())
        if (shuffled) {
            // rows no longer sit in subscription order
            const rand = mulberry32(n)
            const keys = new Map<number, number>(source.data.map(x => [x, rand()]))
            source.sortSelf((a, b) => keys.get(a)! - keys.get(b)!)
        }
        const off = countPrimitiveRawReads(() => { threshold(n) })
        expect(filtered.data).toEqual([])
        const on = countPrimitiveRawReads(() => { threshold(-1) })
        expect(filtered.data).toEqual(source.data)
        const half = countPrimitiveRawReads(() => { threshold(n / 2) })
        expect(filtered.data).toEqual(source.data.filter(x => x > n / 2))
        filtered.destroy(); source.destroy()
        return {off, on, half}
    }
    test('differential: waves of flips x structural changes x reorders x batches', () => {
        for (let seed = 1; seed <= 120; seed++) {
            const rand = mulberry32(seed)
            let next = 0
            const threshold = atom(3)
            const parity = atom(0)
            const pred = (x: number) => x > threshold() && (x + parity()) % 3 !== 0
            const source = new RxList<number>(Array.from({length: 150}, () => next++))   // longer than the locate window
            const filtered = source.filter(pred)
            const mirror = filtered.map(x => x)
            const history: string[] = []
            const op = () => {
                const len = source.data.length, r = rand()
                if (r < 0.30) { const v = Math.floor(rand() * (next + 2)) - 1; history.push(`threshold=${v}`); threshold(v) }
                else if (r < 0.45) { const v = Math.floor(rand() * 3); history.push(`parity=${v}`); parity(v) }
                else if (r < 0.55) { history.push('push'); source.push(next++) }
                else if (r < 0.62) { history.push('unshift'); source.unshift(next++) }
                else if (r < 0.72 && len) { const i = Math.floor(rand() * len), d = Math.floor(rand() * 4); history.push(`splice(${i},${d},+1)`); source.splice(i, d, next++) }
                else if (r < 0.80 && len) { const i = Math.floor(rand() * len); history.push(`set(${i})`); source.set(i, next++) }
                else if (r < 0.88 && len >= 2) { const a = Math.floor(rand() * (len - 1)); history.push(`swap(${a},${len - 1})`); source.swap(a, len - 1) }
                else if (r < 0.94 && len >= 2) { history.push('sortSelf(desc)'); source.sortSelf((a, b) => b - a) }
                else if (len >= 2) { history.push('sortSelf(by x*7%11)'); source.sortSelf((a, b) => (a * 7) % 11 - (b * 7) % 11) }
            }
            for (let step = 0; step < 40; step++) {
                if (rand() < 0.3) { history.push('batch{'); batch(() => { op(); op(); op() }); history.push('}') }
                else op()
                const model = source.data.filter(x => x > threshold.raw && (x + parity.raw) % 3 !== 0)
                const ctx = `seed ${seed} step ${step}: ${history.slice(-12).join(' | ')}`
                expect(filtered.data, ctx).toEqual(model)
                expect(mirror.data, ctx).toEqual(model)
            }
            mirror.destroy(); filtered.destroy(); source.destroy()
        }
    })
    test('the error-recovery full recompute invalidates the locate cursor (rows shift under a parked cursor)', () => {
        vi.spyOn(console, 'error').mockImplementation(() => undefined)
        const boom = atom(true)
        const row = (id: string) => ({id, on: atom(true)})
        const rows = 'ABCDEFGHIJ'.split('').map(row)
        const [, , , , , , G, , , J] = rows
        const late = row('Z')
        const source = new RxList(rows.slice())
        const filtered = source.filter(x => { if (x.id === 'Z' && boom()) throw new Error('predicate boom'); return x.on() })
        expect(() => source.push(late)).toThrow('predicate boom')   // the row list's patch throws: it recovers by a full recompute
        G.on(false)                                                    // a toggle parks the cursor on row 6
        boom(false)
        source.splice(0, 3)                                            // recovery rebuild: every row moves up by 3
        J.on(false)                                                    // J is row 6 now: a stale cursor would place it at 6
        expect(filtered.data.map(x => x.id)).toEqual(source.data.filter(x => x.on.raw).map(x => x.id))
        vi.restoreAllMocks()
        filtered.destroy(); source.destroy()
    })
    test('rows scanned: linear in subscription order, never worse than the scan from row 0 when shuffled', () => {
        const small = flipAll(1000, false), large = flipAll(4000, false)
        // in subscription order every row is located from the previous one: 4x rows -> ~4x reads
        // (the scan from row 0 per flip is ~n^2/2 reads: 16x)
        for (const phase of ['off', 'on', 'half'] as const) {
            expect(large[phase] / small[phase], phase).toBeLessThan(5)
        }
        // shuffled rows fall back to the scan from row 0 (at most one extra window of reads per flip)
        const shuffled = flipAll(1000, true)
        const scanFromZero = 1000 * 1000                       // upper bound of the old per-flip prefix scans
        expect(shuffled.on).toBeLessThan(scanFromZero + 1000 * 64 + 10 * 1000)
    })
})

describe('map: reactive rows recomputing while a structural change is still pending', () => {
    // Deterministic work counter instead of wall clock: every reactive row's frame is [rowComputed]; turn its
    // slot 0 into a counting accessor, so each frame probe of the pending-window row locator (indexOf during a
    // linear scan or a validation, a read while building the position table) counts one.
    const countFrameProbes = (mapped: RxList<any>) => {
        const counter = {probes: 0}
        for (const frame of (mapped as any)._effectFramesArray as unknown[][]) {
            if (!frame || frame.length === 0 || Object.isFrozen(frame)) continue
            const effect = frame[0]
            Object.defineProperty(frame, 0, {get() { counter.probes++; return effect }, configurable: true, enumerable: true})
        }
        return counter
    }
    const n = 2000
    test('one row recomputing inside the window costs a linear lookup up to that row, no position table', () => {
        const k = n / 2
        const marks = Array.from({length: n}, () => atom(0))
        const source = new RxList<number>(Array.from({length: n}, (_, i) => i))
        const mapped = source.map(i => i >= 0 ? i + marks[i]() : i)
        const counter = countFrameProbes(mapped)
        batch(() => { marks[k](1); source.push(-1) })            // the row re-runs before the push is replayed
        expect(counter.probes).toBe(k + 1)
        expect(mapped.data).toEqual(source.data.map(i => i >= 0 ? i + marks[i].raw : i))
        // deferred map: the push waits in the scheduler while the row re-runs
        const pending: (() => void)[] = []
        const deferred = source.map(i => i >= 0 ? i + marks[i]() : i, {scheduleRecompute: (run: any) => { pending.push(run) }})
        const deferredCounter = countFrameProbes(deferred)
        source.push(-2)
        marks[k](2)
        expect(deferredCounter.probes).toBe(k + 1)
        pending.splice(0).forEach(run => run())
        expect(deferred.data).toEqual(source.data.map(i => i >= 0 ? i + marks[i].raw : i))
        mapped.destroy(); deferred.destroy(); source.destroy()
    })
    test('every row recomputing inside the window costs O(n) in total, not O(n^2)', () => {
        const factor = atom(1)
        const source = new RxList<number>(Array.from({length: n}, (_, i) => i))
        const mapped = source.map(x => x * factor())
        const counter = countFrameProbes(mapped)
        batch(() => { factor(3); source.push(1) })
        expect(mapped.data).toEqual(source.data.map(x => x * 3))
        // one linear lookup (row 0), one table build (n), one validation per later row: ~2n; one findIndex per row would be ~n^2/2
        expect(counter.probes).toBeLessThanOrEqual(2 * n + 1)
        const pending: (() => void)[] = []
        const deferred = source.map(x => x + factor(), {scheduleRecompute: (run: any) => { pending.push(run) }})
        const deferredCounter = countFrameProbes(deferred)
        source.unshift(7)
        factor(4)                                                  // every row re-runs while the unshift is pending
        expect(deferredCounter.probes).toBeLessThanOrEqual(2 * (n + 1) + 1)
        pending.splice(0).forEach(run => run())
        expect(deferred.data).toEqual(source.data.map(x => x + 4))
        mapped.destroy(); deferred.destroy(); source.destroy()
    })
    test('a row moved by the running patch is still found at its current position (the position cache validates itself)', () => {
        // deferred map patched outside any digest: writes made by mapFn / cleanups during the patch
        // trigger the surviving rows inline, before and after the row bookkeeping is spliced
        const pending: (() => void)[] = []
        const flush = () => pending.splice(0).forEach(run => run())
        const x = atom(0)
        const source = new RxList<string>(['r0', 'r1', 'r2'])
        const mapped = source.map((item, _index, {onCleanup}) => {
            if (item.startsWith('n')) x(x.raw + 1)                    // a new row announces itself: older rows recompute inline
            onCleanup(() => { if (item === 'r0') x(x.raw + 1) })      // ...and again when the removed row is cleaned
            return item.startsWith('r') ? `${item}:${x()}` : item
        }, {scheduleRecompute: (run: any) => { pending.push(run) }})
        expect(mapped.data).toEqual(['r0:0', 'r1:0', 'r2:0'])
        source.splice(0, 1, 'n1', 'n2')                                // r1, r2 shift from 1, 2 to 2, 3
        flush(); flush()
        expect(mapped.data).toEqual(['n1', 'n2', `r1:${x.raw}`, `r2:${x.raw}`])
        x(x.raw + 1)
        expect(mapped.data).toEqual(['n1', 'n2', `r1:${x.raw}`, `r2:${x.raw}`])
    })
})

describe('map options: every patch round that called beforePatch also calls afterPatch', () => {
    const setup = (mapFn: (x: number | 'live', index?: any) => number, initial: (number | 'live')[]) => {
        const source = new RxList<number | 'live'>(initial)
        const log: string[] = []
        const mapped = source.map(mapFn as any, {
            beforePatch: info => { log.push(`before:${info.method ?? 'set'}`) },
            afterPatch: infos => { log.push(`after:${infos.length}`) },
        })
        mapped.on('fullRecompute', () => log.push('fullRecompute'))
        return {source, mapped, log}
    }
    const live = atom(5)
    test('normal patch rounds (single and multi info)', () => {
        const {source, mapped, log} = setup(x => x === 'live' ? live() : x, [1, 2])
        source.push(3)
        batch(() => { source.push(4); source.shift() })
        expect(log).toEqual(['before:splice', 'after:1', 'before:splice', 'before:splice', 'after:2'])
        expect(mapped.data).toEqual([2, 3, 4])
    })
    test('round that bails out to a full recompute because the first reactive row appeared mid-replay', () => {
        const {source, mapped, log} = setup(x => x === 'live' ? live() : x, [1, 2])
        batch(() => { source.push('live'); source.push(3) })
        expect(log).toEqual(['before:splice', 'before:splice', 'after:2', 'fullRecompute'])
        expect(mapped.data).toEqual([1, 2, 5, 3])
    })
    test('round that bails out because an operation-time index is beyond the final source', () => {
        const {source, mapped, log} = setup(x => x === 'live' ? live() : x, [1, 2])
        batch(() => { source.push('live'); source.splice(0, 3) })
        expect(log).toEqual(['before:splice', 'before:splice', 'after:2', 'fullRecompute'])
        expect(mapped.data).toEqual([])
    })
    test('round rejected before any info is replayed (index-atom map, multi info): neither hook runs', () => {
        const {source, mapped, log} = setup((x, index) => (x as number) + index(), [1, 2])
        batch(() => { source.push(3); source.push(4) })
        expect(log).toEqual(['fullRecompute'])
        expect(mapped.data).toEqual([1, 3, 5, 7])
    })
})
