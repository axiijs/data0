import {describe, expect, test} from 'vitest'
import {atom, batch, onChange, recompute, RxList, RxMap} from '../src/index'
import {countPrimitiveRawReads, countSourceScans} from './listWorkCounters'

/**
 * Audit findings for RxList derived operators. Every test in this file is
 * expected to FAIL on the audited source (v2.16.0 + pending PR).
 */

describe('F6 values()/entries() of a derived RxMap (groupBy / indexBy / toMap) emit a transient undefined row when a key is deleted', () => {
    test('groupBy(...).entries().map(([key, group]) => ...) throws to the mutation caller when a group empties', () => {
        const source = new RxList([{k: 'x'}, {k: 'y'}])
        const sizes = source.groupBy(r => r.k).entries().map(([key, group]) => `${key}:${group.data.length}`)
        expect(sizes.data).toEqual(['x:1', 'y:1'])
        expect(() => source.pop()).not.toThrow()
        expect(sizes.data).toEqual(['x:1'])
    })
    test('indexBy(...).values(): subscribers are told set(0, undefined) before the row is removed', () => {
        const source = new RxList([{id: 1}, {id: 2}])
        const values = source.indexBy('id').values()
        const seen: string[] = []
        const stop = onChange(values, (infos: any[]) => infos.forEach(i => seen.push(i.method ?? `set(${i.key}, ${JSON.stringify(i.newValue)})`)))
        source.shift()
        expect(values.data).toEqual([{id: 2}])
        expect(seen).toEqual(['splice'])
        stop()
    })
    test('plain RxMap: values().map(v => v.n) + delete(key)', () => {
        const map = new RxMap<string, {n: number}>([['a', {n: 1}], ['b', {n: 2}]])
        const ns = map.values().map(v => v.n)
        expect(() => map.delete('a')).not.toThrow()
        expect(ns.data).toEqual([2])
    })
})

describe('F1 map: lazy index-atom upgrade inside a multi-info replay leaves source.atomIndexes longer than source.data', () => {
    test('row with a reactive dependency is inserted and the list is truncated in the same batch', () => {
        const live = atom(7)
        // rows 1 and 2 are static, the 'live' row reads an atom: the map only starts
        // using index atoms when the first reactive row appears
        const source = new RxList<number | 'live'>([1, 2])
        const mapped = source.map(x => x === 'live' ? live() : x)
        batch(() => {
            source.push('live')     // operation-time index 2
            source.splice(0, 3)     // final length 0
        })
        expect(mapped.data).toEqual([])
        // a perfectly valid later mutation must not throw
        expect(() => source.push(3)).not.toThrow()
    })
    test('same thing on an empty source: push + pop in one batch', () => {
        const factor = atom(2)
        const source = new RxList<number>([])
        const mapped = source.map(x => x * factor())
        batch(() => {
            source.push(1)
            source.pop()
        })
        expect(mapped.data).toEqual([])
        expect((source.atomIndexes ?? []).length).toBeLessThanOrEqual(source.data.length)
    })
})

describe('F2 forced recompute of a patch-type derived structure inside a batch double-applies the queued change', () => {
    test('map / groupBy / length after batch(() => { push; recompute(derived, true) })', () => {
        const source = new RxList([1, 2, 3])
        const mapped = source.map(x => x * 2)
        const grouped = source.groupBy(x => x % 2)
        batch(() => {
            source.push(4)
            recompute(mapped, true)
            recompute(grouped, true)
        })
        // README §3: after the batch exits every derived structure equals a full recompute of the final source
        expect(grouped.data.get(0)!.data).toEqual([2, 4])
        expect(mapped.data).toEqual([2, 4, 6, 8])
    })
    test('no batch needed: a subscriber that runs earlier in the same digest forces the recompute', () => {
        const source = new RxList([1, 2, 3])
        let mapped: RxList<number> | undefined
        const stop = onChange(source, () => { mapped?.recompute(true) })   // subscribed before `mapped`
        mapped = source.map(x => x * 2)
        source.push(4)
        expect(mapped.data).toEqual([2, 4, 6, 8])
        stop()
    })
    test('non-forced: a deferred map whose scheduler is flushed inside a later batch takes the wrong index atom', () => {
        const pending: (() => void)[] = []
        const flush = () => pending.splice(0).forEach(run => run())
        const source = new RxList(['a', 'b', 'c', 'd'])
        const mapped = source.map((x, i) => `${x}@${i()}`, {scheduleRecompute: (run: any) => { pending.push(run) }})
        source.splice(3, 1, 'D')        // info 1 waits in the scheduler
        batch(() => {
            source.shift()              // info 2 waits in the notifier session
            flush()                     // the scheduler happens to run here
        })
        flush()
        expect(mapped.data).toEqual(source.data.map((x, i) => `${x}@${i}`))   // ['b@0', 'c@1', 'D@2']
    })
})

describe('F3 map options.onCleanup is not called for the value replaced by a row-level recompute', () => {
    test('every mapped value that leaves the list gets options.onCleanup exactly once', () => {
        const theme = atom('light')
        const source = new RxList<number>([1, 2])
        const disposed: string[] = []
        const mapped = source.map(item => `${item}-${theme()}`, {
            onCleanup: value => disposed.push(value),
        })
        expect(mapped.data).toEqual(['1-light', '2-light'])
        theme('dark')                                  // both rows recompute, old values leave the list
        expect(mapped.data).toEqual(['1-dark', '2-dark'])
        mapped.destroy()
        expect(disposed.slice().sort()).toEqual(['1-dark', '1-light', '2-dark', '2-light'])
    })
})

describe('F4 indexBy / toMap: a batch whose final state is valid throws "key is already exist" and leaves the map wrong', () => {
    test('indexBy: swap two rows with two set() calls in a batch', () => {
        const a = {id: 'a'}, b = {id: 'b'}
        const list = new RxList([a, b])
        const byId = list.indexBy('id')
        expect(() => batch(() => {
            list.set(0, b)
            list.set(1, a)
        })).not.toThrow()
        expect(list.data).toEqual([b, a])
        expect([...byId.data.keys()].sort()).toEqual(['a', 'b'])
    })
    test('toMap: move a row to the end (insert the copy first, then remove the original) in a batch', () => {
        const list = new RxList<[string, number]>([['a', 1], ['b', 2]])
        const map = list.toMap()
        expect(() => batch(() => {
            list.push(['a', 10])
            list.splice(0, 1)
        })).not.toThrow()
        expect(map.data.get('a')).toBe(10)
    })
})

describe('F5 sortSelf hands undefined elements to the comparator (Array#sort never does)', () => {
    test('numeric comparator: list ends up unsorted and disagrees with toSorted of the same data', () => {
        const data = [3, undefined, 1, undefined, 2]
        const list = new RxList<number | undefined>(data.slice())
        const cmp = (x: number | undefined, y: number | undefined) => (x as number) - (y as number)
        const sorted = list.toSorted(cmp)                  // [1, 2, 3, undefined, undefined]
        expect(sorted.data).toEqual(data.slice().sort(cmp))
        list.sortSelf(cmp)
        expect(list.data).toEqual(data.slice().sort(cmp))
    })
    test('object comparator: throws TypeError where Array#sort works', () => {
        const rows = [{name: 'b'}, undefined, {name: 'a'}]
        const byName = (x: any, y: any) => x.name.localeCompare(y.name)
        expect(() => rows.slice().sort(byName)).not.toThrow()
        const list = new RxList<any>(rows.slice())
        expect(() => list.sortSelf(byName)).not.toThrow()
    })
})

describe('F7 filter: a subscriber that reacts while one row toggle is being dispatched sees a stale indicator; the filtered list ends up wrong', () => {
    const row = (id: string, on: boolean) => ({id, on: atom(on)})
    test('toggle on: the subscriber turns on a later row -> wrong order', () => {
        const a = row('a', false), b = row('b', false), c = row('c', true)
        const source = new RxList([a, b, c])
        const filtered = source.filter(x => x.on())
        let done = false
        const stop = onChange(filtered, () => { if (!done) { done = true; b.on(true) } })
        a.on(true)
        expect(filtered.data.map(x => x.id)).toEqual(['a', 'b', 'c'])
        stop()
    })
    test('toggle off: the subscriber turns off a later row -> that row stays in the list', () => {
        const a = row('a', true), b = row('b', true), c = row('c', true)
        const source = new RxList([a, b, c])
        const filtered = source.filter(x => x.on())
        let done = false
        const stop = onChange(filtered, () => { if (!done) { done = true; c.on(false) } })
        a.on(false)
        expect(filtered.data.map(x => x.id)).toEqual(['b'])
        stop()
    })
    test('toggle off: the subscriber removes a later row from the source -> the wrong row is removed', () => {
        const a = row('a', true), b = row('b', true), z = row('z', true)
        const source = new RxList([a, b, z])
        const filtered = source.filter(x => x.on())
        let done = false
        const stop = onChange(filtered, () => { if (!done) { done = true; source.splice(1, 1) } })
        a.on(false)
        expect(source.data.map(x => x.id)).toEqual(['a', 'z'])
        expect(filtered.data.map(x => x.id)).toEqual(['z'])
        stop()
    })
})

// Copied from the core reviewer's spec (findings/d0-core.audit-core.spec.ts, group F8): the loop lives in RxList.map's onDestroy.
describe('F8 (core reviewer) destroy with a throwing context.onCleanup skips the rest of destroyResources', () => {
    test('RxList side of the same hook chain: map rows after a throwing row cleanup are never cleaned', () => {
        const dep = atom(0)
        const source = new RxList<number>([1, 2, 3])
        const cleaned: number[] = []
        const mapped = source.map((item, _i, {onCleanup}: any) => {
            dep()
            onCleanup(() => { cleaned.push(item); if (item === 1) throw new Error('row cleanup boom') })
            return item
        })
        expect(() => mapped.destroy()).toThrow('row cleanup boom')
        expect(cleaned).toEqual([1, 2, 3])
        source.destroy()
    })
})

// Scale findings are asserted with deterministic work counters (rows scanned / elements visited), not wall clock.

describe('P1 filter: a tail push rescans every row (O(n) per push, loops are quadratic)', () => {
    test('rows scanned by 2000 pushes must not depend on how long the list already is', () => {
        const run = (n: number) => {
            const source = new RxList<number>(Array.from({length: n}, (_, i) => i))
            const filtered = source.filter(x => x % 2 === 0)
            const reads = countPrimitiveRawReads(() => { for (let i = 0; i < 2000; i++) source.push(i) })
            expect(filtered.data).toEqual(source.data.filter(x => x % 2 === 0))
            filtered.destroy(); source.destroy()
            return reads
        }
        const small = run(2000), large = run(32000)
        // the old prefix scan reads every row on every push: large - small = 2000 x 30000
        expect(large).toBe(small)
    })
})

describe('P2 toSet: bulk delete is O(deleted x remaining)', () => {
    test('removing half of the list does not scan the source once per removed row', () => {
        const n = 20000
        const source = new RxList<number>(Array.from({length: n}, (_, i) => i))
        const set = source.toSet()
        const counter = countSourceScans(source)
        source.splice(0, n / 2)
        expect(set.data.size).toBe(n / 2)
        // one scan per removed row would visit (n/2) x (n/2) = 10^8 elements
        expect(counter.scanned).toBe(0)
        set.destroy(); source.destroy()
    })
})
