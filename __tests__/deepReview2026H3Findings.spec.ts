/**
 * Method 16–17 deep-review findings (2026-H3).
 *
 * Equivalence classes pinned here:
 * 1. groupBy empty-key retention — incremental remove/set that empties a group
 *    must delete the map key (≡ full recompute key set). Prior fuzz only asserted
 *    per-key contents, so empty ≡ empty passed while phantom keys survived.
 * 2. object-atom null-proto / constructor overwrite — isPlainObject must use
 *    prototype chain, not constructor field, so Object.create(null) and
 *    constructor=null still surface properties + track.
 * 3. AsyncRxSlice destroy × in-flight — destroyResources must invalidate
 *    fetchReceipt so late reject/resolve cannot zombie-write loadError/isLoading.
 */
import {afterEach, describe, expect, test} from 'vitest'
import {atom} from '../src/atom.js'
import {autorun} from '../src/common.js'
import {AsyncRxSlice} from '../src/AsyncRxSlice.js'
import {RxList} from '../src/RxList.js'
import {notifier} from '../src/notify.js'
import {ReactiveEffect} from '../src/reactiveEffect.js'
import {isPlainObject} from '../src/util.js'

function assertQuiescent(label: string) {
    expect(notifier.isDigesting, `${label}: isDigesting`).toBe(false)
    expect(ReactiveEffect.activeScopes.length, `${label}: activeScopes`).toBe(0)
}

afterEach(() => assertQuiescent('afterEach'))

type Deferred<T> = {promise: Promise<T>, resolve: (v: T) => void, reject: (e: any) => void}
function deferred<T>(): Deferred<T> {
    let resolve!: (v: T) => void
    let reject!: (e: any) => void
    const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej })
    return {promise, resolve, reject}
}
const tick = () => new Promise<void>(r => setTimeout(r, 0))

describe('groupBy empty-key retention (method 17)', () => {
    test('splice emptying a group deletes the key', () => {
        const list = new RxList([
            {k: 'a', v: 1},
            {k: 'b', v: 2},
            {k: 'a', v: 3},
        ])
        const grouped = list.groupBy(x => x.k)
        try {
            list.splice(2, 1)
            list.splice(0, 1)
            expect(grouped.data.has('a')).toBe(false)
            expect([...grouped.data.keys()]).toEqual(['b'])
            expect(grouped.data.get('b')!.data.map(x => x.v)).toEqual([2])
            expect(grouped.data.size).toBe(1)
        } finally {
            for (const g of grouped.data.values()) g.destroy()
            grouped.destroy()
            list.destroy()
        }
    })

    test('set moving last member out of group deletes empty key', () => {
        const list = new RxList([
            {k: 'a', v: 1},
            {k: 'b', v: 2},
        ])
        const grouped = list.groupBy(x => x.k)
        try {
            list.set(0, {k: 'b', v: 9})
            expect(grouped.data.has('a')).toBe(false)
            expect([...grouped.data.keys()]).toEqual(['b'])
            expect(grouped.data.get('b')!.data.map(x => x.v)).toEqual([9, 2])
        } finally {
            for (const g of grouped.data.values()) g.destroy()
            grouped.destroy()
            list.destroy()
        }
    })

    test('replacing sole member of a group with same key keeps the key', () => {
        const list = new RxList([{k: 'a', v: 1}])
        const grouped = list.groupBy(x => x.k)
        try {
            list.set(0, {k: 'a', v: 2})
            expect(grouped.data.has('a')).toBe(true)
            expect(grouped.data.get('a')!.data.map(x => x.v)).toEqual([2])
        } finally {
            for (const g of grouped.data.values()) g.destroy()
            grouped.destroy()
            list.destroy()
        }
    })
})

describe('object-atom isPlainObject gate (method 16)', () => {
    test('isPlainObject accepts null-prototype dictionaries', () => {
        expect(isPlainObject(Object.create(null))).toBe(true)
        expect(isPlainObject({})).toBe(true)
        expect(isPlainObject([])).toBe(true)
        class Box { n = 1 }
        expect(isPlainObject(new Box())).toBe(false)
    })

    test('null-prototype object atom: property get + track', () => {
        const raw = Object.create(null) as Record<string, number>
        raw.x = 1
        const obj = atom(raw)
        let seen = -1
        const stop = autorun(() => {
            seen = (obj as any).x
        }, true)
        expect(seen).toBe(1)
        ;(obj as any).x = 9
        expect(seen).toBe(9)
        stop()
    })

    test('constructor overwrite does not blind subsequent property access', () => {
        const obj = atom({n: 1} as Record<string, unknown>)
        ;(obj as any).constructor = null
        expect((obj as any).n).toBe(1)
        ;(obj as any).n = 2
        expect((obj as any).n).toBe(2)
        expect(obj.raw.n).toBe(2)
    })
})

describe('AsyncRxSlice destroy × in-flight (method 16)', () => {
    test('update reject after destroy must not set loadError', async () => {
        const pending: Deferred<number[]>[] = []
        const slice = new AsyncRxSlice<number>([1], () => {
            const d = deferred<number[]>()
            pending.push(d)
            return d.promise
        }, x => x)
        const u = slice.update(1, 1)
        slice.destroy()
        pending[0].reject(new Error('net'))
        await u
        await tick()
        expect(slice.data).toEqual([1])
        expect(slice.loadError.raw).toBe(null)
        expect(slice.isLoading.raw).toBe(false)
    })

    test('fetch resolve after destroy must not mutate data or resurrect loading', async () => {
        const pending: Deferred<number[]>[] = []
        const slice = new AsyncRxSlice<number>([0], () => {
            const d = deferred<number[]>()
            pending.push(d)
            return d.promise
        })
        const p = slice.fetch()
        slice.destroy()
        pending[0].resolve([1, 2, 3])
        await p
        await tick()
        expect(slice.data).toEqual([0])
        expect(slice.isLoading.raw).toBe(false)
        expect(slice.loadError.raw).toBe(null)
    })
})
