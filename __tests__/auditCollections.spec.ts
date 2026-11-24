/**
 * Audit of RxMap / RxSet / RxTime / AsyncRxSlice / debug / retainedDiagnostics and their seams
 * with RxList. Every test here is a minimal reproduction that FAILS on the audited tree
 * (v2.16.0 + the teardown-isolation PR). Run:
 *   fnm exec --using 22 npx vitest run __tests__/audit-structs.spec.ts
 * Fixes for F1-F3, F5-F8, M1, M3, M4: audit-final.patch in the repo root (applied in this copy;
 * regression tests in audit-fixes.spec.ts). F4 and M2 have their root cause in computed.ts /
 * notify.ts (core reviewer) and stay red here.
 */
import {afterEach, describe, expect, test, vi} from 'vitest'
import {
    atom, autorun, batch, computed, destroyComputed, getDebugName, notifier, onChange, recompute,
    disableData0RetainedObjectDiagnostics, enableData0RetainedObjectDiagnostics, getData0RetainedObjectDiagnosticsSnapshot,
    AsyncRxSlice, RxList, RxMap, RxSet, RxTime,
} from '../src/index'

const subscribers = (t: object) => [...(notifier.getDepEffects(t) ?? [])]

// ---------------------------------------------------------------------------
// F1  RxMap.values()/entries(): removing a key pushes a phantom `undefined`
//     row through the derived list before the row is removed
// ---------------------------------------------------------------------------
describe('F1 RxMap.values()/entries() x key removal', () => {
    test('F1a values().map(v => v.name): delete() throws TypeError to the caller', () => {
        const users = new RxMap<string, {name: string}>([['a', {name: 'Ann'}], ['b', {name: 'Bob'}]])
        const names = users.values().map(u => u.name)
        expect(names.data).toEqual(['Ann', 'Bob'])
        expect(() => users.delete('a')).not.toThrow()
        expect(names.data).toEqual(['Bob'])
    })

    test('F1b the values list announces set(index, undefined) for a value the map never held', () => {
        const m = new RxMap<string, number>([['a', 1], ['b', 2]])
        const values = m.values()
        const seen: unknown[] = []
        const stop = onChange(values, (infos: any[]) => {
            for (const info of infos) seen.push(info.method ?? `set(${info.key}, ${info.newValue})`)
        })
        m.delete('a')
        stop()
        expect(values.data).toEqual([2])
        // one structural change in the source = one splice in the derived list
        expect(seen).toEqual(['splice'])
    })

    test('F1c same class through a derived map: list.indexBy().values().map()', () => {
        const list = new RxList([{id: 1, name: 'Ann'}, {id: 2, name: 'Bob'}])
        const names = list.indexBy('id').values().map(u => u.name)
        expect(() => list.shift()).not.toThrow()
        expect(names.data).toEqual(['Bob'])
    })

    test('F1d entries().map(([k, v]) => ...): clear() throws', () => {
        const m = new RxMap<string, {name: string}>([['a', {name: 'Ann'}], ['b', {name: 'Bob'}]])
        const labels = m.entries().map(([k, v]) => `${k}:${v.name}`)
        expect(() => m.clear()).not.toThrow()
        expect(labels.data).toEqual([])
    })
})

// ---------------------------------------------------------------------------
// F2  indexBy / toMap: a batch whose final state has unique keys asserts
//     "key is already exist" when the new row is inserted before the old one
//     is removed, and leaves the derived map stale
// ---------------------------------------------------------------------------
describe('F2 indexBy/toMap x batch with a transient duplicate key', () => {
    test('F2a indexBy: insert new version, then drop the old one', () => {
        const list = new RxList([{id: 1, v: 'old'}, {id: 2, v: 'x'}])
        const byId = list.indexBy('id')
        expect(() => batch(() => {
            list.push({id: 1, v: 'new'})
            list.shift()
        })).not.toThrow()
        expect(byId.data.get(1)!.v).toBe('new')
    })

    test('F2b after the throw the derived map differs from a full recompute of the final source', () => {
        const list = new RxList([{id: 1, v: 'old'}, {id: 2, v: 'x'}])
        const byId = list.indexBy('id')
        try {
            batch(() => {
                list.push({id: 1, v: 'new'})
                list.shift()
            })
        } catch { /* reported by F2a */ }
        const scratch = new RxList(list.data.slice()).indexBy('id')   // final source is valid: no throw here
        const byKey = (m: Map<any, any>) => [...m.entries()].sort((x, y) => x[0] - y[0])   // content only, key order ignored
        expect(byKey(byId.data)).toEqual(byKey(scratch.data))
    })

    test('F2c toMap: move a row with two splices (insert at the new place, remove the old one)', () => {
        const a: [string, number] = ['a', 1]
        const list = new RxList<[string, number]>([a, ['b', 2]])
        const asMap = list.toMap()
        expect(() => batch(() => {
            list.splice(2, 0, a)
            list.splice(0, 1)
        })).not.toThrow()
        expect([...asMap.data.entries()].sort()).toEqual([['a', 1], ['b', 2]])
    })
})

// ---------------------------------------------------------------------------
// F3  RxTime: the wake-up timer is armed at most once per evaluation and only
//     when the crossing is strictly in the future
// ---------------------------------------------------------------------------
describe('F3 RxTime never re-arms its timer', () => {
    afterEach(() => vi.useRealTimers())

    test('F3a gt(deadline) evaluated exactly at the deadline stays false forever', () => {
        vi.useFakeTimers()
        vi.setSystemTime(1_000_000)
        const deadline = atom(1_005_000)
        const t = new RxTime()
        const expired = t.gt(deadline)
        deadline(Date.now())             // "expire now"
        expect(expired.raw).toBe(false)  // now > now is false: fine
        vi.advanceTimersByTime(60_000)   // one minute later it must be true
        expect(expired.raw).toBe(true)
        t.destroy()
    })

    test('F3b a deadline more than 2^31-1 ms away never flips (setTimeout overflow, single shot)', () => {
        vi.useFakeTimers()
        vi.setSystemTime(1_000_000)
        const thirtyDays = 30 * 24 * 3600 * 1000
        const t = new RxTime()
        const expired = t.gt(1_000_000 + thirtyDays)
        vi.advanceTimersByTime(thirtyDays + 10_000)
        expect(expired.raw).toBe(true)
        t.destroy()
    })

    test('F3c wall clock stepped back while waiting: the single wake-up fires early and nothing re-arms', () => {
        vi.useFakeTimers()
        vi.setSystemTime(1_000_000)
        const t = new RxTime()
        const expired = t.gt(1_001_000)
        vi.setSystemTime(999_000)        // clock corrected backwards by 1s
        vi.advanceTimersByTime(1_100)    // wake-up fires at wall 1_000_100: still before the deadline
        expect(expired.raw).toBe(false)
        vi.advanceTimersByTime(5_000)    // wall 1_005_100: deadline long passed
        expect(expired.raw).toBe(true)
        t.destroy()
    })
})

// ---------------------------------------------------------------------------
// F4  A forced full recompute while infos for the same structure are still
//     queued in the batch/digest: the queued infos are replayed on top of the
//     fresh result (root cause in computed.ts; seen through toList/keys/map)
// ---------------------------------------------------------------------------
describe('F4 forced recompute x infos queued in the session', () => {
    test('F4a RxSet.toList(): recompute(list, true) inside a batch duplicates the row', () => {
        const s = new RxSet<number>([1])
        const list = s.toList()
        batch(() => {
            s.add(2)
            recompute(list, true)        // "I need it fresh inside the batch"
            expect(list.data).toEqual([1, 2])
        })
        expect(list.data).toEqual([...s.data])
    })

    test('F4b RxMap.keys()', () => {
        const m = new RxMap<string, number>([['a', 1]])
        const keys = m.keys()
        batch(() => {
            m.set('b', 2)
            recompute(keys, true)
        })
        expect(keys.data).toEqual([...m.data.keys()])
    })

    test('F4c no batch: a subscriber that runs first forces a sibling during the digest', () => {
        const s = new RxSet<number>([1])
        let list!: RxList<number>
        const stop = onChange(s, () => { recompute(list, true) })
        list = s.toList()
        s.add(2)
        stop()
        expect(list.data).toEqual([...s.data])
    })
})

// ---------------------------------------------------------------------------
// F5  RxTime.destroy() called from a subscriber of the result atom: the
//     evaluation that is still running arms a timer after the teardown
// ---------------------------------------------------------------------------
describe('F5 RxTime timer armed after destroy', () => {
    afterEach(() => vi.useRealTimers())

    test('destroy() inside a subscriber leaves a live timer that later flips the result', () => {
        vi.useFakeTimers()
        vi.setSystemTime(1_000_000)
        const deadline = atom(999_000)
        const t = new RxTime()
        const expired = t.gt(deadline)
        expect(expired.raw).toBe(true)
        // stop watching as soon as the deadline is extended
        const stop = autorun(() => { if (!expired()) t.destroy() }, true)
        deadline(1_001_000)
        expect(expired.raw).toBe(false)
        expect(vi.getTimerCount()).toBe(0)
        vi.advanceTimersByTime(5_000)
        expect(expired.raw).toBe(false)
        stop()
    })
})

// ---------------------------------------------------------------------------
// F6  AsyncRxSlice.fetch() after destroy(): a live computed is created on the
//     destroyed slice and keeps calling the remote on every parameter change
// ---------------------------------------------------------------------------
describe('F6 AsyncRxSlice.fetch() after destroy', () => {
    test('no subscription and no further remote calls after destroy', async () => {
        const page = atom(1)
        let remoteCalls = 0
        const slice = new AsyncRxSlice<number>([], async () => { remoteCalls++; return [page()] })
        slice.destroy()
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
        try {
            await slice.fetch()
            const callsAfterFirstFetch = remoteCalls
            page(2)
            await Promise.resolve()
            expect(subscribers(page)).toHaveLength(0)
            expect(remoteCalls).toBe(callsAfterFirstFetch)
        } finally {
            warn.mockRestore()
        }
    })
})

// ---------------------------------------------------------------------------
// F7  Lazily cached meta destroyed by a consumer stays cached (dead) forever
// ---------------------------------------------------------------------------
describe('F7 cached meta x consumer destroy', () => {
    test('F7a RxMap.keys(): after a consumer destroys the list, keys() keeps returning the dead instance', () => {
        const m = new RxMap<string, number>([['a', 1]])
        const keysOfConsumer1 = m.keys()
        keysOfConsumer1.destroy()          // README 6: derived structures need .destroy()
        m.set('b', 2)
        expect(m.keys().data).toEqual(['a', 'b'])
    })

    test('F7b RxSet.size', () => {
        const s = new RxSet<number>([1])
        destroyComputed(s.size)
        s.add(2)
        expect(s.size.raw).toBe(2)
    })

    test('F7c RxMap.values() built on a keys() list that a consumer destroyed is frozen but reports active', () => {
        const m = new RxMap<string, number>([['a', 1]])
        m.keys().destroy()
        const values = m.values()
        m.set('b', 2)
        expect(values.active).toBe(true)
        expect(values.data).toEqual([1, 2])
    })
})

// ---------------------------------------------------------------------------
// F8  (performance) RxList.toSet(): bulk removal is O(removed x remaining)
// ---------------------------------------------------------------------------
describe('F8 toSet bulk removal is quadratic', () => {
    const median = (xs: number[]) => xs.slice().sort((a, b) => a - b)[Math.floor(xs.length / 2)]
    // returns [median ms of replaceData, number of source elements scanned by the patch]
    const measure = (n: number): [number, number] => {
        const runs: number[] = []
        let scanned = 0
        for (let r = 0; r < 3; r++) {
            const list = new RxList<object>(Array.from({length: n}, (_, i) => ({i})))
            const set = list.toSet()
            const fresh = Array.from({length: n}, (_, i) => ({i}))
            // deterministic work counter: every linear membership scan over the source array
            scanned = 0
            const data = list.data as any
            data.includes = function (this: unknown[], x: unknown) {
                scanned += this.length
                return Array.prototype.includes.call(this, x)
            }
            const t0 = performance.now()
            list.replaceData(fresh)          // e.g. a refetched page of rows
            runs.push(performance.now() - t0)
            expect(set.data.size).toBe(n)
            set.destroy(); list.destroy()
        }
        return [median(runs), scanned]
    }
    test('replaceData(n fresh rows) with a toSet() attached scans the source once per removed row', () => {
        measure(2_000) // warm-up
        const [smallMs, smallScanned] = measure(10_000)
        const [largeMs, largeScanned] = measure(40_000)
        const fullRecompute = (() => {
            const data = Array.from({length: 40_000}, (_, i) => ({i}))
            const t0 = performance.now(); new Set(data); return performance.now() - t0
        })()
        console.log(`[F8] toSet x replaceData: n=10000 ${smallMs.toFixed(1)}ms (${smallScanned} elements scanned), `
            + `n=40000 ${largeMs.toFixed(1)}ms (${largeScanned} scanned), time ratio ${(largeMs / smallMs).toFixed(1)}; `
            + `new Set(40000 rows) ${fullRecompute.toFixed(2)}ms`)
        // a linear patch may look at each source element a constant number of times
        expect(largeScanned).toBeLessThanOrEqual(4 * 40_000)
    })
})

// ---------------------------------------------------------------------------
// Below the cut: confirmed, lower impact
// ---------------------------------------------------------------------------
describe('minor', () => {
    test('M1 AsyncRxSlice.fetch(): a getRemoteData that throws synchronously leaves isLoading stuck (update() handles it)', async () => {
        const getRemoteData = (): Promise<number[]> => { throw new Error('offline') }

        const viaUpdate = new AsyncRxSlice<number>([], getRemoteData)
        await viaUpdate.update(0)
        expect(viaUpdate.isLoading.raw).toBe(false)
        expect(viaUpdate.loadError.raw).toBeInstanceOf(Error)

        const viaFetch = new AsyncRxSlice<number>([], getRemoteData)
        try { await viaFetch.fetch() } catch { /* sync throw surfaces here */ }
        expect(viaFetch.isLoading.raw).toBe(false)
    })

    test('M2 retained diagnostics report a leak after an async computed that read one atom twice is destroyed', async () => {
        enableData0RetainedObjectDiagnostics()
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
        try {
            const a = atom(1)
            const sum = computed(async () => (a() as number) + (a() as number))
            await new Promise(resolve => setTimeout(resolve, 5))
            destroyComputed(sum)
            expect(subscribers(a)).toHaveLength(0)     // really gone
            const {primitiveAtomDeps} = getData0RetainedObjectDiagnosticsSnapshot()
            expect(primitiveAtomDeps).toMatchObject({activeDeps: 0, activeEffects: 0})
        } finally {
            warn.mockRestore()
            disableData0RetainedObjectDiagnostics()
        }
    })

    test('M3 -0: incremental RxSet.toList() keeps -0 while the Set (and a list built from scratch) holds +0', () => {
        const s = new RxSet<number>([])
        const list = s.toList()
        s.add(-0)
        const scratch = s.toList()
        expect(Object.is(list.data[0], scratch.data[0])).toBe(true)
    })

    test('M3b -0: RxMap.keys()', () => {
        const m = new RxMap<number, string>([])
        const keys = m.keys()
        m.set(-0, 'zero')
        expect(Object.is(keys.data[0], [...m.data.keys()][0])).toBe(true)
    })

    test('M4 debug name of an object atom is registered on the inner function, not on the returned proxy', () => {
        expect(getDebugName(atom(1, undefined, 'count'))).toBe('count')        // primitive atom: ok
        expect(getDebugName(atom({x: 1}, undefined, 'point'))).toBe('point')   // object atom: undefined
    })
})
