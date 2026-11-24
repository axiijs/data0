/**
 * Regression tests for the fixes of the audit findings: branches of the fixes that the
 * minimal reproductions in audit-structs.spec.ts do not reach.
 */
import {afterEach, describe, expect, test, vi} from 'vitest'
import {
    atom, autorun, batch, computed, destroyComputed, getDebugName, notifier, onChange,
    AsyncRxSlice, RxList, RxMap, RxSet, RxTime,
} from '../src/index'

import {getComputedInternal} from '../src/computed'

const subscribers = (t: object) => [...(notifier.getDepEffects(t) ?? [])]

// ---------------------------------------------------------------------------
// F1  values()/entries() are patch lists replaying the map's METHOD infos
// ---------------------------------------------------------------------------
describe('F1 regression: values()/entries() replay the map infos in causal order', () => {
    const expectMirrors = (m: RxMap<any, any>, values: RxList<any>, entries: RxList<any>, ctx = '') => {
        expect(values.data, `values ${ctx}`).toEqual([...m.data.values()])
        expect(entries.data, `entries ${ctx}`).toEqual([...m.data.entries()])
    }

    test('F1e re-entrant delete from a structural subscriber that runs between keys() and the old row effect', () => {
        // fails on the audited tree AND with "METHOD before key trigger" reordering alone
        const m = new RxMap<string, {name: string}>([['x', {name: 'old'}]])
        const names = m.values().map(v => v.name)
        const stop = onChange(m, () => { if (m.data.get('x')?.name === 'new') m.delete('x') })
        expect(() => m.set('x', {name: 'new'})).not.toThrow()
        expect(names.data).toEqual([])
        stop()
    })

    test('exactly one downstream info per map operation, with the operation-time value', () => {
        const m = new RxMap<string, number>([['a', 1], ['b', 2]])
        const values = m.values()
        const entries = m.entries()
        const seen: string[] = []
        const stop = onChange(values, (infos: any[]) => {
            for (const i of infos) seen.push(i.method ? `${i.method}(${i.argv.join(',')})` : `set(${i.key},${i.newValue})`)
        })
        m.set('c', 3)       // add
        m.set('a', 10)      // update
        m.set('a', 10)      // same value: no info at all
        m.delete('b')       // remove
        m.delete('zzz')     // missing key: no info
        stop()
        expect(seen).toEqual(['splice(2,0,3)', 'set(0,10)', 'splice(1,1)'])
        expectMirrors(m, values, entries)
    })

    test('position bookkeeping: update after removals in front, FIFO eviction, re-add, tail churn', () => {
        const m = new RxMap<string, number>([['a', 1], ['b', 2], ['c', 3], ['d', 4], ['e', 5]])
        const values = m.values()
        const entries = m.entries()
        m.set('e', 50); expectMirrors(m, values, entries, 'exact hint')
        m.delete('c'); m.set('e', 51); expectMirrors(m, values, entries, 'one removal in front (not at the head)')
        m.delete('a'); m.set('d', 40); m.set('e', 52); expectMirrors(m, values, entries, 'head removal')
        m.delete('b'); m.delete('d'); m.set('e', 53); expectMirrors(m, values, entries, 'several head removals')
        m.set('a', 100); m.set('f', 6); m.set('a', 101); expectMirrors(m, values, entries, 're-added key goes to the end')
        m.delete('f'); m.set('g', 7); m.delete('g'); m.set('e', 54); m.set('a', 102); expectMirrors(m, values, entries, 'tail churn')
        m.delete('e'); m.delete('a'); expectMirrors(m, values, entries, 'emptied')
        expect(values.data).toEqual([])
        m.set('z', 1); m.set('z', 2); expectMirrors(m, values, entries, 'refilled')
    })

    test('NaN / undefined / object keys: update and delete are located by SameValueZero', () => {
        const o = {}
        const m = new RxMap<any, number>([[NaN, 1], [undefined, 2], [o, 3], ['k', 4]])
        const values = m.values()
        const entries = m.entries()
        m.set(NaN, 10); m.set(undefined, 20); m.set(o, 30); expectMirrors(m, values, entries, 'update')
        m.delete(undefined); m.set(o, 31); m.delete(NaN); m.set('k', 41); expectMirrors(m, values, entries, 'delete')
        m.set(NaN, 11); m.set(NaN, 12); expectMirrors(m, values, entries, 're-add NaN')
    })

    test('multi-info replay in one batch and the clear/replace fallbacks', () => {
        const m = new RxMap<string, number>([['a', 1], ['b', 2], ['c', 3]])
        const values = m.values()
        const entries = m.entries()
        batch(() => { m.set('d', 4); m.set('a', 10); m.delete('b'); m.set('b', 20); m.delete('a'); m.set('d', 40) })
        expectMirrors(m, values, entries, 'batch')
        batch(() => { m.set('x', 1); m.clear(); m.set('y', 2); m.set('y', 3) })
        expectMirrors(m, values, entries, 'clear inside batch')
        m.replace([['p', 1], ['y', 9], ['q', 2]])
        expectMirrors(m, values, entries, 'replace')
        m.set('y', 10); m.delete('p'); m.set('q', 3)
        expectMirrors(m, values, entries, 'incremental again after the fallback')
    })

    test('an entry row is a fresh [key, value] tuple on every value update', () => {
        const m = new RxMap<string, number>([['a', 1]])
        const entries = m.entries()
        const before = entries.data[0]
        m.set('a', 2)
        expect(entries.data[0]).toEqual(['a', 2])
        expect(entries.data[0]).not.toBe(before)
        expect(before).toEqual(['a', 1])
    })

    test('first read inside a batch, after a removal the keys() list has not replayed yet, is already consistent', () => {
        const m = new RxMap<string, {name: string}>([['a', {name: 'A'}], ['b', {name: 'B'}]])
        m.keys()
        let inside: unknown
        batch(() => {
            m.delete('a')
            inside = m.values().data.slice()
        })
        expect(inside).toEqual([{name: 'B'}])
        expect(m.values().data).toEqual([{name: 'B'}])
    })

    test('no per-key effect: the number of subscribers of the map does not grow with its size', () => {
        const m = new RxMap<number, number>([])
        m.values(); m.entries()
        const base = subscribers(m).length
        for (let i = 0; i < 50; i++) m.set(i, i)
        expect(subscribers(m).length).toBe(base)
        m.destroy()
        expect(subscribers(m)).toHaveLength(0)
    })

    test('derived maps: indexBy/groupBy/toMap feed values() without phantom rows', () => {
        const list = new RxList([{id: 1, g: 'x'}, {id: 2, g: 'y'}, {id: 3, g: 'x'}])
        const byId = list.indexBy('id')
        const groups = list.groupBy(item => item.g)
        const ids = byId.values().map(item => item.id)
        const sizes = groups.values().map(group => group.data.length)
        const seenUndefined: unknown[] = []
        const stop = onChange(groups.values(), (infos: any[]) => {
            for (const i of infos) if (!i.method && i.newValue === undefined) seenUndefined.push(i.key)
        })
        expect(() => {
            list.splice(1, 1)                 // removes the only member of group y
            list.set(0, {id: 9, g: 'z'})      // indexBy: delete 1 + set 9; groupBy: x shrinks, z appears
            batch(() => { list.push({id: 4, g: 'y'}); list.shift(); list.pop() })
        }).not.toThrow()
        stop()
        expect(seenUndefined).toEqual([])
        expect(ids.data.slice().sort()).toEqual(list.data.map(item => item.id).sort())
        expect(byId.values().data).toEqual([...byId.data.values()])
        expect(groups.values().data).toEqual([...groups.data.values()])
        expect(sizes.data.length).toBe(groups.data.size)
    })
})

describe('F1 regression: seeded differential fuzz of the values()/entries() bookkeeping', () => {
    const mulberry32 = (seed: number) => {
        let a = seed >>> 0
        return () => {
            a |= 0; a = a + 0x6D2B79F5 | 0
            let t = Math.imul(a ^ a >>> 15, 1 | a)
            t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t
            return ((t ^ t >>> 14) >>> 0) / 4294967296
        }
    }
    // replays the infos a consumer receives on its own mirror (what a list host does)
    const mirrorOf = (list: RxList<any>) => {
        const mirror = list.data.slice()
        const stop = onChange(list, (infos: any[]) => {
            for (const info of infos) {
                if (info.method === 'splice') mirror.splice(info.argv[0], info.argv[1], ...info.argv.slice(2))
                else if (info.method === undefined) mirror[info.key] = info.newValue
                else throw new Error(`unexpected info ${info.method}`)
            }
        })
        return {mirror, stop}
    }
    for (let seed = 1; seed <= 30; seed++) {
        test(`seed=${seed}`, () => {
            const rand = mulberry32(seed * 7919)
            const keySpace = 8 + Math.floor(rand() * 40)
            const key = (): any => { const k = Math.floor(rand() * keySpace); return k === 0 ? NaN : k === 1 ? undefined : k }
            let stamp = 0
            const m = new RxMap<any, number>([])
            const values = m.values(), entries = m.entries(), keys = m.keys()
            const valuesMirror = mirrorOf(values), entriesMirror = mirrorOf(entries)
            // incrementality witness: set/delete must never fall back to a full recompute
            let fullRecomputes = 0
            values.on('fullRecompute', () => fullRecomputes++)
            entries.on('fullRecompute', () => fullRecomputes++)
            let fallbackAllowed = false
            const history: string[] = []
            const op = () => {
                const r = rand()
                const live = [...m.data.keys()]
                if (r < 0.30) { const k = key(); history.push(`set(${String(k)})`); m.set(k, ++stamp) }
                else if (r < 0.45 && live.length) { const k = live[Math.floor(rand() * live.length)]; history.push(`update(${String(k)})`); m.set(k, ++stamp) }
                else if (r < 0.60 && live.length) { history.push('deleteOldest'); m.delete(live[0]) }
                else if (r < 0.70 && live.length) { history.push('deleteNewest'); m.delete(live[live.length - 1]) }
                else if (r < 0.90 && live.length) { const k = live[Math.floor(rand() * live.length)]; history.push(`delete(${String(k)})`); m.delete(k) }
                else if (r < 0.95) { const e = Array.from({length: Math.floor(rand() * 6)}, () => [key(), ++stamp] as [any, number]); history.push('replace'); fallbackAllowed = true; m.replace(e) }
                else if (r < 0.97) { history.push('clear'); fallbackAllowed = true; m.clear() }
                else { const k = key(); history.push(`delete?(${String(k)})`); m.delete(k) }
            }
            for (let step = 0; step < 400; step++) {
                fallbackAllowed = false
                const fullBefore = fullRecomputes
                if (rand() < 0.3) {
                    const n = 2 + Math.floor(rand() * 4)
                    history.push('batch{')
                    batch(() => { for (let i = 0; i < n; i++) op() })
                    history.push('}')
                } else op()
                const ctx = `seed=${seed} step=${step} recent=${history.slice(-10).join(';')}`
                if (!fallbackAllowed) expect(fullRecomputes, `set/delete stayed incremental ${ctx}`).toBe(fullBefore)
                const E = [...m.data.entries()]
                expect(values.data, `values ${ctx}`).toEqual(E.map(e => e[1]))
                expect(entries.data, `entries ${ctx}`).toEqual(E)
                expect(keys.data, `keys ${ctx}`).toEqual(E.map(e => e[0]))
                expect(valuesMirror.mirror, `values mirror ${ctx}`).toEqual(values.data)
                expect(entriesMirror.mirror, `entries mirror ${ctx}`).toEqual(entries.data)
                expect(values.data.includes(undefined as any), `phantom undefined ${ctx}`).toBe(false)
            }
            valuesMirror.stop(); entriesMirror.stop(); m.destroy()
        })
    }
})

// ---------------------------------------------------------------------------
// F2  indexBy/toMap: a transient duplicate key inside a multi-info replay
// ---------------------------------------------------------------------------
describe('F2 regression: indexBy/toMap fall back on a transient collision, still reject real duplicates', () => {
    const countFullRecomputes = (target: {on: Function}) => {
        const counter = {n: 0}
        target.on('fullRecompute', () => counter.n++)
        return counter
    }

    test('a REAL duplicate left in the final source still throws the same assert message (batch)', () => {
        const list = new RxList([{id: 1, v: 'a'}, {id: 2, v: 'b'}])
        const byId = list.indexBy('id')
        expect(() => batch(() => { list.push({id: 1, v: 'dup'}); list.push({id: 3, v: 'c'}) }))
            .toThrow('indexBy key is already exist')
        // recovery probe: once the source is valid again the next change rebuilds the map
        list.splice(2, 1)
        expect([...byId.data.keys()].sort()).toEqual([1, 2, 3])
        expect(byId.data.get(1)!.v).toBe('a')

        const pairs = new RxList<[string, number]>([['a', 1], ['b', 2]])
        const asMap = pairs.toMap()
        expect(() => batch(() => { pairs.push(['a', 9]); pairs.push(['c', 3]) }))
            .toThrow('toMap key is already exist')
        pairs.splice(2, 1)
        expect([...asMap.data.entries()].sort()).toEqual([['a', 1], ['b', 2], ['c', 3]])
    })

    test('single info (no batch): a duplicate is still rejected on the spot, splice and set entries', () => {
        const list = new RxList([{id: 1}, {id: 2}])
        list.indexBy('id')
        expect(() => list.push({id: 2})).toThrow('indexBy key is already exist')
        const list2 = new RxList([{id: 1}, {id: 2}])
        list2.indexBy('id')
        expect(() => list2.set(0, {id: 2})).toThrow('indexBy key is already exist')
        const pairs = new RxList<[string, number]>([['a', 1], ['b', 2]])
        pairs.toMap()
        expect(() => pairs.set(0, ['b', 9])).toThrow('toMap key is already exist')
    })

    test('two set() calls that swap keys inside a batch (EKC entry of the same class)', () => {
        const list = new RxList([{id: 1, v: 'a'}, {id: 2, v: 'b'}])
        const byId = list.indexBy('id')
        batch(() => { list.set(0, {id: 2, v: 'B'}); list.set(1, {id: 1, v: 'A'}) })
        expect(byId.data.get(1)!.v).toBe('A')
        expect(byId.data.get(2)!.v).toBe('B')
        expect(byId.data.size).toBe(2)

        const pairs = new RxList<[string, number]>([['a', 1], ['b', 2]])
        const asMap = pairs.toMap()
        batch(() => { pairs.set(0, ['b', 20]); pairs.set(1, ['a', 10]) })
        expect([...asMap.data.entries()].sort()).toEqual([['a', 10], ['b', 20]])
    })

    test('only the colliding replay falls back: delete-then-insert in a batch stays incremental', () => {
        const list = new RxList([{id: 1, v: 'old'}, {id: 2, v: 'x'}])
        const byId = list.indexBy('id')
        const full = countFullRecomputes(byId)
        batch(() => { list.shift(); list.push({id: 1, v: 'new'}) })       // remove first, then insert
        expect(full.n).toBe(0)
        expect(byId.data.get(1)!.v).toBe('new')
        batch(() => { list.push({id: 2, v: 'newer'}); list.shift() })     // insert first: transient duplicate
        expect(full.n).toBe(1)
        expect(byId.data.get(2)!.v).toBe('newer')
        expect(byId.data.size).toBe(2)
    })

    test('meta of the derived map follow the fallback', () => {
        const list = new RxList([{id: 1, v: 'old'}, {id: 2, v: 'x'}])
        const byId = list.indexBy('id')
        const keys = byId.keys(), values = byId.values(), size = byId.size
        batch(() => { list.push({id: 1, v: 'new'}); list.shift(); list.push({id: 3, v: 'y'}) })
        expect(keys.data).toEqual([...byId.data.keys()])
        expect(values.data).toEqual([...byId.data.values()])
        expect(size.raw).toBe(3)
        expect(values.data.map(item => item.v).sort()).toEqual(['new', 'x', 'y'])
    })
})

// ---------------------------------------------------------------------------
// F3 / F5  RxTime: at most one timer armed, none after destroy, no spinning
// ---------------------------------------------------------------------------
describe('F3/F5 regression: RxTime timer accounting (fake timers)', () => {
    afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers() })
    const T0 = 1_000_000

    test('future deadline: one timer while waiting, zero after the flip', () => {
        vi.useFakeTimers(); vi.setSystemTime(T0)
        const t = new RxTime()
        const expired = t.gt(T0 + 1000)
        expect(expired.raw).toBe(false)
        expect(vi.getTimerCount()).toBe(1)
        vi.advanceTimersByTime(1001)
        expect(expired.raw).toBe(false)          // woken at +1002 only
        expect(vi.getTimerCount()).toBe(1)
        vi.advanceTimersByTime(1)
        expect(expired.raw).toBe(true)
        expect(vi.getTimerCount()).toBe(0)
        expect((t as any).timeoutId).toBe(null)
        vi.advanceTimersByTime(100_000)
        expect(expired.raw).toBe(true)
        expect(vi.getTimerCount()).toBe(0)
        t.destroy()
    })

    test('operand change while waiting replaces the timer (never two)', () => {
        vi.useFakeTimers(); vi.setSystemTime(T0)
        const deadline = atom(T0 + 1000)
        const t = new RxTime()
        const expired = t.gt(deadline)
        expect(vi.getTimerCount()).toBe(1)
        deadline(T0 + 5000)
        expect(vi.getTimerCount()).toBe(1)
        deadline(T0 - 1)                          // already passed: nothing to wait for
        expect(expired.raw).toBe(true)
        expect(vi.getTimerCount()).toBe(0)
        deadline(T0 + 300)
        expect(expired.raw).toBe(false)
        expect(vi.getTimerCount()).toBe(1)
        vi.advanceTimersByTime(302)
        expect(expired.raw).toBe(true)
        expect(vi.getTimerCount()).toBe(0)
        t.destroy()
    })

    test('evaluated exactly at the crossing: one 2ms wake-up, then done (gt flips, lt stays)', () => {
        vi.useFakeTimers(); vi.setSystemTime(T0)
        const a = new RxTime(); const after = a.gt(T0)
        const b = new RxTime(); const before = b.lt(T0)
        expect([after.raw, before.raw]).toEqual([false, false])
        expect(vi.getTimerCount()).toBe(2)       // one per instance
        vi.advanceTimersByTime(2)
        expect([after.raw, before.raw]).toEqual([true, false])
        expect(vi.getTimerCount()).toBe(0)
        a.destroy(); b.destroy()
    })

    test('crossing already behind, or no crossing at all: no timer', () => {
        vi.useFakeTimers(); vi.setSystemTime(T0)
        const a = new RxTime(); expect(a.gt(T0 - 10).raw).toBe(true)
        const b = new RxTime(); expect(b.lt(T0 - 10).raw).toBe(false)
        const c = new RxTime(); const d = new RxTime()
        expect(c.gt(d.add(1000)).raw).toBe(false)      // coefficient cancels
        expect(vi.getTimerCount()).toBe(0)
        a.destroy(); b.destroy(); c.destroy(); d.destroy()
    })

    test('deadline beyond the setTimeout range: clamped wake-ups, one timer at a time, flips on time', () => {
        vi.useFakeTimers(); vi.setSystemTime(T0)
        const MAX = 2 ** 31 - 1
        const far = T0 + 60 * 24 * 3600 * 1000     // 60 days: needs three wake-ups
        const t = new RxTime()
        const expired = t.gt(far)
        expect(vi.getTimerCount()).toBe(1)
        vi.advanceTimersByTime(10)                 // an overflowing delay would have fired by now
        expect(vi.getTimerCount()).toBe(1)
        vi.advanceTimersByTime(MAX)                // first clamped wake-up: re-armed
        expect(expired.raw).toBe(false)
        expect(vi.getTimerCount()).toBe(1)
        vi.advanceTimersByTime(MAX)
        expect(expired.raw).toBe(false)
        expect(vi.getTimerCount()).toBe(1)
        vi.advanceTimersByTime(far - Date.now() + 2)
        expect(expired.raw).toBe(true)
        expect(vi.getTimerCount()).toBe(0)
        t.destroy()
    })

    test('clock stepped back: re-armed for the remaining time, one timer at a time', () => {
        vi.useFakeTimers(); vi.setSystemTime(T0)
        const t = new RxTime()
        const expired = t.gt(T0 + 1000)
        vi.setSystemTime(T0 - 5000)
        vi.advanceTimersByTime(1002)               // original wake-up: wall is T0 - 3998
        expect(expired.raw).toBe(false)
        expect(vi.getTimerCount()).toBe(1)
        vi.advanceTimersByTime(4998)               // wall T0 + 1000: not yet (strictly greater needed)
        expect(expired.raw).toBe(false)
        expect(vi.getTimerCount()).toBe(1)
        vi.advanceTimersByTime(2)
        expect(expired.raw).toBe(true)
        expect(vi.getTimerCount()).toBe(0)
        t.destroy()
    })

    test('clock standing still (timers faked, Date frozen): bounded back-off re-arms, then no timer', () => {
        vi.useFakeTimers({toFake: ['setTimeout', 'clearTimeout']})
        vi.spyOn(Date, 'now').mockReturnValue(T0)
        const a = new RxTime(); const later = a.gt(T0 + 1000)
        const b = new RxTime(); const boundary = b.gt(T0)
        expect(vi.getTimerCount()).toBe(2)
        vi.runAllTimers()                          // would abort with "infinite loop" if the wake-ups re-armed without bound
        expect(vi.getTimerCount()).toBe(0)
        expect([later.raw, boundary.raw]).toEqual([false, false])   // the clock never moved
        a.destroy(); b.destroy()
    })

    test('lt and a negative coefficient: flips to false at the crossing', () => {
        vi.useFakeTimers(); vi.setSystemTime(T0)
        const a = new RxTime(); const pending = a.lt(T0 + 500)
        const b = new RxTime(); const remaining = b.mul(-1).add(T0 + 500).gt(0)   // deadline - now > 0
        expect([pending.raw, remaining.raw]).toEqual([true, true])
        expect(vi.getTimerCount()).toBe(2)
        vi.advanceTimersByTime(502)
        expect([pending.raw, remaining.raw]).toEqual([false, false])
        expect(vi.getTimerCount()).toBe(0)
        a.destroy(); b.destroy()
    })

    test('destroy(): zero timers whatever the state, and the result never moves again', () => {
        vi.useFakeTimers(); vi.setSystemTime(T0)
        // (1) while waiting
        const deadline = atom(T0 + 1000)
        const t1 = new RxTime(); const r1 = t1.gt(deadline)
        t1.destroy(); t1.destroy()
        expect(vi.getTimerCount()).toBe(0)
        deadline(T0 + 10)                           // autorun stopped: no new timer
        expect(vi.getTimerCount()).toBe(0)
        // (2) from a subscriber while the evaluation publishes (F5), immediate and deferred subscribers
        const d2 = atom(T0 - 1000)
        const t2 = new RxTime(); const r2 = t2.gt(d2)
        const stop2 = autorun(() => { if (!r2()) t2.destroy() }, true)
        d2(T0 + 1000)
        expect(r2.raw).toBe(false)
        expect(vi.getTimerCount()).toBe(0)
        // (3) from a subscriber while the wake-up publishes
        const t3 = new RxTime(); const r3 = t3.lt(T0 + 100)     // true now, false after the crossing
        const stop3 = autorun(() => { if (!r3()) t3.destroy() }, true)
        expect(vi.getTimerCount()).toBe(1)
        vi.advanceTimersByTime(102)
        expect(r3.raw).toBe(false)
        expect(vi.getTimerCount()).toBe(0)
        // (4) resolve + subscribe on one instance
        const t4 = new RxTime(); const r4 = t4.gt(T0 + 50_000); t4.subscribe(100)
        expect(vi.getTimerCount()).toBe(2)          // one timeout + one interval
        t4.stopAutorun!()
        expect(vi.getTimerCount()).toBe(0)

        vi.advanceTimersByTime(200_000)
        expect([r1.raw, r2.raw, r3.raw, r4.raw]).toEqual([false, false, false, false])
        expect(vi.getTimerCount()).toBe(0)
        stop2(); stop3()
    })

    test('a subscriber that extends the deadline after the flip arms exactly one new timer', async () => {
        vi.useFakeTimers(); vi.setSystemTime(T0)
        const deadline = atom(T0 + 100)
        const t = new RxTime(); const expired = t.gt(deadline)
        let extensions = 0
        // default (microtask) scheduling: writing an operand synchronously from a subscriber of the
        // result is a sync rewrite cycle, which the core rejects by design (README 2)
        const stop = autorun(() => { if (expired() && extensions < 2) { extensions++; deadline(Date.now() + 100) } })
        for (let round = 1; round <= 2; round++) {
            vi.advanceTimersByTime(102)
            expect(expired.raw).toBe(true)
            expect(vi.getTimerCount()).toBe(0)
            await Promise.resolve(); await Promise.resolve()
            expect(extensions).toBe(round)
            expect(expired.raw).toBe(false)
            expect(vi.getTimerCount()).toBe(1)
        }
        vi.advanceTimersByTime(102)
        await Promise.resolve(); await Promise.resolve()
        expect(expired.raw).toBe(true)              // third time nobody extends
        expect(vi.getTimerCount()).toBe(0)
        stop(); t.destroy()
    })

    // Float corner: with coefficient 1.1 and constant -187 the value is already > 0 at t = 170 while the
    // computed crossing -constant/coefficient is 170 as well, so a wake-up landing exactly on 170 flips the
    // result AND still looks "not past the crossing". Subscribers run inside that write; the continuation
    // of the wake-up must not re-arm on behalf of a destroyed instance or of a superseded evaluation.
    test('wake-up that flips exactly on the computed crossing: no re-arm after a destroy from a subscriber', () => {
        expect(170 * 1.1 - 187 > 0 && 170 <= 187 / 1.1).toBe(true)     // the corner this test stands on
        vi.useFakeTimers(); vi.setSystemTime(100)
        const t = new RxTime()
        const passed = t.mul(1.1).gt(187)
        const stop = autorun(() => { if (passed()) t.destroy() }, true)
        expect(passed.raw).toBe(false)
        expect(vi.getTimerCount()).toBe(1)          // due at wall 172
        vi.setSystemTime(98)                        // 2ms back: the wake-up lands on wall 170
        vi.advanceTimersByTime(72)
        expect(Date.now()).toBe(170)
        expect(passed.raw).toBe(true)
        expect(vi.getTimerCount()).toBe(0)
        stop()
    })

    test('wake-up that flips exactly on the computed crossing: no re-arm with the parameters of a superseded evaluation', () => {
        vi.useFakeTimers(); vi.setSystemTime(100)
        const limit = atom(187)
        const t = new RxTime()
        const passed = t.mul(1.1).gt(limit)
        // moves the limit down from inside the wake-up; the result stays true, so no rewrite cycle
        const stop = autorun(() => { if (passed()) limit(110) }, true)
        vi.setSystemTime(98)
        vi.advanceTimersByTime(72)
        expect(Date.now()).toBe(170)
        expect(passed.raw).toBe(true)
        expect(limit.raw).toBe(110)
        expect(vi.getTimerCount()).toBe(0)          // new evaluation: crossing 100 is behind; old one must not re-arm
        stop(); t.destroy()
    })
})

// ---------------------------------------------------------------------------
// F6 / M1  AsyncRxSlice: destroyed instance, synchronous throw
// ---------------------------------------------------------------------------
describe('F6/M1 regression: AsyncRxSlice', () => {
    const tick = () => new Promise(resolve => setTimeout(resolve, 0))

    test('F6 destroyed before any fetch: fetch() starts nothing and resolves', async () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
        const page = atom(1)
        let remoteCalls = 0
        const slice = new AsyncRxSlice<number>([7], async () => { remoteCalls++; return [page()] })
        slice.destroy()
        await expect(slice.fetch()).resolves.toBeUndefined()
        expect(remoteCalls).toBe(0)
        expect(slice.autoFetchPromise).toBeUndefined()
        expect(slice.data).toEqual([7])
        expect(slice.isLoading.raw).toBe(false)
        expect(subscribers(page)).toHaveLength(0)
        warn.mockRestore()
    })

    test('F6 fetched, then destroyed: fetch() hands back the last promise, no new request, no refetch on param change', async () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
        const page = atom(1)
        let remoteCalls = 0
        const slice = new AsyncRxSlice<number>([], async () => { remoteCalls++; return [page()] })
        const first = slice.fetch()
        await first
        expect(slice.data).toEqual([1])
        slice.destroy()
        expect(slice.fetch()).toBe(first)
        page(2)
        await tick()
        expect(remoteCalls).toBe(1)
        expect(slice.data).toEqual([1])
        expect(subscribers(page)).toHaveLength(0)
        warn.mockRestore()
    })

    test('F6 update/append/prepend/moveForward/moveBackward on a destroyed slice: no request, control atoms untouched', async () => {
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {})
        let remoteCalls = 0
        const slice = new AsyncRxSlice<number>([1, 2], async () => { remoteCalls++; return [9] }, (item: number) => item)
        slice.destroy()
        const loadingStates: boolean[] = []
        const stop = autorun(() => { loadingStates.push(slice.isLoading()) }, true)
        await slice.update(0)
        await slice.append(); await slice.prepend(); await slice.moveForward(); await slice.moveBackward()
        stop()
        expect(remoteCalls).toBe(0)
        expect(loadingStates).toEqual([false])
        expect(slice.loadError.raw).toBe(null)
        expect(slice.data).toEqual([1, 2])
        warn.mockRestore()
    })

    test('a live slice is unaffected: fetch/refetch/update still work', async () => {
        const page = atom(1)
        const slice = new AsyncRxSlice<number>([], async (cursor?: number) => cursor === undefined ? [page()] : [cursor + 100], (item: number) => item)
        await slice.fetch()
        expect(slice.data).toEqual([1])
        page(2)
        await slice.fetch()
        expect(slice.data).toEqual([2])
        await slice.append()
        expect(slice.data).toEqual([2, 102])
        expect(slice.isLoading.raw).toBe(false)
        slice.destroy()
    })

    test('M1 synchronous throw in fetch(): recorded like a rejection, and the reactive params still trigger a retry', async () => {
        const page = atom(1)
        const slice = new AsyncRxSlice<number>([], () => {
            if (page() === 1) throw new Error('offline')
            return Promise.resolve([page()])
        })
        await expect(slice.fetch()).resolves.toBeUndefined()      // same contract as a rejected request
        expect(slice.isLoading.raw).toBe(false)
        expect(slice.loadError.raw?.message).toBe('offline')
        expect(slice.data).toEqual([])
        page(2)                                                    // dependency read before the throw
        await slice.fetch()
        expect(slice.loadError.raw).toBe(null)
        expect(slice.isLoading.raw).toBe(false)
        expect(slice.data).toEqual([2])
        slice.destroy()
    })

    test('M1 a stale synchronous failure does not clobber a newer request', async () => {
        const page = atom(1)
        let resolveSecond!: (rows: number[]) => void
        const slice = new AsyncRxSlice<number>([], () => {
            if (page() === 1) throw new Error('offline')
            return new Promise<number[]>(resolve => { resolveSecond = resolve })
        })
        slice.fetch()                 // request 1: throws synchronously (handled a microtask later)
        page(2)                       // request 2 starts before request 1's failure is processed
        await tick()
        expect(slice.isLoading.raw).toBe(true)        // request 2 still in flight
        expect(slice.loadError.raw).toBe(null)        // request 1's error is stale
        resolveSecond([2])
        await tick()
        expect(slice.data).toEqual([2])
        expect(slice.isLoading.raw).toBe(false)
        slice.destroy()
    })
})

// ---------------------------------------------------------------------------
// F7  cached meta: a consumer may destroy a meta, the next read rebuilds it
// ---------------------------------------------------------------------------
describe('F7 regression: every cached meta is rebuilt after a consumer destroyed it', () => {
    type Meta = {
        name: string
        // builds a host, returns accessors for one meta of it
        make: () => {
            host: {active: boolean, destroy: () => void}
            read: () => any                       // the meta (list or atom)
            mutate: () => void                    // one source mutation
            value: (meta: any) => unknown         // current content of a meta instance
            expected: () => unknown               // what a live meta must show
        }
    }
    const isActive = (meta: any): boolean => meta instanceof RxList ? meta.active : getComputedInternal(meta)!.active
    const destroyMeta = (meta: any) => meta instanceof RxList ? meta.destroy() : destroyComputed(meta)
    const listValue = (meta: RxList<any>) => meta.data.slice()
    const atomValue = (meta: any) => meta.raw

    const metas: Meta[] = [
        {name: 'RxMap.keys()', make: () => { let n = 0; const m = new RxMap<string, number>([['a', 1]]); return {host: m, read: () => m.keys(), mutate: () => m.set(`k${n++}`, n), value: listValue, expected: () => [...m.data.keys()]} }},
        {name: 'RxMap.values()', make: () => { let n = 0; const m = new RxMap<string, number>([['a', 1]]); return {host: m, read: () => m.values(), mutate: () => m.set(`k${n++}`, n), value: listValue, expected: () => [...m.data.values()]} }},
        {name: 'RxMap.entries()', make: () => { let n = 0; const m = new RxMap<string, number>([['a', 1]]); return {host: m, read: () => m.entries(), mutate: () => m.set(`k${n++}`, n), value: listValue, expected: () => [...m.data.entries()]} }},
        {name: 'RxMap.size', make: () => { let n = 0; const m = new RxMap<string, number>([['a', 1]]); return {host: m, read: () => m.size, mutate: () => m.set(`k${n++}`, n), value: atomValue, expected: () => m.data.size} }},
        {name: 'RxSet.size', make: () => { let n = 10; const s = new RxSet<number>([1]); return {host: s, read: () => s.size, mutate: () => s.add(n++), value: atomValue, expected: () => s.data.size} }},
        {name: 'RxList.length', make: () => { let n = 10; const l = new RxList<number>([1]); return {host: l, read: () => l.length, mutate: () => l.push(n++), value: atomValue, expected: () => l.data.length} }},
    ]

    for (const {name, make} of metas) {
        test(`${name}: destroyed by a consumer -> rebuilt live on the next read; the old instance stays a dead snapshot`, () => {
            const {host, read, mutate, value, expected} = make()
            const first = read()
            expect(read()).toBe(first)                       // cached while alive
            const snapshot = value(first)
            destroyMeta(first)
            mutate()
            const second = read()
            expect(second).not.toBe(first)
            expect(isActive(second)).toBe(true)
            expect(value(second)).toEqual(expected())
            expect(read()).toBe(second)                      // cached again
            mutate()
            expect(value(second)).toEqual(expected())        // and it keeps following the host
            expect(isActive(first)).toBe(false)
            expect(value(first)).toEqual(snapshot)           // the consumer's instance is not resurrected
            host.destroy()
            expect(isActive(second)).toBe(false)             // the rebuilt one belongs to the host
            expect(subscribers(host)).toHaveLength(0)
        })

        test(`${name}: host destroyed -> reads keep returning one dead snapshot, also when a consumer destroyed the meta first`, () => {
            const a = make()
            const metaA = a.read()
            a.host.destroy()
            expect(a.read()).toBe(metaA)
            expect(isActive(metaA)).toBe(false)

            const b = make()
            const metaB = b.read()
            destroyMeta(metaB)
            b.host.destroy()                                 // cached instance already dead: destroy stays idempotent
            const afterDestroy = b.read()
            expect(b.read()).toBe(afterDestroy)
            expect(isActive(afterDestroy)).toBe(false)
            expect(b.value(afterDestroy)).toEqual(b.expected())
            expect(subscribers(b.host)).toHaveLength(0)

            const c = make()                                 // never read before the destroy
            c.host.destroy()
            const late = c.read()
            expect(c.read()).toBe(late)
            expect(isActive(late)).toBe(false)
            expect(subscribers(c.host)).toHaveLength(0)
        })
    }

    test('the four RxMap meta are independent: destroying keys() leaves values()/entries()/size alive', () => {
        const m = new RxMap<string, number>([['a', 1]])
        const keys = m.keys(), values = m.values(), entries = m.entries(), size = m.size
        keys.destroy()
        m.set('b', 2)
        expect(values.data).toEqual([1, 2])
        expect(entries.data).toEqual([['a', 1], ['b', 2]])
        expect(size.raw).toBe(2)
        expect(m.keys().data).toEqual(['a', 'b'])
        expect(m.values()).toBe(values)
        m.destroy()
    })

    test('a meta created inside a reactive scope is still not collected by that scope', () => {
        const m = new RxMap<string, number>([['a', 1]])
        const tick = atom(0)
        const stop = autorun(() => { tick(); m.keys(); m.values(); m.size }, true)
        const keys = m.keys()
        tick(1); tick(2)
        expect(m.keys()).toBe(keys)
        expect(keys.active).toBe(true)
        stop()
        m.set('b', 2)
        expect(keys.data).toEqual(['a', 'b'])
        m.destroy()
    })
})

// ---------------------------------------------------------------------------
// F8  toSet: bulk removal in one pass, small removals unchanged
// ---------------------------------------------------------------------------
describe('F8 regression: toSet removal cost and correctness', () => {
    const median = (xs: number[]) => xs.slice().sort((a, b) => a - b)[Math.floor(xs.length / 2)]
    // counts the elements visited by linear membership scans (Array#includes) over the source array
    const instrument = (list: RxList<any>) => {
        const counter = {scanned: 0}
        ;(list.data as any).includes = function (this: unknown[], x: unknown) {
            counter.scanned += this.length
            return Array.prototype.includes.call(this, x)
        }
        return counter
    }

    test('timings: replaceData(n fresh rows) at 10 000 and 40 000, and few removals on 40 000 rows', () => {
        const bulk = (n: number) => {
            const runs: number[] = []
            let scanned = 0
            for (let r = 0; r < 5; r++) {
                const list = new RxList<object>(Array.from({length: n}, (_, i) => ({i})))
                const set = list.toSet()
                const counter = instrument(list)
                const fresh = Array.from({length: n}, (_, i) => ({i}))
                const t0 = performance.now()
                list.replaceData(fresh)
                runs.push(performance.now() - t0)
                scanned = counter.scanned
                expect(set.data.size).toBe(n)
                set.destroy(); list.destroy()
            }
            return {ms: median(runs), scanned}
        }
        // `rounds` splices removing k rows each from a 40 000-row list
        const few = (k: number, rounds: number) => {
            const runs: number[] = []
            let scanned = 0, expectedScanned = 0
            for (let r = 0; r < 5; r++) {
                const n = 40_000
                const list = new RxList<object>(Array.from({length: n}, (_, i) => ({i})))
                const set = list.toSet()
                const counter = instrument(list)
                expectedScanned = 0
                const t0 = performance.now()
                for (let i = 0; i < rounds; i++) {
                    list.splice((i * 7919) % (list.data.length - k), k)
                    expectedScanned += k * list.data.length
                }
                runs.push(performance.now() - t0)
                scanned = counter.scanned
                expect(set.data.size).toBe(list.data.length)
                set.destroy(); list.destroy()
            }
            return {ms: median(runs), scanned, expectedScanned}
        }
        bulk(2_000); few(1, 50)     // warm-up
        const b10 = bulk(10_000), b40 = bulk(40_000)
        const f1 = few(1, 500), f8 = few(8, 200), f64 = few(64, 100), f127 = few(127, 100), f128 = few(128, 100), f500 = few(500, 40)
        console.log(`[F8] replaceData with toSet(): n=10000 ${b10.ms.toFixed(1)}ms, n=40000 ${b40.ms.toFixed(1)}ms (ratio ${(b40.ms / b10.ms).toFixed(1)}), `
            + `elements scanned ${b10.scanned} / ${b40.scanned}`)
        console.log(`[F8] few removals on 40000 rows (ms per splice): k=1 ${(f1.ms / 500).toFixed(4)}, k=8 ${(f8.ms / 200).toFixed(4)}, `
            + `k=64 ${(f64.ms / 100).toFixed(4)}, k=127 ${(f127.ms / 100).toFixed(4)}, k=128 ${(f128.ms / 100).toFixed(4)}, k=500 ${(f500.ms / 40).toFixed(4)}`)
        // bulk: no per-removed-row scan of the source any more
        expect(b40.scanned).toBeLessThanOrEqual(4 * 40_000)
        // small removals: exactly the scans the previous implementation did (same code path, cannot be slower)
        expect(f1.scanned).toBe(f1.expectedScanned)
        expect(f8.scanned).toBe(f8.expectedScanned)
        expect(f64.scanned).toBe(f64.expectedScanned)
        expect(f127.scanned).toBe(f127.expectedScanned)
        // from the bulk threshold (128 removed rows) on, one pass
        expect(f128.scanned).toBe(0)
        expect(f500.scanned).toBe(0)
    })

    test('bulk path keeps a removed value that still occurs elsewhere in the source (duplicates, NaN, -0, undefined)', () => {
        const values: any[] = []
        for (let i = 0; i < 300; i++) values.push(i % 50)                  // every value six times
        values.push(NaN, NaN, -0, 0, undefined, undefined, 'tail')
        const list = new RxList<any>(values.slice())
        const set = list.toSet()
        const same = () => expect([...set.data].map(String).sort()).toEqual([...new Set(list.data)].map(String).sort())
        list.splice(0, 150); same()                    // 0..49 three times: all still present three times
        list.splice(0, 140); same()                    // leaves 40..49 once (bulk path, 140 >= 128)
        expect(set.data.has(39)).toBe(false)
        expect(set.data.has(40)).toBe(true)
        list.splice(0, list.data.length - 4, ...Array.from({length: 200}, (_, i) => 1000 + i)); same()  // keeps 0/undef/undef/tail
        list.splice(0, 150); same()                    // bulk removal of 150 of the 200 fresh values
        expect(set.data.has(NaN)).toBe(false)
        expect(set.data.has(0)).toBe(true)
        expect(set.data.has(undefined)).toBe(true)
        list.replaceData(Array.from({length: 200}, (_, i) => (i % 2 ? NaN : -0))); same()
        expect([...set.data].length).toBe(2)
        list.clear(); same()
        set.destroy(); list.destroy()
    })

    test('bulk path inside a multi-info batch is evaluated against the final source', () => {
        const list = new RxList<number>(Array.from({length: 400}, (_, i) => i))
        const set = list.toSet()
        batch(() => {
            list.splice(0, 200)                 // removes 0..199
            list.push(5, 6, 7)                  // three of them come back
            list.splice(0, 150)                 // removes 200..349
            list.unshift(210)
        })
        expect([...set.data].sort((a, b) => a - b)).toEqual([...new Set(list.data)].sort((a, b) => a - b))
        expect(set.data.has(5)).toBe(true)
        expect(set.data.has(210)).toBe(true)
        expect(set.data.has(4)).toBe(false)
        set.destroy(); list.destroy()
    })
})

// ---------------------------------------------------------------------------
// M3  -0 members / keys are normalised at the write, like the native Set / Map
// ---------------------------------------------------------------------------
describe('M3 regression: -0 and +0 are one member / one key everywhere', () => {
    const isPlusZero = (x: unknown) => Object.is(x, 0)
    // trigger payloads carry what the caller passed (AGENTS: argv is an external protocol, internal
    // consumers normalize); the incremental structures hold the stored +0
    const zeroSign = (x: unknown) => Object.is(x, -0) ? '-0' : String(x)

    test('RxSet: add / has / delete with -0 and +0, incremental structures hold the stored +0, payloads stay raw', () => {
        const s = new RxSet<number>([])
        const other = new RxSet<number>([5])
        const list = s.toList()
        const union = s.union(other)
        const hasZero = s.has(0)
        const hasMinusZero = s.has(-0)
        const payloads: unknown[] = []
        const stop = onChange(s, (infos: any[]) => { for (const i of infos) payloads.push(...i.argv) })

        s.add(-0)
        expect(s.data.size).toBe(1)
        expect(isPlusZero([...s.data][0])).toBe(true)
        expect(isPlusZero(list.data[0])).toBe(true)
        expect([...union.data].some(isPlusZero)).toBe(true)
        expect([hasZero.raw, hasMinusZero.raw]).toEqual([true, true])
        s.add(0); s.add(-0)                               // already there: no second member, no info
        expect(s.data.size).toBe(1)
        expect(list.data.length).toBe(1)

        s.delete(-0)                                      // deletes the stored +0
        expect(s.data.size).toBe(0)
        expect(list.data).toEqual([])
        expect([...union.data]).toEqual([5])
        expect([hasZero.raw, hasMinusZero.raw]).toEqual([false, false])

        s.add(0); s.delete(0); s.add(-0); s.delete(0)
        expect(s.data.size).toBe(0)
        expect(list.data).toEqual([])
        stop()
        // add(-0), delete(-0), add(0), delete(0), add(-0), delete(0): the payload is the caller's argument
        expect(payloads.map(zeroSign)).toEqual(['-0', '-0', '0', '0', '-0', '0'])
        destroyComputed(hasZero); destroyComputed(hasMinusZero)
    })

    test('RxSet.replace / constructor with -0', () => {
        const s = new RxSet<number>([-0, 1])
        const list = s.toList()
        expect(isPlusZero(list.data[0])).toBe(true)
        const [added, removed] = s.replace([-0, 2])
        expect(added).toEqual([2])
        expect(removed).toEqual([1])                      // the zero stays
        expect(list.data.some(isPlusZero)).toBe(true)
        expect(list.data.length).toBe(2)
    })

    test('RxMap: set / get / delete with -0 and +0 keys, keys()/entries() hold the stored +0, payloads stay raw', () => {
        const m = new RxMap<number, string>([])
        const keys = m.keys(), entries = m.entries(), values = m.values()
        const viaMinusZero = computed(() => m.get(-0))
        const viaZero = computed(() => m.get(0))
        const payloadKeys: unknown[] = []
        const stop = onChange(m, (infos: any[]) => { for (const i of infos) payloadKeys.push(i.argv[0]) })

        m.set(-0, 'a')
        expect(isPlusZero([...m.data.keys()][0])).toBe(true)
        expect(isPlusZero(keys.data[0])).toBe(true)
        expect(isPlusZero(entries.data[0][0])).toBe(true)
        expect([m.get(-0), m.get(0), viaMinusZero.raw, viaZero.raw]).toEqual(['a', 'a', 'a', 'a'])

        m.set(0, 'b')                                     // same entry
        expect(m.data.size).toBe(1)
        expect([keys.data.length, values.data[0], viaMinusZero.raw, viaZero.raw]).toEqual([1, 'b', 'b', 'b'])
        m.set(-0, 'c')
        expect([m.data.size, values.data[0], viaMinusZero.raw, viaZero.raw]).toEqual([1, 'c', 'c', 'c'])

        m.delete(-0)
        expect(m.data.size).toBe(0)
        expect([keys.data, values.data, entries.data]).toEqual([[], [], []])
        expect([viaMinusZero.raw, viaZero.raw]).toEqual([undefined, undefined])
        m.set(0, 'd'); m.delete(0); m.set(-0, 'e'); m.delete(0)
        expect([keys.data, values.data]).toEqual([[], []])
        stop()
        // set(-0) set(0) set(-0) delete(-0) set(0) delete(0) set(-0) delete(0): raw caller arguments
        expect(payloadKeys.map(zeroSign)).toEqual(['-0', '0', '-0', '-0', '0', '0', '-0', '0'])
        destroyComputed(viaMinusZero); destroyComputed(viaZero)
    })

    test('a -0 VALUE in an RxMap is untouched (0 -> -0 is an observable change)', () => {
        const m = new RxMap<string, number>([['k', 0]])
        const values = m.values()
        m.set('k', -0)
        expect(Object.is(m.data.get('k'), -0)).toBe(true)
        expect(Object.is(values.data[0], -0)).toBe(true)
    })

    test('derived structures fed with -0: toSet().toList(), groupBy keys, indexBy keys', () => {
        const list = new RxList<number>([1])
        const members = list.toSet().toList()
        const groupKeys = list.groupBy(x => x).keys()
        const indexKeys = list.map(x => ({id: x})).indexBy('id').keys()
        list.push(-0)
        for (const derived of [members, groupKeys, indexKeys]) {
            expect(derived.data.length).toBe(2)
            expect(derived.data.some(isPlusZero)).toBe(true)
            expect(derived.data.some(x => Object.is(x, -0))).toBe(false)
        }
        list.pop()
        for (const derived of [members, groupKeys, indexKeys]) expect(derived.data).toEqual([1])
    })
})

// ---------------------------------------------------------------------------
// M4  debug name of object atoms
// ---------------------------------------------------------------------------
describe('M4 regression: debug names are registered on the atom the caller holds', () => {
    test('every atom shape and both naming entries', () => {
        expect(getDebugName(atom(1, undefined, 'count'))).toBe('count')
        expect(getDebugName(atom({x: 1}, undefined, 'point'))).toBe('point')
        expect(getDebugName(atom([1, 2], undefined, 'pair'))).toBe('pair')
        expect(getDebugName((atom as any).as.total(1))).toBe('total')
        expect(getDebugName((atom as any).as.user({name: 'Ann'}))).toBe('user')
        expect(getDebugName(atom({x: 1}))).toBeUndefined()
        expect(getDebugName(atom(1))).toBeUndefined()
    })

    test('naming does not change how an object atom behaves', () => {
        const point = atom({x: 1, y: 2}, undefined, 'point')
        const sum = computed(() => (point as any).x + (point as any).y)
        expect(sum.raw).toBe(3)
        ;(point as any).x = 10
        expect(sum.raw).toBe(12)
        point({x: 0, y: 0})
        expect(sum.raw).toBe(0)
        expect(getDebugName(point)).toBe('point')
        destroyComputed(sum)
    })
})
