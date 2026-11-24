/**
 * Regression tests for the review of the collection fixes (PR #24, commit 6b52f72 follow-ups).
 * Each describe block belongs to one root cause; every test failed on the tree before its fix.
 */
import {afterEach, describe, expect, test, vi} from 'vitest'
import {AsyncRxSlice, atom, batch, onChange, RxList, RxMap, RxSet, RxTime} from '../src/index'
import {Computed, getComputedInternal} from '../src/computed.js'
import {ITERATE_KEY, notifier} from '../src/notify.js'
import {TrackOpTypes} from '../src/operations.js'

const subscribersOf = (target: object) => [...(notifier.getDepEffects(target) ?? [])].length
const zeroSign = (x: unknown) => Object.is(x, -0) ? '-0' : String(x)
const sameSeq = (a: readonly unknown[], b: readonly unknown[]) => a.length === b.length && a.every((x, i) => Object.is(x, b[i]))

// ---------------------------------------------------------------------------
// -0: trigger payloads / argv stay what the caller passed (external protocol, AGENTS: internal consumers
// normalize); the incremental consumers that rebuild a key sequence from the payload normalize themselves
// ---------------------------------------------------------------------------
describe('-0 keys: raw payloads, normalizing internal consumers', () => {
    const addKeyOf = (write: (m: RxMap<number, string>) => void) => {
        const m = new RxMap<number, string>([])
        let key: unknown = 'none'
        const c = new Computed(function (this: Computed) { this.manualTrack(m, TrackOpTypes.ITERATE, ITERATE_KEY) },
            function (_d: unknown, infos: any[]) { for (const i of infos) if (i.type === 'add') key = i.key })
        c.run([], true)
        write(m)
        c.destroy()
        return zeroSign(key)
    }

    test('RxMap.set / delete and RxSet.add / delete: argv carries the raw -0', () => {
        const m = new RxMap<number, string>([])
        const s = new RxSet<number>([])
        const argv: string[] = []
        const stopM = onChange(m, (infos: any[]) => { for (const i of infos) argv.push(`map.${i.method}(${zeroSign(i.argv[0])})`) })
        const stopS = onChange(s, (infos: any[]) => { for (const i of infos) argv.push(`set.${i.method}(${zeroSign(i.argv[0])})`) })
        m.set(-0, 'a'); m.delete(-0)
        s.add(-0); s.delete(-0)
        stopM(); stopS()
        expect(argv).toEqual(['map.set(-0)', 'map.delete(-0)', 'set.add(-0)', 'set.delete(-0)'])
    })

    test('set() and replace() / replaceData() emit the same ADD key for -0', () => {
        expect([addKeyOf(m => m.set(-0, 'v')), addKeyOf(m => m.replace([[-0, 'v']])), addKeyOf(m => m.replaceData([[-0, 'v']]))])
            .toEqual(['-0', '-0', '-0'])
    })

    test('consumers rebuilt from raw payloads hold the stored +0, directly and through derived maps / sets, in and out of batch', () => {
        const m = new RxMap<number, string>([[1, 'one']])
        const keys = m.keys(), entries = m.entries()
        const s = new RxSet<number>([1])
        const members = s.toList()
        const unionMembers = s.union(new RxSet<number>([5])).toList()
        const list = new RxList<number>([1])
        const indexKeys = list.map(x => ({id: x})).indexBy('id').keys()
        const groupEntryKeys = list.groupBy(x => x).entries().map(([k]) => k)
        const setMembers = list.toSet().toList()

        m.set(-0, 'z'); s.add(-0); list.push(-0)
        batch(() => { m.delete(1); m.set(-0, 'z2'); m.set(2, 'two'); s.delete(1); s.add(2) })

        expect(sameSeq(keys.data, [...m.data.keys()])).toBe(true)
        expect(sameSeq(entries.data.map(e => e[0]), [...m.data.keys()])).toBe(true)
        expect(sameSeq(members.data, [...s.data])).toBe(true)
        expect(unionMembers.data.some(x => Object.is(x, -0))).toBe(false)
        for (const derived of [indexKeys, groupEntryKeys, setMembers]) {
            expect(derived.data.some(x => Object.is(x, -0)), 'derived key sequence holds -0').toBe(false)
            expect(derived.data.some(x => Object.is(x, 0))).toBe(true)
        }
    })
})

// ---------------------------------------------------------------------------
// RxTime x a Date.now() coarser than the +2 ms wake-up: the wake-up lands in the same clock tick, which the
// old "clock has not moved -> stop" guard read as a frozen clock, so gt(deadline) stayed false forever
// ---------------------------------------------------------------------------
describe('RxTime: wake-up inside the same Date.now() tick re-arms with a bounded back-off', () => {
    afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers() })
    const coarseClock = (resolution: number) => {
        const fakeNow = Date.now.bind(Date)   // the faked Date: advances with the fake timers
        vi.spyOn(Date, 'now').mockImplementation(() => Math.floor(fakeNow() / resolution) * resolution)
    }

    test('100 ms clock: deadline(Date.now()) -> gt(deadline) turns true, then no timer is left', () => {
        vi.useFakeTimers(); vi.setSystemTime(2_000_050)
        coarseClock(100)
        const deadline = atom(9_999_999)
        const t = new RxTime()
        const expired = t.gt(deadline)
        deadline(Date.now())                   // "expire now", evaluated exactly at the crossing
        expect(expired.raw).toBe(false)
        vi.advanceTimersByTime(200)            // the next 100 ms tick is reached within the back-off
        expect({expired: expired.raw, pendingTimers: vi.getTimerCount()}).toEqual({expired: true, pendingTimers: 0})
        t.destroy()
    })

    test('100 ms clock: a 50 ms timeout gt(Date.now() + 50) turns true on the next tick', () => {
        vi.useFakeTimers(); vi.setSystemTime(2_000_010)
        coarseClock(100)
        const t = new RxTime()
        const expired = t.gt(Date.now() + 50)
        vi.advanceTimersByTime(60)             // first wake-up: still inside the same 100 ms tick
        expect(expired.raw).toBe(false)
        vi.advanceTimersByTime(200)
        expect({expired: expired.raw, pendingTimers: vi.getTimerCount()}).toEqual({expired: true, pendingTimers: 0})
        t.destroy()
    })

    test('frozen Date: at most 1 + 16 wake-ups, then no timer; destroy mid back-off leaves none', () => {
        vi.useFakeTimers({toFake: ['setTimeout', 'clearTimeout']})
        vi.spyOn(Date, 'now').mockReturnValue(2_000_000)
        const t = new RxTime()
        const expired = t.gt(2_000_000)
        let wakeUps = 0
        while (vi.getTimerCount() > 0 && wakeUps < 100) { vi.advanceTimersToNextTimer(); wakeUps++ }
        expect(wakeUps).toBe(17)
        expect(expired.raw).toBe(false)
        t.destroy()

        const t2 = new RxTime()
        t2.gt(2_000_000)
        vi.advanceTimersToNextTimer(); vi.advanceTimersToNextTimer()   // in the middle of the back-off
        expect(vi.getTimerCount()).toBe(1)
        t2.destroy()
        expect(vi.getTimerCount()).toBe(0)
    })
})

// ---------------------------------------------------------------------------
// destroyResources after a throwing user cleanup ("销毁必须做完", README §6): every remaining step still runs,
// the first error reaches the caller, later ones go to console.error. RxMap / RxSet / AsyncRxSlice used to
// run their steps in sequence, so one throwing meta left its siblings (and the base hook) undone.
// ---------------------------------------------------------------------------
describe('collection destroyResources finishes every step after a throwing cleanup', () => {
    const boom = (tag: string) => () => { throw new Error(`boom:${tag}`) }
    const destroyAndCatch = (destroy: () => void) => {
        try { destroy(); return 'no throw' } catch (e: any) { return e.message as string }
    }
    const teardownReports = (spy: ReturnType<typeof vi.spyOn>) =>
        spy.mock.calls.filter(c => c[0] === '[data0] uncaught error in teardown:').map(c => (c[1] as Error).message)

    test('RxMap: a throwing values() destroy listener does not keep entries()/keys()/size alive or skip context.onCleanup', () => {
        const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
        try {
            let cleaned = 0
            const m = new RxMap<string, number>(function (this: RxMap<string, number>, {onCleanup}: any) {
                onCleanup(() => { cleaned++ })
                return [['a', 1]]
            } as any)
            const values = m.values(), entries = m.entries(), keys = m.keys(), size = m.size
            values.on('destroy', boom('values'))
            keys.on('destroy', boom('keys'))
            expect(destroyAndCatch(() => m.destroy())).toBe('boom:values')
            expect([m.active, values.active, entries.active, keys.active, getComputedInternal(size)!.active])
                .toEqual([false, false, false, false, false])
            expect(cleaned).toBe(1)
            expect(teardownReports(errorSpy)).toEqual(['boom:keys'])
            expect(subscribersOf(m)).toBe(0)
        } finally {
            errorSpy.mockRestore()
        }
    })

    test('RxSet: a throwing size destroy listener does not skip the base hook (context.onCleanup)', () => {
        const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
        try {
            let cleaned = 0
            const s = new RxSet<number>(function (this: RxSet<number>, {onCleanup}: any) {
                onCleanup(() => { cleaned++ })
                return [1]
            } as any)
            const size = s.size
            getComputedInternal(size)!.on('destroy', boom('size'))
            expect(destroyAndCatch(() => s.destroy())).toBe('boom:size')
            expect([s.active, getComputedInternal(size)!.active]).toEqual([false, false])
            expect(cleaned).toBe(1)
        } finally {
            errorSpy.mockRestore()
        }
    })

    test('AsyncRxSlice: a throwing isLoading subscriber / autoFetch destroy listener does not keep the fetch computed or length alive', async () => {
        const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})
        try {
            const page = atom(1)
            let remoteCalls = 0
            const slice = new AsyncRxSlice<number>([], async () => { remoteCalls++; return [page()] })
            await slice.fetch()
            const length = slice.length
            const fetcher = getComputedInternal(slice.autoFetchPromise!)!
            fetcher.on('destroy', boom('autoFetch'))
            slice.isLoading(true)
            const stopLoadingWatcher = onChange(slice.isLoading, () => { throw new Error('boom:isLoading') })

            expect(destroyAndCatch(() => slice.destroy())).toBe('boom:isLoading')
            expect([slice.active, fetcher.active, getComputedInternal(length)!.active]).toEqual([false, false, false])
            expect(teardownReports(errorSpy)).toEqual(['boom:autoFetch'])
            const callsAtDestroy = remoteCalls
            page(2)                                   // the fetch computed no longer follows its parameters
            await new Promise(resolve => setTimeout(resolve, 0))
            expect(remoteCalls).toBe(callsAtDestroy)
            stopLoadingWatcher()
        } finally {
            errorSpy.mockRestore()
        }
    })
})

// ---------------------------------------------------------------------------
// values()/entries() position bookkeeping (createEntryList's key -> position hints) is bounded: a deleted key
// leaves no entry behind. Not observable through behaviour (stale hints are never looked up), so this is a
// real-GC reachability check: churn unique object keys through a long-lived map, delete them all, and every
// key object must be collectable. The GC is exposed through v8 flags inside the test (no CLI flag needed).
// ---------------------------------------------------------------------------
describe('values()/entries() bookkeeping keeps nothing for deleted keys (GC churn)', () => {
    const exposeGc = async (): Promise<() => void> => {
        const v8 = await import('node:v8')
        const vm = await import('node:vm')
        v8.setFlagsFromString('--expose-gc')
        return vm.runInNewContext('gc') as () => void
    }
    const settle = async (gc: () => void) => {
        for (let i = 0; i < 5; i++) {
            gc()
            await new Promise(resolve => setTimeout(resolve, 5))
        }
    }
    // allocation in its own stack frame: a same-frame local would be pinned by conservative stack scanning
    const churn = (m: RxMap<object, number>, rounds: number): WeakRef<object>[] => {
        const refs: WeakRef<object>[] = []
        for (let i = 0; i < rounds; i++) {
            const key = {i}
            m.set(key, i)
            m.set(key, i + 1)          // an update locates the key: the position hints now hold it
            m.delete(key)
            refs.push(new WeakRef(key))
        }
        return refs
    }
    const pinnedChurn = (m: RxMap<object, number>, pin: object[]): WeakRef<object>[] => {
        const key = {pinned: true}
        pin.push(key)                  // negative control: a key that is still referenced must stay alive
        m.set(key, 1); m.set(key, 2); m.delete(key)
        return [new WeakRef(key)]
    }

    test('200 unique keys set, updated and deleted with values()/entries() attached: all collectable', async () => {
        const gc = await exposeGc()
        const anchor = {}
        const m = new RxMap<object, number>([[anchor, 0]])
        const values = m.values(), entries = m.entries()
        const refs = churn(m, 200)
        const pin: object[] = []
        const control = pinnedChurn(m, pin)
        await settle(gc)
        expect(refs.filter(r => r.deref() !== undefined).length).toBe(0)
        expect(control[0].deref()).toBe(pin[0])
        expect([values.data, entries.data.map(e => e[1])]).toEqual([[0], [0]])
        m.destroy()
    })
})
