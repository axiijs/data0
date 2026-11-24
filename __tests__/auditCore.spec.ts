import {afterEach, describe, expect, test, vi} from 'vitest'
import {
    atom,
    autorun,
    batch,
    computed,
    Computed,
    destroyComputed,
    disableData0RetainedObjectDiagnostics,
    enableData0RetainedObjectDiagnostics,
    getData0RetainedObjectDiagnosticsSnapshot,
    Notifier,
    notifier,
    onChange,
    ReactiveEffect,
    once,
    recompute,
    scheduleNextMicroTask,
    scheduleNextTick,
    RxList,
    RxMap,
    RxSet,
    TrackOpTypes,
    TriggerOpTypes,
} from '../src/index'
import {getComputedInternal} from '../src/computed'

/**
 * Audit of the data0 core (atom / notify / dep / reactiveEffect / computed / common).
 *
 * F1..F8, X1..X5: found by this audit on v2.16.0 + the pending teardown-isolation PR. Every one of them
 *   failed there; they pass with the fixes in src/, except X5, kept as `test.fails` (a contract decision,
 *   reason stated there). F8's RxList.map onDestroy half is fixed by the list commit.
 * R-X5 (`test.fails`): a known gap found in review, reason stated there.
 * S* / L*: copied from the collections / RxList reviewers (root cause in the core files); fixed here.
 * R*: regression tests for branches the fixes touch that the audit tests do not reach. Most fail on the
 *   pristine tree; the rest pin behaviour that must not change (sync first run, the DIRTY retry at digest…).
 */

const subscribers = (t: object) => [...(notifier.getDepEffects(t) ?? [])]
const tick = () => new Promise<void>(r => setTimeout(r, 0))
const wait = (ms: number) => new Promise<void>(r => setTimeout(r, ms))

// a failing case must not poison the next one: put the global singletons back
afterEach(() => {
    notifier.sessionDepth = 0
    notifier.inEffectSession = false
    notifier.isDigesting = false
    notifier.sessionQueue.length = 0
    notifier.trackStack.length = 0
    notifier.shouldTrack = true
    notifier.effectTrackDepth = 0
    Notifier.trackOpBit = 1
    ReactiveEffect.activeScopes.length = 0
    vi.restoreAllMocks()
})

describe('F1 a failed recompute drops the dependencies read by the failing run', () => {
    test('sync computed: fixing the value it failed on triggers the retry', () => {
        const mode = atom('a')
        const b = atom(-1)
        const c = computed(() => {
            if (mode() === 'a') return 1
            const v = b()
            if (v < 0) throw new Error('negative')
            return v
        })
        expect(c()).toBe(1)
        expect(() => mode('b')).toThrow('negative')
        b(5) // the value the failed run depended on is fixed
        expect(c()).toBe(5)
        destroyComputed(c)
    })
    test('async computed whose first run rejects retries when its dependency changes', async () => {
        vi.spyOn(console, 'error').mockImplementation(() => undefined)
        const q = atom('bad')
        const c = computed(async () => {
            const v = q()
            await Promise.resolve()
            if (v === 'bad') throw new Error('fetch failed')
            return v
        })
        await wait(5)
        q('good')
        await wait(5)
        expect(c.raw).toBe('good')
        destroyComputed(c)
    })
})

describe('F2 generator applyPatch returning false while more infos are queued', () => {
    test('falls back to a full recompute (derived ≡ source)', async () => {
        const source = new RxList<number>([1, 2])
        const derived = new RxList<number>(
            function computation(this: RxList<number>) {
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                return source.data.map(x => x * 10)
            },
            function* applyPatch(this: RxList<number>, _data: any, triggerInfos: any[]): Generator<any, any, any> {
                yield Promise.resolve()
                for (const info of triggerInfos) {
                    // deletions are not patchable here: ask for a full recompute
                    if ((info.argv![1] as number) > 0) return false
                    this.spliceArray(info.argv![0] as number, 0, (info.argv!.slice(2) as number[]).map(x => x * 10))
                }
            } as any,
        )
        expect(derived.data).toEqual([10, 20])
        source.splice(0, 1) // round 1 (suspended at the yield) will return false
        source.push(5)      // arrives while round 1 is suspended: queued for the next round
        await wait(10)
        expect(derived.data).toEqual(source.data.map(x => x * 10))
        derived.destroy()
        source.destroy()
    })
})

describe('F3 async computed re-triggered from a nested scope during its own tracked section', () => {
    test('async getter keeps its dependencies', async () => {
        const x = atom(0)
        const y = atom(10)
        const z = atom(0)
        // a synchronous subscriber that feeds back into a dependency of c
        const stop = autorun(() => { if (z() === 1) x(1) }, true)
        const c = computed(async () => {
            const v = x()
            const w = y()
            if (v === 0) z(1)
            return v + w
        })
        await wait(5)
        expect(c()).toBe(11)
        y(20)
        await wait(5)
        expect(c()).toBe(21)
        stop()
        destroyComputed(c)
    })
    test('generator getter (re-trigger in the last segment) keeps its dependencies', async () => {
        const x = atom(0)
        const y = atom(10)
        const z = atom(0)
        const stop = autorun(() => { if (z() === 1) x(1) }, true)
        const c = computed(function* (): Generator<any, number, any> {
            const v = x()
            const w = y()
            yield Promise.resolve()
            if (v === 0) z(1)
            return v + w
        } as any)
        await wait(5)
        expect(c()).toBe(11)
        y(20)
        await wait(5)
        expect(c()).toBe(21)
        stop()
        destroyComputed(c)
    })
})

describe('F4 a destroyed or superseded generator / async patch round keeps running', () => {
    test('destroy mid-yield: later generator getter segments still run and their child stays subscribed', async () => {
        const src = atom(0)
        const late = atom(0)
        let release!: () => void
        const c = computed(function* (): Generator<any, any, any> {
            src()
            yield new Promise<void>(r => { release = r })
            const child = computed(() => late())
            return child()
        } as any)
        await tick()
        destroyComputed(c)
        release()
        await wait(5)
        expect(subscribers(late)).toEqual([])
    })
    test('destroy mid-yield: a later generator patch segment re-subscribes the destroyed computed', async () => {
        const src = atom(0)
        const late = atom(0)
        let release!: () => void
        const c = new Computed(
            function (this: Computed) {
                this.manualTrack(src, TrackOpTypes.ATOM, 'value')
                return null
            },
            function* (this: Computed): Generator<any, any, any> {
                yield new Promise<void>(r => { release = r })
                this.manualTrack(late, TrackOpTypes.ATOM, 'value')
            } as any,
        )
        c.run([], true)
        src(1) // patch round starts and suspends at the yield
        await tick()
        c.destroy()
        release()
        await wait(5)
        expect(subscribers(late)).toEqual([])
    })
    test('forced full recompute: the suspended generator patch is still applied on top of it', async () => {
        const source = new RxList<number>([1])
        let release!: () => void
        const derived = new RxList<number>(
            function computation(this: RxList<number>) {
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                return source.data.map(x => x * 10)
            },
            function* applyPatch(this: RxList<number>, _data: any, triggerInfos: any[]): Generator<any, void, any> {
                yield new Promise<void>(r => { release = r })
                for (const info of triggerInfos) {
                    this.spliceArray(info.argv![0] as number, info.argv![1] as number, (info.argv!.slice(2) as number[]).map(x => x * 10))
                }
            } as any,
        )
        expect(derived.data).toEqual([10])
        source.push(2)          // patch suspended at the yield
        await tick()
        derived.recompute(true) // forced full recompute
        expect(derived.data).toEqual([10, 20])
        release()
        await wait(10)
        expect(derived.data).toEqual(source.data.map(x => x * 10))
        derived.destroy()
        source.destroy()
    })
    test('forced full recompute: the superseded async patch loop keeps taking new infos (rounds overlap)', async () => {
        const src = atom(0)
        const gates: (() => void)[] = []
        const starts: number[][] = []
        const c = new Computed(
            function (this: Computed) {
                this.manualTrack(src, TrackOpTypes.ATOM, 'value')
                return null
            },
            async function (this: Computed, _data: any, infos: any[]) {
                starts.push(infos.map(i => i.newValue))
                await new Promise<void>(r => gates.push(r))
            },
        )
        c.run([], true)
        src(1)            // round A suspended (gate 0)
        await tick()
        c.recompute(true) // forced full recompute supersedes round A
        src(2)            // round B suspended (gate 1)
        await tick()
        src(3)            // queued behind round B
        gates[0]()        // the superseded round A resumes
        await tick()
        // round B has not finished: [3] must not have started yet
        expect(starts).toEqual([[1], [2]])
        for (let i = 0; i < 4; i++) { gates.forEach(g => g()); await tick() }
        c.destroy()
    })
    test('forced full recompute of an async-getter patch computed is cancelled by the next trigger', async () => {
        vi.spyOn(console, 'warn').mockImplementation(() => undefined)
        let external = 0 // something the getter reads that is not reactive: the reason to force
        const src = atom(0)
        let release!: () => void
        const c = computed(async function (this: Computed) {
            this.manualTrack(src, TrackOpTypes.ATOM, 'value')
            const base = external
            const s = src.raw
            await new Promise<void>(r => { release = r })
            return base + s
        }, function (this: Computed, data: any, infos: any[]) {
            for (const i of infos) data(data.raw + (i.newValue - i.oldValue))
        }, true)
        await tick(); release(); await tick()
        expect(c.raw).toBe(0)
        external = 100
        const done = recompute(c, true) // forced full recompute, suspended at the await
        src(1)                          // a trigger while the forced round is in flight
        await tick(); release(); await done; await tick()
        expect(c.raw).toBe(101)
        destroyComputed(c)
    })
})

describe('F5 deferred destroy: a later destroy() drops the fromParent flag', () => {
    test('the re-running parent loses a child of the new generation (leaked, still subscribed)', () => {
        const x = atom(0)
        const go = atom(0)
        const late = atom(0)
        let stopChild: (() => void) | undefined
        const stopParent = autorun(() => {
            const generation = x()
            stopChild?.() // redundant manual stop of the previous child
            stopChild = autorun(() => {
                // generation 0 child: on its re-run, re-trigger the parent from inside this child
                if (go() === 1 && generation === 0) x(1)
            }, true)
            autorun(() => { late() }, true)
        }, true)
        go(1)
        expect(x.raw).toBe(1)
        expect(subscribers(go).length).toBe(1)
        expect(subscribers(late).length).toBe(1)
        stopParent()
        // destroying the parent must destroy every child of the current generation
        expect(subscribers(go)).toEqual([])
        expect(subscribers(late)).toEqual([])
    })
})

describe('F6 replaceData throwing inside finishFullRecompute', () => {
    test('a computed returning a lazy atom whose getter throws must not freeze every later trigger', () => {
        expect(() => computed(() => atom.lazy(() => { throw new Error('boom') }))).toThrow('boom')
        // an unrelated atom / computed pair created afterwards
        const a = atom(1)
        const d = computed(() => a() + 1)
        a(2)
        expect(d()).toBe(3)
        expect({inSession: notifier.inEffectSession, depth: notifier.sessionDepth}).toEqual({inSession: false, depth: 0})
    })
})

describe('F7 a scheduler that runs recompute synchronously × re-entrant trigger', () => {
    test('leaves a stale dep marker: an unrelated effect silently fails to subscribe', () => {
        const a = atom(0)
        const b = atom(0)
        const z = atom(0)
        // a synchronous subscriber that feeds back into a dependency of c
        const stopFeedback = autorun(() => { if (z() === 1) a(1) }, true)
        let phase = 0
        const c = computed(() => {
            const v = a()
            if (phase === 0) return v + b()      // first run: deps = {a, b}
            if (phase === 1) { phase = 2; z(1) } // second run: re-triggers c from a nested scope
            return v                              // neither the outer nor the nested run reads b
        }, undefined, recomputeFn => recomputeFn())
        expect(c()).toBe(0)
        phase = 1
        try { recompute(c, true) } catch (e) { /* a loud failure here would be acceptable */ }
        // a fresh, unrelated effect that reads b
        const seen: number[] = []
        const stop = autorun(() => { seen.push(b()) }, true)
        b(7)
        expect(seen).toEqual([0, 7])
        stop()
        stopFeedback()
        destroyComputed(c)
    })
})

describe('F8 destroy with a throwing context.onCleanup skips the rest of destroyResources', () => {
    test('cleanPromise of an in-flight async computed is never settled', async () => {
        const a = atom(0)
        let release!: () => void
        const c = computed(async ({onCleanup}: any) => {
            a()
            onCleanup(() => { throw new Error('cleanup boom') })
            await new Promise<void>(r => { release = r })
            return 1
        })
        const internal = getComputedInternal(c)!
        await tick()
        const p = internal.cleanPromise!
        expect(() => destroyComputed(c)).toThrow('cleanup boom')
        expect(internal.active).toBe(false)
        release()
        const state = await Promise.race([p.then(() => 'settled', () => 'settled'), wait(30).then(() => 'pending')])
        expect(state).toBe('settled')
    })
    // The loop that runs the rows' `context.onCleanup` at destroy lives in the `onDestroy` callback of
    // RxList.map (an operator body), not in `RxList.destroyResources`; it is fixed together with the
    // other row-cleanup loops of `map` (see auditListFixes.spec.ts).
    test('RxList.map onDestroy: rows after a throwing row cleanup are still cleaned', () => {
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

describe('X1 async computed × moving key: dep bookkeeping is unbounded', () => {
    test('RxMap.get(key) from an async computed leaves one empty Dep per visited key', async () => {
        vi.spyOn(console, 'warn').mockImplementation(() => undefined)
        const map = new RxMap<number, number>([])
        const k = atom(0)
        const c = computed(async () => map.get(k()))
        await tick()
        for (let i = 1; i <= 50; i++) { k(i); await tick() }
        expect(notifier.targetMap.get(map)!.size).toBeLessThanOrEqual(2)
        destroyComputed(c)
        map.destroy()
    })
})

describe('X2 markDirty from the scheduler of a data-less Computed', () => {
    test('autorun with a scheduler that calls markDirty does not throw TypeError', () => {
        const a = atom(0)
        const seen: number[] = []
        let flush: (() => void) | undefined
        const stop = autorun(() => { seen.push(a()) }, (recomputeFn, markDirty) => {
            markDirty()
            flush = recomputeFn
        })
        expect(() => a(1)).not.toThrow()
        flush?.()
        expect(seen).toEqual([0, 1])
        stop()
    })
})

describe('X3 destroy inside batch after a queued trigger', () => {
    test('recompute() of the destroyed computed does not return a never-settling promise', async () => {
        const a = atom(0)
        const c = computed(() => a() + 1)
        batch(() => {
            a(1)
            destroyComputed(c)
        })
        const p = recompute(c)
        const state = p === undefined ? 'sync' : await Promise.race([p.then(() => 'settled', () => 'settled'), wait(30).then(() => 'pending')])
        expect(state).not.toBe('pending')
    })
})

describe('X4 scheduleNextMicroTask dedupe drops a forced recompute', () => {
    test('a run started while a recompute is queued, re-triggered from a nested scope, stays stale', async () => {
        const a = atom(0)
        const z = atom(0)
        const stopFeedback = autorun(() => { if (z() === 1) a(a.raw + 10) }, true)
        let fire = false
        const c = computed(() => { const v = a(); if (fire) { fire = false; z(1) } return v }, undefined, scheduleNextMicroTask)
        a(1)         // DIRTY, recompute queued
        fire = true
        recompute(c) // runs now; the nested write re-triggers c while it is recomputing
        await tick()
        expect(c.raw).toBe(a.raw)
        stopFeedback()
        destroyComputed(c)
    })
})

describe('X5 computed created while skipIndicator.skip is true', () => {
    // NOT FIXED, kept as executable evidence (AGENTS §3 `test.fails`): this is a contract decision, not a
    // clean bug fix. README §3.2 says a skipped computed "不重算" and names `recompute(c, true)` as the exit;
    // letting the creation run through the gate (one token in Computed.run) would make user getters execute
    // while skipped. Either document "created while skipped = inert until recompute(c, true)" and delete this
    // test, or let the first run through and turn it into `test(...)`.
    test.fails('computes after the skip is lifted and a dependency is written', () => {
        const skip = {skip: true}
        const a = atom(1)
        const c = computed(() => a() * 2, undefined, undefined, undefined, skip)
        skip.skip = false
        a(2)
        expect(c.raw).toBe(4)
        destroyComputed(c)
    })
})

// ---------------------------------------------------------------------------
// Copied from the collections reviewer (findings/d0-structs.audit-structs.spec.ts)
// ---------------------------------------------------------------------------
describe('S-F4 forced recompute x infos queued in the session', () => {
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

describe('S-M2 retained diagnostics x async computed', () => {
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
})

// ---------------------------------------------------------------------------
// Copied from the RxList reviewer (findings/d0-list.audit-list.spec.ts, group F2)
// ---------------------------------------------------------------------------
describe('L-F2 forced recompute of a patch-type derived structure inside a batch double-applies the queued change', () => {
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

// ---------------------------------------------------------------------------
// Regression tests for branches the fixes touch
// ---------------------------------------------------------------------------
describe('R-F1 failed recompute: which dependencies survive', () => {
    test('a sync first run that throws to its creator leaves nothing subscribed', () => {
        const a = atom(-1)
        const b = atom(0)
        let runs = 0
        expect(() => computed(() => { runs++; b(); if (a() < 0) throw new Error('first run') ; return a() })).toThrow('first run')
        expect(() => autorun(() => { runs++; b(); if (a() < 0) throw new Error('first run') }, true)).toThrow('first run')
        expect(subscribers(a)).toEqual([])
        expect(subscribers(b)).toEqual([])
        a(1); b(1)
        expect(runs).toBe(2) // nobody holds a handle: no ghost re-run
    })
    test('after the retry succeeds the dependency set is exact again', () => {
        const mode = atom('a')
        const a = atom(1)
        const b = atom(-1)
        let runs = 0
        const c = computed(() => {
            runs++
            if (mode() === 'a') return a()
            const v = b(); if (v < 0) throw new Error('negative'); return v
        })
        const internal = getComputedInternal(c)!
        expect(() => mode('b')).toThrow('negative')
        // previous ∪ collected: the old branch is still subscribed until a run succeeds
        expect(subscribers(a)).toContain(internal)
        expect(subscribers(b)).toContain(internal)
        expect(() => b(-2)).toThrow('negative') // the writer of the failing dependency sees the retry fail
        b(5)
        expect(c()).toBe(5)
        expect(subscribers(a)).toEqual([])      // the branch that is no longer read is gone
        const before = runs
        a(2)
        expect(runs).toBe(before)
        destroyComputed(c)
        expect(subscribers(mode)).toEqual([]); expect(subscribers(b)).toEqual([])
    })
    test('a dependency on a structure created inside the failing run: retried through it, then released', () => {
        const flag = atom(false)
        const inner = atom(-1)
        let child: any
        const c = computed(() => {
            if (!flag()) return 0
            child = computed(() => inner() * 2)
            const v = child()
            if (v < 0) throw new Error('negative child')
            return v
        })
        expect(() => flag(true)).toThrow('negative child')
        const failedChild = getComputedInternal(child)!
        inner(4) // the child recomputes and triggers the parent, which re-creates its child
        expect(c()).toBe(8)
        expect(failedChild.active).toBe(false)
        expect(subscribers(inner)).toEqual([getComputedInternal(child)])
        destroyComputed(c)
        expect(subscribers(inner)).toEqual([])
        expect(subscribers(flag)).toEqual([])
    })
    test('patch computed: manualTrack registrations of a failed full recompute are kept and only cause a full recompute', () => {
        const which = atom<'x' | 'y'>('x')
        const x = atom(1)
        const y = atom(-1)
        const patches: number[] = []
        const c = new Computed(
            function (this: Computed) {
                this.manualTrack(which, TrackOpTypes.ATOM, 'value')
                const src = which.raw === 'x' ? x : y
                this.manualTrack(src, TrackOpTypes.ATOM, 'value')
                if (src.raw < 0) throw new Error('negative source')
                return null
            },
            function (this: Computed, _data: any, infos: any[]) {
                // a change of `which` cannot be patched
                if (infos.some(i => i.source === which)) return false
                patches.push(infos.length)
            },
        )
        c.run([], true)
        x(2)
        expect(patches).toEqual([1])
        expect(() => which('y')).toThrow('negative source') // patch returns false -> full recompute fails
        expect(subscribers(y)).toContain(c)
        y(3)                                               // retried as a FULL recompute, not as a patch
        expect(patches).toEqual([1])
        expect(c._status).toBe(3 /* STATUS_CLEAN */)
        expect(subscribers(x)).toEqual([])
        y(4)
        expect(patches).toEqual([1, 1])
        c.destroy()
    })
    test('generator computed failing after a yield keeps what the round read so far', async () => {
        vi.spyOn(console, 'error').mockImplementation(() => undefined)
        const q = atom('bad')
        const c = computed(function* (): Generator<any, any, any> {
            const v = q(); q()
            yield Promise.resolve()
            if (v === 'bad') throw new Error('late failure')
            return v
        } as any)
        await wait(5)
        expect(subscribers(q)).toEqual([getComputedInternal(c)])
        q('good'); await wait(5)
        expect(c.raw).toBe('good')
        destroyComputed(c)
        expect(subscribers(q)).toEqual([])
    })
})

describe('R-F3 async tracking when a newer round starts inside the tracked section', () => {
    test('reads the superseded round makes after the re-entry do not unsubscribe the winner; the next round is exact', async () => {
        const x = atom(0), y = atom(10), z = atom(0), extra = atom(0)
        const stop = autorun(() => { if (z() === 1) x(1) }, true)
        let runs = 0
        const c = computed(async () => {
            runs++
            const v = x()
            const w = y()
            if (v === 0) { z(1); extra() } // only the superseded round reads `extra`, after the re-entry
            return v + w
        })
        const internal = getComputedInternal(c)!
        await wait(5)
        expect(c()).toBe(11)
        expect(subscribers(x)).toContain(internal)
        expect(subscribers(y)).toContain(internal)
        // the next complete round drops what only the superseded round had read
        y(20); await wait(5)
        expect(c()).toBe(21)
        expect(subscribers(extra)).toEqual([])
        const before = runs
        extra(1); await wait(5)
        expect(runs).toBe(before)
        stop(); destroyComputed(c)
        expect(subscribers(x)).toEqual([]); expect(subscribers(y)).toEqual([])
    })
    test('generator: re-entry in a middle segment — the old round stops at its next resume point, deps stay complete', async () => {
        const x = atom(0), y = atom(10), z = atom(0)
        const stop = autorun(() => { if (z() === 1) x(1) }, true)
        const tail: number[] = []
        const c = computed(function* (): Generator<any, number, any> {
            const v = x()
            yield Promise.resolve()
            const w = y()
            if (v === 0) z(1)   // middle segment of the first round
            yield Promise.resolve()
            tail.push(v)
            return v + w
        } as any)
        await wait(10)
        expect(c()).toBe(11)
        expect(tail).toEqual([1]) // the superseded round never reached its last segment
        y(20); await wait(10)
        expect(c()).toBe(21)
        x(2); await wait(10)
        expect(c()).toBe(22)
        stop(); destroyComputed(c)
        expect(subscribers(x)).toEqual([]); expect(subscribers(y)).toEqual([])
    })
})

describe('R-F4 a recompute owns everything queued for the computed when it starts', () => {
    const mirror = (source: RxList<number>, log: number[], scheduler?: any) => new RxList<number>(
        function computation(this: RxList<number>) {
            this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
            return source.data.slice()
        },
        function applyPatch(this: RxList<number>, _data: any, infos: any[]) {
            log.push(infos.length)
            for (const info of infos) this.spliceArray(info.argv![0], info.argv![1], info.argv!.slice(2))
        } as any,
        scheduler,
    )
    test('a change made after the forced recompute, in the same batch, is applied exactly once', () => {
        const source = new RxList<number>([1])
        const log: number[] = []
        const derived = mirror(source, log)
        batch(() => {
            source.push(2)
            derived.recompute(true)
            source.push(3)
        })
        expect(derived.data).toEqual([1, 2, 3])
        expect(log).toEqual([1]) // only the info that arrived after the full recompute
        derived.destroy(); source.destroy()
    })
    test('nothing arrives after the forced recompute: the queued digest run does not call applyPatch / the scheduler', () => {
        const source = new RxList<number>([1])
        const log: number[] = []
        const derived = mirror(source, log)
        let scheduled = 0
        const log2: number[] = []
        const deferred = mirror(source, log2, (recomputeFn: any, _markDirty: any, _infos: any[]) => { scheduled++; recomputeFn() })
        batch(() => {
            source.push(2)
            derived.recompute(true)
            deferred.recompute(true)
        })
        expect(derived.data).toEqual([1, 2])
        expect(deferred.data).toEqual([1, 2])
        expect(log).toEqual([])
        expect(log2).toEqual([])
        expect(scheduled).toBe(0)
        // and the structures are still live afterwards
        source.push(3)
        expect(derived.data).toEqual([1, 2, 3])
        expect(deferred.data).toEqual([1, 2, 3])
        expect(log).toEqual([1]); expect(log2).toEqual([1]); expect(scheduled).toBe(1)
        derived.destroy(); deferred.destroy(); source.destroy()
    })
    test('the forced recompute throws inside the batch: the queued run retries and the batch still converges', () => {
        const source = new RxList<number>([1])
        let fail = false
        const derived = new RxList<number>(
            function computation(this: RxList<number>) {
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                if (fail) { fail = false; throw new Error('full boom') }
                return source.data.slice()
            },
            function applyPatch(this: RxList<number>, _data: any, infos: any[]) {
                for (const info of infos) this.spliceArray(info.argv![0], info.argv![1], info.argv!.slice(2))
            } as any,
        )
        batch(() => {
            source.push(2)
            fail = true
            expect(() => derived.recompute(true)).toThrow('full boom')
        })
        expect(derived.data).toEqual([1, 2])
        derived.destroy(); source.destroy()
    })
    test('markDirty-style run() inside a batch patches with the queued infos instead of an empty list', () => {
        const source = new RxList<number>([1])
        const log: number[] = []
        const derived = mirror(source, log)
        batch(() => {
            source.push(2)
            derived.run()          // what recursiveMarkDirty does to a downstream
            expect(derived.data).toEqual([1, 2])
        })
        expect(derived.data).toEqual([1, 2])
        expect(log).toEqual([1])
        derived.destroy(); source.destroy()
    })
    test('async patch: after a forced recompute the new infos are handled by one fresh serial round', async () => {
        const src = atom(0)
        const gates: (() => void)[] = []
        const events: string[] = []
        const c = new Computed(
            function (this: Computed) { this.manualTrack(src, TrackOpTypes.ATOM, 'value'); return null },
            async function (this: Computed, _data: any, infos: any[]) {
                const id = infos.map(i => i.newValue).join(',')
                events.push('start ' + id)
                await new Promise<void>(r => gates.push(r))
                events.push('end ' + id)
            },
        )
        c.run([], true)
        src(1); await tick()
        c.recompute(true)
        src(2); await tick()
        src(3)
        gates[0](); await tick()   // the superseded round finishes its own applyPatch and stops
        gates[1](); await tick()
        gates[2](); await tick()
        expect(events).toEqual(['start 1', 'start 2', 'end 1', 'end 2', 'start 3', 'end 3'])
        expect(c._status).toBe(3 /* STATUS_CLEAN */)
        c.destroy()
    })
    test('generator patch destroyed mid-yield: asyncStatus settles, nothing is left on the scope stack', async () => {
        const src = atom(0)
        let release!: () => void
        let after = 0
        const c = new Computed(
            function (this: Computed) { this.manualTrack(src, TrackOpTypes.ATOM, 'value'); return null },
            function* (this: Computed): Generator<any, any, any> { yield new Promise<void>(r => { release = r }); after++ } as any,
        )
        c.run([], true)
        src(1); await tick()
        expect(c.asyncStatus!.raw).toBe(true)
        c.destroy(); release(); await wait(5)
        expect(after).toBe(0)
        expect(c.asyncStatus!.raw).toBe(false)
        expect(ReactiveEffect.activeScopes.length).toBe(0)
    })
    test('patch returning false while a full recompute is the fallback: a trigger during the async full restarts it', async () => {
        vi.spyOn(console, 'warn').mockImplementation(() => undefined)
        const src = atom(0)
        let release!: () => void
        const c = computed(async function (this: Computed) {
            this.manualTrack(src, TrackOpTypes.ATOM, 'value')
            const s = src.raw
            await new Promise<void>(r => { release = r })
            return s * 10
        }, function (this: Computed, data: any, infos: any[]) {
            if (infos.some(i => i.newValue === 1)) return false // not patchable
            for (const i of infos) data(data.raw + (i.newValue - i.oldValue) * 10) // relative to the current data
        }, true)
        await tick(); release(); await tick()
        expect(c.raw).toBe(0)
        src(1)                  // patch says false -> async full recompute in flight
        src(2)                  // arrives during the fallback: must not be patched onto the old data
        await tick(); release(); await tick(); release(); await tick()
        expect(c.raw).toBe(20)
        src(3)                  // and incremental again afterwards
        expect(c.raw).toBe(30)
        destroyComputed(c)
    })
})

describe('R-F5 deferred destroy keeps every flag it was given', () => {
    test('ignoreChildren is not dropped by a later plain destroy()', () => {
        const a = atom(0), dep = atom(0)
        let child: Computed | undefined
        const e = new Computed(function (this: Computed) {
            a()
            child = new Computed(() => dep(), undefined, true)
            child.run([], true)
            if (a.raw === 1) { this.destroy(true); this.destroy() } // "I keep the children", then a plain destroy
        }, undefined, true)
        e.run([], true)
        a(1)
        expect(e.active).toBe(false)
        expect(child!.active).toBe(true)        // handed over by destroy(true)
        expect(child!.parent).toBeUndefined()   // and detached from the destroyed parent
        child!.destroy()
        expect(subscribers(dep)).toEqual([])
        expect(subscribers(a)).toEqual([])
    })
    test('plain destroy() first, the parent re-run second: no sibling of the new generation is dropped', () => {
        const x = atom(0), go = atom(0), late = atom(0)
        const stopParent = autorun(() => {
            const generation = x()
            let stopSelf!: () => void
            stopSelf = autorun(() => {
                if (go() === 1 && generation === 0) { stopSelf(); x(1) } // self stop, then re-trigger the parent
            }, true)
            autorun(() => { late() }, true)
        }, true)
        go(1)
        expect(subscribers(go).length).toBe(1)
        expect(subscribers(late).length).toBe(1)
        stopParent()
        expect(subscribers(go)).toEqual([])
        expect(subscribers(late)).toEqual([])
    })
})

describe('R-F6 an error while writing the result is a normal recompute error', () => {
    const quiet = () => ({inSession: notifier.inEffectSession, depth: notifier.sessionDepth, queue: notifier.sessionQueue.length})
    test('sync recompute: thrown to the writer, state machine recovers, session quiescent', () => {
        const flag = atom(false)
        const c = computed(() => flag() ? atom.lazy(() => { throw new Error('boom') }) : 1)
        const internal = getComputedInternal(c)!
        expect(c()).toBe(1)
        expect(() => flag(true)).toThrow('boom')
        expect(quiet()).toEqual({inSession: false, depth: 0, queue: 0})
        expect(internal._status).toBe(-1 /* STATUS_DIRTY */)
        flag(false) // no "detect recompute triggerred in sync recompute"
        expect(c()).toBe(1)
        expect(internal._status).toBe(3 /* STATUS_CLEAN */)
        destroyComputed(c)
    })
    test('async recompute: cleanPromise rejects, console fallback, later change recovers', async () => {
        const errors = vi.spyOn(console, 'error').mockImplementation(() => undefined)
        vi.spyOn(console, 'warn').mockImplementation(() => undefined)
        const flag = atom(true)
        const c = computed(async () => flag() ? atom.lazy(() => { throw new Error('boom') }) : 1)
        const internal = getComputedInternal(c)!
        await expect(internal.cleanPromise).rejects.toThrow('boom')
        await tick()
        expect(errors).toHaveBeenCalled()
        expect(quiet()).toEqual({inSession: false, depth: 0, queue: 0})
        flag(false); await wait(5)
        expect(c.raw).toBe(1)
        destroyComputed(c)
    })
    test('inside a batch: the batch digest still runs the other subscribers', () => {
        const flag = atom(false)
        const other = atom(0)
        const c = computed(() => flag() ? atom.lazy(() => { throw new Error('boom') }) : 1)
        const d = computed(() => other() + 1)
        expect(() => batch(() => { flag(true); other(1) })).toThrow('boom')
        expect(d()).toBe(2)
        expect(quiet()).toEqual({inSession: false, depth: 0, queue: 0})
        destroyComputed(c); destroyComputed(d)
    })
})

describe('R-F7 a synchronous scheduler re-entered while the run is on the stack', () => {
    const feedbackInto = (trigger: any, target: any, value: number) => autorun(() => { if (trigger() === 1) target(value) }, true)
    test('the forced recompute is deferred to the end of the run: never nested, final value fresh, stale result not published', () => {
        const a = atom(0), z = atom(0)
        const stopFeedback = feedbackInto(z, a, 1)
        let depth = 0, maxDepth = 0, runs = 0
        let fire = false
        const c = computed(() => {
            depth++; maxDepth = Math.max(maxDepth, depth); runs++
            const v = a()
            if (fire) { fire = false; z(1) }
            depth--
            return v
        }, undefined, recomputeFn => recomputeFn())
        const published: number[] = []
        const stop = autorun(() => { published.push(c()) }, true)
        runs = 0
        fire = true
        recompute(c, true)
        expect(maxDepth).toBe(1)
        expect(runs).toBe(2)             // the run that was re-triggered + one rerun
        expect(c.raw).toBe(1)
        expect(published).toEqual([0, 1]) // the stale 0 of the first run was not published again
        stop(); stopFeedback(); destroyComputed(c)
    })
    test('patch computed: the patch that re-triggered itself is not applied on top of a nested full recompute', () => {
        // an atom source: its writes are dispatched inline (no session), so the nested trigger really re-enters
        const src = atom(0), z = atom(0)
        const stopFeedback = autorun(() => { if (z() === 1) src(src.raw + 10) }, true)
        let fire = false
        const applied: number[] = []
        const c = new Computed(
            function (this: Computed) {
                this.manualTrack(src, TrackOpTypes.ATOM, 'value')
                applied.length = 0
                applied.push(src.raw) // a full recompute starts over from the current source
                return null
            },
            function (this: Computed, _data: any, infos: any[]) {
                for (const info of infos.slice()) {
                    if (fire) { fire = false; z(1) } // re-triggers this computed from a nested scope, mid-patch
                    applied.push(info.newValue)
                }
            },
            (recomputeFn: any) => recomputeFn(),
        )
        c.run([], true)
        expect(applied).toEqual([0])
        fire = true
        src(1)
        expect(src.raw).toBe(11)
        expect(applied).toEqual([11])
        expect(c._status).toBe(3 /* STATUS_CLEAN */)
        src(12)
        expect(applied).toEqual([11, 12]) // back to incremental
        stopFeedback(); c.destroy()
    })
    test('the run throws after being re-triggered: the pending rerun is dropped, the next recompute runs once', () => {
        const a = atom(0), z = atom(0)
        const stopFeedback = feedbackInto(z, a, 1)
        let mode: 'ok' | 'fire-then-throw' = 'ok'
        let runs = 0
        const c = computed(() => {
            runs++
            const v = a()
            if (mode === 'fire-then-throw') { mode = 'ok'; z(1); throw new Error('boom') }
            return v
        }, undefined, recomputeFn => recomputeFn())
        mode = 'fire-then-throw'
        expect(() => recompute(c, true)).toThrow('boom')
        runs = 0
        a(2)
        expect(runs).toBe(1)
        expect(c.raw).toBe(2)
        stopFeedback(); destroyComputed(c)
    })
    test('autorun with a synchronous scheduler drains a work list to balance', () => {
        const pendingList = new RxList<number>([1, 2, 3])
        const z = atom(0)
        const done: number[] = []
        // moving an item happens in a nested scope (another synchronous subscriber)
        const mover = autorun(() => { if (z() > 0 && pendingList.data.length) done.push(pendingList.shift()!) }, true)
        const stop = autorun(() => { if (pendingList.length() > 0) z(z.raw + 1) }, recomputeFn => recomputeFn())
        expect(done).toEqual([1, 2, 3])
        expect(pendingList.data).toEqual([])
        stop(); mover(); pendingList.destroy()
    })
})

describe('R-F8 destroyResources finishes after a throwing cleanup', () => {
    test('RxList.destroyResources: a row effect whose teardown throws does not keep the other rows alive', () => {
        const errors = vi.spyOn(console, 'error').mockImplementation(() => undefined)
        const dep = atom(0)
        const inner = atom(0)
        const source = new RxList<number>([1, 2, 3])
        const cleaned: number[] = []
        let ownCleanup = 0
        const mapped = new RxList<number>(
            function computation(this: RxList<number>, {onCleanup}: any) {
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                onCleanup(() => { ownCleanup++; throw new Error('list cleanup boom') })
                return source.data.slice()
            },
            function applyPatch() { return false },
        )
        // rows with their own child effect: destroyed through the row frames in destroyResources
        const rows = source.map(item => {
            dep()
            computed(({onCleanup}: any) => { onCleanup(() => { cleaned.push(item); if (item !== 2) throw new Error('row ' + item) }); return inner() })
            return item
        })
        const length = rows.length
        expect(subscribers(inner).length).toBe(3)
        expect(() => rows.destroy()).toThrow('row 1')
        expect(cleaned).toEqual([1, 2, 3])
        expect(subscribers(inner)).toEqual([])
        expect(subscribers(dep)).toEqual([])
        expect(getComputedInternal(length)!.active).toBe(false)
        // the later error is reported, not lost and not thrown
        expect(errors.mock.calls.some(call => call[0] === '[data0] uncaught error in teardown:' && String(call[1]).includes('row 3'))).toBe(true)
        // the structure's own context.onCleanup (base hook) still runs when it is destroyed
        expect(() => mapped.destroy()).toThrow('list cleanup boom')
        expect(ownCleanup).toBe(1)
        expect(mapped.active).toBe(false)
        expect(subscribers(source)).toEqual([])
        source.destroy()
    })
    test('Computed.destroyResources: the cleanup runs exactly once and a second destroy is a no-op', () => {
        let calls = 0
        const a = atom(0)
        const c = computed(({onCleanup}: any) => { onCleanup(() => { calls++; throw new Error('cleanup boom') }); return a() })
        expect(() => destroyComputed(c)).toThrow('cleanup boom')
        destroyComputed(c)
        expect(calls).toBe(1)
        expect(subscribers(a)).toEqual([])
    })
})

describe('R-X1/M2 async tracking bookkeeping', () => {
    test('a dependency that stays read across async rounds stays subscribed (the replay is still queued when already subscribed)', async () => {
        vi.spyOn(console, 'warn').mockImplementation(() => undefined)
        const a = atom(1)
        const c = computed(async () => (a() as number) + (a() as number))
        const internal = getComputedInternal(c)!
        await tick()
        for (const v of [2, 3, 4]) {
            a(v); await tick()
            expect(c.raw).toBe(v * 2)
            expect(subscribers(a)).toEqual([internal])
            expect(internal.deps.length).toBe(1) // listed once, not once per read
        }
        destroyComputed(c)
        expect(subscribers(a)).toEqual([])
    })
    test('generator reading the same keyed dep in two segments; keyed deps of a destroyed async computed are pruned', async () => {
        const map = new RxMap<string, number>([['k', 1]])
        const c = computed(function* (): Generator<any, any, any> {
            const first = map.get('k')
            yield Promise.resolve()
            return first! + map.get('k')!
        } as any)
        await wait(5)
        expect(c.raw).toBe(2)
        map.set('k', 5); await wait(5)
        expect(c.raw).toBe(10)
        expect(getComputedInternal(c)!.deps.length).toBe(1)
        destroyComputed(c)
        expect(notifier.targetMap.get(map)?.size ?? 0).toBe(0)
        map.destroy()
    })
    test('pruning after an async round keeps the deps that were re-read and the ones other effects still use', async () => {
        vi.spyOn(console, 'warn').mockImplementation(() => undefined)
        const map = new RxMap<number, number>([[0, 0], [1, 10], [2, 20]])
        const k = atom(0)
        const fixed = computed(() => map.get(1))             // another subscriber of key 1
        const c = computed(async () => (map.get(k()) ?? -1) + (map.get(2) ?? -1))
        await tick()
        k(1); await tick()
        k(0); await tick()
        expect(c.raw).toBe(20)
        const keys = [...notifier.targetMap.get(map)!.keys()].sort()
        expect(keys).toEqual([0, 1, 2])                      // 0 and 2 by c, 1 by `fixed`
        map.set(0, 5); await tick()
        expect(c.raw).toBe(25)
        map.set(2, 30); await tick()
        expect(c.raw).toBe(35)
        destroyComputed(c); destroyComputed(fixed); map.destroy()
    })
})

describe('R-X2 markDirty without downstream', () => {
    test('once / onChange / bare Computed schedulers may call markDirty', () => {
        const a = atom(0)
        const calls: string[] = []
        const stopOnce = once(() => { calls.push('once:' + a()); return false }, (recomputeFn, markDirty) => { markDirty(); recomputeFn() })
        const bare = new Computed(() => { calls.push('bare:' + a()) }, undefined, (recomputeFn, markDirty) => { markDirty(); recomputeFn() })
        bare.run([], true)
        calls.length = 0
        expect(() => a(1)).not.toThrow()
        expect(calls.sort()).toEqual(['bare:1', 'once:1'])
        stopOnce(); bare.destroy()
    })
})

describe('R-X3 a destroyed computed is silent', () => {
    test('queued in a batch then destroyed: no dirty event, status untouched, scheduler not called, no pending cleanPromise', () => {
        const a = atom(0)
        let scheduled = 0
        const c = computed(() => a() + 1, undefined, (recomputeFn) => { scheduled++; recomputeFn() })
        const internal = getComputedInternal(c)!
        let dirty = 0
        internal.on('dirty', () => dirty++)
        batch(() => { a(1); destroyComputed(c) })
        expect({dirty, scheduled, status: internal._status}).toEqual({dirty: 0, scheduled: 0, status: 3 /* STATUS_CLEAN */})
        expect(internal.cleanPromise).toBeUndefined()
        expect(c.raw).toBe(1) // snapshot at destroy
    })
    test('destroyed by an earlier subscriber of the same inline dispatch: the snapshot entry does nothing', () => {
        const a = atom(0)
        let second: any
        let dirty = 0
        const stopFirst = autorun(() => { if (a() === 1) destroyComputed(second) }, true)
        second = computed(() => a() * 2)
        const internal = getComputedInternal(second)!
        internal.on('dirty', () => dirty++)
        a(1)
        expect(dirty).toBe(0)
        expect(second.raw).toBe(0)
        expect(internal.cleanPromise).toBeUndefined()
        expect(recompute(second, true)).toBeUndefined()
        expect(second.raw).toBe(0)
        stopFirst()
    })
    test('a patch computed destroyed while queued does not keep the queued infos', () => {
        const source = new RxList<number>([1])
        const mapped = source.map(x => x * 2)
        batch(() => { source.push(2); mapped.destroy() })
        expect(mapped.data).toEqual([2])
        expect(mapped._triggerInfos?.length ?? 0).toBe(0)
        source.destroy()
    })
})

describe('R-X4 scheduled recompute dedupe', () => {
    test('scheduleNextTick has the same upgrade; a plain second trigger is still deduped to one run', async () => {
        const a = atom(0), z = atom(0)
        const stopFeedback = autorun(() => { if (z() === 1) a(a.raw + 10) }, true)
        let fire = false
        let runs = 0
        const c = computed(() => { runs++; const v = a(); if (fire) { fire = false; z(1) } return v }, undefined, scheduleNextTick)
        a(1); a(2); a(3)
        await tick()
        expect(runs).toBe(2)       // creation + one deduped recompute
        a(4)                       // queued
        fire = true
        recompute(c)               // runs now, re-triggered from a nested scope while recomputing
        await tick()
        expect(c.raw).toBe(a.raw)
        expect(c.raw).toBe(14)
        stopFeedback(); destroyComputed(c)
    })
})

// review of this PR: R-F6 handled the write-result entry without the sync-first-run rule R-F1 pins for the
// getter entry, and R-F1's union grew with every failure of a streak (AGENTS §4: bounded bookkeeping).
describe('R-F9 failure recovery: same rules at every entry, bounded across a failure streak', () => {
    test('sync first run whose result write throws: nothing stays subscribed', () => {
        const a = atom(0)
        expect(() => computed(() => { a(); return atom.lazy(() => { throw new Error('lazy boom') }) })).toThrow('lazy boom')
        expect(subscribers(a)).toEqual([])
        expect(() => a(1)).not.toThrow()

        const remote = atom<[string, number][] | undefined>(undefined)
        expect(() => new RxMap<string, number>(function () { return remote() as any } as any)).toThrow(TypeError)
        expect(subscribers(remote)).toEqual([])
        remote([['k', 1]])
        expect(subscribers(remote)).toEqual([])
    })

    test('sync: a failure streak keeps only (last success ∪ the current failing run)', () => {
        const map = new RxMap<number, string>([[0, 'zero']])
        const k = atom(0)
        const c = computed(() => {
            const key = k()
            const v = map.get(key)
            if (v === undefined) throw new Error('missing ' + key)
            return v
        })
        for (let i = 1; i <= 200; i++) expect(() => k(i)).toThrow('missing ' + i)
        // {k, map[0]} from the last success ∪ {k, map[200]} from the current failure
        expect(getComputedInternal(c)!.deps.length).toBeLessThanOrEqual(3)
        // key 3 was read by a failure long gone: writing it is not this computed's business
        expect(() => map.set(3, 'three')).not.toThrow()
        // the last success's dependency still retries, the current failure's key still recovers
        expect(() => map.set(0, 'ZERO')).toThrow('missing 200')
        map.set(200, 'two hundred')
        expect(c()).toBe('two hundred')
        // success: the dependency set is exact again
        expect(getComputedInternal(c)!.deps.length).toBe(2)
        destroyComputed(c)
        map.destroy()
    })

    test('async: the same streak stays bounded, stale keys do not retry', async () => {
        const errors = vi.spyOn(console, 'error').mockImplementation(() => undefined)
        vi.spyOn(console, 'warn').mockImplementation(() => undefined)
        const map = new RxMap<number, string>([[0, 'zero']])
        const k = atom(0)
        const c = computed(async () => {
            const key = k()
            const v = map.get(key)
            if (v === undefined) throw new Error('missing ' + key)
            return v
        })
        await tick()
        for (let i = 1; i <= 100; i++) { k(i); await tick() }
        expect(getComputedInternal(c)!.deps.length).toBeLessThanOrEqual(3)
        const before = errors.mock.calls.length
        map.set(3, 'three'); await tick()
        expect(errors.mock.calls.length - before).toBe(0)
        map.set(100, 'hundred'); await tick()
        expect(c.raw).toBe('hundred')
        destroyComputed(c)
        map.destroy()
    })
})

// review of this PR: the round check in runGeneratorPatch's loop carries weight but no test reached it
// (reverting it let a superseded round take the infos queued for the new round and drop them: 20 instead of 30).
describe('R-F10 a superseded generator patch round does not take the next round’s infos', () => {
    test('superseded round resumes after a forced recompute and a new round', async () => {
        const src = atom(0)
        const gates: (() => void)[] = []
        const c = computed(function (this: any) {
            this.manualTrack(src, TrackOpTypes.ATOM, 'value')
            return src.raw * 10
        }, function* (this: any, data: any, infos: any[]): Generator<any, any, any> {
            yield new Promise<void>(r => gates.push(r))
            for (const i of infos) data(data.raw + (i.newValue - i.oldValue) * 10)
        } as any)
        src(1)                 // round A suspended (gate 0)
        await tick()
        recompute(c, true)     // supersedes A: c = 10
        src(2)                 // round B suspended (gate 1)
        await tick()
        src(3)                 // queued behind B
        gates[0]()             // the superseded round A resumes
        await tick(); await tick()
        for (let k = 0; k < 4; k++) { gates.forEach(g => g()); await tick() }
        expect(c.raw).toBe(src.raw * 10)
        destroyComputed(c)
    })
})

/**
 * Known gap, not fixed here (review of this PR): "a recompute owns everything queued for the computed when it
 * starts" (R-F4) is enforced where the info is queued — the session (batch / digest) and _triggerInfos. When
 * the source is written outside a batch, the info is not queued yet: it is in flight in the inline dispatch, and
 * an earlier subscriber that forces a recompute of a delta-consuming patch computed gets the delta applied on top
 * of the full result. 2.16.0 does the same for both entries (20 / 20); this PR fixed the batch entry only.
 * Reach: user-written applyPatch that consumes atom deltas, forced from an earlier subscriber of the same source.
 * A fix needs a per-trigger sequence stamp on the dispatch path, and is only correct if every source writes its
 * data before dispatching (unverified for RxList's dispatchStructuralThen) — left for a separate change.
 */
describe('R-X5 forced recompute × in-flight info, inline (non-batch) entry', () => {
    const make = () => {
        const src = atom(0)
        const box: {c?: any} = {}
        const forcer = autorun(() => { src(); if (box.c) recompute(box.c, true) }, true)   // subscribed first
        box.c = computed(function (this: any) {
            this.manualTrack(src, TrackOpTypes.ATOM, 'value')
            return src.raw * 10
        }, function (this: any, data: any, infos: any[]) {
            for (const i of infos) data(data.raw + (i.newValue - i.oldValue) * 10)
        })
        return {src, c: box.c, stop: () => { forcer(); destroyComputed(box.c) }}
    }
    test('batch entry equals the full recompute', () => {
        const y = make(); batch(() => y.src(1)); const batched = y.c.raw; y.stop()
        expect(batched).toBe(10)
    })
    test.fails('inline entry equals the full recompute (known gap: 20)', () => {
        const x = make(); x.src(1); const inline = x.c.raw; x.stop()
        expect(inline).toBe(10)
    })
})

// review of this PR: stopping an abandoned generator at its yield also skipped its own `finally` (2.16.0 ran it,
// because it kept running the whole generator after destroy). The library now closes it with return():
// the body after the yield does not run, the generator's finally does — as teardown code.
describe('R-F11 an abandoned generator is closed: its finally runs, the rest of its body does not', () => {
    test('destroyed generator getter', async () => {
        const src = atom(0)
        let release!: () => void
        const ran: string[] = []
        const c = computed(function* (): Generator<any, any, any> {
            src()
            try {
                yield new Promise<void>(r => { release = r })
                ran.push('after yield')
                return 1
            } finally {
                ran.push('finally')
            }
        } as any)
        await tick()
        destroyComputed(c)
        release()
        await tick(); await tick()
        expect(ran).toEqual(['finally'])
    })

    test('superseded generator getter: the old run is closed, the new one completes', async () => {
        const src = atom(0)
        const releases: Array<() => void> = []
        const ran: string[] = []
        const c = computed(function* (): Generator<any, any, any> {
            const v = src()
            try {
                yield new Promise<void>(r => { releases.push(r) })
                ran.push(`after yield ${v}`)
                return v
            } finally {
                ran.push(`finally ${v}`)
            }
        } as any)
        await tick()
        src(1)
        await tick()
        releases.forEach(r => r())
        await tick(); await tick(); await tick()
        expect(ran).toEqual(['finally 0', 'after yield 1', 'finally 1'])
        expect(c.raw).toBe(1)
        destroyComputed(c)
    })

    test('destroyed generator patch round', async () => {
        const src = atom(0)
        let release!: () => void
        const ran: string[] = []
        const c = new Computed(
            function (this: Computed) {
                this.manualTrack(src, TrackOpTypes.ATOM, 'value')
                return null
            },
            function* (this: Computed): Generator<any, any, any> {
                try {
                    yield new Promise<void>(r => { release = r })
                    ran.push('after yield')
                } finally {
                    ran.push('finally')
                }
            } as any,
        )
        c.run([], true)
        src(1)
        await tick()
        c.destroy()
        release()
        await tick(); await tick()
        expect(ran).toEqual(['finally'])
    })

    test('finally is teardown: its reads subscribe nobody, its error is reported', async () => {
        const errors = vi.spyOn(console, 'error').mockImplementation(() => undefined)
        const src = atom(0)
        const read = atom(0)
        let release!: () => void
        const c = computed(function* (): Generator<any, any, any> {
            src()
            try {
                yield new Promise<void>(r => { release = r })
            } finally {
                read()
                throw new Error('finally boom')
            }
        } as any)
        await tick()
        let outerRuns = 0
        const stop = autorun(() => { outerRuns++; if (src() === 1) destroyComputed(c) }, true)
        src(1)
        release()
        await tick(); await tick()
        expect(subscribers(read)).toEqual([])
        read(1)
        expect(outerRuns).toBe(2)
        expect(errors.mock.calls.some(args => String(args[0]).includes('teardown') && (args[1] as Error)?.message === 'finally boom')).toBe(true)
        stop()
    })
})
