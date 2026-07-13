/**
 * Method 16 axis 3: scheduleRecompute custom schedulers — sync vs defer footguns.
 */
import {afterEach, describe, expect, test} from 'vitest'
import {atom} from '../src/atom.js'
import {computed, destroyComputed, getComputedInternal} from '../src/computed.js'
import {notifier} from '../src/notify.js'
import {ReactiveEffect} from '../src/reactiveEffect.js'
import {RxList} from '../src/RxList.js'

function assertQuiescent(label: string) {
    expect(notifier.isDigesting, `${label}: isDigesting`).toBe(false)
    expect(notifier.inEffectSession, `${label}: inEffectSession`).toBe(false)
    expect(ReactiveEffect.activeScopes.length, `${label}: activeScopes`).toBe(0)
}

afterEach(() => assertQuiescent('afterEach'))

describe('M16-3 custom scheduleRecompute sync vs defer', () => {
    test('sync scheduler: dep write during recompute triggers nested recompute without immediate-assert', () => {
        const a = atom(0)
        const b = atom(0)
        const runs: string[] = []
        const syncSched = (recompute: (force?: boolean) => void) => {
            recompute()
        }

        const secondary = computed(() => {
            runs.push(`sec:${b()}`)
            return b()
        }, undefined, syncSched)

        const primary = computed(() => {
            runs.push(`pri:${a()}`)
            if (a.raw === 1) b(a.raw * 10)
            return a() + b()
        }, undefined, syncSched)

        expect(primary()).toBe(0)
        expect(secondary()).toBe(0)

        // Does this throw 'detect recompute triggerred in sync recompute' or recurse badly?
        a(1)
        expect(primary()).toBe(11)
        expect(secondary()).toBe(10)
        destroyComputed(primary)
        destroyComputed(secondary)
    })

    test('rest-args scheduler length=0 silently drops triggerInfos (scheduleNeedsInfos false)', () => {
        const source = new RxList([1])
        const received: any[] = []
        // Common footgun: (...args) => has length 0 — scheduleNeedsInfos uses .length > 2
        const sched = (...args: any[]) => {
            received.push(args)
            args[0]()
        }
        expect(sched.length).toBe(0)

        const c = computed(
            () => source.data.length,
            undefined,
            sched as any
        )
        destroyComputed(c)

        const c2 = computed(() => {
            // track list length via reading through a tracked path
            return source.at(0)
        }, undefined, sched as any)
        expect(c2()).toBe(1)

        source.push(2)
        source.splice(0, 1, 9)
        expect(received.length).toBeGreaterThanOrEqual(1)
        const last = received[received.length - 1]
        // length-0 scheduler never receives infos as 3rd arg
        expect(last[2]).toBeUndefined()
        destroyComputed(c2)
    })

    test('scheduler that only markDirty never recomputes — permanent DIRTY (contract probe)', () => {
        const dep = atom(1)
        const c = computed(() => dep() * 2, undefined, (_recompute, markDirty) => {
            markDirty()
        })
        expect(c()).toBe(2)
        dep(5)
        // After trigger, value stays stale if scheduler never calls recompute
        expect(c()).toBe(2)
        const internal = getComputedInternal(c)!
        expect(internal._status).toBe(-1) // STATUS_DIRTY
        destroyComputed(c)
    })

    test('self-write during recompute is suppressed for same effect (no infinite loop)', () => {
        // Notifier.trigger* skips activeEffect === subscriber, so writing a dep
        // while computing does not re-enter. Atom value updates; computed value
        // reflects the pre-write read until an external trigger.
        const n = atom(0)
        let iterations = 0
        const syncSched = (recompute: (force?: boolean) => void) => {
            recompute()
        }
        const c = computed(() => {
            iterations++
            const v = n()
            if (v < 5) n(v + 1)
            return v
        }, undefined, syncSched)

        expect(c()).toBe(0)
        expect(n()).toBe(1) // write applied, self-trigger suppressed
        expect(iterations).toBe(1)

        n(2) // external trigger
        expect(c()).toBe(2)
        expect(n()).toBe(3) // another suppressed self-write
        expect(iterations).toBe(2)
        destroyComputed(c)
    })

    test('defer-then-sync hybrid: queued recompute runs after microtask with fresh deps', async () => {
        const dep = atom(1)
        let scheduled = 0
        const c = computed(() => dep() * 3, undefined, (recompute) => {
            scheduled++
            queueMicrotask(() => recompute())
        })
        expect(c()).toBe(3)
        dep(2)
        dep(3)
        expect(c()).toBe(3) // still deferred
        await Promise.resolve()
        await Promise.resolve()
        expect(c()).toBe(9)
        expect(scheduled).toBeGreaterThanOrEqual(1)
        destroyComputed(c)
    })
})
