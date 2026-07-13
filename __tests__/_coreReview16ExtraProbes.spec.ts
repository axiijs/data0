/**
 * Method 16 extra probes: CompactDep demotion, scheduleNeedsInfos arrow length,
 * fetch success after destroy still writing isLoading, nested batch+autorun.
 */
import {describe, expect, test} from 'vitest'
import {atom} from '../src/atom.js'
import {computed, destroyComputed, Computed} from '../src/computed.js'
import {batch} from '../src/notify.js'
import {createCompactDep, CompactDep} from '../src/dep.js'
import {AsyncRxSlice} from '../src/AsyncRxSlice.js'
import {ReactiveEffect} from '../src/reactiveEffect.js'
import {TrackOpTypes, TriggerOpTypes} from '../src/operations.js'
import {RxList} from '../src/RxList.js'

type Deferred<T> = {promise: Promise<T>, resolve: (v: T) => void, reject: (e: any) => void}
function deferred<T>(): Deferred<T> {
    let resolve!: (v: T) => void
    let reject!: (e: any) => void
    const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej })
    return {promise, resolve, reject}
}
const tick = () => new Promise<void>(r => setTimeout(r, 0))

describe('M16-extra CompactDep / scheduleNeedsInfos / destroy race', () => {
    test('CompactDep: demote on size===1 then delete leaves clean empty (no empty-overflow linger)', () => {
        const e1 = new ReactiveEffect(() => 1)
        const e2 = new ReactiveEffect(() => 2)
        e1.active = true
        e2.active = true
        const dep = createCompactDep() as CompactDep
        dep.add(e1)
        dep.add(e2)
        dep.delete(e1) // demote to single
        expect(dep.single).toBe(e2)
        expect(dep.overflow).toBeUndefined()
        dep.delete(e2)
        expect(dep.single).toBeUndefined()
        expect(dep.overflow).toBeUndefined()
        dep.add(e1)
        expect(dep.single).toBe(e1) // single fast path restored
    })

    test('arrow scheduler with 3 params gets infos; rest-args does not', () => {
        const source = new RxList([0])
        const withInfos: any[] = []
        const withoutInfos: any[] = []

        const c1 = new Computed(
            function (this: Computed) {
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
            },
            undefined,
            (recompute, _md, infos) => {
                withInfos.push(infos)
                recompute()
            }
        )
        c1.run([], true)

        const rest = (...args: any[]) => {
            withoutInfos.push(args[2])
            args[0]()
        }
        const c2 = new Computed(
            function (this: Computed) {
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
            },
            undefined,
            rest as any
        )
        c2.run([], true)

        source.push(1)
        expect(withInfos.at(-1)).toBeDefined()
        expect(Array.isArray(withInfos.at(-1))).toBe(true)
        expect(withoutInfos.at(-1)).toBeUndefined()
        c1.destroy()
        c2.destroy()
    })

    test('fetch success after destroy must not write isLoading (destroyed inert)', async () => {
        const pending: Deferred<number[]>[] = []
        const slice = new AsyncRxSlice<number>([0], () => {
            const d = deferred<number[]>()
            pending.push(d)
            return d.promise
        })
        slice.fetch()
        expect(slice.isLoading.raw).toBe(true)
        slice.destroy()
        // destroyResources clears isLoading and bumps receipt so late finally is a no-op.
        expect(slice.isLoading.raw).toBe(false)
        pending[0].resolve([1])
        await tick()
        await tick()
        expect(slice.isLoading.raw).toBe(false)
        expect(slice.data).toEqual([0])
    })

    test('sync scheduler during batch: recomputes at schedule time still sees A2 stale reads of other computeds', () => {
        const a = atom(1)
        const b = computed(() => a() * 2) // immediate
        let seen = -1
        const c = computed(() => {
            seen = b()
            return seen
        }, undefined, (recompute) => recompute())

        expect(c()).toBe(2)
        batch(() => {
            a(5)
            // c's sync scheduler runs when? a triggers b scheduled, c scheduled...
            // With sync sched on c: when c is digested, b may still be dirty (A2).
        })
        // After batch, both should be consistent
        expect(b()).toBe(10)
        expect(c()).toBe(10)
        destroyComputed(b)
        destroyComputed(c)
    })
})
