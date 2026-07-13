/**
 * Method 16 axis 2: Creating/destroying computed DURING another computed's recompute/patch.
 */
import {afterEach, describe, expect, test} from 'vitest'
import {atom} from '../src/atom.js'
import {
    computed,
    destroyComputed,
    getComputedInternal,
    Computed,
} from '../src/computed.js'
import {notifier} from '../src/notify.js'
import {ReactiveEffect} from '../src/reactiveEffect.js'
import {TrackOpTypes, TriggerOpTypes} from '../src/operations.js'
import {RxList} from '../src/RxList.js'

function assertQuiescent(label: string) {
    expect(notifier.isDigesting, `${label}: isDigesting`).toBe(false)
    expect(notifier.inEffectSession, `${label}: inEffectSession`).toBe(false)
    expect(notifier.sessionDepth, `${label}: sessionDepth`).toBe(0)
    expect(ReactiveEffect.activeScopes.length, `${label}: activeScopes`).toBe(0)
}

afterEach(() => assertQuiescent('afterEach'))

describe('M16-2 create/destroy computed mid-recompute', () => {
    test('create child computed inside parent getter; parent destroy tears down child', () => {
        const src = atom(1)
        let child: ReturnType<typeof computed> | undefined
        const parent = computed(() => {
            const v = src()
            child = computed(() => v * 100 + src())
            return child()
        })
        expect(parent()).toBe(101)
        expect(child!()).toBe(101)

        const childInternal = getComputedInternal(child!)!
        src(2)
        // Parent recomputes: destroyChildren kills old child, creates new child
        expect(parent()).toBe(202)
        expect(childInternal.active).toBe(false)

        destroyComputed(parent)
        expect(getComputedInternal(child!)!.active).toBe(false)
    })

    test('destroy sibling computed from inside another computed getter', () => {
        const src = atom(1)
        const victim = computed(() => src() * 2)
        expect(victim()).toBe(2)

        const killer = computed(() => {
            const v = src()
            if (v === 2) destroyComputed(victim)
            return v
        })
        expect(killer()).toBe(1)
        src(2)
        expect(killer()).toBe(2)
        expect(getComputedInternal(victim)!.active).toBe(false)
        // Victim must not react further
        const before = victim.raw
        src(3)
        expect(victim.raw).toBe(before)
        destroyComputed(killer)
    })

    test('create+destroy ephemeral computed inside patch applyPatch leaves system quiescent', () => {
        const list = new RxList([1, 2])
        let ephemeralActiveAfterPatch: boolean | undefined
        const mirror = new RxList<number>([])
        const derived = new Computed(
            function (this: Computed) {
                this.manualTrack(list, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                this.manualTrack(list, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE)
                mirror.spliceArray(0, Infinity, list.data.slice())
                return mirror
            },
            function (this: Computed) {
                const ephemeral = computed(() => list.data.length)
                ephemeral()
                destroyComputed(ephemeral)
                ephemeralActiveAfterPatch = getComputedInternal(ephemeral)!.active
                mirror.spliceArray(0, Infinity, list.data.slice())
            }
        )
        derived.data = mirror
        derived.run([], true)
        expect(mirror.data).toEqual([1, 2])

        list.push(3)
        expect(mirror.data).toEqual([1, 2, 3])
        expect(ephemeralActiveAfterPatch).toBe(false)
        derived.destroy()
    })

    test('destroy parent from child getter (error-boundary style) does not leak scopes', () => {
        const src = atom(1)
        let parent: ReturnType<typeof computed> | undefined
        parent = computed(() => {
            const inner = computed(() => {
                const v = src()
                if (v === 9) {
                    destroyComputed(parent!)
                }
                return v
            })
            return inner()
        })
        expect(parent()).toBe(1)
        src(9)
        expect(getComputedInternal(parent!)!.active).toBe(false)
        assertQuiescent('after destroy-parent-from-child')
    })
})
