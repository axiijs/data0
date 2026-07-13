/**
 * Method 16 axis 1: Nested batch / batch inside digest-running effect.
 * Attack: sessionDepth / isDigesting early-return imbalance; effects lost or
 * permanently deferred; notifier stuck in session.
 */
import {afterEach, describe, expect, test} from 'vitest'
import {atom} from '../src/atom.js'
import {computed, getComputedInternal} from '../src/computed.js'
import {autorun} from '../src/common.js'
import {batch, notifier} from '../src/notify.js'
import {ReactiveEffect} from '../src/reactiveEffect.js'

function assertQuiescent(label: string) {
    expect(notifier.isDigesting, `${label}: isDigesting`).toBe(false)
    expect(notifier.inEffectSession, `${label}: inEffectSession`).toBe(false)
    expect(notifier.sessionDepth, `${label}: sessionDepth`).toBe(0)
    expect(notifier.sessionQueue.length, `${label}: sessionQueue`).toBe(0)
    expect(ReactiveEffect.activeScopes.length, `${label}: activeScopes`).toBe(0)
}

afterEach(() => {
    assertQuiescent('afterEach')
})

describe('M16-1 nested batch / batch-inside-digest', () => {
    test('batch called from effect during outer digest still applies writes', () => {
        const a = atom(0)
        const b = atom(0)
        const seen: number[] = []
        const stopOuter = autorun(() => {
            seen.push(a())
            if (a.raw === 1) {
                // Running inside digest of the write that set a=1.
                batch(() => {
                    b(10)
                    b(20)
                })
            }
        }, true)
        const stopInner = autorun(() => {
            seen.push(1000 + b())
        }, true)

        expect(seen).toEqual([0, 1000])
        a(1)
        // Expected: a-effect sees 1, batch writes b→20, b-effect sees 1020.
        expect(seen).toEqual([0, 1000, 1, 1020])
        expect(b()).toBe(20)
        assertQuiescent('after batch-in-digest')
        stopOuter()
        stopInner()
    })

    test('triple-nested batch depth exits only at outermost', () => {
        const x = atom(0)
        const y = atom(0)
        let runs = 0
        const c = computed(() => {
            runs++
            return x() + y()
        })
        expect(c()).toBe(0)
        expect(runs).toBe(1)

        batch(() => {
            x(1)
            batch(() => {
                y(1)
                batch(() => {
                    x(2)
                    expect(c()).toBe(0) // A2: stale inside batch — architecture, not a bug
                    expect(runs).toBe(1)
                })
                expect(notifier.sessionDepth).toBe(2)
            })
            expect(notifier.sessionDepth).toBe(1)
        })
        expect(c()).toBe(3)
        expect(runs).toBe(2)
        assertQuiescent('after nested batch')
    })

    test('computed finishFullRecompute session nested under digest does not drop dependents', () => {
        const src = atom(1)
        const mid = computed(() => src() * 10)
        const leaf = computed(() => mid() + 1)
        expect(leaf()).toBe(11)

        // Sync push: src → mid.finishFullRecompute session → leaf scheduled → digest
        src(2)
        expect(mid()).toBe(20)
        expect(leaf()).toBe(21)
        assertQuiescent('after chained finishFullRecompute')
        destroyQuiet(mid)
        destroyQuiet(leaf)
    })

    test('batch inside digest that throws still leaves notifier quiescent', () => {
        const a = atom(0)
        const b = atom(0)
        const stop = autorun(() => {
            a()
            if (a.raw === 1) {
                expect(() => {
                    batch(() => {
                        b(5)
                        throw new Error('batch-in-digest-boom')
                    })
                }).toThrow('batch-in-digest-boom')
            }
        }, true)
        a(1)
        expect(b()).toBe(5)
        assertQuiescent('after throwing batch-in-digest')
        stop()
    })
})

function destroyQuiet(data: any) {
    const internal = getComputedInternal(data)
    internal?.destroy()
}
