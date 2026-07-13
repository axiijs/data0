/**
 * Method 16 axis 1 deeper: batch inside digest when finishFullRecompute nests;
 * effect scheduled during digest that itself calls batch with further writes.
 */
import {describe, expect, test} from 'vitest'
import {atom} from '../src/atom.js'
import {computed, destroyComputed} from '../src/computed.js'
import {autorun} from '../src/common.js'
import {batch, notifier} from '../src/notify.js'

describe('M16-1b digest×batch stress', () => {
    test('chain with batch-in-digest leaf write converges', () => {
        const a = atom(1)
        const b = computed(() => a() + 1)
        const c = computed(() => a() * 10)
        const side = atom(0)
        const stop2 = autorun(() => {
            if (b() === 3) {
                batch(() => side(side.raw + 1))
            }
        }, true)

        a(2)
        expect(b()).toBe(3)
        expect(c()).toBe(20)
        expect(side()).toBe(1)
        expect(notifier.sessionDepth).toBe(0)
        expect(notifier.isDigesting).toBe(false)
        stop2()
        destroyComputed(b)
        destroyComputed(c)
    })

    test('creating computed inside batch-in-digest and destroying it before exit', () => {
        const a = atom(0)
        let tmpActive = true
        const stop = autorun(() => {
            a()
            if (a.raw === 1) {
                batch(() => {
                    const tmp = computed(() => a() * 2)
                    expect(tmp()).toBe(2)
                    destroyComputed(tmp)
                    tmpActive = false
                })
            }
        }, true)
        a(1)
        expect(tmpActive).toBe(false)
        expect(notifier.sessionDepth).toBe(0)
        expect(notifier.isDigesting).toBe(false)
        stop()
    })
})
