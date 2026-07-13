/**
 * Method 16 axis 5: RxTime multi-entry disposer under fake timers —
 * subscribe+resolve interleaving after the known fix.
 */
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest'
import {atom} from '../src/atom.js'
import {RxTime} from '../src/RxTime.js'
import {notifier} from '../src/notify.js'
import {ReactiveEffect} from '../src/reactiveEffect.js'

function assertQuiescent(label: string) {
    expect(notifier.isDigesting, `${label}: isDigesting`).toBe(false)
    expect(ReactiveEffect.activeScopes.length, `${label}: activeScopes`).toBe(0)
}

describe('M16-5 RxTime disposer interleaving', () => {
    beforeEach(() => {
        vi.useFakeTimers({toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'Date']})
    })
    afterEach(() => {
        vi.useRealTimers()
        assertQuiescent('afterEach')
    })

    test('subscribe then resolve then destroy: both interval and autorun stop', () => {
        const t = new RxTime()
        const ticker = t.subscribe(50)
        const at0 = ticker.raw
        const passed = t.gt(Date.now() + 200)
        expect(passed.raw).toBe(false)
        t.destroy()
        vi.advanceTimersByTime(500)
        expect(ticker.raw).toBe(at0)
        expect(passed.raw).toBe(false)
    })

    test('resolve then subscribe then destroy (known fix direction)', () => {
        const t = new RxTime()
        const passed = t.gt(Date.now() + 200)
        const ticker = t.subscribe(50)
        const at0 = ticker.raw
        t.destroy()
        vi.advanceTimersByTime(500)
        expect(passed.raw).toBe(false)
        expect(ticker.raw).toBe(at0)
    })

    test('subscribe → resolve → subscribe again → destroy cleans all three', () => {
        const t = new RxTime()
        const t1 = t.subscribe(40)
        const a1 = t1.raw
        const passed = t.gt(Date.now() + 300)
        const t2 = t.subscribe(40)
        const a2 = t2.raw
        t.destroy()
        vi.advanceTimersByTime(600)
        expect(t1.raw).toBe(a1)
        expect(t2.raw).toBe(a2)
        expect(passed.raw).toBe(false)
    })

    test('threshold atom thrash between subscribe/resolve then destroy', () => {
        const threshold = atom(Date.now() + 1000)
        const t = new RxTime()
        const ticker = t.subscribe(25)
        const passed = t.gt(threshold)
        threshold(Date.now() + 50)
        threshold(Date.now() + 5000)
        const tickAt = ticker.raw
        t.destroy()
        vi.advanceTimersByTime(10_000)
        expect(ticker.raw).toBe(tickAt)
        expect(passed.raw).toBe(false)
    })

    test('stopAutorun after mixed entries then late timer advance', () => {
        const t = new RxTime()
        t.eq(Date.now()) // register a resolve-side autorun alongside subscribe
        const ticker = t.subscribe(30)
        const snap = ticker.raw
        t.stopAutorun!()
        vi.advanceTimersByTime(200)
        expect(ticker.raw).toBe(snap)
        // destroy idempotent after stopAutorun
        t.destroy()
    })

    test('double resolve is asserted (cannot modify after resolved)', () => {
        const t = new RxTime()
        t.gt(Date.now() + 100)
        expect(() => t.lt(Date.now() + 200)).toThrow()
        t.destroy()
    })
})
