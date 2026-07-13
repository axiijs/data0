/**
 * Method 16 axis 4: object-atom proxy edge cases — arrays-as-plain-objects,
 * null-prototype, prototype-pollution-style keys.
 */
import {afterEach, describe, expect, test} from 'vitest'
import {atom, isAtom} from '../src/atom.js'
import {autorun} from '../src/common.js'
import {notifier} from '../src/notify.js'
import {ReactiveEffect} from '../src/reactiveEffect.js'

function assertQuiescent(label: string) {
    expect(notifier.isDigesting, `${label}: isDigesting`).toBe(false)
    expect(ReactiveEffect.activeScopes.length, `${label}: activeScopes`).toBe(0)
}

afterEach(() => assertQuiescent('afterEach'))

describe('M16-4 object-atom proxy edges', () => {
    test('array atom: index set via proxy notifies; push mutates through proxy', () => {
        const arr = atom([1, 2, 3] as number[])
        expect(isAtom(arr)).toBe(true)
        const seen: number[][] = []
        const stop = autorun(() => {
            seen.push([arr[0], arr[1], arr[2], (arr as any).length])
        }, true)
        expect(seen[0]).toEqual([1, 2, 3, 3])

        ;(arr as any)[0] = 9
        expect(seen.at(-1)).toEqual([9, 2, 3, 3])

        ;(arr as any).push(4)
        expect(arr.raw).toEqual([9, 2, 3, 4])
        expect(seen.at(-1)?.[3]).toBe(4)
        stop()
    })

    test.fails('null-prototype object atom: properties must be readable via proxy', () => {
        // DYNAMIC REPRO: isPlainObject requires constructor===Object|Array;
        // Object.create(null) is misclassified → get forwards to updater fn → undefined.
        const raw = Object.create(null) as Record<string, number>
        raw.x = 1
        const obj = atom(raw)
        expect((obj as any).x).toBe(1)
        expect(obj.raw.x).toBe(1)
    })

    test('__proto__ assignment via atom proxy does not pollute Object.prototype', () => {
        const obj = atom({a: 1} as Record<string, unknown>)
        const marker = {polluted: true}
        ;(obj as any)['__proto__'] = marker
        expect(({} as any).polluted).toBeUndefined()
        expect(Object.prototype).not.toHaveProperty('polluted')
        const valueProto = Object.getPrototypeOf(obj.raw)
        if (valueProto && valueProto !== Object.prototype && 'polluted' in valueProto) {
            delete (valueProto as any).polluted
        }
    })

    test.fails('constructor key overwrite breaks subsequent object-atom property access', () => {
        // DYNAMIC REPRO: set constructor=null via proxy → isPlainObject becomes false
        // → further gets hit the updater function, not the stored object.
        const obj = atom({n: 1} as Record<string, unknown>)
        ;(obj as any).constructor = null
        expect(obj.raw.n).toBe(1)
        expect((obj as any).n).toBe(1)
        ;(obj as any).n = 2
        expect(obj.raw.n).toBe(2)
        expect((obj as any).n).toBe(2)
    })

    test('constructor overwrite: document actual broken reads', () => {
        const obj = atom({n: 1} as Record<string, unknown>)
        ;(obj as any).constructor = null
        // After overwrite, proxy get no longer surfaces value fields:
        expect((obj as any).n).toBeUndefined()
        expect(obj.raw.n).toBe(1) // raw still intact
        // set trap still writes into value (typeof object), but get path is broken
        ;(obj as any).n = 2
        expect(obj.raw.n).toBe(2)
        expect((obj as any).n).toBeUndefined()
    })

    test('array atom: length shrink notifies subscribers', () => {
        const arr = atom([1, 2, 3, 4] as number[])
        let lastLen = -1
        const stop = autorun(() => {
            lastLen = (arr as any).length
        }, true)
        expect(lastLen).toBe(4)
        ;(arr as any).length = 1
        expect(lastLen).toBe(1)
        expect(arr.raw).toEqual([1])
        stop()
    })

    test('class-instance atom: property get does not surface instance fields (same isPlainObject gate)', () => {
        class Box { constructor(public n: number) {} }
        const obj = atom(new Box(3) as any)
        expect(obj.raw.n).toBe(3)
        // Not a plain object/array → get hits updater
        expect((obj as any).n).toBeUndefined()
    })

    test('null-proto atom: set trap still writes value but get remains blind', () => {
        const raw = Object.create(null) as Record<string, number>
        raw.x = 1
        const obj = atom(raw)
        ;(obj as any).x = 5
        expect(obj.raw.x).toBe(5)
        expect((obj as any).x).toBeUndefined()
    })

    test.fails('null-proto atom: property reads must establish subscriptions', () => {
        const raw = Object.create(null) as Record<string, number>
        raw.x = 1
        const obj = atom(raw)
        let seen = -1
        const stop = autorun(() => {
            seen = (obj as any).x ?? -1
        }, true)
        expect(seen).toBe(1)
        ;(obj as any).x = 9
        expect(seen).toBe(9)
        stop()
    })

    test('null-proto atom: document that autorun never tracks property path', () => {
        const raw = Object.create(null) as Record<string, number>
        raw.x = 1
        const obj = atom(raw)
        let seen = -1
        const stop = autorun(() => {
            seen = (obj as any).x ?? -99
        }, true)
        expect(seen).toBe(-99) // read was undefined, never tracked value
        ;(obj as any).x = 9
        expect(seen).toBe(-99) // set notifies nobody subscribed via property get
        expect(obj.raw.x).toBe(9)
        stop()
    })
})
