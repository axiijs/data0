/**
 * Method 16 axis 6: AsyncRxSlice destroy mid-fetch with overlapping update/fetch.
 */
import {describe, expect, test} from 'vitest'
import {AsyncRxSlice} from '../src/AsyncRxSlice.js'
import {getComputedInternal} from '../src/computed.js'

type Deferred<T> = {promise: Promise<T>, resolve: (v: T) => void, reject: (e: any) => void}
function deferred<T>(): Deferred<T> {
    let resolve!: (v: T) => void
    let reject!: (e: any) => void
    const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej })
    return {promise, resolve, reject}
}
const tick = () => new Promise<void>(r => setTimeout(r, 0))

describe('M16-6 AsyncRxSlice destroy × overlapping fetch/update', () => {
    test('destroy mid-fetch: late resolve must not mutate data; isLoading must not zombie-stuck true', async () => {
        const pending: Deferred<number[]>[] = []
        const slice = new AsyncRxSlice<number>([0], () => {
            const d = deferred<number[]>()
            pending.push(d)
            return d.promise
        })
        const p = slice.fetch()
        expect(slice.isLoading.raw).toBe(true)
        slice.destroy()
        expect(slice.active).toBe(false)

        pending[0].resolve([1, 2, 3])
        await p
        await tick()
        // Data must remain pre-destroy (splice is no-op on destroyed)
        expect(slice.data).toEqual([0])
        // Loading should not stay stuck true forever — if late finally still runs, it clears.
        // If it never clears, that's a stuck-loading bug on a destroyed instance.
        expect(slice.isLoading.raw).toBe(false)
    })

    test('destroy mid-update: late resolve must not append', async () => {
        const pending: Deferred<number[]>[] = []
        const slice = new AsyncRxSlice<number>([1], () => {
            const d = deferred<number[]>()
            pending.push(d)
            return d.promise
        }, x => x)
        const u = slice.update(1, 1)
        expect(slice.isLoading.raw).toBe(true)
        slice.destroy()
        pending[0].resolve([2])
        await u
        await tick()
        expect(slice.data).toEqual([1])
        expect(slice.isLoading.raw).toBe(false)
    })

    test('overlapping fetch then update: older fetch discarded; destroy before either settles', async () => {
        const pending: Deferred<number[]>[] = []
        const slice = new AsyncRxSlice<number>([], () => {
            const d = deferred<number[]>()
            pending.push(d)
            return d.promise
        }, x => x)
        const f = slice.fetch()
        const u = slice.update(0, 1, undefined, false, true)
        expect(pending.length).toBe(2)
        slice.destroy()
        pending[0].resolve([9, 9])
        pending[1].resolve([7])
        await f
        await u
        await tick()
        expect(slice.data).toEqual([])
        expect(slice.isLoading.raw).toBe(false)
    })

    test('overlapping update then fetch: same destroy guarantee', async () => {
        const pending: Deferred<number[]>[] = []
        const slice = new AsyncRxSlice<number>([5], () => {
            const d = deferred<number[]>()
            pending.push(d)
            return d.promise
        }, x => x)
        const u = slice.update(5, 1)
        const f = slice.fetch()
        slice.destroy()
        pending[1].resolve([1, 2]) // fetch newer
        pending[0].resolve([99])   // update older
        await u
        await f
        await tick()
        expect(slice.data).toEqual([5])
    })

    test('fetch after destroy: must not resurrect writes when prior promise settles', async () => {
        const pending: Deferred<number[]>[] = []
        const slice = new AsyncRxSlice<number>([], () => {
            const d = deferred<number[]>()
            pending.push(d)
            return d.promise
        })
        const p1 = slice.fetch()
        slice.destroy()
        // Second fetch on destroyed instance — document behavior
        const p2 = slice.fetch()
        pending[0].resolve([1])
        await p1
        await p2
        await tick()
        expect(slice.data).toEqual([])
        expect(slice.active).toBe(false)
        // autoFetchPromise still points at destroyed computed?
        if (slice.autoFetchPromise) {
            const internal = getComputedInternal(slice.autoFetchPromise)
            expect(internal?.active).toBe(false)
        }
    })

    test('update reject after destroy must not set loadError', async () => {
        const pending: Deferred<number[]>[] = []
        const slice = new AsyncRxSlice<number>([1], () => {
            const d = deferred<number[]>()
            pending.push(d)
            return d.promise
        }, x => x)
        const u = slice.update(1, 1)
        slice.destroy()
        pending[0].reject(new Error('net'))
        await u
        await tick()
        expect(slice.data).toEqual([1])
        expect(slice.active).toBe(false)
        expect(slice.loadError.raw).toBe(null)
        expect(slice.isLoading.raw).toBe(false)
    })
})
