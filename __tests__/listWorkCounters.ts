/**
 * Deterministic work counters for the list scale tests (wall-clock ratios flake on loaded CI machines).
 */
import {atom, RxList} from '../src/index'

/**
 * Counts `.raw` reads of primitive atoms while `fn` runs. All primitive atoms share one prototype
 * accessor; filter's row indicators are primitive atoms and its position bookkeeping reads
 * `indicator.raw` once per row it scans, so the count tracks how many rows a change visits.
 */
export function countPrimitiveRawReads(fn: () => void): number {
    const proto = Object.getPrototypeOf(atom(0))
    const descriptor = Object.getOwnPropertyDescriptor(proto, 'raw')!
    let reads = 0
    Object.defineProperty(proto, 'raw', {
        ...descriptor,
        get(this: unknown) {
            reads++
            return descriptor.get!.call(this)
        },
    })
    try {
        fn()
    } finally {
        Object.defineProperty(proto, 'raw', descriptor)
    }
    return reads
}

/**
 * Counts the elements visited by linear membership scans (`Array#includes`) over `list.data`.
 */
export function countSourceScans(list: RxList<any>) {
    const counter = {scanned: 0}
    ;(list.data as any).includes = function (this: unknown[], x: unknown) {
        counter.scanned += this.length
        return Array.prototype.includes.call(this, x)
    }
    return counter
}
