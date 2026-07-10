import {bench, describe} from 'vitest'
import {atom, batch, computed, Computed, RxList} from '../src/index'

/**
 * Core hot-path benchmarks for data0.
 *
 * These cover the paths touched most frequently by rendering frameworks built
 * on data0 (axii): atom read/write, computed propagation, incremental RxList
 * patches. Run with `pnpm bench bench/core.bench.ts` and compare before/after
 * when touching notify/computed/RxList internals.
 */

describe('atom hot paths', () => {
    bench('create primitive atom', () => {
        atom(0)
    })

    {
        const a = atom(0)
        let i = 0
        // 1 subscriber so trigger path is exercised
        computed(() => a() + 1)
        bench('write atom with 1 computed subscriber', () => {
            a(++i)
        })
    }

    {
        const atoms = Array.from({length: 10}, () => atom(0))
        computed(() => {
            let sum = 0
            for (const a of atoms) sum += a()
            return sum
        })
        let i = 0
        bench('read 10 atoms inside computed recompute', () => {
            // triggers one recompute that re-reads (tracks) 10 atoms
            atoms[0](++i)
        })
    }
})

describe('computed propagation', () => {
    {
        const source = atom(0)
        let last = source
        for (let d = 0; d < 10; d++) {
            const prev = last
            last = computed(() => prev() + 1)
        }
        let i = 0
        bench('chain of 10 computed, single source write', () => {
            source(++i)
        })
    }

    {
        const atoms = Array.from({length: 10}, () => atom(0))
        computed(() => {
            let sum = 0
            for (const a of atoms) sum += a()
            return sum
        })
        let i = 0
        bench('batch write 10 atoms with 1 computed subscriber', () => {
            batch(() => {
                for (const a of atoms) a(i)
                i++
            })
        })
    }

    bench('create + destroy computed', () => {
        const a = atom(0)
        const internal = new Computed(() => a() + 1)
        internal.run([], true)
        internal.destroy()
    })
})

describe('RxList incremental patches', () => {
    const LENGTH = 1000

    {
        const source = new RxList(Array.from({length: LENGTH}, (_, i) => ({value: i})))
        source.map(item => ({value: item.value + 1}))
        bench('push+pop with map subscriber', () => {
            source.push({value: 1})
            source.pop()
        })
    }

    {
        const source = new RxList(Array.from({length: LENGTH}, (_, i) => ({value: i})))
        source.map(item => ({value: item.value + 1}))
        let i = 0
        bench('splice replace middle with map subscriber', () => {
            source.splice(LENGTH >> 1, 1, {value: ++i})
        })
    }

    {
        const source = new RxList(Array.from({length: LENGTH}, (_, i) => ({value: i})))
        source.map(item => ({value: item.value + 1}))
        let i = 0
        bench('set() in-bounds with map subscriber', () => {
            source.set(LENGTH >> 1, {value: ++i})
        })
    }

    {
        const source = new RxList(Array.from({length: LENGTH}, (_, i) => i))
        const found = source.findIndex(item => item === LENGTH - 1)
        found()
        bench('findIndex patch via push+pop', () => {
            source.push(LENGTH * 2)
            source.pop()
        })
    }

    {
        const source = new RxList(Array.from({length: LENGTH}, (_, i) => i))
        source.slice(10, 20)
        let i = 0
        bench('slice(10,20) patch via splice inside range', () => {
            source.splice(15, 1, ++i)
        })
    }

    {
        const source = new RxList(Array.from({length: 100}, (_, i) => i % 10))
        source.groupBy(item => item)
        bench('groupBy patch via push+pop', () => {
            source.push(3)
            source.pop()
        })
    }

    bench('create + destroy RxList(100) with map', () => {
        const list = new RxList(Array.from({length: 100}, (_, i) => i))
        const mapped = list.map(item => item + 1)
        mapped.destroy()
        list.destroy()
    })
})

describe('RxList reads', () => {
    const LENGTH = 1000
    const source = new RxList(Array.from({length: LENGTH}, (_, i) => i))

    bench('at() read outside effect x100', () => {
        for (let i = 0; i < 100; i++) {
            source.at(i)
        }
    })

    {
        let i = 0
        const reader = new Computed(function (this: Computed) {
            let sum = 0
            for (let k = 0; k < 100; k++) sum += source.at(k)!
            return sum
        })
        reader.run([], true)
        bench('at() read x100 inside computed (tracked)', () => {
            source.set(0, ++i)
        })
    }
})
