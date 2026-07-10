import {bench, describe} from 'vitest'
import {atom, batch, computed, RxList, RxMap} from '../src/index'

/**
 * Structural benchmarks complementing core.bench.ts:
 * propagation topology (diamond/deep chain), iteration granularity,
 * derived structure creation cost. Compare before/after when touching
 * notify/computed/RxList internals.
 */

describe('propagation topology', () => {
    {
        const source = atom(0)
        const left = computed(() => source() + 1)
        const right = computed(() => source() * 2)
        const bottom = computed(() => left() + right())
        bottom()
        let i = 0
        bench('diamond a->(b,c)->d, single write', () => {
            source(++i)
        })
    }

    {
        const source = atom(0)
        let last = source
        for (let d = 0; d < 50; d++) {
            const prev = last
            last = computed(() => prev() + 1)
        }
        let i = 0
        bench('chain of 50 computed, single write', () => {
            source(++i)
        })
    }

    {
        const source = atom(0)
        const subscribers = Array.from({length: 100}, () => computed(() => source() + 1))
        subscribers.forEach(s => s())
        let i = 0
        bench('fanout 1 atom -> 100 computed', () => {
            source(++i)
        })
    }

    {
        const atoms = Array.from({length: 100}, () => atom(0))
        computed(() => {
            let sum = 0
            for (const a of atoms) sum += a()
            return sum
        })
        let i = 0
        bench('batch write 100 atoms, 1 computed subscriber', () => {
            batch(() => {
                for (const a of atoms) a(i)
                i++
            })
        })
    }
})

describe('iteration granularity', () => {
    const LENGTH = 1000

    {
        const list = new RxList(Array.from({length: LENGTH}, (_, i) => i))
        computed(() => {
            let sum = 0
            list.forEach(v => { sum += v })
            return sum
        })
        let i = 0
        bench('forEach(1000) in computed, splice middle', () => {
            list.splice(LENGTH >> 1, 1, ++i)
        })
    }

    {
        const list = new RxList(Array.from({length: LENGTH}, (_, i) => i))
        computed(() => {
            let sum = 0
            for (const v of list) sum += v!
            return sum
        })
        let i = 0
        bench('for..of(1000) in computed, splice middle', () => {
            list.splice(LENGTH >> 1, 1, ++i)
        })
    }
})

describe('derived structures', () => {
    bench('filter(1000) create + destroy', () => {
        const list = new RxList(Array.from({length: 1000}, (_, i) => i))
        const filtered = list.filter(v => v % 2 === 0)
        filtered.destroy()
        list.destroy()
    })

    {
        const list = new RxList(Array.from({length: 1000}, (_, i) => i))
        const filtered = list.filter(v => v % 2 === 0)
        filtered.at(0)
        bench('filter(1000) patch via push+pop', () => {
            list.push(1001)
            list.pop()
        })
    }

    bench('RxMap create + destroy', () => {
        const map = new RxMap<string, number>({a: 1, b: 2})
        map.destroy()
    })

    bench('RxList create + destroy (no derived)', () => {
        const list = new RxList([1, 2, 3])
        list.destroy()
    })
})
