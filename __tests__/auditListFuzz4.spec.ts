import {describe, expect, test} from 'vitest'
import {atom, batch, computed, RxList, RxSet} from '../src/index'
import {mulberry32, adversarialSpliceStart, adversarialSpliceDeleteCount} from './fuzzKit'

// weird value domain: undefined / NaN / -0 / duplicates / shared object refs, three-deep chains
const enc = (v: any): string => {
    if (Array.isArray(v)) return '[' + v.map(enc).join(',') + ']'
    if (v === undefined) return 'u'
    if (typeof v === 'number') return Object.is(v, -0) ? '-0' : String(v)
    if (v && typeof v === 'object') return '{' + Object.keys(v).map(k => k + ':' + enc(v[k])).join(',') + '}'
    return JSON.stringify(v)
}
// total order: numbers ascending, -0 < 0, NaN after numbers, undefined last (Array#sort never passes undefined)
const cmp = (a: any, b: any) => {
    const rank = (x: any) => x === undefined ? 3 : Number.isNaN(x) ? 2 : 1
    if (rank(a) !== rank(b)) return rank(a) - rank(b)
    if (rank(a) !== 1) return 0
    if (a === b) return (Object.is(a, -0) ? 0 : 1) - (Object.is(b, -0) ? 0 : 1)
    return a - b
}
const key = (x: any) => x === undefined ? 'u' : Number.isNaN(x) ? 'nan' : Math.abs(x) % 3

function runSeed(seed: number, steps: number) {
    const rand = mulberry32(seed)
    let counter = 10
    const nextVal = (): any => {
        const r = rand()
        if (r < 0.15) return undefined
        if (r < 0.25) return NaN
        if (r < 0.35) return -0
        if (r < 0.45) return 0
        if (r < 0.7) return Math.floor(rand() * 5)
        return counter++
    }
    const source = new RxList<any>([3, undefined, NaN, -0, 0, 1])
    const history: string[] = []
    const S = () => source.data
    const mods: [string, RxList<any> | (() => any), () => any][] = []
    const add = (n: string, a: any, m: () => any) => mods.push([n, a, m])
    const gb = (arr: any[]) => { const m = new Map<any, any[]>(); for (const x of arr) { const k = key(x); if (!m.has(k)) m.set(k, []); m.get(k)!.push(x) } return m }

    add('map.filter.toSorted', source.map(x => x === undefined ? undefined : x * 1).filter(x => x !== 1).toSorted(cmp), () => S().map(x => x === undefined ? undefined : x * 1).filter(x => x !== 1).sort(cmp))
    add('toSorted.slice.map(i)', source.toSorted(cmp).slice(1, 5).map((x, i) => [x, i()]), () => S().slice().sort(cmp).slice(1, 5).map((x, i) => [x, i]))
    add('filter.slice.toSorted', source.filter(x => x !== 2).slice(1, 4).toSorted(cmp), () => S().filter(x => x !== 2).slice(1, 4).sort(cmp))
    add('slice.concat.filter', source.slice(0, 3).concat(source.slice(2)).filter(x => !Number.isNaN(x)), () => S().slice(0, 3).concat(S().slice(2)).filter(x => !Number.isNaN(x)))
    const g = source.filter(x => x !== 3).groupBy(key)
    add('filter.groupBy', () => [...g.data.entries()].map(([k, l]) => [k, l.data]).sort((a: any, b: any) => String(a[0]).localeCompare(String(b[0]))), () => [...gb(S().filter(x => x !== 3)).entries()].sort((a, b) => String(a[0]).localeCompare(String(b[0]))))
    const st = source.map(x => x).filter(x => x !== 4).toSet()
    add('map.filter.toSet', () => [...st.data].sort(cmp), () => [...new Set(S().filter(x => x !== 4))].sort(cmp))
    const fi = source.toSorted(cmp).slice(1).findIndex(x => Number.isNaN(x))
    add('toSorted.slice.findIndex(NaN)', () => fi.raw, () => S().slice().sort(cmp).slice(1).findIndex(x => Number.isNaN(x)))
    const fd = source.filter(x => x !== 0).find(x => x === undefined || Object.is(x, -0))
    add('filter.find', () => [fd.raw], () => [S().filter(x => x !== 0).find(x => x === undefined || Object.is(x, -0))])
    const selSet = new RxSet<any>([NaN, 0])
    const sel = source.filter(x => x !== 1).createSelection(selSet)
    add('filter.createSelection(set)', () => sel.data.map(r => [r[0], r[1].raw]), () => S().filter(x => x !== 1).map(x => [x, selSet.data.has(x)]))
    const selAtom = atom<any>(NaN)
    const sel2 = source.toSorted(cmp).createSelection(selAtom)
    add('toSorted.createSelection(atom)', () => sel2.data.map(r => [r[0], r[1].raw]), () => S().slice().sort(cmp).map(x => [x, x === selAtom.raw || (x !== x && selAtom.raw !== selAtom.raw)]))
    const red = source.filter(x => x !== undefined).reduceToAtom((acc: number, x: number) => acc + (Number.isNaN(x) ? 100 : x), 0)
    add('filter.reduceToAtom', () => red.raw, () => S().filter(x => x !== undefined).reduce((acc: number, x: number) => acc + (Number.isNaN(x) ? 100 : x), 0))
    const sorted2 = source.toSorted(cmp)
    const at3 = computed(() => [sorted2.at(1), sorted2.at(-1), sorted2.at(0)])
    add('toSorted.at', () => at3.raw, () => { const s = S().slice().sort(cmp); return [s.at(1), s.at(-1), s.at(0)] })

    const check = (stage: string) => {
        for (const [name, a, m] of mods) {
            const av = enc(typeof a === 'function' ? a() : a.data), mv = enc(m())
            if (av !== mv) throw new Error(`DIVERGE seed=${seed} at ${stage} [${name}]\n actual=${av}\n model =${mv}\n src=${enc(S())}\n history=\n  ${history.join('\n  ')}`)
        }
    }
    const doOp = () => {
        const list = source, len = list.data.length, r = rand()
        if (r < 0.22) {
            const start = adversarialSpliceStart(rand, len), dc = adversarialSpliceDeleteCount(rand, len)
            const items = Array.from({length: Math.floor(rand() * 3)}, nextVal)
            history.push(`splice(${start},${dc},${enc(items)})`); list.splice(start, dc, ...items)
        } else if (r < 0.32) { const v = nextVal(); history.push(`push(${enc(v)})`); list.push(v) }
        else if (r < 0.38) { const v = nextVal(); history.push(`unshift(${enc(v)})`); list.unshift(v) }
        else if (r < 0.44) { history.push(`pop`); list.pop() }
        else if (r < 0.50) { history.push(`shift`); list.shift() }
        else if (r < 0.64 && len) { const i = Math.floor(rand() * len), v = rand() < 0.2 ? list.data[i] : nextVal(); history.push(`set(${i},${enc(v)})`); list.set(i, v) }
        else if (r < 0.70 && len >= 2) { const a = Math.floor(rand() * (len - 1)), b = a + 1 + Math.floor(rand() * (len - a - 1)); history.push(`swap(${a},${b})`); list.swap(a, b) }
        else if (r < 0.76 && len >= 2) { const limit = 1 + Math.floor(rand() * (len - 1)); const a = Math.floor(rand() * (len - limit + 1)), b = Math.floor(rand() * (len - limit + 1)); history.push(`reposition(${a},${b},${limit})`); list.reposition(a, b, limit) }
        else if (r < 0.80 && len >= 2) {
            // sortSelf must equal Array#sort with the same comparator (undefined never reaches it, always last)
            const before = list.data.slice()
            const hostile = rand() < 0.5
            const numeric = (x: any) => Number.isNaN(x) ? 1e9 : x
            const comparator = hostile
                ? (a: any, b: any) => { if (a === undefined || b === undefined) throw new Error('comparator received undefined'); return numeric(b) - numeric(a) || (Object.is(a, -0) ? 1 : 0) - (Object.is(b, -0) ? 1 : 0) }
                : (a: any, b: any) => cmp(b, a)
            history.push(`sortSelf(${hostile ? 'hostile' : 'desc'})`); list.sortSelf(comparator)
            const expected = before.slice().sort(comparator)
            if (enc(list.data) !== enc(expected)) throw new Error(`DIVERGE seed=${seed} sortSelf != Array#sort\n actual=${enc(list.data)}\n native=${enc(expected)}\n before=${enc(before)}`)
        }
        else if (r < 0.85) { const items = Array.from({length: Math.floor(rand() * 6)}, nextVal); history.push(`replaceAll(${enc(items)})`); list.spliceArray(0, list.data.length, items) }
        else if (r < 0.87) { history.push(`clear`); list.clear() }
        else if (r < 0.93) { const v = nextVal(); history.push(`selSet toggle ${enc(v)}`); selSet.data.has(v) ? selSet.delete(v) : selSet.add(v) }
        else { const v = nextVal(); history.push(`selAtom=${enc(v)}`); selAtom(v) }
    }
    check('init')
    for (let step = 0; step < steps; step++) {
        try {
            if (rand() < 0.35) { history.push('batch{'); batch(() => { const n = 2 + Math.floor(rand() * 3); for (let i = 0; i < n; i++) doOp() }); history.push('}') }
            else doOp()
        } catch (e: any) {
            throw new Error(`THROW seed=${seed} step ${step}: ${e.message}\n${String(e.stack).split('\n').slice(1, 8).join('\n')}\n history=\n  ${history.join('\n  ')}`)
        }
        check(`step ${step}`)
    }
}

describe('audit fuzz 4: weird values x 3-deep chains', () => {
    test('seeds', () => {
        const failures: string[] = []
        for (let seed = 1; seed <= 400; seed++) {
            try { runSeed(seed, 60) } catch (e: any) {
                failures.push(String(e.message).slice(0, 2500))
                if (failures.length >= 3) break
            }
        }
        if (failures.length) console.log(failures.join('\n\n=====\n\n'))
        expect(failures.length).toBe(0)
    })
})
