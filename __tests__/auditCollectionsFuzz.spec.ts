/**
 * Seeded differential driver used by the audit (green on the fixed tree; on the audited tree only the
 * strict-downstream assertion of the RxMap block fails, which is finding F1):
 * derived structure after a random mutation sequence (40% of the steps are 2-4 op batches)
 * === the same thing computed from scratch over the final source data.
 */
import {describe, expect, test} from 'vitest'
import {batch, computed, RxList, RxMap, RxSet} from '../src/index'

function mulberry32(seed: number) {
    let a = seed >>> 0
    return () => {
        a |= 0; a = a + 0x6D2B79F5 | 0
        let t = Math.imul(a ^ a >>> 15, 1 | a)
        t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t
        return ((t ^ t >>> 14) >>> 0) / 4294967296
    }
}
const norm = (x: any) => Object.is(x, -0) ? '-0' : Number.isNaN(x) ? 'NaN' : x
const sortN = (xs: any[]) => xs.map(norm).map(String).sort()

describe('fuzz: RxSet algebra stacked + batch', () => {
    for (let seed = 1; seed <= 60; seed++) {
        test(`seed=${seed}`, () => {
            const rand = mulberry32(seed)
            const val = () => Math.floor(rand() * 6)
            const a = new RxSet<number>([val(), val()])
            const b = new RxSet<number>([val(), val()])
            const c = new RxSet<number>([val()])
            const uni = a.union(b)
            const inter = a.intersection(b)
            const diff = a.difference(b)
            const sym = a.symmetricDifference(b)
            // stacked
            const uMinusA = uni.difference(a)       // == b \ a
            const symU = sym.union(inter)           // == a ∪ b
            const diffI = diff.intersection(c)
            const uniList = uni.toList()
            const uniListMapped = uniList.map(x => x * 2)
            const aListSet = a.toList().toSet()
            const selfU = a.union(a)
            const selfD = a.difference(a)
            const sub = a.isSubsetOf(b)
            const sup = a.isSupersetOf(b)
            const dis = a.isDisjointFrom(b)
            const has3 = a.has(3)
            const sizeU = uni.size
            const history: string[] = []
            const check = (ctx: string) => {
                const A = a.data, B = b.data, C = c.data
                const U = new Set([...A, ...B])
                expect(sortN([...uni.data]), `uni ${ctx}`).toEqual(sortN([...U]))
                expect(sortN([...inter.data]), `inter ${ctx}`).toEqual(sortN([...A].filter(x => B.has(x))))
                expect(sortN([...diff.data]), `diff ${ctx}`).toEqual(sortN([...A].filter(x => !B.has(x))))
                expect(sortN([...sym.data]), `sym ${ctx}`).toEqual(sortN([...[...A].filter(x => !B.has(x)), ...[...B].filter(x => !A.has(x))]))
                expect(sortN([...uMinusA.data]), `uMinusA ${ctx}`).toEqual(sortN([...B].filter(x => !A.has(x))))
                expect(sortN([...symU.data]), `symU ${ctx}`).toEqual(sortN([...U]))
                expect(sortN([...diffI.data]), `diffI ${ctx}`).toEqual(sortN([...A].filter(x => !B.has(x) && C.has(x))))
                expect(uniList.data, `uniList ${ctx}`).toEqual([...uni.data])
                expect(uniListMapped.data, `uniListMapped ${ctx}`).toEqual([...uni.data].map(x => x * 2))
                expect(sortN([...aListSet.data]), `aListSet ${ctx}`).toEqual(sortN([...A]))
                expect(sortN([...selfU.data]), `selfU ${ctx}`).toEqual(sortN([...A]))
                expect([...selfD.data], `selfD ${ctx}`).toEqual([])
                expect(sub.raw, `sub ${ctx}`).toBe([...A].every(x => B.has(x)))
                expect(sup.raw, `sup ${ctx}`).toBe([...B].every(x => A.has(x)))
                expect(dis.raw, `dis ${ctx}`).toBe([...A].every(x => !B.has(x)))
                expect(has3.raw, `has3 ${ctx}`).toBe(A.has(3))
                expect(sizeU.raw, `sizeU ${ctx}`).toBe(U.size)
            }
            const op = () => {
                const which = rand()
                const target = which < 0.45 ? a : which < 0.9 ? b : c
                const name = target === a ? 'a' : target === b ? 'b' : 'c'
                const r = rand()
                if (r < 0.4) { const v = val(); history.push(`${name}.add(${v})`); target.add(v) }
                else if (r < 0.75) { const v = val(); history.push(`${name}.delete(${v})`); target.delete(v) }
                else if (r < 0.92) { const arr = Array.from({length: Math.floor(rand() * 4)}, val); history.push(`${name}.replace(${JSON.stringify(arr)})`); target.replace(arr) }
                else { history.push(`${name}.clear()`); target.clear() }
            }
            check('init')
            for (let step = 0; step < 60; step++) {
                if (rand() < 0.4) {
                    const n = 2 + Math.floor(rand() * 3)
                    history.push('batch{')
                    batch(() => { for (let i = 0; i < n; i++) op() })
                    history.push('}')
                } else op()
                check(`seed=${seed} step=${step} recent=${history.slice(-8).join(';')}`)
            }
        })
    }
})

describe('fuzz: RxMap derived stacked + batch', () => {
    for (let seed = 1; seed <= 60; seed++) {
        test(`seed=${seed}`, () => {
            const rand = mulberry32(seed + 1000)
            const key = () => Math.floor(rand() * 6)
            const val = () => ({n: Math.floor(rand() * 100)})
            const m = new RxMap<number, {n: number}>([[key(), val()], [key(), val()]])
            const keys = m.keys()
            const values = m.values()
            const entries = m.entries()
            const size = m.size
            let strictErrors = 0
            const strict = values.map(v => v.n)   // throws on transient undefined
            const entryStr = entries.map(([k, v]) => `${k}:${v?.n}`)
            const keySet = keys.toSet()
            const keysSorted = keys.toSorted((x, y) => x - y)
            const back = entries.toMap()
            const iter = computed(() => { const out: any[] = []; m.forEach((v, k) => out.push([k, v.n])); return out })
            const get2 = computed(() => m.get(2)?.n)
            const history: string[] = []
            const check = (ctx: string) => {
                const E = [...m.data.entries()]
                expect(keys.data, `keys ${ctx}`).toEqual(E.map(e => e[0]))
                expect(values.data, `values ${ctx}`).toEqual(E.map(e => e[1]))
                expect(entries.data, `entries ${ctx}`).toEqual(E)
                expect(size.raw, `size ${ctx}`).toBe(E.length)
                expect(strict.data, `strict ${ctx}`).toEqual(E.map(e => e[1].n))
                expect(entryStr.data, `entryStr ${ctx}`).toEqual(E.map(e => `${e[0]}:${e[1].n}`))
                expect(sortN([...keySet.data]), `keySet ${ctx}`).toEqual(sortN(E.map(e => e[0])))
                expect(keysSorted.data, `keysSorted ${ctx}`).toEqual(E.map(e => e[0]).sort((x, y) => x - y))
                expect([...back.data.entries()].sort((x, y) => x[0] - y[0]), `back ${ctx}`).toEqual(E.slice().sort((x, y) => x[0] - y[0]))
                expect(iter.raw, `iter ${ctx}`).toEqual(E.map(e => [e[0], e[1].n]))
                expect(get2.raw, `get2 ${ctx}`).toBe(m.data.get(2)?.n)
            }
            const op = () => {
                const r = rand()
                if (r < 0.45) { const k = key(), v = val(); history.push(`set(${k},${v.n})`); m.set(k, v) }
                else if (r < 0.8) { const k = key(); history.push(`delete(${k})`); m.delete(k) }
                else if (r < 0.93) { const e = Array.from({length: Math.floor(rand() * 4)}, () => [key(), val()] as [number, {n: number}]); history.push(`replace(${JSON.stringify(e)})`); m.replace(e) }
                else { history.push('clear()'); m.clear() }
            }
            check('init')
            for (let step = 0; step < 60; step++) {
                try {
                    if (rand() < 0.4) {
                        const n = 2 + Math.floor(rand() * 3)
                        history.push('batch{')
                        batch(() => { for (let i = 0; i < n; i++) op() })
                        history.push('}')
                    } else op()
                } catch (e: any) {
                    if (/Cannot read properties of undefined/.test(e.message)) strictErrors++
                    else throw new Error(`unexpected ${e.message} seed=${seed} step=${step} recent=${history.slice(-8).join(';')}`)
                }
                check(`seed=${seed} step=${step} recent=${history.slice(-8).join(';')}`)
            }
            // F1: on the audited tree every delete of an existing key threw here (12-23 per seed);
            //  with the fix (audit-final.patch) a strict downstream never sees a phantom undefined row.
            expect(strictErrors, `seed=${seed} phantom undefined rows`).toBe(0)
            
        })
    }
})

describe('fuzz: RxList -> toSet/toMap/indexBy/groupBy stacked + batch', () => {
    for (let seed = 1; seed <= 60; seed++) {
        test(`seed=${seed}`, () => {
            const rand = mulberry32(seed + 5000)
            let nextId = 1
            const item = () => ({id: nextId++, g: Math.floor(rand() * 3)})
            const list = new RxList<{id: number, g: number}>([item(), item(), item()])
            const nums = list.map(x => x.g)
            const numSet = nums.toSet()
            const numSetList = numSet.toList()
            const byId = list.indexBy('id')
            const byIdKeys = byId.keys()
            const byIdValues = byId.values()
            const pairs = list.map(x => [x.id, x.g] as [number, number])
            const asMap = pairs.toMap()
            const asMapEntries = asMap.entries()
            const groups = list.groupBy(x => x.g)
            const groupKeys = groups.keys()
            const groupSize = groups.size
            const groupLens = groups.entries().map(([k, g]) => `${k}:${g ? g.length() : 'undef'}`)
            const history: string[] = []
            const check = (ctx: string) => {
                const D = list.data
                expect(sortN([...numSet.data]), `numSet ${ctx}`).toEqual(sortN([...new Set(D.map(x => x.g))]))
                expect(numSetList.data, `numSetList ${ctx}`).toEqual([...numSet.data])
                expect(sortN([...byId.data.keys()]), `byId ${ctx}`).toEqual(sortN(D.map(x => x.id)))
                for (const x of D) expect(byId.data.get(x.id), `byId.get ${ctx}`).toBe(x)
                expect(byIdKeys.data, `byIdKeys ${ctx}`).toEqual([...byId.data.keys()])
                expect(byIdValues.data, `byIdValues ${ctx}`).toEqual([...byId.data.values()])
                expect([...asMap.data.entries()].sort((x, y) => x[0] - y[0]), `asMap ${ctx}`).toEqual(D.map(x => [x.id, x.g]).sort((x, y) => x[0] - y[0]))
                expect(asMapEntries.data, `asMapEntries ${ctx}`).toEqual([...asMap.data.entries()])
                const model = new Map<number, any[]>()
                for (const x of D) { if (!model.has(x.g)) model.set(x.g, []); model.get(x.g)!.push(x) }
                expect(sortN([...groups.data.keys()]), `groups.keys ${ctx}`).toEqual(sortN([...model.keys()]))
                for (const [k, arr] of model) expect(groups.data.get(k)!.data, `group ${k} ${ctx}`).toEqual(arr)
                expect(groupKeys.data, `groupKeys ${ctx}`).toEqual([...groups.data.keys()])
                expect(groupSize.raw, `groupSize ${ctx}`).toBe(model.size)
                expect(groupLens.data, `groupLens ${ctx}`).toEqual([...groups.data.entries()].map(([k, g]) => `${k}:${g.data.length}`))
            }
            const op = () => {
                const r = rand()
                const len = list.data.length
                if (r < 0.3) { const it = item(); history.push(`push(${it.id}/${it.g})`); list.push(it) }
                else if (r < 0.45) { const it = item(); const i = Math.floor(rand() * (len + 1)); history.push(`splice(${i},0,${it.id}/${it.g})`); list.splice(i, 0, it) }
                else if (r < 0.65 && len) { const i = Math.floor(rand() * len); const n = 1 + Math.floor(rand() * 2); history.push(`splice(${i},${n})`); list.splice(i, n) }
                else if (r < 0.8 && len) { const i = Math.floor(rand() * len); const it = item(); history.push(`set(${i},${it.id}/${it.g})`); list.set(i, it) }
                else if (r < 0.9 && len > 1) { const i = Math.floor(rand() * len); let j = Math.floor(rand() * len); if (i === j) j = (j + 1) % len; history.push(`swap(${i},${j})`); list.swap(i, j) }
                else if (r < 0.95 && len) { const i = Math.floor(rand() * len); const j = Math.floor(rand() * len); history.push(`reposition(${i},${j})`); list.reposition(i, j) }
                else { history.push('clear'); list.clear() }
            }
            check('init')
            for (let step = 0; step < 60; step++) {
                if (rand() < 0.4) {
                    const n = 2 + Math.floor(rand() * 3)
                    history.push('batch{')
                    batch(() => { for (let i = 0; i < n; i++) op() })
                    history.push('}')
                } else op()
                check(`seed=${seed} step=${step} recent=${history.slice(-10).join(';')}`)
            }
        })
    }
})
