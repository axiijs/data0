import {describe, expect, test} from 'vitest'
import {atom, Atom, batch, RxList, destroyComputed, notifier} from '../src/index'
import {mulberry32} from './fuzzKit'

type Item = { id: number, flag: Atom<boolean>, n: Atom<number>, k: number }

function runSeed(seed: number, steps: number, opts: {dups: boolean}) {
    const rand = mulberry32(seed)
    let nextId = 1
    const pool: Item[] = []
    const mk = (): Item => {
        const it = {id: nextId++, flag: atom(rand() < 0.5), n: atom(Math.floor(rand() * 10)), k: Math.floor(rand() * 3)}
        pool.push(it)
        return it
    }
    const pick = (): Item => (opts.dups && pool.length && rand() < 0.3) ? pool[Math.floor(rand() * pool.length)] : mk()
    const source = new RxList<Item>([mk(), mk(), mk(), mk()])
    const history: string[] = []

    const derived: [string, () => any, () => any][] = []
    const add = (name: string, actual: () => any, model: () => any) => derived.push([name, actual, model])
    const S = () => source.data

    const f1 = source.filter(it => it.flag())
    add('filter(flag)', () => f1.data.map(i => i.id), () => S().filter(i => i.flag.raw).map(i => i.id))
    const m1 = source.map(it => it.n() * 2)
    add('map(n*2)', () => m1.data, () => S().map(i => i.n.raw * 2))
    const m2 = source.map((it, i) => `${it.id}:${it.n()}@${i()}`)
    add('map(n,index)', () => m2.data, () => S().map((i, x) => `${i.id}:${i.n.raw}@${x}`))
    const m2b = source.map((it, i) => `${it.id}@${i()}`)
    add('map(index only)', () => m2b.data, () => S().map((i, x) => `${i.id}@${x}`))
    // lifecycle accounting
    const live = new Map<number, number>()
    let cleanupErrors = 0
    let token = 0
    const m3 = source.map((it, _i, ctx) => {
        const t = ++token
        live.set(t, it.id)
        ctx.onCleanup(() => {
            if (!live.has(t)) cleanupErrors++
            live.delete(t)
        })
        return it.n() + 1
    })
    add('map(ctx) data', () => m3.data, () => S().map(i => i.n.raw + 1))
    add('map(ctx) live cleanups', () => [...live.values()].sort((a, b) => a - b), () => S().map(i => i.id).sort((a, b) => a - b))
    add('map(ctx) cleanupErrors', () => cleanupErrors, () => 0)

    // options.onCleanup accounting: every mapFn call returns a fresh token; tokens not yet cleaned === tokens in the list
    const optLive = new Set<object>()
    let optErrors = 0
    const m4 = source.map(it => { const tk = {v: it.n()}; optLive.add(tk); return tk }, {
        onCleanup: (tk: object) => { if (!optLive.has(tk)) optErrors++; optLive.delete(tk) },
    })
    add('map(opt cleanup) data', () => m4.data.map(t => (t as any).v), () => S().map(i => i.n.raw))
    add('map(opt cleanup) live === in list', () => optLive.size === m4.data.length && m4.data.every(t => optLive.has(t)), () => true)
    add('map(opt cleanup) errors', () => optErrors, () => 0)

    const c1 = f1.map(it => it.n())
    add('filter.map(n)', () => c1.data, () => S().filter(i => i.flag.raw).map(i => i.n.raw))
    const c2src = source.map(it => ({it, v: it.n()}))
    const c2 = c2src.filter(x => x.v % 2 === 0)
    add('map.filter', () => c2.data.map(x => `${x.it.id}:${x.v}`), () => S().filter(i => i.n.raw % 2 === 0).map(i => `${i.id}:${i.n.raw}`))
    const c3 = f1.toSorted((a, b) => a.id - b.id)
    add('filter.toSorted', () => c3.data.map(i => i.id), () => S().filter(i => i.flag.raw).slice().sort((a, b) => a.id - b.id).map(i => i.id))
    const c4 = f1.slice(1, 4)
    add('filter.slice(1,4)', () => c4.data.map(i => i.id), () => S().filter(i => i.flag.raw).slice(1, 4).map(i => i.id))
    const c5 = f1.groupBy(it => it.k)
    add('filter.groupBy', () => JSON.stringify([...c5.data.entries()].map(([k, l]) => [k, l.data.map(i => i.id)]).sort((a: any, b: any) => a[0] - b[0])),
        () => {
            const m = new Map<number, number[]>()
            for (const i of S().filter(i => i.flag.raw)) {
                if (!m.has(i.k)) m.set(i.k, [])
                m.get(i.k)!.push(i.id)
            }
            return JSON.stringify([...m.entries()].sort((a, b) => a[0] - b[0]))
        })
    const c6 = f1.findIndex(it => it.n() > 5)
    add('filter.findIndex(n>5)', () => c6.raw, () => S().filter(i => i.flag.raw).findIndex(i => i.n.raw > 5))
    const c7 = source.find(it => it.flag())
    add('find(flag)', () => c7.raw?.id, () => S().find(i => i.flag.raw)?.id)
    const c7b = source.findIndex(it => it.k === 1)
    add('findIndex(k==1)', () => c7b.raw, () => S().findIndex(i => i.k === 1))
    const c7c = source.find(it => it.k === 1)
    add('find(k==1)', () => c7c.raw?.id, () => S().find(i => i.k === 1)?.id)
    const c8 = source.some(it => it.n() > 7)
    add('some(n>7)', () => c8.raw, () => S().some(i => i.n.raw > 7))
    const c9 = source.every(it => it.n() < 9)
    add('every(n<9)', () => c9.raw, () => S().every(i => i.n.raw < 9))
    const d1a = f1.map(it => it.n()); const d1 = d1a.filter(v => v % 2 === 0)
    add('filter.map.filter', () => d1.data, () => S().filter(i => i.flag.raw).map(i => i.n.raw).filter(v => v % 2 === 0))
    const d2a = source.toSorted((a, b) => a.id - b.id); const d2b = d2a.slice(0, 3); const d2 = d2b.map((x, i) => `${x.id}@${i()}:${x.n()}`)
    add('toSorted.slice.map', () => d2.data, () => S().slice().sort((a, b) => a.id - b.id).slice(0, 3).map((x, i) => `${x.id}@${i}:${x.n.raw}`))
    const d3a = m1.toSorted((a, b) => a - b); const d3 = d3a.slice(1)
    add('map.toSorted.slice', () => d3.data, () => S().map(i => i.n.raw * 2).sort((a, b) => a - b).slice(1))
    const d4a = f1.concat(source); const d4 = d4a.map((it, i) => `${it.id}@${i()}`)
    add('filter.concat(source).map', () => d4.data, () => S().filter(i => i.flag.raw).concat(S()).map((it, i) => `${it.id}@${i}`))
    const sum = source.reduceToAtom((acc: number, it) => acc + it.id, 0)
    add('reduceToAtom', () => sum.raw, () => S().reduce((a, i) => a + i.id, 0))
    const selAtom = atom<Item | null>(null)
    const sel = f1.createSelection(selAtom as any)
    add('filter.createSelection', () => sel.data.map(r => `${r[0].id}:${r[1].raw}`), () => S().filter(i => i.flag.raw).map(i => `${i.id}:${i === selAtom.raw}`))
    const idxAtom = atom<number | null>(1)
    const isel = f1.createIndexKeySelection(idxAtom)
    add('filter.createIndexKeySelection', () => isel.data.map(r => `${r[0].id}:${r[1].raw}`), () => S().filter(i => i.flag.raw).map((i, x) => `${i.id}:${x === idxAtom.raw}`))
    const len = f1.length
    add('filter.length', () => len.raw, () => S().filter(i => i.flag.raw).length)
    const set1 = f1.toSet()
    add('filter.toSet', () => [...set1.data].map(i => i.id).sort((a, b) => a - b), () => [...new Set(S().filter(i => i.flag.raw))].map(i => i.id).sort((a, b) => a - b))
    const last = computedAt(f1)
    add('filter.at(-1)/at(0)', () => last(), () => { const f = S().filter(i => i.flag.raw); return `${f[0]?.id}|${f[f.length - 1]?.id}` })

    function computedAt(list: RxList<Item>) {
        // plain getter through at() without effect
        return () => `${list.at(0)?.id}|${list.at(-1)?.id}`
    }

    const check = (stage: string) => {
        for (const [name, actual, model] of derived) {
            const a = JSON.stringify(actual())
            const m = JSON.stringify(model())
            if (a !== m) {
                throw new Error(`DIVERGE seed=${seed} dups=${opts.dups} at ${stage} [${name}]\n actual=${a}\n model =${m}\n history=\n  ${history.join('\n  ')}`)
            }
        }
    }

    const doOp = () => {
        const len = source.data.length
        const r = rand()
        if (r < 0.10) { const it = pick(); history.push(`push(#${it.id})`); source.push(it) }
        else if (r < 0.16) { const it = pick(); history.push(`unshift(#${it.id})`); source.unshift(it) }
        else if (r < 0.28) {
            const start = Math.floor(rand() * (len + 2)) - (rand() < 0.2 ? len : 0)
            const dc = Math.floor(rand() * 3)
            const items = Array.from({length: Math.floor(rand() * 3)}, () => pick())
            history.push(`splice(${start},${dc},[${items.map(i => '#' + i.id)}])`)
            source.splice(start, dc, ...items)
        }
        else if (r < 0.32 && len) { history.push('pop'); source.pop() }
        else if (r < 0.36 && len) { history.push('shift'); source.shift() }
        else if (r < 0.44 && len) { const i = Math.floor(rand() * len); const it = rand() < 0.2 ? source.data[i] : pick(); history.push(`set(${i},#${it.id})`); source.set(i, it) }
        else if (r < 0.50 && len >= 2) {
            const limit = 1 + Math.floor(rand() * Math.floor(len / 2))
            const a = Math.floor(rand() * (len - 2 * limit + 1))
            const b = a + limit + Math.floor(rand() * (len - a - 2 * limit + 1))
            history.push(`swap(${a},${b},${limit})`); source.swap(a, b, limit)
        }
        else if (r < 0.56 && len >= 2) {
            const limit = 1 + Math.floor(rand() * (len - 1))
            const a = Math.floor(rand() * (len - limit + 1))
            const b = Math.floor(rand() * (len - limit + 1))
            history.push(`reposition(${a},${b},${limit})`); source.reposition(a, b, limit)
        }
        else if (r < 0.60 && len >= 2) { history.push('sortSelf(id desc)'); source.sortSelf((a, b) => b.id - a.id) }
        else if (r < 0.63 && len >= 3) {
            const a = Math.floor(rand() * len); const b = (a + 1) % len; const c = (a + 2) % len
            history.push(`reorder 3-cycle ${a},${b},${c}`); source.reorder([[a, b], [b, c], [c, a]])
        }
        else if (r < 0.66) {
            const items = Array.from({length: Math.floor(rand() * 5)}, () => pick())
            history.push(`replaceAll([${items.map(i => '#' + i.id)}])`); source.spliceArray(0, source.data.length, items)
        }
        else if (r < 0.68) { history.push('clear'); source.clear() }
        else if (r < 0.82) { const it = pool[Math.floor(rand() * pool.length)]; history.push(`flag(#${it.id})=${!it.flag.raw}`); it.flag(!it.flag.raw) }
        else if (r < 0.94) { const it = pool[Math.floor(rand() * pool.length)]; const v = Math.floor(rand() * 10); history.push(`n(#${it.id})=${v}`); it.n(v) }
        else if (r < 0.97) { const f = f1.data; const v = f.length && rand() < 0.8 ? f[Math.floor(rand() * f.length)] : null; history.push(`sel=${v?.id}`); selAtom(v) }
        else { const v = rand() < 0.2 ? null : Math.floor(rand() * 5); history.push(`idx=${v}`); idxAtom(v) }
    }

    check('init')
    for (let step = 0; step < steps; step++) {
        try {
            if (rand() < 0.4) {
                const n = 2 + Math.floor(rand() * 3)
                history.push('batch{')
                batch(() => { for (let i = 0; i < n; i++) doOp() })
                history.push('}')
            } else {
                doOp()
            }
        } catch (e: any) {
            throw new Error(`THROW seed=${seed} dups=${opts.dups} step ${step}: ${e.message}\n history=\n  ${history.join('\n  ')}\n src=${source.data.map(i => `#${i.id}(f=${i.flag.raw},n=${i.n.raw},k=${i.k})`)}`)
        }
        check(`step ${step}`)
    }
    // teardown accounting
    ;[d1, d1a, d2, d2b, d2a, d3, d3a, d4, d4a, sel, isel, c1, c2, c2src, c3, c4, c5, m1, m2, m2b, m3, m4, f1].forEach(d => d.destroy())
    if (optLive.size !== 0 || optErrors) throw new Error(`OPT CLEANUP LEAK seed=${seed}: live=${optLive.size} errors=${optErrors}`)
    if (live.size !== 0 || cleanupErrors) throw new Error(`LEAK seed=${seed}: live=${live.size} cleanupErrors=${cleanupErrors}`)
    ;[c6, c7, c7b, c7c, c8, c9, sum].forEach(c => destroyComputed(c))
    set1.destroy()
    const leaks: string[] = []
    const count = (t: any) => { const e = notifier.getDepEffects(t); return e ? [...e].filter(x => x.active).length + [...e].filter(x => !x.active).length * 1000 : 0 }
    // f1.length is a lazily-created meta of a destroyed list: should be gone too
    if (count(source)) leaks.push(`source:${count(source)}`)
    for (const it of pool) { if (count(it.flag)) leaks.push(`flag#${it.id}:${count(it.flag)}`); if (count(it.n)) leaks.push(`n#${it.id}:${count(it.n)}`) }
    if (count(selAtom)) leaks.push(`selAtom:${count(selAtom)}`)
    if (count(idxAtom)) leaks.push(`idxAtom:${count(idxAtom)}`)
    if (source.atomIndexes !== undefined || source.atomIndexesDepCount !== 0) leaks.push(`atomIndexes:${source.atomIndexes?.length}/${source.atomIndexesDepCount}`)
    source.push(mk())
    if (source.pruneIndexKeyDeps()) leaks.push(`indexKeyDeps:${source._indexKeyDeps?.size}`)
    if (leaks.length) throw new Error(`SUBSCRIPTION LEAK seed=${seed} dups=${opts.dups}: ${leaks.slice(0, 12).join(' ')}\n history=\n  ${history.join('\n  ')}`)
}

describe('audit fuzz 1: reactive rows x structural x batch', () => {
    for (const dups of [false, true]) {
        test(`dups=${dups}`, () => {
            const failures: string[] = []
            for (let seed = 1; seed <= 300; seed++) {
                try {
                    runSeed(seed, 60, {dups})
                } catch (e: any) {
                    failures.push(String(e.message).slice(0, 3000))
                    if (failures.length >= 3) break
                }
            }
            if (failures.length) console.log(failures.join('\n\n=====\n\n'))
            expect(failures.length).toBe(0)
        })
    }
})
