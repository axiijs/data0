import {describe, expect, test} from 'vitest'
import {atom, batch, RxList, RxMap, RxSet} from '../src/index'
import {mulberry32} from './fuzzKit'

type Row = { id: number, k: number }

// unique-key rows: indexBy / toMap / reduce(RxMap) / groupBy group observers / selections with autoReset
function runSeed(seed: number, steps: number) {
    const rand = mulberry32(seed)
    let nextId = 1
    const mk = (): Row => ({id: nextId++, k: Math.floor(rand() * 3)})
    const source = new RxList<Row>([mk(), mk(), mk(), mk()])
    const history: string[] = []
    const S = () => source.data

    const byId = source.indexBy('id')
    const byFn = source.indexBy(r => `k${r.id}`)
    const pairs = source.map(r => [r.id, r.k] as [number, number])
    const asMap = pairs.toMap()
    const red = source.reduce<RxMap<number, number>>((acc, r) => { acc.set(r.id, r.k) }, RxMap as any) as unknown as RxMap<number, number>
    const grouped = source.groupBy(r => r.k)
    const keysSorted = grouped.keys().toSorted((a, b) => a - b)
    // AUDIT_FUZZ5_SAFE_ENTRIES=1: tolerate the transient [key, undefined] row that RxMap.entries() emits on
    // delete (RxMap dispatch-order bug, owned by the collections reviewer) so the rest of the driver still runs.
    const safeEntries = process.env.AUDIT_FUZZ5_SAFE_ENTRIES === '1'
    const sizes = grouped.entries().map(([k, l]) => `${k}:${safeEntries && l === undefined ? '?' : l.length()}`)
    // selections with autoReset
    const selAtom = atom<Row | null>(null)
    const sel = source.createSelection(selAtom as any, true)
    const selSet = new RxSet<Row>([])
    const sel2 = source.createSelection(selSet as any, true)
    const idxAtom = atom<number | null>(null)
    const isel = source.createIndexKeySelection(idxAtom, true)
    const idxSet = new RxSet<number>([])
    const isel2 = source.createIndexKeySelection(idxSet, true)
    // autoReset models: reset only happens through removal events
    let mSelAtom: Row | null = null
    let mSelSet = new Set<Row>()
    let mIdx: number | null = null
    let mIdxSet = new Set<number>()
    let removedInStep: Row[] = []
    let splicedInStep = false

    const settle = () => {
        for (const r of removedInStep) {
            if (!S().includes(r)) {
                if (mSelAtom === r) mSelAtom = null
                mSelSet.delete(r)
            }
        }
        if (splicedInStep) {
            if (mIdx !== null && mIdx >= S().length) mIdx = null
            for (const i of [...mIdxSet]) if (i >= S().length) mIdxSet.delete(i)
        }
        removedInStep = []; splicedInStep = false
    }
    const gmodel = () => { const m = new Map<number, Row[]>(); for (const r of S()) { if (!m.has(r.k)) m.set(r.k, []); m.get(r.k)!.push(r) } return m }
    const check = (stage: string) => {
        const eq = (name: string, a: any, m: any) => { if (JSON.stringify(a) !== JSON.stringify(m)) throw new Error(`DIVERGE seed=${seed} at ${stage} [${name}]\n actual=${JSON.stringify(a)}\n model =${JSON.stringify(m)}\n src=${JSON.stringify(S())}\n history=\n  ${history.join('\n  ')}`) }
        const sortE = (e: any[]) => e.slice().sort((a, b) => String(a[0]).localeCompare(String(b[0])))
        eq('indexBy(prop)', sortE([...byId.data].map(([k, v]) => [k, v.id])), sortE(S().map(r => [r.id, r.id])))
        eq('indexBy(fn)', sortE([...byFn.data].map(([k, v]) => [k, v.id])), sortE(S().map(r => [`k${r.id}`, r.id])))
        eq('toMap', sortE([...asMap.data]), sortE(S().map(r => [r.id, r.k])))
        eq('reduce(RxMap)', sortE([...red.data]), sortE(S().map(r => [r.id, r.k])))
        const gm = gmodel()
        eq('groupBy', sortE([...grouped.data].map(([k, l]) => [k, l.data.map(r => r.id)])), sortE([...gm].map(([k, l]) => [k, l.map(r => r.id)])))
        eq('groupBy.keys.toSorted', keysSorted.data, [...gm.keys()].sort((a, b) => a - b))
        eq('groupBy.entries.map(len)', sizes.data.slice().sort(), [...gm].map(([k, l]) => `${k}:${l.length}`).sort())
        eq('sel atom value', selAtom.raw?.id ?? null, mSelAtom?.id ?? null)
        eq('sel atom rows', sel.data.map(r => [r[0].id, r[1].raw]), S().map(r => [r.id, r === mSelAtom]))
        eq('sel set value', [...selSet.data].map(r => r.id).sort(), [...mSelSet].map(r => r.id).sort())
        eq('sel set rows', sel2.data.map(r => [r[0].id, r[1].raw]), S().map(r => [r.id, mSelSet.has(r)]))
        eq('idx atom value', idxAtom.raw, mIdx)
        eq('idx atom rows', isel.data.map(r => [r[0].id, r[1].raw]), S().map((r, i) => [r.id, i === mIdx]))
        eq('idx set value', [...idxSet.data].sort(), [...mIdxSet].sort())
        eq('idx set rows', isel2.data.map(r => [r[0].id, r[1].raw]), S().map((r, i) => [r.id, mIdxSet.has(i)]))
    }
    const doOp = () => {
        const len = S().length, r = rand()
        if (r < 0.12) { const it = mk(); history.push(`push(#${it.id})`); source.push(it); splicedInStep = true }
        else if (r < 0.18) { const it = mk(); history.push(`unshift(#${it.id})`); source.unshift(it); splicedInStep = true }
        else if (r < 0.30) {
            const start = Math.floor(rand() * (len + 1)), dc = Math.floor(rand() * 3)
            const items = Array.from({length: Math.floor(rand() * 3)}, mk)
            history.push(`splice(${start},${dc},[${items.map(i => '#' + i.id)}])`); removedInStep.push(...source.splice(start, dc, ...items)); splicedInStep = true
        }
        else if (r < 0.36 && len) { history.push('pop'); removedInStep.push(source.pop()!); splicedInStep = true }
        else if (r < 0.42 && len) { history.push('shift'); removedInStep.push(source.shift()!); splicedInStep = true }
        else if (r < 0.50 && len) { const i = Math.floor(rand() * len); const it = rand() < 0.15 ? S()[i] : mk(); history.push(`set(${i},#${it.id})`); removedInStep.push(source.set(i, it)) }
        else if (r < 0.55 && len >= 2) { const a = Math.floor(rand() * (len - 1)), b = a + 1 + Math.floor(rand() * (len - a - 1)); history.push(`swap(${a},${b})`); source.swap(a, b) }
        else if (r < 0.58 && len >= 2) { history.push('sortSelf'); source.sortSelf((a, b) => b.id - a.id) }
        else if (r < 0.60 && len) { // move row: remove then re-insert (never a transient duplicate)
            const i = Math.floor(rand() * len); const j = Math.floor(rand() * len); batch(() => { const [row] = source.splice(i, 1); source.splice(j, 0, row) }); splicedInStep = true
            history.push(`move(${i}->${j})`)
        }
        else if (r < 0.62) { history.push('clear'); if (len) splicedInStep = true; removedInStep.push(...source.clear()) }
        else if (r < 0.72) { const v = len && rand() < 0.85 ? S()[Math.floor(rand() * len)] : null; history.push(`selAtom=#${v?.id}`); selAtom(v); mSelAtom = v }
        else if (r < 0.84 && len) { const v = S()[Math.floor(rand() * len)]; history.push(`selSet toggle #${v.id}`); if (mSelSet.has(v)) { mSelSet.delete(v); selSet.delete(v) } else { mSelSet.add(v); selSet.add(v) } }
        else if (r < 0.92) { const v = rand() < 0.2 ? null : Math.floor(rand() * (len + 1)); history.push(`idx=${v}`); idxAtom(v); mIdx = v }
        else { const v = Math.floor(rand() * (len + 1)); history.push(`idxSet toggle ${v}`); if (mIdxSet.has(v)) { mIdxSet.delete(v); idxSet.delete(v) } else { mIdxSet.add(v); idxSet.add(v) } }
    }
    check('init')
    for (let step = 0; step < steps; step++) {
        try {
            if (rand() < 0.35) { history.push('batch{'); batch(() => { const n = 2 + Math.floor(rand() * 3); for (let i = 0; i < n; i++) doOp() }); history.push('}') }
            else doOp()
        } catch (e: any) {
            throw new Error(`THROW seed=${seed} step ${step}: ${e.message}\n${String(e.stack).split('\n').slice(1, 8).join('\n')}\n history=\n  ${history.join('\n  ')}`)
        }
        settle()
        check(`step ${step}`)
    }
}

describe('audit fuzz 5: keyed maps / groups / autoReset selections', () => {
    test('seeds', () => {
        const failures: string[] = []
        for (let seed = 1; seed <= 400; seed++) {
            try { runSeed(seed, 60) } catch (e: any) {
                failures.push(String(e.message).slice(0, 500) + "\n ...TAIL... " + String(e.message).slice(-500))
                if (failures.length >= 3) break
            }
        }
        if (failures.length) console.log(failures.join('\n\n=====\n\n'))
        expect(failures.length).toBe(0)
    })
})
