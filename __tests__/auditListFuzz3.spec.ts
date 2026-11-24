import {describe, expect, test} from 'vitest'
import {atom, Atom, batch, RxList} from '../src/index'
import {mulberry32} from './fuzzKit'

type Item = { id: number, flag: Atom<boolean>, n: Atom<number>, k: number, live: boolean }

// deferred-scheduler maps: recompute only when flushed; row-level recomputes happen in between
function runSeed(seed: number, steps: number) {
    const rand = mulberry32(seed)
    let nextId = 1
    const pool: Item[] = []
    const mk = (): Item => {
        const it = {id: nextId++, flag: atom(rand() < 0.5), n: atom(Math.floor(rand() * 10)), k: Math.floor(rand() * 3), live: rand() < 0.5}
        pool.push(it)
        return it
    }
    const source = new RxList<Item>([mk(), mk(), mk()])
    const history: string[] = []
    const S = () => source.data
    const pendingRecomputes: (() => void)[] = []
    const sched = (recompute: () => void) => { pendingRecomputes.push(recompute) }
    const flush = () => { const q = pendingRecomputes.splice(0); q.forEach(r => r()) }

    const cleanupCount = new Map<number, number>()
    let tok = 0
    const liveTok = new Map<number, number>()
    const m1 = source.map(it => it.live ? it.n() * 2 : -it.id, {scheduleRecompute: sched as any})
    const m2 = source.map((it, i) => `${it.id}:${it.live ? it.n() : 'x'}@${i()}`, {scheduleRecompute: sched as any})
    const m3 = source.map((it, _i, ctx) => {
        const t = ++tok; liveTok.set(t, it.id)
        ctx.onCleanup(() => { cleanupCount.set(t, (cleanupCount.get(t) ?? 0) + 1); liveTok.delete(t) })
        return it.live ? it.n() + 1 : 0
    }, {scheduleRecompute: sched as any})
    const m4 = m1.map(v => v + 1)
    const m5 = m1.filter(v => v % 2 === 0)

    const check = (stage: string) => {
        const pairs: [string, any, any][] = [
            ['m1', m1.data, S().map(it => it.live ? it.n.raw * 2 : -it.id)],
            ['m2', m2.data, S().map((it, i) => `${it.id}:${it.live ? it.n.raw : 'x'}@${i}`)],
            ['m3', m3.data, S().map(it => it.live ? it.n.raw + 1 : 0)],
            ['m3 live', [...liveTok.values()].sort((a, b) => a - b), S().map(i => i.id).sort((a, b) => a - b)],
            ['m3 dup cleanup', [...cleanupCount.values()].filter(c => c !== 1), []],
            ['m4', m4.data, S().map(it => (it.live ? it.n.raw * 2 : -it.id) + 1)],
            ['m5', m5.data, S().map(it => it.live ? it.n.raw * 2 : -it.id).filter(v => v % 2 === 0)],
        ]
        for (const [name, a, m] of pairs) {
            if (JSON.stringify(a) !== JSON.stringify(m)) throw new Error(`DIVERGE seed=${seed} at ${stage} [${name}]\n actual=${JSON.stringify(a)}\n model =${JSON.stringify(m)}\n history=\n  ${history.join('\n  ')}`)
        }
    }
    const doOp = () => {
        const len = source.data.length
        const r = rand()
        if (r < 0.12) { const it = mk(); history.push(`push(#${it.id}${it.live ? 'L' : ''})`); source.push(it) }
        else if (r < 0.18) { const it = mk(); history.push(`unshift(#${it.id}${it.live ? 'L' : ''})`); source.unshift(it) }
        else if (r < 0.30) {
            const start = Math.floor(rand() * (len + 1)), dc = Math.floor(rand() * 3)
            const items = Array.from({length: Math.floor(rand() * 3)}, mk)
            history.push(`splice(${start},${dc},[${items.map(i => '#' + i.id + (i.live ? 'L' : ''))}])`); source.splice(start, dc, ...items)
        }
        else if (r < 0.36 && len) { history.push('pop'); source.pop() }
        else if (r < 0.42 && len) { history.push('shift'); source.shift() }
        else if (r < 0.50 && len) { const i = Math.floor(rand() * len); const it = mk(); history.push(`set(${i},#${it.id}${it.live ? 'L' : ''})`); source.set(i, it) }
        else if (r < 0.56 && len >= 2) { const a = Math.floor(rand() * (len - 1)), b = a + 1 + Math.floor(rand() * (len - a - 1)); history.push(`swap(${a},${b})`); source.swap(a, b) }
        else if (r < 0.60 && len >= 2) { history.push('sortSelf'); source.sortSelf((a, b) => b.id - a.id) }
        else if (r < 0.62) { history.push('clear'); source.clear() }
        else if (r < 0.85) { const it = pool[Math.floor(rand() * pool.length)]; const v = Math.floor(rand() * 10); history.push(`n(#${it.id})=${v}`); it.n(v) }
        else if (!inBatch) { history.push('flush'); flush() }
    }
    let inBatch = false
    flush(); check('init')
    for (let step = 0; step < steps; step++) {
        try {
            if (rand() < 0.3) { history.push('batch{'); inBatch = true; try { batch(() => { const n = 2 + Math.floor(rand() * 3); for (let i = 0; i < n; i++) doOp() }) } finally { inBatch = false }; history.push('}') }
            else doOp()
            if (rand() < 0.35) { history.push('flush+check'); flush(); flush(); check(`step ${step}`) }
        } catch (e: any) {
            if (String(e.message).startsWith('DIVERGE')) throw e
            throw new Error(`THROW seed=${seed} step ${step}: ${e.message}\n${String(e.stack).split('\n').slice(1, 8).join('\n')}\n history=\n  ${history.join('\n  ')}`)
        }
    }
    flush(); flush(); check('end')
    ;[m5, m4, m3, m2, m1].forEach(d => d.destroy())
    if (liveTok.size) throw new Error(`LEAK seed=${seed} live=${liveTok.size}`)
}

describe('audit fuzz 3: deferred scheduler maps', () => {
    test('seeds', () => {
        const failures: string[] = []
        for (let seed = 1; seed <= 400; seed++) {
            try { runSeed(seed, 50) } catch (e: any) {
                failures.push(String(e.message).slice(0, 2500))
                if (failures.length >= 3) break
            }
        }
        if (failures.length) console.log(failures.join('\n\n=====\n\n'))
        expect(failures.length).toBe(0)
    })
})
