import {describe, expect, test} from 'vitest'
import {batch, computed, destroyComputed, RxList} from '../src/index'
import {mulberry32, adversarialSpliceStart, adversarialSpliceDeleteCount} from './fuzzKit'

type D = { name: string, list: RxList<any>, model: () => any[], destroy: () => void }

function runSeed(seed: number, steps: number) {
    const rand = mulberry32(seed)
    let counter = 10
    const nextVal = () => rand() < 0.35 ? Math.floor(rand() * 6) : counter++
    const source = new RxList<number>([3, 1, 2, 5, 4])
    const other = new RxList<number>([100, 101])
    const history: string[] = []
    const S = () => source.data
    const O = () => other.data

    const builders: (() => D)[] = [
        () => { const l = source.map(x => x * 2); return {name: 'map', list: l, model: () => S().map(x => x * 2), destroy: () => l.destroy()} },
        () => { const l = source.map((x, i) => x * 1000 + i()); return {name: 'map(i)', list: l, model: () => S().map((x, i) => x * 1000 + i), destroy: () => l.destroy()} },
        () => { const l = source.filter(x => x % 2 === 0); return {name: 'filter', list: l, model: () => S().filter(x => x % 2 === 0), destroy: () => l.destroy()} },
        () => { const l = source.toSorted((a, b) => a - b); return {name: 'toSorted', list: l, model: () => S().slice().sort((a, b) => a - b), destroy: () => l.destroy()} },
        () => { const l = source.toSorted(); return {name: 'toSorted(default)', list: l, model: () => S().slice().sort((a, b) => a < b ? -1 : a > b ? 1 : 0), destroy: () => l.destroy()} },
        () => { const a = Math.floor(rand() * 4), b = a + Math.floor(rand() * 4); const l = source.slice(a, b); return {name: `slice(${a},${b})`, list: l, model: () => S().slice(a, b), destroy: () => l.destroy()} },
        () => { const a = -Math.floor(rand() * 4), b = rand() < 0.5 ? undefined : -Math.floor(rand() * 2); const l = source.slice(a, b); return {name: `slice(${a},${b})`, list: l, model: () => S().slice(a, b), destroy: () => l.destroy()} },
        () => { const a = rand() * 3, b = 2 + rand() * 4; const l = source.slice(a, b); return {name: `slice(${a},${b})`, list: l, model: () => S().slice(a, b), destroy: () => l.destroy()} },
        () => { const l = source.concat(other); return {name: 'concat(other)', list: l, model: () => S().concat(O()), destroy: () => l.destroy()} },
        () => { const l = other.concat(source, other); return {name: 'other.concat(source,other)', list: l, model: () => O().concat(S(), O()), destroy: () => l.destroy()} },
        () => { const m = source.map(x => x + 1); const l = source.concat(m); return {name: 'concat(map)', list: l, model: () => S().concat(S().map(x => x + 1)), destroy: () => { l.destroy(); m.destroy() }} },
        () => { const f = source.filter(x => x % 3 !== 0); const l = f.concat(source.toSorted((a, b) => b - a)); return {name: 'filter.concat(sorted)', list: l, model: () => S().filter(x => x % 3 !== 0).concat(S().slice().sort((a, b) => b - a)), destroy: () => l.destroy()} },
        () => { const l = source.filter(x => x % 2 === 1).map((x, i) => `${x}@${i()}`).slice(1, 4); return {name: 'filter.map(i).slice', list: l, model: () => S().filter(x => x % 2 === 1).map((x, i) => `${x}@${i}`).slice(1, 4), destroy: () => l.destroy()} },
        () => { const l = source.toSorted((a, b) => b - a).filter(x => x > 2).map(x => -x); return {name: 'toSorted.filter.map', list: l, model: () => S().slice().sort((a, b) => b - a).filter(x => x > 2).map(x => -x), destroy: () => l.destroy()} },
        () => { const l = source.slice(1).toSorted((a, b) => a - b).slice(0, 3); return {name: 'slice.toSorted.slice', list: l, model: () => S().slice(1).sort((a, b) => a - b).slice(0, 3), destroy: () => l.destroy()} },
        () => { const l = source.map(x => x % 4).toSet().toList().toSorted((a, b) => a - b); return {name: 'map.toSet.toList.toSorted', list: l, model: () => [...new Set(S().map(x => x % 4))].sort((a, b) => a - b), destroy: () => l.destroy()} },
        () => { const g = source.groupBy(x => x % 3); const l = g.keys().toSorted((a, b) => a - b); return {name: 'groupBy.keys.toSorted', list: l, model: () => [...new Set(S().map(x => x % 3))].sort((a, b) => a - b), destroy: () => { l.destroy(); g.destroy() }} },
        () => { const l = source.reduce<RxList<number>>((acc, x, i) => { acc.push(x + i) }) as RxList<number>; return {name: 'reduce', list: l, model: () => S().map((x, i) => x + i), destroy: () => l.destroy()} },
        () => { const l = source.createSelection(new RxSet<number>([1, 2]) as any).map(([x, ind]) => `${x}:${ind()}`); return {name: 'selection.map', list: l, model: () => S().map(x => `${x}:${x === 1 || x === 2}`), destroy: () => l.destroy()} },
    ]

    type Obs = { name: string, actual: () => any, model: () => any, destroy: () => void }
    const live: {d: D, obs: Obs[]}[] = []
    const addDerived = () => {
        const d = builders[Math.floor(rand() * builders.length)]()
        const obs: Obs[] = []
        for (const k of [0, 2, -1, -2]) {
            const c = computed(() => d.list.at(k))
            obs.push({name: `at(${k})`, actual: () => c.raw, model: () => d.model().at(k), destroy: () => destroyComputed(c)})
        }
        const it = computed(() => [...d.list])
        obs.push({name: 'iter', actual: () => it.raw, model: () => d.model(), destroy: () => destroyComputed(it)})
        const fe = computed(() => { const r: any[] = []; d.list.forEach((x, i) => r.push([i, x])); return r })
        obs.push({name: 'forEach', actual: () => fe.raw, model: () => d.model().map((x, i) => [i, x]), destroy: () => destroyComputed(fe)})
        const len = d.list.length
        obs.push({name: 'length', actual: () => len.raw, model: () => d.model().length, destroy: () => {}})
        const fi = d.list.findIndex((x: any) => typeof x === 'number' && x % 5 === 0)
        obs.push({name: 'findIndex', actual: () => fi.raw, model: () => d.model().findIndex((x: any) => typeof x === 'number' && x % 5 === 0), destroy: () => destroyComputed(fi)})
        const fd = d.list.find((x: any) => typeof x === 'number' && x % 5 === 0)
        obs.push({name: 'find', actual: () => fd.raw, model: () => d.model().find((x: any) => typeof x === 'number' && x % 5 === 0), destroy: () => destroyComputed(fd)})
        history.push(`+derived ${d.name}`)
        live.push({d, obs})
    }
    const check = (stage: string) => {
        for (const {d, obs} of live) {
            const a = JSON.stringify(d.list.data), m = JSON.stringify(d.model())
            if (a !== m) throw new Error(`DIVERGE seed=${seed} at ${stage} [${d.name}]\n actual=${a}\n model =${m}\n history=\n  ${history.join('\n  ')}`)
            for (const o of obs) {
                const a = JSON.stringify(o.actual()), m = JSON.stringify(o.model())
                if (a !== m) throw new Error(`DIVERGE seed=${seed} at ${stage} [${d.name} / ${o.name}]\n actual=${a}\n model =${m}\n src=${JSON.stringify(S())}\n history=\n  ${history.join('\n  ')}`)
            }
        }
    }
    const doOp = () => {
        const list = rand() < 0.85 ? source : other
        const tag = list === source ? 's' : 'o'
        const len = list.data.length
        const r = rand()
        if (r < 0.2) {
            const start = adversarialSpliceStart(rand, len), dc = adversarialSpliceDeleteCount(rand, len)
            const items = Array.from({length: Math.floor(rand() * 3)}, nextVal)
            history.push(`${tag}.splice(${start},${dc},[${items}])`); list.splice(start, dc, ...items)
        } else if (r < 0.3) { const v = nextVal(); history.push(`${tag}.push(${v})`); list.push(v) }
        else if (r < 0.36) { const v = nextVal(); history.push(`${tag}.unshift(${v})`); list.unshift(v) }
        else if (r < 0.42) { history.push(`${tag}.pop`); list.pop() }
        else if (r < 0.48) { history.push(`${tag}.shift`); list.shift() }
        else if (r < 0.6 && len) { const i = Math.floor(rand() * len), v = rand() < 0.2 ? list.data[i] : nextVal(); history.push(`${tag}.set(${i},${v})`); list.set(i, v) }
        else if (r < 0.66 && len >= 2) { const a = Math.floor(rand() * (len - 1)), b = a + 1 + Math.floor(rand() * (len - a - 1)); history.push(`${tag}.swap(${a},${b})`); list.swap(a, b) }
        else if (r < 0.72 && len >= 2) { const limit = 1 + Math.floor(rand() * (len - 1)); const a = Math.floor(rand() * (len - limit + 1)), b = Math.floor(rand() * (len - limit + 1)); history.push(`${tag}.reposition(${a},${b},${limit})`); list.reposition(a, b, limit) }
        else if (r < 0.77 && len >= 2) { history.push(`${tag}.sortSelf`); list.sortSelf((a, b) => (a % 4) - (b % 4)) }
        else if (r < 0.81) { const items = Array.from({length: Math.floor(rand() * 6)}, nextVal); history.push(`${tag}.replaceAll([${items}])`); list.spliceArray(0, list.data.length, items) }
        else if (r < 0.83) { history.push(`${tag}.clear`); list.clear() }
        else if (r < 0.92) addDerived()
        else if (live.length) {
            const i = Math.floor(rand() * live.length); const [x] = live.splice(i, 1)
            history.push(`-derived ${x.d.name}`)
            x.obs.forEach(o => o.destroy()); x.d.destroy()
        }
    }
    addDerived(); addDerived(); addDerived()
    check('init')
    for (let step = 0; step < steps; step++) {
        try {
            if (rand() < 0.35) {
                const n = 2 + Math.floor(rand() * 3)
                history.push('batch{'); batch(() => { for (let i = 0; i < n; i++) doOp() }); history.push('}')
            } else doOp()
        } catch (e: any) {
            throw new Error(`THROW seed=${seed} step ${step}: ${e.message}\n${String(e.stack).split('\n').slice(1, 8).join('\n')}\n history=\n  ${history.join('\n  ')}`)
        }
        check(`step ${step}`)
    }
}
import {RxSet} from '../src/index'

describe('audit fuzz 2: observers on derived lists, dynamic create/destroy', () => {
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
