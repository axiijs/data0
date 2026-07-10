import {describe, expect, test} from "vitest";
import {readdirSync, readFileSync} from "node:fs";
import {resolve, dirname} from "node:path";
import {fileURLToPath} from "node:url";
import {atom, batch, computed, Computed, LinkedList, Notifier, oncePromise, RxList, RxMap, RxSet} from "../src/index.js";
import {getComputedInternal, STATUS_DIRTY} from "../src/computed.js";
import {ReactiveEffect} from "../src/reactiveEffect.js";

const srcDir = resolve(dirname(fileURLToPath(import.meta.url)), '../src')

describe('shipped code hygiene', () => {
    test('no bare debugger statement in src (only allowed behind __DEV__)', () => {
        for (const file of readdirSync(srcDir)) {
            if (!file.endsWith('.ts')) continue
            const lines = readFileSync(resolve(srcDir, file), 'utf-8').split('\n')
            lines.forEach((line, i) => {
                const trimmed = line.trim()
                if (trimmed === 'debugger') {
                    expect.fail(`${file}:${i + 1} contains a bare debugger statement`)
                }
            })
        }
    })
})

describe('Notifier.collectTrackTarget frame leak', () => {
    test('trackTargetFrames stays empty after findIndex computation and patches', () => {
        const framesBefore = Notifier.instance.trackTargetFrames.length
        const list = new RxList([{v: 1}, {v: 2}, {v: 3}])
        const found = list.findIndex(item => item.v === 3)
        expect(found()).toBe(2)
        list.push({v: 0})
        list.splice(0, 1)
        expect(Notifier.instance.trackTargetFrames.length).toBe(framesBefore)
    })
})

describe('exception safety', () => {
    test('throwing sync getter propagates synchronously and leaves global state intact', () => {
        const depthBefore = Notifier.instance.effectTrackDepth
        const scopesBefore = ReactiveEffect.activeScopes.length

        expect(() => computed(() => { throw new Error('user error') })).toThrow('user error')

        expect(ReactiveEffect.activeScopes.length).toBe(scopesBefore)
        expect(Notifier.instance.effectTrackDepth).toBe(depthBefore)

        // reactivity still works afterwards
        const a = atom(1)
        const c = computed(() => a() + 1)
        expect(c()).toBe(2)
        a(5)
        expect(c()).toBe(6)
    })

    test('getter throwing on re-run propagates to the mutation site and can recover', () => {
        const a = atom(0)
        const c = computed(() => {
            if (a() === 1) throw new Error('boom')
            return a() * 10
        })
        expect(c()).toBe(0)
        expect(() => a(1)).toThrow('boom')
        // status is back to dirty, a following write recovers the computed
        expect(getComputedInternal(c)!.status.raw).toBe(STATUS_DIRTY)
        a(2)
        expect(c()).toBe(20)
    })

    test('throwing effect inside batch does not freeze the effect session', () => {
        const a = atom(0)
        computed(() => {
            if (a() === 1) throw new Error('boom')
            return a()
        })
        expect(() => batch(() => a(1))).toThrow('boom')
        expect(Notifier.instance.inEffectSession).toBe(false)
        expect(Notifier.instance.isDigesting).toBe(false)
        expect(Notifier.instance.effectsInSession.size).toBe(0)

        // batching still works afterwards
        const b = atom(1)
        const c = computed(() => b() * 2)
        batch(() => b(3))
        expect(c()).toBe(6)
    })

    test('throwing applyPatch propagates, resets state and full recompute recovers', () => {
        const source = new RxList([1, 2, 3])
        let shouldThrow = false
        const derived = new RxList(
            function computation(this: RxList<number>) {
                this.manualTrack(source, 'method' as any, 'method' as any)
                return source.data.map(x => x * 2)
            },
            function applyPatch() {
                if (shouldThrow) throw new Error('patch boom')
                return false // always fall back to full recompute
            }
        )
        expect(derived.data).toEqual([2, 4, 6])
        shouldThrow = true
        expect(() => source.push(4)).toThrow('patch boom')
        expect(ReactiveEffect.activeScopes.length).toBe(0)

        // next change retries: patch throws are recoverable via full recompute
        shouldThrow = false
        source.push(5)
        expect(derived.data).toEqual([2, 4, 6, 8, 10])
    })

    test('async getter error rejects cleanPromise, resets asyncStatus and status', async () => {
        const a = atom(1)
        const c = computed(async function(this: Computed) {
            const v = a()
            if (v === 2) throw new Error('async boom')
            return v * 10
        })
        const internal = getComputedInternal(c)!
        await internal.cleanPromise
        expect(c.raw).toBe(10)

        a(2)
        await expect(internal.cleanPromise).rejects.toThrow('async boom')
        expect(internal.status.raw).toBe(STATUS_DIRTY)
        expect(internal.asyncStatus!.raw).toBe(false)

        // recovers on next change
        a(3)
        await internal.cleanPromise
        expect(c.raw).toBe(30)
    })
})

describe('index/key 0 in incremental patches', () => {
    test('groupBy handles explicit set at index 0', () => {
        const list = new RxList([1, 2, 3])
        const groups = list.groupBy(item => item % 2)
        list.set(0, 5)
        expect(groups.data.get(1)!.data).toEqual([3, 5])
        expect(groups.data.get(0)!.data).toEqual([2])
    })

    test('groupBy handles falsy item values on explicit key change', () => {
        const list = new RxList([0, 1])
        const groups = list.groupBy(item => item % 2)
        list.set(0, 2)
        expect(groups.data.get(0)!.data).toEqual([2])
    })

    test('indexBy handles explicit set at index 0', () => {
        const list = new RxList([{id: 1}, {id: 2}])
        const indexed = list.indexBy('id')
        list.set(0, {id: 9})
        expect([...indexed.data.keys()].sort()).toEqual([2, 9])
    })

    test('toMap handles explicit set at index 0', () => {
        const list = new RxList<[string, number]>([['a', 1], ['b', 2]])
        const map = list.toMap()
        list.set(0, ['c', 3])
        expect(map.data.has('a')).toBe(false)
        expect(map.data.get('c')).toBe(3)
    })

    test('toSet handles explicit set at index 0', () => {
        const list = new RxList([1, 2])
        const set = list.toSet()
        list.set(0, 9)
        expect(set.data.has(1)).toBe(false)
        expect(set.data.has(9)).toBe(true)
    })

    test('findIndex handles explicit set at index 0', () => {
        const list = new RxList([9, 2, 3])
        const idx = list.findIndex(item => item === 9)
        expect(idx()).toBe(0)
        list.set(0, 1)
        expect(idx()).toBe(-1)
        list.set(0, 9)
        expect(idx()).toBe(0)
    })
})

describe('findIndex batched trigger infos', () => {
    test('all triggerInfos in a batch are applied', () => {
        const list = new RxList([10, 99, 20])
        const idx = list.findIndex(i => i === 99)
        expect(idx()).toBe(1)
        batch(() => {
            // first info: removal after the current match (no re-search needed)
            list.splice(2, 1)
            // second info must not be skipped: a new match appears at the head
            list.splice(0, 0, 99)
        })
        expect(idx()).toBe(0)
    })
})

describe('slice with reorder', () => {
    test('slice stays consistent after sortSelf', () => {
        const list = new RxList([3, 1, 2])
        const sliced = list.slice(0, 2)
        expect(sliced.data).toEqual([3, 1])
        list.sortSelf((a, b) => a - b)
        expect(sliced.data).toEqual([1, 2])
    })

    test('slice stays consistent after reposition and swap', () => {
        const list = new RxList([0, 1, 2, 3, 4])
        const sliced = list.slice(1, 4)
        expect(sliced.data).toEqual([1, 2, 3])
        list.reposition(0, 4)
        expect(sliced.data).toEqual(list.data.slice(1, 4))
        list.swap(0, 1)
        expect(sliced.data).toEqual(list.data.slice(1, 4))
    })

    test('slice patch handles negative splice start', () => {
        const list = new RxList([0, 1, 2, 3, 4])
        const sliced = list.slice(2, 5)
        expect(sliced.data).toEqual([2, 3, 4])
        list.splice(-1, 1, 9)
        expect(list.data).toEqual([0, 1, 2, 3, 9])
        expect(sliced.data).toEqual([2, 3, 9])
    })
})

describe('RxList.set out of bounds', () => {
    test('length and iteration stay consistent', () => {
        const list = new RxList<number|undefined>([1])
        const len = list.length
        expect(len()).toBe(1)
        list.set(3, 42)
        expect(list.data.length).toBe(4)
        expect(len()).toBe(4)
        expect(list.data).toEqual([1, undefined, undefined, 42])
    })

    test('downstream map receives the extension', () => {
        const list = new RxList<number>([1])
        const mapped = list.map(item => (item ?? 0) * 10)
        list.set(2, 5)
        expect(mapped.data).toEqual([10, 0, 50])
    })
})

describe('atom equality', () => {
    test('setting NaN repeatedly does not retrigger', () => {
        const a = atom(NaN)
        let runs = 0
        const c = computed(() => { runs++; return a() })
        c()
        const before = runs
        a(NaN)
        a(NaN)
        expect(runs).toBe(before)
    })

    test('proxy atom with object value: NaN-safe compare keeps normal updates working', () => {
        const a = atom<{v: number}|number>({v: 1})
        a(NaN)
        let runs = 0
        computed(() => { runs++; return a() })
        const before = runs
        a(NaN)
        expect(runs).toBe(before)
        a(2)
        expect(runs).toBe(before + 1)
    })
})

describe('destroy cleans computed metas', () => {
    test('RxMap.destroy destroys keys/values/entries/size', () => {
        const map = new RxMap<string, number>({a: 1})
        const keys = map.keys()
        const values = map.values()
        const entries = map.entries()
        const size = map.size
        map.destroy()
        expect(keys.active).toBe(false)
        expect(values.active).toBe(false)
        expect(entries.active).toBe(false)
        expect(getComputedInternal(size)!.active).toBe(false)
    })

    test('RxSet.destroy destroys size', () => {
        const set = new RxSet([1, 2])
        const size = set.size
        set.destroy()
        expect(getComputedInternal(size)!.active).toBe(false)
    })
})

describe('LinkedList removeBetween', () => {
    test('cleans itemToNode and triggers subscribers', () => {
        const items = [{id: 1}, {id: 2}, {id: 3}]
        const linkedList = new LinkedList(items)
        let runs = 0
        const c = computed(() => {
            runs++
            return linkedList.map(node => node.item.id)
        })
        expect(c()).toEqual([1, 2, 3])

        const node2 = linkedList.getNodeByItem(items[1])!
        linkedList.removeBetween(node2, node2)
        expect(c()).toEqual([1, 3])
        expect(linkedList.getNodeByItem(items[1])).toBeUndefined()
        expect(linkedList.getNodeByItem(items[0])).toBeDefined()
    })
})

describe('oncePromise error handling', () => {
    test('rejects when fn throws', async () => {
        const a = atom(0)
        const promise = oncePromise(() => {
            if (a() === 1) throw new Error('once boom')
            return false
        })
        a(1)
        await expect(promise).rejects.toThrow('once boom')
    })
})
