import {describe, expect, test} from 'vitest'
import {atom} from '../src/atom.js'
import {computed, destroyComputed} from '../src/computed.js'
import {autorun} from '../src/common.js'
import {RxList} from '../src/RxList.js'
import {RxMap} from '../src/RxMap.js'
import {RxSet} from '../src/RxSet.js'
import {
    disableData0RetainedObjectDiagnostics,
    enableData0RetainedObjectDiagnostics,
    getData0RetainedObjectDiagnosticsSnapshot,
} from '../src/retainedDiagnostics.js'

/**
 * 缺陷类 II/III 的常驻防线（AGENTS.md §3.2 方法 8）：destroy 对称性横扫。
 *
 * 两条不变量，对派生结构族逐一断言：
 * 1. 僵尸检查：destroy() 后源的任何变更不得再改变派生结构的数据。
 *    （直接断言行为，不依赖 retainedDiagnostics——历史上诊断只统计
 *    active=true 的 effect，源模式结构在其中不可见，审计因此漏报。）
 * 2. 泄漏检查：create+destroy 一轮后活跃 effect 计数回到基线
 *    （源模式结构现在也计入诊断）。
 */

type Scenario = {
    name: string
    // 返回 [销毁入口, 读取派生当前值的快照函数]
    create: (source: RxList<number>) => {destroy: () => void, snapshot: () => unknown}
}

const scenarios: Scenario[] = [
    {name: 'map', create: (s) => {
        const d = s.map(x => x * 2)
        return {destroy: () => d.destroy(), snapshot: () => [...d.data]}
    }},
    {name: 'map(item,index)', create: (s) => {
        const d = s.map((item, index) => ({item, i: index}))
        return {destroy: () => d.destroy(), snapshot: () => d.data.map(e => e.item)}
    }},
    {name: 'map with reactive row', create: (s) => {
        const dep = atom(3)
        const d = s.map(x => x * dep())
        return {destroy: () => d.destroy(), snapshot: () => [...d.data]}
    }},
    {name: 'filter', create: (s) => {
        const d = s.filter(x => x % 2 === 0)
        return {destroy: () => d.destroy(), snapshot: () => [...d.data]}
    }},
    {name: 'toSorted', create: (s) => {
        const d = s.toSorted((a, b) => a - b)
        return {destroy: () => d.destroy(), snapshot: () => [...d.data]}
    }},
    {name: 'slice', create: (s) => {
        const d = s.slice(0, 2)
        return {destroy: () => d.destroy(), snapshot: () => [...d.data]}
    }},
    {name: 'concat', create: (s) => {
        const other = new RxList([1000])
        const d = s.concat(other)
        return {destroy: () => { d.destroy(); other.destroy() }, snapshot: () => [...d.data]}
    }},
    {name: 'groupBy', create: (s) => {
        const d = s.groupBy(x => x % 2)
        return {
            destroy: () => { for (const g of d.data.values()) g.destroy(); d.destroy() },
            snapshot: () => [...d.data.entries()].map(([k, g]) => [k, [...g.data]]),
        }
    }},
    {name: 'indexBy', create: (s) => {
        const d = s.indexBy(x => x)
        return {destroy: () => d.destroy(), snapshot: () => [...d.data.entries()]}
    }},
    {name: 'toSet', create: (s) => {
        const d = s.toSet()
        return {destroy: () => d.destroy(), snapshot: () => [...d.data].sort((a, b) => a - b)}
    }},
    {name: 'findIndex', create: (s) => {
        const d = s.findIndex(x => x % 5 === 0)
        return {destroy: () => destroyComputed(d), snapshot: () => d.raw}
    }},
    {name: 'find', create: (s) => {
        const d = s.find(x => x % 5 === 0)
        return {destroy: () => destroyComputed(d), snapshot: () => d.raw}
    }},
    {name: 'some', create: (s) => {
        const d = s.some(x => x > 100)
        return {destroy: () => destroyComputed(d), snapshot: () => d.raw}
    }},
    {name: 'every', create: (s) => {
        const d = s.every(x => x < 100)
        return {destroy: () => destroyComputed(d), snapshot: () => d.raw}
    }},
    {name: 'reduceToAtom', create: (s) => {
        const d = s.reduceToAtom((acc: number, item) => acc + item, 0)
        return {destroy: () => destroyComputed(d), snapshot: () => d.raw}
    }},
    {name: 'createSelection(atom)', create: (s) => {
        const current = atom<number | null>(null)
        const d = s.createSelection(current)
        return {destroy: () => d.destroy(), snapshot: () => d.data.map(([item]) => item)}
    }},
    {name: 'createSelection(RxSet, autoReset)', create: (s) => {
        const current = new RxSet<number>([])
        const d = s.createSelection(current, true)
        return {destroy: () => { d.destroy(); current.destroy() }, snapshot: () => d.data.map(([item]) => item)}
    }},
    {name: 'createIndexKeySelection', create: (s) => {
        const current = atom<number | null>(0)
        const d = s.createIndexKeySelection(current)
        return {destroy: () => d.destroy(), snapshot: () => d.data.map(([item]) => item)}
    }},
    {name: 'length', create: (s) => {
        const d = s.length
        // length 是挂在 source 上的惰性 meta，随 source 销毁；这里验证它可以被显式销毁
        return {destroy: () => destroyComputed(d), snapshot: () => d.raw}
    }},
]

describe('destroy 对称性横扫: destroy 后不再接收更新（僵尸检查）', () => {
    for (const {name, create} of scenarios) {
        test(name, () => {
            const source = new RxList([1, 2, 3, 4, 5])
            const {destroy, snapshot} = create(source)
            const before = JSON.stringify(snapshot())
            destroy()
            source.push(10)
            source.splice(0, 1)
            source.set(0, 50)
            expect(JSON.stringify(snapshot()), name).toBe(before)
            source.destroy()
        })
    }
})

describe('destroy 对称性横扫: create+destroy 后活跃 effect 计数回到基线（泄漏检查）', () => {
    for (const {name, create} of scenarios) {
        test(name, () => {
            const source = new RxList([1, 2, 3, 4, 5])
            enableData0RetainedObjectDiagnostics()
            try {
                const baseline = getData0RetainedObjectDiagnosticsSnapshot().reactiveEffects.totalActive
                for (let i = 0; i < 3; i++) {
                    const {destroy} = create(source)
                    destroy()
                }
                const after = getData0RetainedObjectDiagnosticsSnapshot().reactiveEffects.totalActive
                const detail = JSON.stringify(getData0RetainedObjectDiagnosticsSnapshot().reactiveEffects.activeBySource)
                expect(after, `${name} leaked: ${detail}`).toBe(baseline)
            } finally {
                disableData0RetainedObjectDiagnostics()
                source.destroy()
            }
        })
    }

    test('RxMap keys/values/entries/size 随 map destroy 释放', () => {
        enableData0RetainedObjectDiagnostics()
        try {
            const baseline = getData0RetainedObjectDiagnosticsSnapshot().reactiveEffects.totalActive
            const m = new RxMap<string, number>({a: 1, b: 2})
            m.keys(); m.values(); m.entries(); m.size
            m.destroy()
            const after = getData0RetainedObjectDiagnosticsSnapshot().reactiveEffects.totalActive
            expect(after).toBe(baseline)
        } finally {
            disableData0RetainedObjectDiagnostics()
        }
    })

    test('RxSet 运算与 toList 随 destroy 释放', () => {
        enableData0RetainedObjectDiagnostics()
        try {
            const baseline = getData0RetainedObjectDiagnosticsSnapshot().reactiveEffects.totalActive
            const a = new RxSet([1, 2])
            const b = new RxSet([2, 3])
            const ops = [a.difference(b), a.intersection(b), a.symmetricDifference(b), a.union(b)]
            const list = a.toList()
            const subset = a.isSubsetOf(b)
            const disjoint = a.isDisjointFrom(b)
            a.size
            ops.forEach(o => o.destroy())
            list.destroy()
            destroyComputed(subset)
            destroyComputed(disjoint)
            a.destroy(); b.destroy()
            const after = getData0RetainedObjectDiagnosticsSnapshot().reactiveEffects.totalActive
            expect(after).toBe(baseline)
        } finally {
            disableData0RetainedObjectDiagnostics()
        }
    })
})

describe('destroy 对称性: 源模式结构的销毁事件与双重销毁', () => {
    test('RxList/RxMap/RxSet 源模式 destroy 事件都派发', () => {
        const events: string[] = []
        const list = new RxList([1])
        list.on('destroy', () => events.push('list'))
        list.destroy()
        const map = new RxMap<string, number>({a: 1})
        map.on('destroy', () => events.push('map'))
        map.destroy()
        const set = new RxSet([1])
        set.on('destroy', () => events.push('set'))
        set.destroy()
        expect(events).toEqual(['list', 'map', 'set'])
    })

    test('双重 destroy 幂等且不抛错', () => {
        const list = new RxList([1])
        const mapped = list.map(x => x)
        mapped.destroy()
        mapped.destroy()
        list.destroy()
        list.destroy()
        const c = computed(() => 1)
        destroyComputed(c)
        destroyComputed(c)
    })

    test('destroy 后 computed 型结构不再重算（active 边界）', () => {
        let runs = 0
        const dep = atom(1)
        const c = computed(() => { runs++; return dep() })
        expect(runs).toBe(1)
        destroyComputed(c)
        dep(2)
        expect(runs).toBe(1)
    })
})

describe('destroy 对称性: 宿主重算/停止时 children 全量释放', () => {
    test('autorun 重算与停止后，getter 内创建的响应式结构全部释放', async () => {
        enableData0RetainedObjectDiagnostics()
        try {
            const baseline = getData0RetainedObjectDiagnosticsSnapshot().reactiveEffects.totalActive
            const dep = atom(1)
            const stop = autorun(() => {
                dep()
                const inner = new RxList([1, 2, 3])
                inner.length
                const innerMapped = inner.map(x => x * 2)
                computed(() => innerMapped.data.length)
                const m = new RxMap<string, number>({a: 1})
                m.keys()
            })
            dep(2)
            await new Promise(r => setTimeout(r, 5))
            stop()
            const after = getData0RetainedObjectDiagnosticsSnapshot().reactiveEffects.totalActive
            const detail = JSON.stringify(getData0RetainedObjectDiagnosticsSnapshot().reactiveEffects.activeBySource)
            expect(after, `leaked: ${detail}`).toBe(baseline)
        } finally {
            disableData0RetainedObjectDiagnostics()
        }
    })

    test('嵌套 computed 链随最外层 destroy 全量释放', () => {
        enableData0RetainedObjectDiagnostics()
        try {
            const baseline = getData0RetainedObjectDiagnosticsSnapshot().reactiveEffects.totalActive
            const dep = atom(1)
            const outer = computed(() => {
                const mid = computed(() => {
                    computed(() => dep())
                    return dep() * 2
                })
                return mid()
            })
            destroyComputed(outer)
            const after = getData0RetainedObjectDiagnosticsSnapshot().reactiveEffects.totalActive
            expect(after).toBe(baseline)
        } finally {
            disableData0RetainedObjectDiagnostics()
        }
    })
})

describe('destroy 对称性: 破坏性写入被拒绝后读取仍一致', () => {
    test('已销毁列表的读取接口不受 no-op 写入影响', () => {
        const list = new RxList([1, 2, 3])
        const dataRef = list.data
        list.destroy()
        list.push(9)
        list.splice(0, 1)
        expect(list.data).toBe(dataRef)
        expect([...list.data]).toEqual([1, 2, 3])
        expect(list.at(1)).toBe(2)
    })
})
