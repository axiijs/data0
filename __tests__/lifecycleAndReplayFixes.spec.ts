import {describe, expect, test} from 'vitest'
import {atom} from '../src/atom.js'
import {computed, destroyComputed} from '../src/computed.js'
import {autorun} from '../src/common.js'
import {batch} from '../src/notify.js'
import {TrackOpTypes, TriggerOpTypes} from '../src/operations.js'
import {RxList} from '../src/RxList.js'
import {RxMap} from '../src/RxMap.js'
import {RxSet} from '../src/RxSet.js'
import {
    disableData0RetainedObjectDiagnostics,
    enableData0RetainedObjectDiagnostics,
    getData0RetainedObjectDiagnosticsSnapshot,
} from '../src/retainedDiagnostics.js'

/**
 * 2026-07 深度 review 发现的六个缺陷类的回归测试。
 * 每个用例最初都是 test.fails 的可执行复现（源码与生产构建均复现过），
 * 修复后按 AGENTS.md §3 翻转为普通测试。
 *
 * 等价类与常驻防线（AGENTS.md §3.1）：
 * - 缺陷类 I（多 info 单次 digest 重放含 EXPLICIT_KEY_CHANGE/结构混排）
 *   → `__tests__/batchReplayFuzz.spec.ts`（batch 包裹的全算子差分 fuzz）。
 * - 缺陷类 II/III（destroy 语义：源模式结构、destroyChildren/destroyComputed 绕过覆写）
 *   → `__tests__/destroySemantics.spec.ts`（destroy 对称性横扫）。
 * - 缺陷类 IV（在途 async patch 复活写入）→ 本文件 + asyncPatchInterleavings。
 * - 缺陷类 V/VI（RxSet.replace 重复值域、toSorted tie 顺序）→ 本文件 + 差分 fuzz。
 */
describe('fix: batch 中 set+结构操作的派生一致性（等价类：多 info 重放含 EKC）', () => {
    test('map: batch 内 set 后 shift', () => {
        const source = new RxList([1, 2, 3])
        const mapped = source.map(x => x * 2)
        try {
            batch(() => {
                source.set(2, 10)   // [1,2,10]
                source.shift()      // [2,10]
            })
            expect(mapped.data).toEqual(source.data.map(x => x * 2))
        } finally {
            mapped.destroy(); source.destroy()
        }
    })

    test('map: batch 内 set 后 unshift', () => {
        const source = new RxList([1, 2, 3])
        const mapped = source.map(x => x * 2)
        try {
            batch(() => {
                source.set(1, 10)   // [1,10,3]
                source.unshift(0)   // [0,1,10,3]
            })
            expect(mapped.data).toEqual(source.data.map(x => x * 2))
        } finally {
            mapped.destroy(); source.destroy()
        }
    })

    test('map(item, index): 同类序列行值与 index 全部对齐', () => {
        const source = new RxList([1, 2, 3])
        const mapped = source.map((item, index) => ({item, i: index}))
        try {
            batch(() => {
                source.set(2, 10)
                source.shift()
            })
            expect(mapped.data.map(e => e.item)).toEqual(source.data)
            mapped.data.forEach((e, i) => expect(e.i.raw, `row ${i}`).toBe(i))
        } finally {
            mapped.destroy(); source.destroy()
        }
    })

    test('map: 行级依赖写入与结构操作在同一 batch（写在前）', () => {
        const dep = atom(1)
        const source = new RxList([1, 2, 3])
        const mapped = source.map(x => x * dep())
        try {
            batch(() => {
                dep(10)
                source.shift()
            })
            expect(mapped.data).toEqual(source.data.map(x => x * 10))
            batch(() => {
                dep(100)
                source.unshift(9)
            })
            expect(mapped.data).toEqual(source.data.map(x => x * 100))
        } finally {
            mapped.destroy(); source.destroy()
        }
    })

    test('map: 行级依赖写入与结构操作在同一 batch（写在后）', () => {
        const dep = atom(1)
        const source = new RxList([1, 2, 3])
        const mapped = source.map(x => x * dep())
        try {
            batch(() => {
                source.shift()
                dep(10)
            })
            expect(mapped.data).toEqual(source.data.map(x => x * 10))
        } finally {
            mapped.destroy(); source.destroy()
        }
    })

    test('filter: batch 内 set+unshift 保持与全量一致', () => {
        const source = new RxList([1, 2, 3])
        const filtered = source.filter(x => x % 2 === 0)
        try {
            batch(() => {
                source.set(1, 10)
                source.unshift(0)
            })
            expect(filtered.data).toEqual(source.data.filter(x => x % 2 === 0))
        } finally {
            filtered.destroy(); source.destroy()
        }
    })

    test('groupBy: batch 内 set+unshift 组内容与全量一致', () => {
        const source = new RxList([1, 2, 3])
        const grouped = source.groupBy(x => x % 2)
        try {
            batch(() => {
                source.set(1, 10)
                source.unshift(0)
            })
            expect([...(grouped.data.get(0)?.data ?? [])]).toEqual(source.data.filter(x => x % 2 === 0))
        } finally {
            for (const g of grouped.data.values()) g.destroy()
            grouped.destroy(); source.destroy()
        }
    })

    test('groupBy: batch 内两次 splice 触及同 key 前缀', () => {
        const source = new RxList([2, 4, 6])
        const grouped = source.groupBy(x => x % 2)
        try {
            batch(() => {
                source.splice(2, 1)   // [2,4]
                source.splice(0, 1)   // [4]
            })
            expect([...(grouped.data.get(0)?.data ?? [])]).toEqual(source.data.filter(x => x % 2 === 0))
        } finally {
            for (const g of grouped.data.values()) g.destroy()
            grouped.destroy(); source.destroy()
        }
    })

    test('findIndex: batch 内多 splice(含负 start)回退全量(操作时长度回推在多 info 下失效)', () => {
        const source = new RxList<number>([10, 3, 6])
        const found = source.findIndex(x => x % 5 === 0)
        expect(found.raw).toBe(0)
        batch(() => {
            source.splice(-3, 1, 7, 8) // 操作时长度 3:-3 → 0,删除旧 match 10
            source.push(15)
        })
        expect(found.raw).toBe(source.data.findIndex(x => x % 5 === 0))
        destroyComputed(found)
        source.destroy()
    })

    test('无 batch 也一致：自定义微任务调度下两次连续写积累重放', async () => {
        const {scheduleNextMicroTask} = await import('../src/computed.js')
        const source = new RxList([1, 2, 3])
        const mapped = source.map(x => x * 2, {scheduleRecompute: scheduleNextMicroTask as any})
        try {
            source.set(2, 10)
            source.shift()
            await new Promise(r => setTimeout(r, 10))
            expect(mapped.data).toEqual(source.data.map(x => x * 2))
        } finally {
            mapped.destroy(); source.destroy()
        }
    })

    test('map: 多 info 回退全量重算时旧行的 context cleanup 与 onCleanup 各执行一次', () => {
        const cleanupLog: string[] = []
        const optionLog: number[] = []
        const dep = atom(1)
        const source = new RxList([1, 2])
        // 行读取 dep → 行有响应式依赖 → 使用 index atom → 多 info 触发全量重算回退
        const mapped = source.map((item, _idx, ctx) => {
            ctx.onCleanup(() => cleanupLog.push(`cleanup-${item}`))
            return item * dep()
        }, {onCleanup: (v) => optionLog.push(v as number)})
        try {
            batch(() => {
                source.set(1, 10)
                source.unshift(0)
            })
            expect(mapped.data).toEqual(source.data.map(x => x * dep.raw))
            // 全量重建：两个旧行的 cleanup 各执行一次（set 替换行的 cleanup 可能已在
            // patch 内执行过，这里只断言不重复、不遗漏行 1）
            expect(cleanupLog).toContain('cleanup-1')
            expect(cleanupLog.filter(x => x === 'cleanup-1').length).toBe(1)
        } finally {
            mapped.destroy(); source.destroy()
        }
    })
})

describe('fix: 源模式结构 destroy 语义（等价类：无 getter 结构的销毁路径）', () => {
    test('filter().destroy() 后不再接收更新', () => {
        const source = new RxList([1, 2, 3])
        const filtered = source.filter(x => x % 2 === 0)
        try {
            expect(filtered.data).toEqual([2])
            filtered.destroy()
            source.push(4)
            expect(filtered.data).toEqual([2])
        } finally {
            source.destroy()
        }
    })

    test('filter 反复 create/destroy 不在长命 source 上累积活跃 effect', () => {
        enableData0RetainedObjectDiagnostics()
        try {
            const source = new RxList([1, 2, 3, 4, 5])
            const baseline = getData0RetainedObjectDiagnosticsSnapshot().reactiveEffects.totalActive
            for (let i = 0; i < 10; i++) {
                const f = source.filter(x => x % 2 === 0)
                f.destroy()
            }
            const after = getData0RetainedObjectDiagnosticsSnapshot().reactiveEffects.totalActive
            source.destroy()
            expect(after).toBe(baseline)
        } finally {
            disableData0RetainedObjectDiagnostics()
        }
    })

    test('源模式 RxList 的 destroy 事件正常派发（on 与 callbacks.onDestroy）', () => {
        const list = new RxList([1])
        let onFired = false
        list.on('destroy', () => { onFired = true })
        list.destroy()
        expect(onFired).toBe(true)

        let callbackFired = false
        const list2 = new RxList([1], undefined, undefined, {onDestroy: () => { callbackFired = true }})
        list2.destroy()
        expect(callbackFired).toBe(true)
    })
})

describe('fix: destroyChildren/destroyComputed 走统一资源清理链', () => {
    test('子 computed 的 context.onCleanup 在父重算与停止时各执行一次', async () => {
        const dep = atom(1)
        const log: string[] = []
        const stop = autorun(() => {
            dep()
            computed(({onCleanup}) => {
                onCleanup(() => log.push('cleanup'))
                return 1
            })
        })
        try {
            dep(2)
            await new Promise(r => setTimeout(r, 5))
        } finally {
            stop()
        }
        // 两个子 computed（重算前后各一个）各销毁一次 → cleanup 恰好执行两次
        expect(log).toEqual(['cleanup', 'cleanup'])
    })

    test('destroyComputed 执行 context.onCleanup', async () => {
        const {destroyComputed} = await import('../src/computed.js')
        const log: string[] = []
        const c = computed(({onCleanup}) => {
            onCleanup(() => log.push('cleanup'))
            return 1
        })
        destroyComputed(c)
        expect(log).toEqual(['cleanup'])
    })

    test('父重算销毁的 RxList child 的惰性 meta（length）一并销毁', async () => {
        const dep = atom(1)
        const created: RxList<number>[] = []
        const stop = autorun(() => {
            dep()
            const inner = new RxList([1, 2])
            created.push(inner)
            inner.length // 触发惰性 meta 创建
        })
        try {
            dep(2)
            await new Promise(r => setTimeout(r, 5))
        } finally {
            stop()
        }
        const first = created[0]
        const lenBefore = first._length!.raw
        first.push(9) // 已销毁：no-op
        expect(first._length!.raw).toBe(lenBefore)
    })
})

describe('fix: destroy 取消挂起中的 async patch', () => {
    test('destroy 后在途 async patch 恢复执行也不再写入 data', async () => {
        let release: (() => void) | undefined
        const source = new RxList<number>([1])
        const derived = new RxList<number>(
            function computation(this: RxList<number>) {
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                return source.data.slice()
            },
            async function applyPatch(this: RxList<number>, _data, infos) {
                await new Promise<void>(resolve => { release = resolve })
                for (const info of infos) {
                    if (info.method === 'splice') {
                        this.spliceArray(this.data.length, 0, info.argv!.slice(2))
                    }
                }
            }
        )
        try {
            source.push(2)
            expect(typeof release).toBe('function')
            derived.destroy()
            const snapshot = derived.data.slice()
            release!()
            await new Promise(r => setTimeout(r, 20))
            expect(derived.data).toEqual(snapshot)
        } finally {
            source.destroy()
        }
    })

    test('已销毁 Rx 结构的变更方法一律 no-op', () => {
        const list = new RxList([1])
        list.destroy()
        expect(list.splice(0, 1)).toEqual([])
        expect(list.push(2)).toEqual([])
        list.set(0, 9)
        list.reorder([[0, 0]])
        expect(list.data).toEqual([1])

        const map = new RxMap<string, number>({a: 1})
        map.destroy()
        map.set('b', 2)
        map.delete('a')
        map.clear()
        map.replace({c: 3})
        expect([...map.data.entries()]).toEqual([['a', 1]])

        const set = new RxSet<number>([1])
        set.destroy()
        set.add(2)
        set.delete(1)
        set.replace([9])
        expect([...set.data]).toEqual([1])
    })
})

describe('fix: RxSet.replace 重复值域', () => {
    test('replace([2,2]) 后 toList 无重复行', () => {
        const s = new RxSet<number>([1])
        const list = s.toList()
        try {
            const [newItems] = s.replace([2, 2])
            expect(newItems).toEqual([2])
            expect([...s.data]).toEqual([2])
            expect(list.data).toEqual([2])
        } finally {
            list.destroy()
            s.destroy()
        }
    })

    test('replace 含 NaN 时 SameValueZero 判定不重复触发', () => {
        const s = new RxSet<number>([NaN])
        const list = s.toList()
        try {
            const [newItems, deletedItems] = s.replace([NaN, NaN, 1])
            expect(newItems).toEqual([1])
            expect(deletedItems).toEqual([])
            expect(list.data.length).toBe(2)
        } finally {
            list.destroy()
            s.destroy()
        }
    })
})

describe('fix: toSorted 等值 tie 顺序与全量稳定排序一致', () => {
    test('等 key 增量插入与全量稳定排序顺序一致', () => {
        type Item = {k: number, tag: string}
        const source = new RxList<Item>([{k: 1, tag: 'a'}, {k: 2, tag: 'b'}])
        const sorted = source.toSorted((a, b) => a.k - b.k)
        try {
            source.unshift({k: 1, tag: 'a2'})
            const full = source.data.slice().sort((a, b) => a.k - b.k)
            expect(sorted.data.map(i => i.tag)).toEqual(full.map(i => i.tag))
        } finally {
            sorted.destroy()
            source.destroy()
        }
    })

    test('set 引入等 key 元素时同样回退保持稳定顺序', () => {
        type Item = {k: number, tag: string}
        const a = {k: 1, tag: 'a'}
        const b = {k: 2, tag: 'b'}
        const c = {k: 3, tag: 'c'}
        const source = new RxList<Item>([a, b, c])
        const sorted = source.toSorted((x, y) => x.k - y.k)
        try {
            source.set(2, {k: 1, tag: 'c1'}) // [a, b, c1]，c1 与 a 等 key
            const full = source.data.slice().sort((x, y) => x.k - y.k)
            expect(sorted.data.map(i => i.tag)).toEqual(full.map(i => i.tag))
        } finally {
            sorted.destroy()
            source.destroy()
        }
    })
})
