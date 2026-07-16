import {describe, expect, test} from 'vitest'
import {atom} from '../src/atom.js'
import {computed, Computed, destroyComputed} from '../src/computed.js'
import {notifier} from '../src/notify.js'
import {createIndexKeySelection, createSelection, RxList} from '../src/RxList.js'
import {RxMap} from '../src/RxMap.js'
import {RxSet} from '../src/RxSet.js'
import {ReactiveEffect} from '../src/reactiveEffect.js'
import {TrackOpTypes, TriggerOpTypes} from '../src/operations.js'

/**
 * 深度评估中动态复现问题的回归测试。
 * 每个用例最初都是在修复前失败的最小复现(见 PR 描述),修复后转为普通回归测试。
 */

const tick = () => new Promise<void>(resolve => setTimeout(resolve, 0))

describe('F1: async applyPatch 不再挂起期间霸占 activeScopes', () => {
    function createAsyncDerived(source: RxList<number>) {
        let resolvePatch: (() => void) | undefined
        let patchRuns = 0
        const derived = new RxList<number>(
            function computation(this: RxList<number>) {
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                return source.data.slice()
            },
            async function applyPatch(this: RxList<number>, _data, triggerInfos) {
                patchRuns++
                await new Promise<void>(resolve => { resolvePatch = resolve })
                for (const info of triggerInfos) {
                    if (info.method === 'splice') {
                        this.spliceArray(this.data.length, 0, info.argv!.slice(2) as number[])
                    }
                }
            }
        )
        return {
            derived,
            getPatchRuns: () => patchRuns,
            resolvePatch: () => resolvePatch?.(),
        }
    }

    test('await 挂起期间 activeScopes 为空', async () => {
        const source = new RxList([1])
        const {derived, resolvePatch} = createAsyncDerived(source)
        try {
            source.push(2)
            await Promise.resolve()
            expect(ReactiveEffect.activeScopes.length).toBe(0)
            resolvePatch()
            await tick()
            expect(derived.data).toEqual([1, 2])
        } finally {
            derived.destroy()
            source.destroy()
        }
    })

    test('挂起期间的源写入不丢失', async () => {
        const source = new RxList([1])
        const {derived, resolvePatch} = createAsyncDerived(source)
        try {
            source.push(2)
            await Promise.resolve()
            // 第一轮 patch 挂起期间的第二次写入
            source.push(3)
            resolvePatch()
            await tick()
            // 第二轮 patch 处理排队的 info
            resolvePatch()
            await tick()
            expect(derived.data).toEqual([1, 2, 3])
        } finally {
            derived.destroy()
            source.destroy()
        }
    })

    test('挂起期间无关 atom 的读取不会被幽灵追踪成依赖', async () => {
        const source = new RxList([1])
        const unrelated = atom(0)
        const {derived, getPatchRuns, resolvePatch} = createAsyncDerived(source)
        try {
            source.push(2)
            await Promise.resolve()
            // 挂起期间,毫不相关的普通代码读了一个 atom
            unrelated()
            resolvePatch()
            await tick()
            expect(derived.data).toEqual([1, 2])

            const runsBefore = getPatchRuns()
            unrelated(42)
            await tick()
            resolvePatch()
            await tick()
            expect(getPatchRuns()).toBe(runsBefore)
        } finally {
            derived.destroy()
            source.destroy()
        }
    })

    test('两个 async patch 交错完成不破坏 scope 栈', async () => {
        const sourceA = new RxList([1])
        const sourceB = new RxList([1])
        const a = createAsyncDerived(sourceA)
        const b = createAsyncDerived(sourceB)
        try {
            sourceA.push(2)
            await Promise.resolve()
            sourceB.push(3)
            await Promise.resolve()
            expect(ReactiveEffect.activeScopes.length).toBe(0)
            // A 先完成
            a.resolvePatch()
            await tick()
            b.resolvePatch()
            await tick()
            expect(a.derived.data).toEqual([1, 2])
            expect(b.derived.data).toEqual([1, 3])
            expect(ReactiveEffect.activeScopes.length).toBe(0)
        } finally {
            a.derived.destroy()
            b.derived.destroy()
            sourceA.destroy()
            sourceB.destroy()
        }
    })
})

describe('F2: 同步 getter + generator applyPatch', () => {
    test('generator patch 正常应用,不再因 asyncStatus 缺失崩溃', async () => {
        const source = new RxList([1, 2])
        const doubled = new RxList<number>(
            function computation(this: RxList<number>) {
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                return source.data.map(x => x * 2)
            },
            function* applyPatch(this: RxList<number>, _data, triggerInfos) {
                for (const info of triggerInfos) {
                    if (info.method === 'splice') {
                        const newItems = info.argv!.slice(2) as number[]
                        this.spliceArray(this.data.length, 0, newItems.map(x => x * 2))
                    }
                }
            }
        )
        try {
            expect(doubled.data).toEqual([2, 4])
            source.push(3)
            await tick()
            expect(doubled.data).toEqual([2, 4, 6])
            // 再次变更仍可用(修复前第一次崩溃后永久失联)
            source.push(4)
            await tick()
            expect(doubled.data).toEqual([2, 4, 6, 8])
        } finally {
            doubled.destroy()
            source.destroy()
        }
    })
})

describe('F3: selection 家族支持 reorder 与 set', () => {
    test('createSelection + sortSelf 保持行序与指示器', () => {
        const source = new RxList(['c', 'a', 'b'])
        const selected = new RxSet<string | number>(['a'])
        const selection = createSelection(source, selected)
        try {
            source.sortSelf((x, y) => x.localeCompare(y))
            expect(source.data).toEqual(['a', 'b', 'c'])
            expect(selection.data.map(([item, i]) => [item, i.raw])).toEqual([['a', true], ['b', false], ['c', false]])
        } finally {
            selection.destroy()
            selected.destroy()
            source.destroy()
        }
    })

    test('createSelection(atom) + reposition 保持行序', () => {
        const source = new RxList(['x', 'y', 'z'])
        const selected = atom<string | null | number>('y')
        const selection = createSelection(source, selected)
        try {
            source.reposition(0, 2)
            expect(source.data).toEqual(['y', 'z', 'x'])
            expect(selection.data.map(([item]) => item)).toEqual(['y', 'z', 'x'])
            expect(selection.data.map(([, i]) => i.raw)).toEqual([true, false, false])
        } finally {
            selection.destroy()
            source.destroy()
        }
    })

    test('createSelection(autoResetValue) 下 sortSelf 与 set 不再抛错', () => {
        const source = new RxList(['a', 'b', 'c'])
        const selected = new RxSet<string | number>(['b'])
        const selection = createSelection(source, selected, true)
        try {
            expect(() => source.sortSelf((x, y) => x.localeCompare(y))).not.toThrow()
            // set 替换掉的旧值应从选中集中回收
            expect(() => source.set(0, 'z')).not.toThrow()
            source.set(1, 'w') // 替换选中的 'b'
            expect(selected.data.has('b')).toBe(false)
        } finally {
            selection.destroy()
            selected.destroy()
            source.destroy()
        }
    })

    test('createIndexKeySelection + sortSelf 重排行并按 index 校正指示器', () => {
        const source = new RxList([3, 1, 2])
        const selected = new RxSet<number>([0])
        const selection = createIndexKeySelection(source, selected)
        try {
            expect(() => source.sortSelf((a, b) => a - b)).not.toThrow()
            expect(selection.data.map(([item]) => item)).toEqual([1, 2, 3])
            expect(selection.data.map(([, i]) => i.raw)).toEqual([true, false, false])
        } finally {
            selection.destroy()
            selected.destroy()
            source.destroy()
        }
    })

    test('createIndexKeySelection(autoReset) + sortSelf 不再抛错', () => {
        const source = new RxList([3, 1, 2])
        const selected = new RxSet<number>([1])
        const selection = createIndexKeySelection(source, selected, true)
        try {
            expect(() => source.sortSelf((a, b) => a - b)).not.toThrow()
            expect(selection.data.map(([, i]) => i.raw)).toEqual([false, true, false])
        } finally {
            selection.destroy()
            selected.destroy()
            source.destroy()
        }
    })

    test('createIndexKeySelection + set 替换行内容并保持 index 选中', () => {
        const source = new RxList(['a', 'b', 'c'])
        const selected = new RxSet<number>([1])
        const selection = createIndexKeySelection(source, selected)
        try {
            source.set(1, 'B')
            expect(selection.data.map(([item]) => item)).toEqual(['a', 'B', 'c'])
            expect(selection.data.map(([, i]) => i.raw)).toEqual([false, true, false])
        } finally {
            selection.destroy()
            selected.destroy()
            source.destroy()
        }
    })
})

describe('F4: createIndexKeySelection 指示器', () => {
    test('多选下删除不误关其它选中 index', () => {
        const source = new RxList(['a', 'b', 'c'])
        const selected = new RxSet<number>([0, 1])
        const selection = createIndexKeySelection(source, selected)
        try {
            expect(selection.data.map(([, i]) => i.raw)).toEqual([true, true, false])
            source.splice(1, 1)
            expect(selection.data.map(([, i]) => i.raw)).toEqual([true, true])
        } finally {
            selection.destroy()
            selected.destroy()
            source.destroy()
        }
    })

    test('替换型 splice 命中选中 index 时保持选中', () => {
        const source = new RxList(['a', 'b', 'c'])
        const selected = new RxSet<number>([1])
        const selection = createIndexKeySelection(source, selected)
        try {
            source.splice(1, 1, 'x')
            expect(selection.data.map(([, i]) => i.raw)).toEqual([false, true, false])
        } finally {
            selection.destroy()
            selected.destroy()
            source.destroy()
        }
    })

    test('数值型 item 与选中 index 撞值时插入行不误选', () => {
        const source = new RxList([100, 101, 102, 103, 104, 105])
        const selected = new RxSet<number>([5])
        const selection = createIndexKeySelection(source, selected)
        try {
            // 插入一个值恰好等于选中 index(5)的 item
            source.splice(0, 0, 5)
            expect(selection.data.map(([, i]) => i.raw)).toEqual([false, false, false, false, false, true, false])
        } finally {
            selection.destroy()
            selected.destroy()
            source.destroy()
        }
    })
})

describe('F5: map context.onCleanup 槽位对齐', () => {
    test('插入不注册 cleanup 的行后,删除行执行的仍是自己的 cleanup', () => {
        const log: string[] = []
        const source = new RxList([1, 3])
        const mapped = source.map((item, _idx, ctx) => {
            if (item % 2 === 1) ctx.onCleanup(() => log.push(`cleanup-${item}`))
            return item
        })
        try {
            source.splice(0, 0, 10, 20) // 偶数行不注册 cleanup
            source.splice(0, 1)         // 删除 10:不应执行任何 cleanup
            expect(log).toEqual([])
            source.splice(1, 1)         // 删除 1:执行 cleanup-1
            expect(log).toEqual(['cleanup-1'])
        } finally {
            mapped.destroy()
            source.destroy()
        }
    })

    test('行移动后重算,onCleanup 注册进当前行而不是旧位置', () => {
        const log: string[] = []
        let registration = 0
        const dep = atom(0)
        const source = new RxList(['a', 'b'])
        const mapped = source.map((item, _idx, ctx) => {
            dep()
            const id = ++registration
            ctx.onCleanup(() => log.push(`cleanup-${item}#${id}`))
            return item
        })
        try {
            // 初始注册 a=#1, b=#2;删除 a 后行 b 移到 index 0
            source.splice(0, 1)
            expect(log).toEqual(['cleanup-a#1'])
            log.length = 0
            // 行 b 重算:先执行上一轮注册的 #2(2026-H3 round8 R8-5——与 computed 的
            // context.onCleanup"每轮重算前执行"语义对齐;此前 #2 被新注册静默顶掉,
            // mapFn 每轮分配的资源逐轮泄漏),再注册 #3
            dep(1)
            expect(log).toEqual(['cleanup-b#2'])
            source.splice(0, 1) // 删除 b:执行最新注册的 #3
            expect(log).toEqual(['cleanup-b#2', 'cleanup-b#3'])
            // 槽位对齐的本意仍被验证:两次 cleanup 都属于行 b,不落旧位置
            expect(log.every(entry => entry.startsWith('cleanup-b'))).toBe(true)
        } finally {
            mapped.destroy()
            source.destroy()
        }
    })

    test('destroy 时执行所有已注册的 cleanup', () => {
        const log: string[] = []
        const source = new RxList([1, 2])
        const mapped = source.map((item, _idx, ctx) => {
            ctx.onCleanup(() => log.push(`cleanup-${item}`))
            return item
        })
        mapped.destroy()
        source.destroy()
        expect(log.sort()).toEqual(['cleanup-1', 'cleanup-2'])
    })
})

describe('F6: filter/groupBy 在重复原始值下保持源顺序', () => {
    test('filter: 尾部 push 重复值落在正确位置', () => {
        const source = new RxList([0, 1, 2])
        const filtered = source.filter(x => x % 2 === 0)
        try {
            source.push(0)
            expect(filtered.data).toEqual([0, 2, 0])
        } finally {
            filtered.destroy()
            source.destroy()
        }
    })

    test('filter: 删除重复值中的特定实例保持顺序', () => {
        const source = new RxList([0, 2, 0])
        const filtered = source.filter(x => x % 2 === 0)
        try {
            source.splice(2, 1) // 删除第二个 0
            expect(filtered.data).toEqual([0, 2])
        } finally {
            filtered.destroy()
            source.destroy()
        }
    })

    test('filter: set 替换重复值保持顺序', () => {
        const source = new RxList([0, 2, 0])
        const filtered = source.filter(x => x % 2 === 0)
        try {
            source.set(2, 1) // 把第二个 0 替换为不匹配的 1
            expect(filtered.data).toEqual([0, 2])
            source.set(2, 4)
            expect(filtered.data).toEqual([0, 2, 4])
        } finally {
            filtered.destroy()
            source.destroy()
        }
    })

    test('filter: 中段插入重复值保持顺序(评估最小化用例)', () => {
        const source = new RxList([1, 0, 0])
        const filtered = source.filter(x => x % 2 === 0)
        try {
            source.splice(0, 0, 2)
            source.push(2)
            source.splice(3, 0, 1)
            expect(filtered.data).toEqual(source.data.filter(x => x % 2 === 0))
        } finally {
            filtered.destroy()
            source.destroy()
        }
    })

    test('filter: 响应式行 toggle 在重复值下按行定位', () => {
        const flag = atom(true)
        const items = [{v: 0}, {v: 2}, {v: 0}]
        const source = new RxList(items)
        // 第二个 0 的匹配状态由 flag 控制
        const filtered = source.filter(item => item === items[2] ? flag() && item.v % 2 === 0 : item.v % 2 === 0)
        try {
            expect(filtered.data).toEqual([items[0], items[1], items[2]])
            flag(false)
            expect(filtered.data).toEqual([items[0], items[1]])
            flag(true)
            expect(filtered.data).toEqual([items[0], items[1], items[2]])
        } finally {
            filtered.destroy()
            source.destroy()
        }
    })

    test('groupBy: set 替换重复值保持组内顺序(评估最小化用例)', () => {
        const source = new RxList([0, 2, 3, 0])
        const grouped = source.groupBy(x => x % 2)
        try {
            source.set(3, 2)
            expect(grouped.data.get(0)!.data).toEqual([0, 2, 2])
        } finally {
            for (const g of grouped.data.values()) g.destroy()
            grouped.destroy()
            source.destroy()
        }
    })

    test('groupBy: 删除重复值中的特定实例保持组内顺序', () => {
        const source = new RxList([0, 2, 0, 4])
        const grouped = source.groupBy(x => x % 2)
        try {
            source.splice(2, 1) // 删除第二个 0
            expect(grouped.data.get(0)!.data).toEqual([0, 2, 4])
        } finally {
            for (const g of grouped.data.values()) g.destroy()
            grouped.destroy()
            source.destroy()
        }
    })
})

describe('F7: at() 订阅在列表收缩时收到通知', () => {
    test('splice 收缩后订阅被裁剪 index 的 computed 读到 undefined', () => {
        const source = new RxList(['a', 'b', 'c'])
        const tail = computed(() => source.at(2))
        try {
            expect(tail()).toBe('c')
            source.splice(0, 1)
            expect(tail()).toBe(undefined)
        } finally {
            destroyComputed(tail)
            source.destroy()
        }
    })

    test('pop 后订阅最后一个 index 的 computed 更新', () => {
        const source = new RxList(['a', 'b', 'c'])
        const last = computed(() => source.at(2))
        try {
            expect(last()).toBe('c')
            source.pop()
            expect(last()).toBe(undefined)
            // 恢复长度后重新可见
            source.push('d')
            expect(last()).toBe('d')
        } finally {
            destroyComputed(last)
            source.destroy()
        }
    })
})

describe('F8: 订阅者异常不泄漏全局追踪状态', () => {
    test('throwing applyPatch 从 splice 传播后 trackStack/shouldTrack 完整复位', () => {
        const stackDepthBefore = notifier.trackStack.length
        const shouldTrackBefore = notifier.shouldTrack
        const source = new RxList([1, 2])
        const derived = new RxList(
            function computation(this: RxList<number>) {
                this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                return source.data.slice()
            },
            function applyPatch() {
                throw new Error('patch boom')
            }
        )
        try {
            expect(() => source.push(3)).toThrow('patch boom')
            expect(notifier.trackStack.length).toBe(stackDepthBefore)
            expect(notifier.shouldTrack).toBe(shouldTrackBefore)
            // 2026-H2 起,patch 抛错会把 phase 回退到全量重算(增量状态不可信,
            // 见 handleRecomputeError):下一次变更不再进入 throwing patch,而是
            // 全量重算恢复一致——错误恢复后派生 ≡ 全量重算。
            expect(() => source.clear()).not.toThrow()
            expect(derived.data).toEqual(source.data.slice())
            expect(notifier.trackStack.length).toBe(stackDepthBefore)
            expect(notifier.shouldTrack).toBe(shouldTrackBefore)
        } finally {
            derived.destroy()
            source.destroy()
            notifier.trackStack.length = stackDepthBefore
            notifier.shouldTrack = shouldTrackBefore
        }
    })
})

describe('F9: destroy(ignoreChildren) 之后孤儿 child 可安全销毁', () => {
    test('父 effect destroy(true) 后单独销毁遗留 child 不崩溃', () => {
        const parent = new Computed(function (this: Computed) { return 1 })
        parent.run([], true)
        let child1: Computed
        let child2: Computed
        ReactiveEffect.activeScopes.push(parent)
        try {
            child1 = new Computed(function (this: Computed) { return 2 })
            child2 = new Computed(function (this: Computed) { return 3 })
            child1.run([], true)
            child2.run([], true)
        } finally {
            ReactiveEffect.activeScopes.pop()
        }
        parent.destroy(true)
        expect(child1!.parent).toBe(undefined)
        expect(() => child1!.destroy()).not.toThrow()
        expect(() => child2!.destroy()).not.toThrow()
        expect(child1!.active).toBe(false)
        expect(child2!.active).toBe(false)
    })
})

describe('F10: NaN 键/元素的增量删除', () => {
    test('RxMap.keys 对 delete(NaN) 不误删其它 key', () => {
        const map = new RxMap<number, string>([[1, 'a'], [NaN, 'x'], [2, 'b']])
        const keys = map.keys()
        try {
            expect(keys.data).toEqual([1, NaN, 2])
            map.delete(NaN)
            expect(keys.data).toEqual([1, 2])
        } finally {
            map.destroy()
        }
    })

    test('RxSet.toList 对 delete(NaN) 不误删其它元素', () => {
        const set = new RxSet<number>([1, NaN, 2])
        const list = set.toList()
        try {
            expect(list.data).toEqual([1, NaN, 2])
            set.delete(NaN)
            expect(list.data).toEqual([1, 2])
        } finally {
            list.destroy()
            set.destroy()
        }
    })
})

describe('杂项修复', () => {
    test('负小数 splice start 归一化为 +0,findIndex 不返回 -0', () => {
        const source = new RxList([5, 1, 2])
        const found = source.findIndex(x => x % 5 === 0)
        try {
            expect(found.raw).toBe(0)
            // -0.5 经 ToIntegerOrInfinity 应为 +0(规范语义),而不是 -0
            source.splice(-0.5, 1, 11)
            expect(source.data).toEqual([11, 1, 2])
            expect(Object.is(found.raw, -0)).toBe(false)
            expect(found.raw).toBe(-1)
            source.splice(-0.5, 0, 15)
            expect(Object.is(found.raw, -0)).toBe(false)
            expect(found.raw).toBe(0)
        } finally {
            destroyComputed(found)
            source.destroy()
        }
    })

    test('interceptor atom 持有 null 时属性赋值不抛 TypeError', () => {
        const a = atom<{ x?: number } | null>(null, (updater, handler) => [updater, handler])
        expect(() => { (a as any).x = 1 }).not.toThrow()
        // 与 primitive atom 行为一致:属性落在 updater 函数对象上,可读回
        expect((a as any).x).toBe(1)
    })
})
