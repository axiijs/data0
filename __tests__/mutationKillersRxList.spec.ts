import {describe, expect, test} from 'vitest'
import {batch, notifier} from '../src/notify.js'
import {atom, Atom} from '../src/atom.js'
import {computed, Computed, destroyComputed, getComputedInternal} from '../src/computed.js'
import {createIndexKeySelection, RxList} from '../src/RxList.js'
import {RxSet} from '../src/RxSet.js'
import {onChange} from '../src/common.js'
import {normalizeSpliceStart} from '../src/util.js'

/**
 * 幸存 mutant 语料驱动的杀手资产（方法 15：survivor-corpus 驱动的语义钉扎）。
 *
 * 2026-H2 mutation 债务清仓轮：对 RxList.ts 重跑审计（449 survived / 80 no-cov），
 * 逐个分类幸存 mutant 后发现三类**真实检出盲区**（区别于"回退/范围放宽/快慢路径"
 * 等行为等价类）：
 *   1. 契约守卫的可达性从未被断言（reposition/swap 参数越界、indexBy 重复 key、
 *      已销毁实例变更 no-op 的 clear/reorder/set 入口）；
 *   2. 协议**字段值**从未被钉住（reorderInfo 的 kind/affectedRange/movedCount/
 *      oldIndexToNewIndex——consumerContractReplay 只钉了字段存在性与 argv 形状）；
 *   3. 差分 fuzz 断言粒度太粗：只对比终值数据，不检查"谁被通知了"——
 *      at(index) 订阅的精确触发（不多不少、升序）、派生结构的零操作不触发、
 *      slice 的区间算术在边界相等处的行为，都能被变异而不惊动任何测试。
 *
 * 本文件的每一组测试都对应审计报告中一簇具体的幸存 mutant（注释标注源码行域）。
 */

function witness(target: any): () => number {
    const internal: Computed = target instanceof Computed ? target : getComputedInternal(target)!
    let count = 0
    internal.on('fullRecompute', () => count++)
    return () => count
}

// ---------------------------------------------------------------------------
// 1. 契约守卫可达性（doSplice/reorder 家族的 assert 与 destroyed no-op 分支）
// ---------------------------------------------------------------------------
describe('变更方法契约守卫', () => {
    test('reposition/swap 越界参数抛出精确错误（守卫可达且消息稳定）', () => {
        const list = new RxList([0, 1, 2])
        try {
            expect(() => list.reposition(-1, 0)).toThrow('start index out of range')
            expect(() => list.reposition(0, 0, 0)).toThrow('start index out of range')  // limit <= 0
            expect(() => list.reposition(2, 0, 2)).toThrow('start index out of range')  // start+limit 越界
            expect(() => list.reposition(0, 3)).toThrow('newStart index out of range')
            expect(() => list.reposition(0, 2, 2)).toThrow('newStart index out of range')
            expect(() => list.swap(-1, 0)).toThrow('start index out of range')
            expect(() => list.swap(0, 0, 0)).toThrow('start index out of range')
            expect(() => list.swap(2, 0, 2)).toThrow('start index out of range')
            expect(() => list.swap(0, 3)).toThrow('newStart index out of range')
            // 守卫抛错不破坏数据
            expect(list.data).toEqual([0, 1, 2])
        } finally {
            list.destroy()
        }
    })

    test('已销毁实例的 clear/set/reorder 家族全部 no-op 且不通知', () => {
        const list = new RxList([1, 2, 3])
        const infos: any[] = []
        const stop = onChange(list, (batchInfos: any[]) => infos.push(...batchInfos))
        list.destroy()
        expect(list.clear()).toEqual([])
        expect(list.set(0, 9)).toBe(undefined)
        list.reorder([[0, 1], [1, 0]])
        list.sortSelf((a, b) => b - a)
        expect(list.data).toEqual([1, 2, 3])
        expect(infos.length).toBe(0)
        stop()
    })

    test('indexBy/toMap 重复 key 在全量与增量两侧都被断言拒绝', () => {
        // 全量侧：构造即断言
        const dupSource = new RxList([{k: 1}, {k: 1}])
        expect(() => dupSource.indexBy('k')).toThrow('indexBy key is already exist')
        dupSource.destroy()

        const dupTuples = new RxList<[string, number]>([['a', 1], ['a', 2]])
        expect(() => dupTuples.toMap()).toThrow('indexBy key is already exist')
        dupTuples.destroy()

        // 增量侧：patch 中 push 重复 key
        const source = new RxList([{k: 1}])
        const indexed = source.indexBy('k')
        expect(() => source.push({k: 1})).toThrow('indexBy key is already exist')
        indexed.destroy()
        source.destroy()
    })

    test('clear 语义：空 clear 不触发、非空恰一次、返回删除项', () => {
        const list = new RxList<number>([])
        let calls = 0
        const stop = onChange(list, () => calls++)
        expect(list.clear()).toEqual([])
        expect(calls).toBe(0)

        list.push(1, 2)
        calls = 0
        expect(list.clear()).toEqual([1, 2])
        expect(calls).toBe(1)
        stop()
        list.destroy()
    })

    test('clear 走 splice 路径的条件正确：at() 订阅与 map(index) 都被正确清理', () => {
        // at() 订阅（hasIndexKeyDeps）：clear 后订阅者必须刷新为 undefined
        const withDep = new RxList([7, 8])
        const second = computed(() => withDep.at(1))
        expect(second()).toBe(8)
        withDep.clear()
        expect(second()).toBe(undefined)
        destroyComputed(second)
        withDep.destroy()

        // atomIndexes（map 使用 index）：clear 后重新填充必须保持行/index 对齐
        // （错走 fast path 会残留 stale atomIndexes，dev 不变量或行值断言当场暴露）
        const withIndex = new RxList([5, 6])
        const mapped = withIndex.map((x, idx) => ({x, idx}))
        withIndex.clear()
        expect(mapped.data).toEqual([])
        withIndex.push(9)
        withIndex.unshift(3)
        expect(mapped.data.map(e => e.x)).toEqual([3, 9])
        mapped.data.forEach((e, i) => expect(e.idx.raw).toBe(i))
        mapped.destroy()
        withIndex.destroy()
    })
})

// ---------------------------------------------------------------------------
// 2. reorderInfo 协议字段值（createReorderPatchInfo + reposition/swap 的 newOrder 构造）
// ---------------------------------------------------------------------------
describe('reorderInfo 协议字段值', () => {
    function captureReorderInfo(run: (list: RxList<number>) => void, initial = [0, 1, 2, 3, 4]) {
        const list = new RxList(initial.slice())
        const captured: any[] = []
        const stop = onChange(list, (infos: any[]) => {
            for (const info of infos) if (info.method === 'reorder') captured.push(info)
        })
        run(list)
        const data = list.data.slice()
        stop()
        list.destroy()
        return {captured, data}
    }

    test('reposition 前移（newStart < start）：newOrder/数据/info 字段全对', () => {
        const {captured, data} = captureReorderInfo(list => list.reposition(3, 0, 2))
        // 数据与朴素参考实现一致
        expect(data).toEqual([3, 4, 0, 1, 2])
        expect(captured.length).toBe(1)
        const info = captured[0].reorderInfo
        expect(info.kind).toBe('move')
        expect(info.start).toBe(3)
        expect(info.newStart).toBe(0)
        expect(info.limit).toBe(2)
        expect(info.movedCount).toBe(5)
        expect(info.affectedRange).toEqual([0, 4])
        expect([...info.oldIndexToNewIndex.entries()].sort((a: any, b: any) => a[0] - b[0]))
            .toEqual([[0, 2], [1, 3], [2, 4], [3, 0], [4, 1]])
    })

    test('reposition 后移（newStart > start）：搬移方向与区间正确', () => {
        const {captured, data} = captureReorderInfo(list => list.reposition(0, 2, 2))
        expect(data).toEqual([2, 3, 0, 1, 4])
        const info = captured[0].reorderInfo
        expect(info.kind).toBe('move')
        expect(info.movedCount).toBe(4)
        expect(info.affectedRange).toEqual([0, 3])
        expect([...info.oldIndexToNewIndex.entries()].sort((a: any, b: any) => a[0] - b[0]))
            .toEqual([[0, 2], [1, 3], [2, 0], [3, 1]])
    })

    test('swap 多 limit：两段互换且 info 完整', () => {
        const {captured, data} = captureReorderInfo(list => list.swap(0, 3, 2))
        expect(data).toEqual([3, 4, 2, 0, 1])
        const info = captured[0].reorderInfo
        expect(info.kind).toBe('swap')
        expect(info.start).toBe(0)
        expect(info.newStart).toBe(3)
        expect(info.limit).toBe(2)
        expect(info.movedCount).toBe(4)
        expect(info.affectedRange).toEqual([0, 4])
    })

    test('sortSelf：identity 对不计入 movedCount，affectedRange 只覆盖移动跨度', () => {
        const {captured, data} = captureReorderInfo(list => list.sortSelf((a, b) => a - b), [0, 2, 1, 3])
        expect(data).toEqual([0, 1, 2, 3])
        const info = captured[0].reorderInfo
        expect(info.kind).toBe('sort')
        expect(info.movedCount).toBe(2)
        expect(info.affectedRange).toEqual([1, 2])
    })

    test('identity reorder：movedCount 0、affectedRange null', () => {
        const {captured, data} = captureReorderInfo(list => list.reorder([[0, 0], [1, 1]]), [10, 11])
        expect(data).toEqual([10, 11])
        const info = captured[0].reorderInfo
        expect(info.kind).toBe('reorder')
        expect(info.movedCount).toBe(0)
        expect(info.affectedRange).toBe(null)
    })

    test('createIndexKeySelection：reposition/swap 后指示器仍按 index 语义', () => {
        const source = new RxList(['a', 'b', 'c', 'd', 'e'])
        const selected = new RxSet<number>([1, 3])
        const selection = createIndexKeySelection(source, selected)
        const indicators = () => selection.data.map(([, ind]) => ind())
        try {
            expect(indicators()).toEqual([false, true, false, true, false])
            // 行随源重排，但选中的 index 不动
            source.reposition(3, 0, 2)   // [d,e,a,b,c]
            expect(selection.data.map(([item]) => item)).toEqual(['d', 'e', 'a', 'b', 'c'])
            expect(indicators()).toEqual([false, true, false, true, false])
            source.swap(0, 4)            // [c,e,a,b,d]
            expect(selection.data.map(([item]) => item)).toEqual(['c', 'e', 'a', 'b', 'd'])
            expect(indicators()).toEqual([false, true, false, true, false])
            // 不等长 splice：后续行整体平移后逐行按当前 index 重算
            source.splice(0, 1)          // [e,a,b,d]
            expect(indicators()).toEqual([false, true, false, true])
        } finally {
            selection.destroy()
            selected.destroy()
            source.destroy()
        }
    })

    test('createIndexKeySelection：Atom 单选在不等长 splice 后按 index 校正', () => {
        const source = new RxList(['a', 'b', 'c'])
        const selected = atom<number | null>(2)
        const selection = createIndexKeySelection(source, selected)
        try {
            expect(selection.data.map(([, ind]) => ind())).toEqual([false, false, true])
            source.unshift('z')  // 行平移，但选中的 index 2 不动
            expect(selection.data.map(([, ind]) => ind())).toEqual([false, false, true, false])
        } finally {
            selection.destroy()
            source.destroy()
        }
    })
})

// ---------------------------------------------------------------------------
// 3. at(index) 精确触发枚举（doSplice 的受影响区间算术，L295-311 簇）
// ---------------------------------------------------------------------------
describe('at(index) 精确触发（受影响区间的有界穷举）', () => {
    type Probe = {
        list: RxList<number>,
        runLog: number[],
        subs: Atom<number | undefined>[],
        destroy: () => void,
    }

    // 订阅 0..maxIndex 的 at(i)。倒序创建：indexKeyDeps 的插入序为降序，
    // 若实现丢失 affected 的升序排序，触发顺序断言当场失败。
    function createProbe(len: number, maxIndex: number): Probe {
        const list = new RxList<number>(Array.from({length: len}, (_, i) => i * 10 + 10))
        const runLog: number[] = []
        const subs: Atom<number | undefined>[] = []
        for (let i = maxIndex; i >= 0; i--) {
            subs[i] = computed(() => {
                runLog.push(i)
                return list.at(i)
            })
        }
        return {
            list, runLog, subs,
            destroy() {
                subs.forEach(sub => destroyComputed(sub))
                list.destroy()
            }
        }
    }

    function assertPrecise(probe: Probe, before: (number | undefined)[], op: () => void) {
        const {list, runLog, subs} = probe
        runLog.length = 0
        op()
        const reruns = runLog.slice()
        // (a) 升序触发（affected 排序，kills doSplice 的 sort/比较 mutants）
        expect(reruns).toEqual(reruns.slice().sort((a, b) => a - b))
        // (b) 触发集合 === 值变化集合（值全 distinct 时精确成立；
        //     多触发=幽灵通知，少触发=静默陈旧，双向都杀）
        const changed: number[] = []
        for (let i = 0; i < subs.length; i++) {
            if (!Object.is(before[i], list.data[i])) changed.push(i)
        }
        expect(new Set(reruns)).toEqual(new Set(changed))
        // (c) 订阅者终值 ≡ 数据
        for (let i = 0; i < subs.length; i++) {
            expect(subs[i]()).toBe(list.data[i])
        }
    }

    test('splice 参数域 × 列表长度的穷举：触发不多、不少、升序、值对', () => {
        const lens = [0, 2, 4, 5]
        const starts = [-3, -1, 0, 1, 2, 4, 7]
        const deleteCounts = [0, 1, 2, 5]
        const insertCounts = [0, 1, 3]
        let nextInsert = 1000
        for (const len of lens) {
            for (const start of starts) {
                for (const del of deleteCounts) {
                    for (const ins of insertCounts) {
                        const probe = createProbe(len, len + 1)
                        const before = probe.subs.map((_, i) => probe.list.data[i])
                        const items = Array.from({length: ins}, () => nextInsert++)
                        assertPrecise(probe, before, () => probe.list.splice(start, del, ...items))
                        probe.destroy()
                    }
                }
            }
        }
    })

    test('set 精确触发：只有被替换的 index 重跑', () => {
        for (const len of [1, 3, 5]) {
            for (const key of [0, Math.floor(len / 2), len - 1]) {
                const probe = createProbe(len, len)
                const before = probe.subs.map((_, i) => probe.list.data[i])
                assertPrecise(probe, before, () => probe.list.set(key, 7777 + key))
                probe.destroy()
            }
        }
    })

    test('splice 之前的订阅 index 从不被触发（normalizedStart 下界）', () => {
        const probe = createProbe(5, 6)
        probe.runLog.length = 0
        probe.list.splice(3, 1, 501, 502)
        expect(probe.runLog.every(i => i >= 3)).toBe(true)
        probe.destroy()
    })

    test('reorder 家族：值全对，触发覆盖全部值变化位', () => {
        const ops: Array<(l: RxList<number>) => void> = [
            l => l.swap(0, 3),
            l => l.reposition(1, 3, 2),
            l => l.sortSelf((a, b) => b - a),
        ]
        for (const op of ops) {
            const probe = createProbe(5, 4)
            const before = probe.subs.map((_, i) => probe.list.data[i])
            probe.runLog.length = 0
            op(probe.list)
            const changed = new Set<number>()
            for (let i = 0; i < probe.subs.length; i++) {
                if (!Object.is(before[i], probe.list.data[i])) changed.add(i)
            }
            // reorder 对 identity 对也可能触发（同值重写），只断言不漏
            for (const c of changed) expect(probe.runLog).toContain(c)
            for (let i = 0; i < probe.subs.length; i++) {
                expect(probe.subs[i]()).toBe(probe.list.data[i])
            }
            probe.destroy()
        }
    })
})

// ---------------------------------------------------------------------------
// 4. map(index) × 结构操作枚举（applyMapArrayPatch 的 index atom 维护）
// ---------------------------------------------------------------------------
describe('map(index) 行位置枚举', () => {
    test('splice 参数域穷举后行值与 index atom 全对齐', () => {
        const starts = [-2, 0, 1, 3, 6]
        const deleteCounts = [0, 1, 3]
        const insertCounts = [0, 1, 2]
        let nextInsert = 500
        for (const start of starts) {
            for (const del of deleteCounts) {
                for (const ins of insertCounts) {
                    const source = new RxList([1, 2, 3, 4])
                    const mapped = source.map((x, idx) => ({x, idx}))
                    const items = Array.from({length: ins}, () => nextInsert++)
                    source.splice(start, del, ...items)
                    expect(mapped.data.map(e => e.x)).toEqual(source.data)
                    mapped.data.forEach((e, i) => expect(e.idx.raw).toBe(i))
                    mapped.destroy()
                    source.destroy()
                }
            }
        }
    })

    test('两个 map(index) 共享 atomIndexes：销毁其一后另一个仍正确', () => {
        const source = new RxList([1, 2, 3])
        const m1 = source.map((x, idx) => x * 100 + idx.raw)
        const m2 = source.map((x, idx) => ({x, idx}))
        m1.destroy()
        // 引用计数若被变异为"销毁任意一个就清空 atomIndexes"，后续结构操作会让 m2 失联
        source.splice(1, 0, 9)
        source.unshift(8)
        expect(m2.data.map(e => e.x)).toEqual(source.data)
        m2.data.forEach((e, i) => expect(e.idx.raw).toBe(i))
        m2.destroy()
        source.destroy()
    })

    test('自定义调度器下 map 的应用延迟到调度回调（触发即应用属变异）', () => {
        const source = new RxList([1, 2])
        let scheduled: ((force?: boolean) => void) | undefined
        let markDirtyFn: (() => void) | undefined
        const mapped = source.map(x => x * 10, {
            scheduleRecompute(recompute, markDirty) {
                scheduled = recompute
                markDirtyFn = markDirty
            }
        })
        // 下游读 mapped（建立 ITERATE 依赖），用于 markDirty 传播检查
        let downstreamRuns = 0
        const joined = computed(() => {
            downstreamRuns++
            return mapped.toArray().join(',')
        })
        expect(joined()).toBe('10,20')

        source.push(3)
        // 调度语义：触发只入队，不立即应用
        expect(scheduled).toBeTypeOf('function')
        expect(mapped.data).toEqual([10, 20])

        // markDirty 沿依赖图强制下游重跑（RxList 以类实例为 track 目标）
        const runsBefore = downstreamRuns
        markDirtyFn!()
        expect(downstreamRuns).toBe(runsBefore + 1)

        // 调度回调真正应用
        scheduled!()
        expect(mapped.data).toEqual([10, 20, 30])
        expect(joined()).toBe('10,20,30')

        destroyComputed(joined)
        mapped.destroy()
        source.destroy()
    })
})

// ---------------------------------------------------------------------------
// 5. slice 区间算术枚举（clampIndexes/ucHead/ucTail，L1774-1879 簇）
// ---------------------------------------------------------------------------
describe('slice 区间算术枚举', () => {
    const boundsPairs: Array<[number | undefined, number | undefined]> = [
        [0, 2], [1, 4], [2, 2], [0, undefined], [3, undefined], [4, 5],
        [-2, undefined], [-4, -1], [1, -1], [-10, 10], [5, undefined], [0, -10],
        [undefined, undefined],
    ]
    const makeOps = (): Array<[string, (l: RxList<number>) => void, boolean]> => {
        // [名字, 操作, 是否属于"非负边界下应增量"的结构操作]
        let n = 900
        return [
            ['push', l => l.push(n++), true],
            ['pop', l => { l.pop() }, true],
            ['shift', l => { l.shift() }, true],
            ['unshift', l => l.unshift(n++), true],
            ['splice(1,1,x)', l => l.splice(1, 1, n++), true],
            ['splice(2,0,x,y)', l => l.splice(2, 0, n++, n++), true],
            ['splice(0,3)', l => l.splice(0, 3), true],
            ['set(0)', l => l.set(0, n++), true],
            ['set(mid)', l => l.set(2, n++), true],
            ['sortSelf', l => l.sortSelf((a, b) => b - a), false],
            ['batch(push+shift)', l => batch(() => { l.push(n++); l.shift() }), false],
        ]
    }

    function nativeSlice(arr: number[], start?: number, end?: number) {
        if (start === undefined) return arr.slice()
        if (end === undefined) return arr.slice(start)
        return arr.slice(start, end)
    }

    test('边界 × 操作穷举：终值恒等于原生 slice', () => {
        for (const [start, end] of boundsPairs) {
            for (const [, op] of makeOps()) {
                const source = new RxList([10, 20, 30, 40, 50])
                const sliced = source.slice(start, end)
                op(source)
                expect(sliced.data, `slice(${start},${end})`).toEqual(nativeSlice(source.data, start, end))
                sliced.destroy()
                source.destroy()
            }
        }
    })

    test('短列表/空列表边界：窗口消失与再现都收敛', () => {
        for (const [start, end] of boundsPairs) {
            const source = new RxList<number>([1])
            const sliced = source.slice(start, end)
            source.pop()                       // 窗口消失
            expect(sliced.data).toEqual(nativeSlice(source.data, start, end))
            source.push(7, 8, 9)               // 再现
            expect(sliced.data).toEqual(nativeSlice(source.data, start, end))
            source.splice(0, 2, 5)             // 再收缩
            expect(sliced.data).toEqual(nativeSlice(source.data, start, end))
            sliced.destroy()
            source.destroy()
        }
    })

    test('增量性：非负窗口的单结构操作零回退，负边界结构操作必回退', () => {
        // 非负窗口（含 start=0 的边界窗口）：矩阵承诺 splice/set 增量。
        // 例外：splice 区间完全吞掉窗口（ucHead/ucTail 双空）时实现回退全量,
        // 属"增量格子的已知回退形态"，不在零回退断言之列。
        for (const [ws, we] of [[1, 4], [0, 3]] as Array<[number, number]>) {
            for (const [name, op, incremental] of makeOps()) {
                if (!incremental) continue
                const swallowsWindow = ws === 0 && name === 'splice(0,3)'
                const source = new RxList([10, 20, 30, 40, 50])
                const sliced = source.slice(ws, we)
                const fulls = witness(sliced)
                op(source)
                if (!swallowsWindow) expect(fulls(), `slice(${ws},${we}) op ${name} 应增量`).toBe(0)
                expect(sliced.data).toEqual(source.data.slice(ws, we))
                sliced.destroy()
                source.destroy()
            }
        }
        // 负边界：结构操作必回退（README 脚注只把负边界回退限定在 splice）
        const source = new RxList([10, 20, 30, 40, 50])
        const negSliced = source.slice(-3)
        const fulls = witness(negSliced)
        source.push(60)
        expect(fulls()).toBeGreaterThanOrEqual(1)
        expect(negSliced.data).toEqual(source.data.slice(-3))
        // set（explicit key change）不在负边界回退之列：长度不变，区间平移不发生
        const fullsBeforeSet = fulls()
        source.set(4, 99)
        expect(fulls()).toBe(fullsBeforeSet)
        expect(negSliced.data).toEqual(source.data.slice(-3))
        negSliced.destroy()
        source.destroy()
    })

    test('窗口外操作不产生 slice 通知（幽灵触发钉扎）', () => {
        const source = new RxList([10, 20, 30, 40, 50])
        const sliced = source.slice(1, 3)
        let calls = 0
        const stop = onChange(sliced, () => calls++)
        source.push(99)          // 完全在窗口后
        expect(sliced.data).toEqual(source.data.slice(1, 3))
        expect(calls).toBe(0)
        source.splice(4, 1, 77)  // 仍在窗口后
        expect(calls).toBe(0)
        source.set(0, 5)         // 窗口前的 set：窗口内容不变
        expect(calls).toBe(0)
        source.splice(1, 1, 21)  // 窗口内：恰一次
        expect(calls).toBe(1)
        stop()
        sliced.destroy()
        source.destroy()
    })
})

// ---------------------------------------------------------------------------
// 6. groupBy 的 NaN key 与组内前缀定位（sameKey/removeAtSourcePosition，L1449-1475）
// ---------------------------------------------------------------------------
describe('groupBy × NaN key 与重复 key 前缀定位', () => {
    function referenceGroups<T>(data: T[]): Map<any, T[]> {
        const groups = new Map<any, T[]>()
        for (const item of data) {
            if (!groups.has(item)) groups.set(item, [])
            groups.get(item)!.push(item)
        }
        return groups
    }

    function assertGroupsMatch(groups: any, data: number[]) {
        const ref = referenceGroups(data)
        for (const [key, group] of groups.data) {
            expect(group.data).toEqual(ref.get(key) ?? [])
        }
        for (const [key, items] of ref) {
            expect(groups.data.get(key)?.data).toEqual(items)
        }
    }

    test('identity key 含 NaN 孪生：增量始终等于全量重算', () => {
        const source = new RxList<number>([NaN, 1, NaN, 1, 2])
        const groups = source.groupBy(x => x)
        try {
            assertGroupsMatch(groups, source.data)
            source.splice(1, 0, NaN)         // 中段插入 NaN
            assertGroupsMatch(groups, source.data)
            source.splice(0, 1)              // 删除头部 NaN
            assertGroupsMatch(groups, source.data)
            source.set(1, NaN)               // 1 → NaN
            assertGroupsMatch(groups, source.data)
            source.set(0, 1)                 // NaN → 1
            assertGroupsMatch(groups, source.data)
            source.splice(2, 2, 1, NaN)      // 混合替换
            assertGroupsMatch(groups, source.data)
        } finally {
            for (const g of groups.data.values()) g.destroy()
            groups.destroy()
            source.destroy()
        }
    })

    test('重复 key 中段删除/插入：组内顺序与全量一致（前缀计数）', () => {
        const source = new RxList([1, 2, 1, 2, 1])
        const groups = source.groupBy(x => x % 2)
        const flatten = () => ({odd: groups.data.get(1)?.data.slice(), even: groups.data.get(0)?.data.slice()})
        try {
            source.splice(2, 1)      // 删除中间的 1
            expect(flatten().odd).toEqual(source.data.filter(x => x % 2 === 1))
            source.splice(1, 0, 3)   // 中段插入奇数
            expect(flatten().odd).toEqual(source.data.filter(x => x % 2 === 1))
            expect(flatten().even).toEqual(source.data.filter(x => x % 2 === 0))
        } finally {
            for (const g of groups.data.values()) g.destroy()
            groups.destroy()
            source.destroy()
        }
    })
})

// ---------------------------------------------------------------------------
// 7. filter 的幽灵触发钉扎（flushPending 的差量门，L1291 簇）
// ---------------------------------------------------------------------------
describe('filter 幽灵触发钉扎', () => {
    test('不改变匹配集的操作不产生任何通知', () => {
        const source = new RxList([1, 2, 3, 4])
        const evens = source.filter(x => x % 2 === 0)
        let calls = 0
        const stop = onChange(evens, () => calls++)

        source.set(0, 5)          // 奇 → 奇
        expect(calls).toBe(0)
        source.splice(0, 1, 7)    // 奇替换奇
        expect(calls).toBe(0)
        source.push(9)            // 新增奇数
        expect(calls).toBe(0)
        expect(evens.data).toEqual(source.data.filter(x => x % 2 === 0))

        source.push(10)           // 新增偶数：恰一次
        expect(calls).toBe(1)
        source.splice(0, 1)       // 删除奇数：无通知
        expect(calls).toBe(1)
        expect(evens.data).toEqual(source.data.filter(x => x % 2 === 0))

        stop()
        evens.destroy()
        source.destroy()
    })
})

// ---------------------------------------------------------------------------
// 8. concat 的契约外 key 回退（L1756 簇）
// ---------------------------------------------------------------------------
describe('concat × 契约外 set key', () => {
    test('负/小数 key 的 explicit change 回退全量而不是错位应用', () => {
        for (const badKey of [-1, 1.5]) {
            const a = new RxList<number>([1, 2])
            const b = new RxList<number>([3])
            const cat = a.concat(b)
            const fulls = witness(cat)
            a.set(badKey as number, 99)
            expect(fulls()).toBeGreaterThanOrEqual(1)
            expect(cat.data).toEqual([...a.data, ...b.data])
            cat.destroy()
            a.destroy()
            b.destroy()
        }
    })
})

// ---------------------------------------------------------------------------
// 9. findIndex 简化后的行为回归（增量 cache 删除的等价性证据）
// ---------------------------------------------------------------------------
describe('findIndex 增量 cache 删除后的行为等价回归', () => {
    test('负 start splice 单 info 仍增量且起点归一化正确', () => {
        const source = new RxList([5, 1, 2])
        const found = source.findIndex(x => x % 5 === 0)
        const fulls = witness(found)
        try {
            expect(found.raw).toBe(0)
            source.splice(-3, 1, 11)      // 归一化到 0，旧 match 被删
            expect(found.raw).toBe(-1)
            source.splice(-1, 0, 10)      // 尾部前插入新 match
            expect(found.raw).toBe(2)
            expect(fulls()).toBe(0)       // 全程增量
        } finally {
            destroyComputed(found)
            source.destroy()
        }
    })

    test('reactive 谓词：初次/替换行引入的依赖都触发全量回退且结果正确', () => {
        const source = new RxList([{score: atom(1)}, {score: atom(3)}])
        const found = source.findIndex(item => item.score() >= 3)
        try {
            expect(found.raw).toBe(1)
            source.data[0].score(9)
            expect(found.raw).toBe(0)
            // 替换行（explicit key change 路径的 matchOne）引入的新依赖同样生效
            const replacement = {score: atom(0)}
            source.set(0, replacement)
            expect(found.raw).toBe(1)
            replacement.score(5)
            expect(found.raw).toBe(0)
        } finally {
            destroyComputed(found)
            source.destroy()
        }
    })

    test('谓词抛错后全局追踪栈复原（收集 frame 的 finally 路径）', () => {
        const framesBefore = notifier.trackTargetFrames.length
        const source = new RxList([1])
        expect(() => source.findIndex(() => { throw new Error('predicate failure') }))
            .toThrow('predicate failure')
        expect(notifier.trackTargetFrames.length).toBe(framesBefore)
        expect(notifier.currentTrackFrame).toBe(undefined)
        source.destroy()
    })

    test('explicit set 在当前 match 之前产生更小 match：只验证该位，不全量', () => {
        const source = new RxList([1, 2, 6, 8])
        const found = source.findIndex(x => x % 2 === 0)
        const fulls = witness(found)
        try {
            expect(found.raw).toBe(1)
            source.set(0, 4)          // 更小的 match
            expect(found.raw).toBe(0)
            source.set(3, 7)          // 在 match 之后的变化不影响
            expect(found.raw).toBe(0)
            expect(fulls()).toBe(0)
        } finally {
            destroyComputed(found)
            source.destroy()
        }
    })

    test('normalizeSpliceStart 的越界/负值语义与 findIndex 增量一致（对拍）', () => {
        // 直接对拍工具函数（区间算术的最小锚点）
        expect(normalizeSpliceStart(-2, 5)).toBe(3)
        expect(normalizeSpliceStart(-9, 5)).toBe(0)
        expect(normalizeSpliceStart(9, 5)).toBe(5)
        expect(normalizeSpliceStart(2.9, 5)).toBe(2)
        expect(Object.is(normalizeSpliceStart(-0.5, 5), 0)).toBe(true)
    })
})

// ---------------------------------------------------------------------------
// 5. map 行级 hasPendingStructuralInfos 守卫（2026-H3 round4 mutation 幸存
//    分类第 e 类"套件无法构造触发态"的补杀）
//    源码形状：`if (!hasPendingStructuralInfos) { 直写终态位置 } else { frame 定位 }`。
//    守卫的 load-bearing 场景：batch 内**先写行内依赖、再做结构操作**——
//    行级 rowComputed 排在派生列表 patch 之前运行，此刻 mapped 还是结构变更前
//    的形态。把条件变异成**恒 true**（强制直写分支）会按终态位置写进 pre-patch
//    的 mapped：写错行/写进将被删除的位置，随后 patch 搬移后新值静默丢失、
//    旧值残留——本组第一个测试杀灭该变体（已动态翻转自证）。
//    （条件变异成恒 false = 恒走 frame 定位：非 pending 状态下行的当前位置
//    恰等于终态位置，结果等价、只多一次 O(n) frame 扫描，属安全方向接受项。）
// ---------------------------------------------------------------------------
describe('map 行级重算 × 结构操作在同一 batch 的先后两序', () => {
    // CAUTION 行引用独立持有（不复用传给构造器的数组）：构造采纳外部数组
    //  是零拷贝所有权移交（A3），splice 会就地改写它。
    const mkRow = (label: string) => ({label: atom(label)})

    test('batch 内先写行依赖、再 splice：行新值不得丢失（frame 定位分支 load-bearing）', () => {
        const [a, b, c] = [mkRow('a'), mkRow('b'), mkRow('c')]
        const source = new RxList([a, b, c])
        const mapped = source.map(row => row.label())
        try {
            expect(mapped.data).toEqual(['a', 'b', 'c'])
            batch(() => {
                b.label('B!')           // 行依赖先入队
                source.splice(0, 1)     // 结构操作后入队：digest 时行级重算先运行
            })
            expect(source.data.length).toBe(2)
            expect(mapped.data).toEqual(source.data.map(row => row.label.raw))
            expect(mapped.data).toEqual(['B!', 'c'])
        } finally {
            mapped.destroy()
            source.destroy()
        }
    })

    test('batch 内先 splice、再写行依赖：direct 分支路径同样收敛', () => {
        const [a, b, c] = [mkRow('a'), mkRow('b'), mkRow('c')]
        const source = new RxList([a, b, c])
        const mapped = source.map(row => row.label())
        try {
            batch(() => {
                source.splice(0, 1)
                c.label('C!')
            })
            expect(mapped.data).toEqual(source.data.map(row => row.label.raw))
            expect(mapped.data).toEqual(['b', 'C!'])
        } finally {
            mapped.destroy()
            source.destroy()
        }
    })

    test('batch 内写行依赖后该行被删除：新值既不落错行也不复活', () => {
        const [a, b, c] = [mkRow('a'), mkRow('b'), mkRow('c')]
        const source = new RxList([a, b, c])
        const mapped = source.map(row => row.label())
        try {
            batch(() => {
                b.label('B!')
                source.splice(1, 1)     // 删除被写的行
            })
            expect(mapped.data).toEqual(source.data.map(row => row.label.raw))
            expect(mapped.data).toEqual(['a', 'c'])
        } finally {
            mapped.destroy()
            source.destroy()
        }
    })
})
