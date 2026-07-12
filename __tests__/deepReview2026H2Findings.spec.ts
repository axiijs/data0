import {describe, expect, test} from 'vitest'
import {createSelection, createSelections, RxList} from '../src/RxList.js'
import {RxSet} from '../src/RxSet.js'
import {atom} from '../src/atom.js'

/**
 * 2026-07 深度评估轮（新方法：算子协作面横扫——把"重复值域"与"对抗值域（undefined 作为
 * 合法元素）"两条既有攻击轴推进到此前未覆盖的算子族：createSelection/createSelections
 * 与 toSorted 的 EXPLICIT_KEY_CHANGE 路径）中动态复现的缺陷证据。
 *
 * 按 AGENTS.md §3：已知缺陷以 test.fails 保存可执行证据；修复时必须改为普通测试。
 */

describe('DEFECT CLASS 1: createSelection 家族在重复 item 下 indicator 漂移', () => {
    // 机制：createSelectionInner 的 itemToIndicator 是 Map<item, indicator>。
    // 同一 item（重复原始值或重复对象引用）出现在多行时：
    //   1) createNewIndicator 后写覆盖前写——只有最后一行的 indicator 留在 Map 里；
    //   2) currentValues 变化时 updateIndicatorsFromCurrentValueChange 只更新 Map 中
    //      残留的那一个 indicator，其余同 item 行永不更新；
    //   3) splice 删除任一行会 deleteIndicator(item)，把仍存活的同 item 行的
    //      Map 条目一并误删——之后该行的 indicator 永久失联（可观察为永久卡 true）。
    // 等价类：与历史"重复原始值下按值定位"缺陷同源（duplicateValuesFuzz 防线未覆盖
    // selection 算子族；README 支持矩阵中 createSelection/createSelections 的"增量"
    // 格子缺少重复值差分资产）。

    test.fails('重复原始值：选中后两行都应为 true（实际第一行为 false）', () => {
        const list = new RxList<number>([5, 5, 7])
        const current = atom<number | null>(null)
        const selection = createSelection(list, current)
        try {
            current(5)
            expect(selection.data.map(([, ind]) => ind.raw)).toEqual([true, true, false])
        } finally {
            selection.destroy()
            list.destroy()
        }
    })

    test.fails('重复原始值：反选后第一行应回到 false（实际永久卡 true）', () => {
        const list = new RxList<number>([5, 5])
        const current = atom<number | null>(5)
        const selection = createSelection(list, current)
        try {
            expect(selection.data.map(([, ind]) => ind.raw)).toEqual([true, true])
            current(null)
            expect(selection.data.map(([, ind]) => ind.raw)).toEqual([false, false])
        } finally {
            selection.destroy()
            list.destroy()
        }
    })

    test.fails('重复 item + 行删除：deleteIndicator 误删存活行的条目，反选失效', () => {
        const list = new RxList<number>([5, 5, 7])
        const current = new RxSet<number>([])
        const selection = createSelection(list, current)
        try {
            current.add(5)
            list.splice(0, 1) // 删除第一行 5；另一行 5 仍存活
            current.delete(5)
            expect(selection.data.map(([, ind]) => ind.raw)).toEqual([false, false])
        } finally {
            selection.destroy()
            list.destroy()
            current.destroy()
        }
    })

    test.fails('createSelections 多选集版本同样受影响', () => {
        const list = new RxList<number>([5, 5])
        const cur = atom<number | null>(null)
        const sel = createSelections(list, [cur])
        try {
            cur(5)
            expect(sel.data.map(([, ind]) => ind.raw)).toEqual([true, true])
        } finally {
            sel.destroy()
            list.destroy()
        }
    })
})

describe('DEFECT CLASS 2: toSorted 的 set(EXPLICIT_KEY_CHANGE) 路径丢失 undefined 元素', () => {
    // 机制：toSorted 的 applyPatch 在 explicit key change 分支用
    // `oldValue !== undefined` / `newValue !== undefined` 判断"有无"，
    // 而 undefined 是 RxList 的合法元素值（triggerInfo 的 oldValue/newValue
    // 本身就允许为 undefined）。
    //   - set 引入 undefined：newValue === undefined 被跳过插入 → 派生列表丢行；
    //   - set 替换 undefined：oldValue === undefined 被跳过删除 → 派生列表残留行。
    // 全量重算语义（Array#sort 把 undefined 排最后）保留 undefined 元素，
    // 增量结果 ≠ 全量重算，违反 README「派生结构必须等于全量重算」的核心不变量。
    // 同族对照：splice 路径（argv/methodResult 传递）不受影响；null 值不受影响；
    // map/filter 的 set 路径按 key 定位、不按值判断，也不受影响。

    const compare = (a: number | undefined, b: number | undefined) =>
        (a ?? Infinity) - (b ?? Infinity)

    test.fails('set 引入 undefined：派生列表应保留该元素（实际丢行）', () => {
        const list = new RxList<number | undefined>([3, 1, 2])
        const sorted = list.toSorted(compare)
        try {
            list.set(1, undefined)
            const expected = list.data.slice().sort(compare) // [2, 3, undefined]
            expect(sorted.data).toEqual(expected)
        } finally {
            sorted.destroy()
            list.destroy()
        }
    })

    test.fails('set 替换 undefined：旧的 undefined 行应被移除（实际残留）', () => {
        const list = new RxList<number | undefined>([undefined, 5])
        const sorted = list.toSorted(compare)
        try {
            list.set(0, 1)
            const expected = list.data.slice().sort(compare) // [1, 5]
            expect(sorted.data).toEqual(expected)
        } finally {
            sorted.destroy()
            list.destroy()
        }
    })
})
