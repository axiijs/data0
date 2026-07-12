import {describe, expect, test} from 'vitest'
import {createSelection, createSelections, RxList} from '../src/RxList.js'
import {RxSet} from '../src/RxSet.js'
import {atom} from '../src/atom.js'
import {destroyComputed} from '../src/computed.js'
import {mulberry32, withUndefined} from './fuzzKit.js'

/**
 * 2026-07 深度评估轮（方法 10：既有攻击轴 × 未覆盖算子族的组合横扫）发现并修复的
 * 两个缺陷类。本文件同时承担：
 *   1) 实例级回归（当初的 test.fails 证据，修复后翻转为普通测试）；
 *   2) 等价类常驻防线（AGENTS.md §3.1）：
 *      - 重复 item 值域 × selection 家族的差分 fuzz（不变量：每行 indicator ≡
 *        currentValues 是否含该行 item）；
 *      - undefined 合法元素值域 × toSorted 的差分 fuzz（不变量：增量 ≡ 全量重算）。
 */

describe('defect class 1 (fixed): createSelection 家族在重复 item 下 indicator 漂移', () => {
    // 机制（修复前）：itemToIndicator 是 Map<item, 单个 indicator>，重复行后写覆盖前写；
    // currentValues 变化只更新最后一行，删除任一行会误删存活孪生行的条目。
    // 修复：Map<item, Set<indicator>> 广播 + 按 indicator 身份精确移除。

    test('重复原始值：选中后所有同值行都为 true', () => {
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

    test('重复原始值：反选后所有同值行回到 false', () => {
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

    test('重复 item + 行删除：存活孪生行的记账不被误删，反选仍生效', () => {
        const list = new RxList<number>([5, 5, 7])
        const current = new RxSet<number>([])
        const selection = createSelection(list, current)
        try {
            current.add(5)
            expect(selection.data.map(([, ind]) => ind.raw)).toEqual([true, true, false])
            list.splice(0, 1) // 删除第一行 5；另一行 5 仍存活
            current.delete(5)
            expect(selection.data.map(([, ind]) => ind.raw)).toEqual([false, false])
        } finally {
            selection.destroy()
            list.destroy()
            current.destroy()
        }
    })

    test('重复对象引用同样成组更新', () => {
        const o = {id: 1}
        const list = new RxList<{id: number}>([o, o])
        const current = atom<{id: number} | null>(null)
        const selection = createSelection(list, current)
        try {
            current(o)
            expect(selection.data.map(([, ind]) => ind.raw)).toEqual([true, true])
            current(null)
            expect(selection.data.map(([, ind]) => ind.raw)).toEqual([false, false])
        } finally {
            selection.destroy()
            list.destroy()
        }
    })

    test('createSelections 多选集版本同样成组更新', () => {
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

    test('set 替换行后，旧行 indicator 退出记账（不再被 currentValues 驱动）', () => {
        const list = new RxList<number>([5, 7])
        const current = new RxSet<number>([])
        const selection = createSelection(list, current)
        try {
            const oldIndicator = selection.data[0][1]
            list.set(0, 9)
            current.add(5) // 5 已不在列表中，任何 indicator 都不该响应
            expect(oldIndicator.raw).toBe(false)
            expect(selection.data.map(([, ind]) => ind.raw)).toEqual([false, false])
            current.add(9)
            expect(selection.data.map(([, ind]) => ind.raw)).toEqual([true, false])
        } finally {
            selection.destroy()
            list.destroy()
            current.destroy()
        }
    })

    test('autoResetValue + 重复 item：孪生行存活时不回收选中值', () => {
        const list = new RxList<number>([5, 5, 7])
        const current = new RxSet<number>([])
        const selection = createSelection(list, current, true)
        try {
            current.add(5)
            list.splice(0, 1) // 仍有一行 5 存活，选中值保留
            expect(current.data.has(5)).toBe(true)
            expect(selection.data.map(([, ind]) => ind.raw)).toEqual([true, false])
            list.splice(0, 1) // 最后一行 5 移除，选中值回收
            expect(current.data.has(5)).toBe(false)
        } finally {
            selection.destroy()
            list.destroy()
            current.destroy()
        }
    })
})

describe('resident sweep: 重复 item 值域 × selection 家族差分 fuzz', () => {
    // 不变量：任意操作序列后，每行 indicator.raw ≡ currentValues 含该行 item。
    for (const seed of [61, 62, 63, 64]) {
        test(`RxSet currentValues seed=${seed}`, () => {
            const rand = mulberry32(seed)
            const val = () => Math.floor(rand() * 5) // 窄值域高频重复
            const source = new RxList<number>([val(), val(), val(), val()])
            const current = new RxSet<number>([])
            const selection = createSelection(source, current)
            const history: string[] = []
            try {
                for (let step = 0; step < 120; step++) {
                    const r = rand()
                    const len = source.data.length
                    if (r < 0.25) {
                        const start = Math.floor(rand() * (len + 1))
                        const dc = Math.floor(rand() * 3)
                        const items = Array.from({length: Math.floor(rand() * 3)}, val)
                        history.push(`splice(${start},${dc},[${items}])`)
                        source.splice(start, dc, ...items)
                    } else if (r < 0.4 && len > 0) {
                        const i = Math.floor(rand() * len)
                        const v = val()
                        history.push(`set(${i},${v})`)
                        source.set(i, v)
                    } else if (r < 0.5 && len >= 2) {
                        const a = Math.floor(rand() * len)
                        const b = Math.floor(rand() * len)
                        if (a !== b) {
                            history.push(`reposition(${a},${b})`)
                            source.reposition(a, b, 1)
                        }
                    } else if (r < 0.7) {
                        const v = val()
                        history.push(`current.add(${v})`)
                        current.add(v)
                    } else if (r < 0.9) {
                        const arr = [...current.data]
                        if (arr.length) {
                            const v = arr[Math.floor(rand() * arr.length)]
                            history.push(`current.delete(${v})`)
                            current.delete(v)
                        }
                    } else {
                        const next = Array.from({length: Math.floor(rand() * 3)}, val)
                        history.push(`current.replace([${next}])`)
                        current.replace(next)
                    }

                    const ctx = `seed=${seed} step=${step} src=${JSON.stringify(source.data)} current=${JSON.stringify([...current.data])} recent=${history.slice(-6).join(';')}`
                    expect(selection.data.length, `length ${ctx}`).toBe(source.data.length)
                    selection.data.forEach(([item, indicator], i) => {
                        expect(item, `item[${i}] ${ctx}`).toBe(source.data[i])
                        expect(indicator.raw, `indicator[${i}] item=${item} ${ctx}`).toBe(current.data.has(item))
                    })
                }
            } finally {
                selection.destroy()
                source.destroy()
                current.destroy()
            }
        })
    }

    for (const seed of [71, 72]) {
        test(`Atom currentValues seed=${seed}`, () => {
            const rand = mulberry32(seed)
            const val = () => Math.floor(rand() * 5)
            const source = new RxList<number>([val(), val(), val()])
            const current = atom<number | null>(null)
            const selection = createSelection(source, current)
            try {
                for (let step = 0; step < 120; step++) {
                    const r = rand()
                    const len = source.data.length
                    if (r < 0.3) {
                        source.splice(Math.floor(rand() * (len + 1)), Math.floor(rand() * 2), ...Array.from({length: Math.floor(rand() * 3)}, val))
                    } else if (r < 0.5 && len > 0) {
                        source.set(Math.floor(rand() * len), val())
                    } else if (r < 0.8) {
                        current(val())
                    } else {
                        current(null)
                    }
                    const ctx = `seed=${seed} step=${step} src=${JSON.stringify(source.data)} current=${current.raw}`
                    selection.data.forEach(([item, indicator], i) => {
                        expect(indicator.raw, `indicator[${i}] item=${item} ${ctx}`).toBe(current.raw === item)
                    })
                }
            } finally {
                selection.destroy()
                source.destroy()
            }
        })
    }
})

describe('defect class 2 (fixed): toSorted 的 set(EXPLICIT_KEY_CHANGE) 路径丢失 undefined 元素', () => {
    // 机制（修复前）：patch 用 `!== undefined` 当"有无"判断，而 undefined 是合法元素值：
    // set 引入 undefined 丢行、替换 undefined 残留行。修复：变更涉及 undefined 一律
    // 回退全量重算（与 Array#sort 的 undefined-排尾语义一致）。

    const compare = (a: number | undefined, b: number | undefined) =>
        (a ?? Infinity) - (b ?? Infinity)

    test('set 引入 undefined：派生列表保留该元素', () => {
        const list = new RxList<number | undefined>([3, 1, 2])
        const sorted = list.toSorted(compare)
        try {
            list.set(1, undefined)
            expect(sorted.data).toEqual(list.data.slice().sort(compare)) // [2, 3, undefined]
        } finally {
            sorted.destroy()
            list.destroy()
        }
    })

    test('set 替换 undefined：旧的 undefined 行被移除', () => {
        const list = new RxList<number | undefined>([undefined, 5])
        const sorted = list.toSorted(compare)
        try {
            list.set(0, 1)
            expect(sorted.data).toEqual(list.data.slice().sort(compare)) // [1, 5]
        } finally {
            sorted.destroy()
            list.destroy()
        }
    })

    test('普通数值 compare（不处理 undefined）下 set 引入 undefined 也与全量一致', () => {
        const list = new RxList<number | undefined>([3, 1, 2])
        const sorted = list.toSorted((a, b) => (a as number) - (b as number))
        try {
            list.set(0, undefined)
            // Array#sort 从不对 undefined 调用 compare，undefined 一律排尾
            expect(sorted.data).toEqual(list.data.slice().sort((a, b) => (a as number) - (b as number)))
        } finally {
            sorted.destroy()
            list.destroy()
        }
    })
})

describe('resident sweep: undefined 合法元素值域 × toSorted 差分 fuzz', () => {
    // 不变量：增量结果 ≡ 从当前 source.data 全量 sort（Array#sort 语义：undefined 排尾）。
    const compares: Array<[string, (a: number | undefined, b: number | undefined) => number]> = [
        ['undefined-aware', (a, b) => (a ?? Infinity) - (b ?? Infinity)],
        // 普通数值 compare：全量 sort 从不把 undefined 喂给它；增量路径靠 undefined 回退保护
        ['plain-numeric', (a, b) => (a as number) - (b as number)],
    ]
    for (const [name, compare] of compares) {
        for (const seed of [81, 82, 83]) {
            test(`${name} seed=${seed}`, () => {
                const rand = mulberry32(seed)
                const val = withUndefined(rand, () => Math.floor(rand() * 5), 0.25)
                const source = new RxList<number | undefined>([val(), val(), val(), val()])
                const sorted = source.toSorted(compare)
                const history: string[] = []
                try {
                    for (let step = 0; step < 120; step++) {
                        const r = rand()
                        const len = source.data.length
                        if (r < 0.35) {
                            const start = Math.floor(rand() * (len + 1))
                            const dc = Math.floor(rand() * 3)
                            const items = Array.from({length: Math.floor(rand() * 3)}, val)
                            history.push(`splice(${start},${dc},[${items}])`)
                            source.splice(start, dc, ...items)
                        } else if (r < 0.7 && len > 0) {
                            const i = Math.floor(rand() * len)
                            const v = val()
                            history.push(`set(${i},${v})`)
                            source.set(i, v)
                        } else if (r < 0.85) {
                            const v = val()
                            history.push(`push(${v})`)
                            source.push(v)
                        } else if (len > 0) {
                            history.push('pop')
                            source.pop()
                        }
                        const expected = source.data.slice().sort(compare as (a: any, b: any) => number)
                        const ctx = `cmp=${name} seed=${seed} step=${step} src=${JSON.stringify(source.data)} recent=${history.slice(-6).join(';')}`
                        expect(sorted.data, ctx).toEqual(expected)
                    }
                } finally {
                    sorted.destroy()
                    source.destroy()
                }
            })
        }
    }
})

describe('resident sweep: undefined 合法元素值域 × 核心派生算子差分 fuzz', () => {
    // 覆盖清单 undefinedVal 列的对账资产:map/filter/slice/concat/toSet/groupBy/findIndex
    // 在 undefined 作为合法元素值的值域下,增量结果 ≡ 全量重算。
    for (const seed of [91, 92, 93]) {
        test(`seed=${seed}`, () => {
            const rand = mulberry32(seed)
            const val = withUndefined(rand, () => Math.floor(rand() * 5), 0.25)
            const source = new RxList<number | undefined>([val(), val(), val(), val()])
            const mapped = source.map(x => (x === undefined ? 'U' : x * 10))
            const filtered = source.filter(x => x !== undefined && x % 2 === 0)
            const sliced = source.slice(1, 3)
            const other = new RxList<number | undefined>([undefined, 2])
            const concated = source.concat(other)
            const asSet = source.toSet()
            const grouped = source.groupBy(x => (x === undefined ? 'u' : x % 2))
            const found = source.findIndex(x => x === undefined)
            const history: string[] = []
            try {
                for (let step = 0; step < 120; step++) {
                    const r = rand()
                    const len = source.data.length
                    if (r < 0.3) {
                        const start = Math.floor(rand() * (len + 1))
                        const dc = Math.floor(rand() * 3)
                        const items = Array.from({length: Math.floor(rand() * 3)}, val)
                        history.push(`splice(${start},${dc},[${items}])`)
                        source.splice(start, dc, ...items)
                    } else if (r < 0.55 && len > 0) {
                        const i = Math.floor(rand() * len)
                        const v = val()
                        history.push(`set(${i},${v})`)
                        source.set(i, v)
                    } else if (r < 0.7 && len >= 2) {
                        const a = Math.floor(rand() * len)
                        const b = Math.floor(rand() * len)
                        if (a !== b) {
                            history.push(`reposition(${a},${b})`)
                            source.reposition(a, b, 1)
                        }
                    } else {
                        const v = val()
                        history.push(`push(${v})`)
                        source.push(v)
                    }

                    const src = source.data
                    const ctx = `seed=${seed} step=${step} src=${JSON.stringify(src)} recent=${history.slice(-6).join(';')}`
                    expect(mapped.data, `map ${ctx}`).toEqual(src.map(x => (x === undefined ? 'U' : x * 10)))
                    expect(filtered.data, `filter ${ctx}`).toEqual(src.filter(x => x !== undefined && x % 2 === 0))
                    expect(sliced.data, `slice ${ctx}`).toEqual(src.slice(1, 3))
                    expect(concated.data, `concat ${ctx}`).toEqual([...src, ...other.data])
                    expect([...asSet.data].sort(), `toSet ${ctx}`).toEqual([...new Set(src)].sort())
                    for (const [k, g] of grouped.data) {
                        expect(g.data, `group[${k}] ${ctx}`).toEqual(src.filter(x => (x === undefined ? 'u' : x % 2) === k))
                    }
                    expect(found.raw, `findIndex ${ctx}`).toBe(src.findIndex(x => x === undefined))
                }
            } finally {
                mapped.destroy(); filtered.destroy(); sliced.destroy(); concated.destroy()
                asSet.destroy()
                for (const g of grouped.data.values()) g.destroy()
                grouped.destroy()
                destroyComputed(found)
                other.destroy(); source.destroy()
            }
        })
    }
})
