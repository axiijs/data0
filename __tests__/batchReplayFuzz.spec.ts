import {describe, expect, test} from 'vitest'
import {atom} from '../src/atom.js'
import {batch} from '../src/notify.js'
import {destroyComputed} from '../src/computed.js'
import {RxList} from '../src/RxList.js'

/**
 * 缺陷类 I 的常驻防线（AGENTS.md §3.2 方法 7）：batch/延迟调度下的多 info
 * 单次 digest 重放差分。
 *
 * 既有 broadOperatorsFuzz 在 batch 外逐操作断言，隐含"每次 digest 恰一条 info"
 * 的假设；本资产把随机操作序列（1~3 个，含 set 与结构操作混排、嵌套 batch、
 * batch 内行级依赖写入）包进 batch，batch 退出后断言所有派生结构 ≡ 从终态
 * source.data 全量重算。这正是 A1/A2 划出的"仍属缺陷"边界（batch 结束后必须
 * 与全量重算一致）。
 */
import {adversarialSpliceStart, mulberry32} from './fuzzKit.js'
import {expectGroupByEqualsModel} from './stateOracle.js'

describe('batch replay fuzz: 多操作 batch 后派生结构 ≡ 全量重算', () => {
    for (const seed of [7, 8, 9, 101, 20260712]) {
        test(`seed=${seed}`, () => {
            const rand = mulberry32(seed)
            let counter = 100
            const factor = atom(1)
            const source = new RxList<number>([1, 2, 3, 4, 5])

            const mapped = source.map(x => x * 2)
            const mappedReactive = source.map(x => x * factor())
            const mappedWithIndex = source.map((item, index) => ({item, i: index}))
            const filtered = source.filter(x => x % 2 === 0)
            const sorted = source.toSorted((a, b) => a - b)
            const sliced = source.slice(1, 4)
            const other = new RxList<number>([1000, 1001])
            const concated = source.concat(other)
            const asSet = source.toSet()
            const grouped = source.groupBy(x => x % 3)
            const found = source.findIndex(x => x % 5 === 0)
            const len = source.length

            const history: string[] = []
            const doRandomOp = () => {
                const r = rand()
                const dataLen = source.data.length
                if (r < 0.3) {
                    // 对抗参数域(负数/越界/小数/NaN)统一由 fuzzKit 提供
                    const start = adversarialSpliceStart(rand, dataLen)
                    const deleteCount = Math.floor(rand() * 3)
                    const items = Array.from({length: Math.floor(rand() * 3)}, () => counter++)
                    history.push(`splice(${start},${deleteCount},[${items}])`)
                    source.splice(start, deleteCount, ...items)
                } else if (r < 0.5 && dataLen > 0) {
                    const index = Math.floor(rand() * dataLen)
                    const value = counter++
                    history.push(`set(${index},${value})`)
                    source.set(index, value)
                } else if (r < 0.6) {
                    history.push('sortSelf')
                    source.sortSelf((a, b) => a - b)
                } else if (r < 0.68 && dataLen >= 2) {
                    const limit = 1
                    const start = Math.floor(rand() * dataLen)
                    const newStart = Math.floor(rand() * dataLen)
                    history.push(`reposition(${start},${newStart})`)
                    source.reposition(start, newStart, limit)
                } else if (r < 0.75) {
                    history.push('factor')
                    factor(factor.raw! + 1)
                } else if (r < 0.85) {
                    const items = [counter++]
                    history.push(`push(${items})`)
                    source.push(...items)
                } else if (r < 0.92 && dataLen > 0) {
                    history.push('pop')
                    source.pop()
                } else if (dataLen > 0) {
                    history.push('shift')
                    source.shift()
                } else {
                    history.push('unshift')
                    source.unshift(counter++)
                }
            }

            try {
                for (let step = 0; step < 120; step++) {
                    const opsInBatch = 1 + Math.floor(rand() * 3)
                    const nested = rand() < 0.25
                    history.push(`--batch(${opsInBatch})${nested ? ' nested' : ''}--`)
                    const run = () => { for (let i = 0; i < opsInBatch; i++) doRandomOp() }
                    if (nested) batch(() => batch(run))
                    else batch(run)

                    const src = source.data
                    const ctx = `seed=${seed} step=${step} src=${JSON.stringify(src)} recent=${JSON.stringify(history.slice(-8))}`
                    expect(mapped.data, `map ${ctx}`).toEqual(src.map(x => x * 2))
                    expect(mappedReactive.data, `mapReactive ${ctx}`).toEqual(src.map(x => x * factor.raw!))
                    expect(mappedWithIndex.data.map(e => e.item), `mapIndex ${ctx}`).toEqual(src)
                    mappedWithIndex.data.forEach((e, i) => {
                        expect(e.i.raw, `mapIndex row ${i} ${ctx}`).toBe(i)
                    })
                    expect(filtered.data, `filter ${ctx}`).toEqual(src.filter(x => x % 2 === 0))
                    expect(sorted.data, `toSorted ${ctx}`).toEqual(src.slice().sort((a, b) => a - b))
                    expect(sliced.data, `slice ${ctx}`).toEqual(src.slice(1, 4))
                    expect(concated.data, `concat ${ctx}`).toEqual([...src, ...other.data])
                    expect([...asSet.data].sort((a, b) => a - b), `toSet ${ctx}`).toEqual([...new Set(src)].sort((a, b) => a - b))
                    expectGroupByEqualsModel(grouped, src, x => x % 3, ctx)
                    expect(found.raw, `findIndex ${ctx}`).toBe(src.findIndex(x => x % 5 === 0))
                    expect(len.raw, `length ${ctx}`).toBe(src.length)
                }
            } finally {
                mapped.destroy(); mappedReactive.destroy(); mappedWithIndex.destroy()
                filtered.destroy(); sorted.destroy(); sliced.destroy()
                concated.destroy(); asSet.destroy()
                for (const g of grouped.data.values()) g.destroy()
                grouped.destroy()
                destroyComputed(found)
                source.destroy(); other.destroy()
            }
        })
    }
})

describe('batch replay fuzz: toSorted 等值 tie 与全量稳定排序一致', () => {
    for (const seed of [61, 62]) {
        test(`seed=${seed}`, () => {
            const rand = mulberry32(seed)
            let id = 0
            type Item = {k: number, tag: number}
            // key 域刻意窄（0..3）以高频制造 tie
            const mk = () => ({k: Math.floor(rand() * 4), tag: id++})
            const source = new RxList<Item>([mk(), mk(), mk()])
            const sorted = source.toSorted((a, b) => a.k - b.k)
            try {
                for (let step = 0; step < 150; step++) {
                    const r = rand()
                    const len = source.data.length
                    const inBatch = rand() < 0.4
                    const op = () => {
                        if (r < 0.4) {
                            source.splice(Math.floor(rand() * (len + 1)), Math.floor(rand() * 2), ...Array.from({length: 1 + Math.floor(rand() * 2)}, mk))
                        } else if (r < 0.6 && len > 0) {
                            source.set(Math.floor(rand() * len), mk())
                        } else if (r < 0.8) {
                            source.push(mk())
                        } else if (len > 0) {
                            source.splice(Math.floor(rand() * len), 1)
                        }
                    }
                    if (inBatch) batch(op)
                    else op()

                    const full = source.data.slice().sort((a, b) => a.k - b.k)
                    expect(
                        sorted.data.map(i => i.tag),
                        `seed=${seed} step=${step} src=${JSON.stringify(source.data)}`
                    ).toEqual(full.map(i => i.tag))
                }
            } finally {
                sorted.destroy()
                source.destroy()
            }
        })
    }
})
