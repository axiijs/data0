import {describe, expect, test} from 'vitest'
import {RxList} from '../src/RxList.js'

// mulberry32:固定 seed 的确定性 PRNG,失败信息里输出 seed 与最近操作序列
function mulberry32(seed: number) {
    let a = seed >>> 0
    return function () {
        a |= 0; a = (a + 0x6D2B79F5) | 0
        let t = Math.imul(a ^ (a >>> 15), 1 | a)
        t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296
    }
}

/**
 * 差分 fuzz:重复原始值域(0..4)下,派生列表的增量结果必须等于
 * 从当前 source.data 全量重算的结果。曾经暴露 filter/groupBy 在重复值下
 * 用 indexOf/值对齐定位错误实例、顺序与源分叉的缺陷。
 */
describe('differential fuzz: duplicate primitive values', () => {
    const SEEDS = [11, 12, 13, 14, 15, 19, 37, 38]
    const STEPS = 150

    for (const seed of SEEDS) {
        test(`seed=${seed}`, () => {
            const rand = mulberry32(seed)
            const val = () => Math.floor(rand() * 5)
            const source = new RxList<number>([val(), val(), val(), val()])
            const filtered = source.filter(x => x % 2 === 0)
            const sorted = source.toSorted((a, b) => a - b)
            const asSet = source.toSet()
            const grouped = source.groupBy(x => x % 2)
            const history: string[] = []
            try {
                for (let step = 0; step < STEPS; step++) {
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
                    } else if (r < 0.7) {
                        history.push('sortSelf')
                        source.sortSelf((a, b) => a - b)
                    } else if (r < 0.85 && len >= 2) {
                        const start = Math.floor(rand() * len)
                        const newStart = Math.floor(rand() * len)
                        history.push(`reposition(${start},${newStart})`)
                        source.reposition(start, newStart, 1)
                    } else {
                        const v = val()
                        history.push(`push(${v})`)
                        source.push(v)
                    }

                    const src = source.data
                    const ctx = `seed=${seed} step=${step} src=${JSON.stringify(src)} recent=${history.slice(-8).join(';')}`
                    expect(filtered.data, `filter ${ctx}`).toEqual(src.filter(x => x % 2 === 0))
                    expect(sorted.data, `toSorted ${ctx}`).toEqual(src.slice().sort((a, b) => a - b))
                    expect([...asSet.data].sort(), `toSet ${ctx}`).toEqual([...new Set(src)].sort())
                    for (const [k, g] of grouped.data) {
                        expect(g.data, `group[${k}] ${ctx}`).toEqual(src.filter(x => x % 2 === k))
                    }
                }
            } finally {
                filtered.destroy()
                sorted.destroy()
                asSet.destroy()
                for (const g of grouped.data.values()) g.destroy()
                grouped.destroy()
                source.destroy()
            }
        })
    }
})
