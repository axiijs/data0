import {describe, expect, test} from 'vitest'
import {destroyComputed} from '../src/computed.js'
import {RxList} from '../src/RxList.js'
import {duplicateInts, mulberry32, withWeirdNumbers} from './fuzzKit.js'

/**
 * NaN/-0 元素值域 × 列表派生算子差分 fuzz(2026-H2 契约裁定后的对账资产)。
 *
 * 契约裁定(README「RxList 参数契约」同步):
 * - toSorted 的 comparator 必须对值域内所有元素对构成一致全序(与 Array#sort 的
 *   consistent-comparator 要求相同)。NaN × 裸数值 comparator((a,b)=>a-b 返回 NaN)
 *   违反一致性,属契约外;本 sweep 使用 NaN-aware comparator(NaN 归一化排尾)。
 * - -0 与 0:集合/定位语义按 SameValueZero(-0 === 0,includes/Map/Set 一致);
 *   atom 写入判等按 Object.is(0 → -0 视为变化)——两层语义各自既定。
 * - 不变量:增量结果 ≡ 从终态 source.data 全量重算(NaN 用 Object.is 语义比较,
 *   vitest 的 toEqual 对 NaN 相等)。
 */
describe('differential fuzz: NaN/-0 element values', () => {
    const nanAwareCompare = (a: number, b: number) => {
        const na = Number.isNaN(a) ? Infinity : a
        const nb = Number.isNaN(b) ? Infinity : b
        return na - nb
    }
    for (const seed of [301, 302, 303, 304]) {
        test(`seed=${seed}`, () => {
            const rand = mulberry32(seed)
            const val = withWeirdNumbers(rand, duplicateInts(rand, 4), 0.3)
            const source = new RxList<number>([val(), val(), val(), val()])
            const mapped = source.map(x => (Number.isNaN(x) ? 'nan' : x * 2))
            const filtered = source.filter(x => Number.isNaN(x) || x % 2 === 0)
            const sorted = source.toSorted(nanAwareCompare)
            const asSet = source.toSet()
            const grouped = source.groupBy(x => (Number.isNaN(x) ? 'nan' : x % 2))
            const foundNaN = source.findIndex(x => Number.isNaN(x))
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
                    } else if (r < 0.7) {
                        history.push('sortSelf')
                        source.sortSelf(nanAwareCompare)
                    } else if (r < 0.85 && len >= 2) {
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
                    const ctx = `seed=${seed} step=${step} src=${JSON.stringify(src.map(x => Object.is(x, -0) ? '-0' : Number.isNaN(x) ? 'NaN' : x))} recent=${history.slice(-6).join(';')}`
                    expect(mapped.data, `map ${ctx}`).toEqual(src.map(x => (Number.isNaN(x) ? 'nan' : x * 2)))
                    expect(filtered.data, `filter ${ctx}`).toEqual(src.filter(x => Number.isNaN(x) || x % 2 === 0))
                    expect(sorted.data, `toSorted ${ctx}`).toEqual(src.slice().sort(nanAwareCompare))
                    // Set 语义:SameValueZero(NaN 归一为单成员,-0/0 合并)
                    const modelSet = [...new Set(src)]
                    expect([...asSet.data].sort(nanAwareCompare), `toSet ${ctx}`).toEqual(modelSet.sort(nanAwareCompare))
                    const expectedGroupKeys = [...new Set(src.map(x => Number.isNaN(x) ? 'nan' : x % 2))]
                        .sort((a, b) => String(a).localeCompare(String(b)))
                    expect(
                        [...grouped.data.keys()].sort((a, b) => String(a).localeCompare(String(b))),
                        `group keys ${ctx}`,
                    ).toEqual(expectedGroupKeys)
                    for (const [k, g] of grouped.data) {
                        expect(g.data, `group[${String(k)}] ${ctx}`).toEqual(src.filter(x => {
                            const key = Number.isNaN(x) ? 'nan' : x % 2
                            return key === k || (Number.isNaN(key as number) && Number.isNaN(k as number))
                        }))
                    }
                    expect(foundNaN.raw, `findIndex(NaN) ${ctx}`).toBe(src.findIndex(x => Number.isNaN(x)))
                }
            } finally {
                mapped.destroy(); filtered.destroy(); sorted.destroy(); asSet.destroy()
                for (const g of grouped.data.values()) g.destroy()
                grouped.destroy()
                destroyComputed(foundNaN)
                source.destroy()
            }
        })
    }
})
