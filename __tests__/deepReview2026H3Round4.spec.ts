import {describe, expect, test} from 'vitest'
import {RxList} from '../src/RxList.js'
import {batch} from '../src/notify.js'
import {Computed, getComputedInternal} from '../src/computed.js'
import {onChange} from '../src/common.js'
import {indexReadingMapFn, indexReadingModel, mulberry32} from './fuzzKit.js'
import {expectGroupByEqualsModel} from './stateOracle.js'

/**
 * 2026-H3 round4 深度 review 资产。
 *
 * 方法 20：行级依赖触发序 × 结构 info 派发序的交错审计（非 batch 路径）
 * + patch 插入侧与全量 computation 的行值域守卫不对称横扫。
 *
 * 缺陷类 R4-1（silent 乱序）：mapFn 读取 index() 的行级 rowComputed 订阅源的
 * index atom；reorder 在派发 METHOD 结构 info **之前**逐个写 index atom——
 * 非 batch 下原子写同步执行行级 getter，hasPendingStructuralInfos 守卫看不到
 * 任何 pending 结构 info，行按终态位置直写派生列表；随后派生列表的 reorder
 * patch 又按 order 搬移一次（双重搬移）。修复：reorder 与 doSplice 对齐——
 * 结构 info 先入队、index atom 值写入进同一 session，digest 时结构 patch
 * 先应用。既有 fuzz 盲区：全部 map(item, index) 生成器只把 index atom 存进
 * 结果（{item, index}），从不在 mapFn 里**读** index()，行从不升级为带
 * index 依赖的 rowComputed。
 *
 * 缺陷类 R4-2（变更方法抛 TypeError）：indexBy/toMap 的全量 computation 显式
 * 跳过 null/undefined 行，patch 的**删除侧**也有守卫，但**插入侧**（splice
 * 新增项、EKC newValue）直接做属性读/解构——push(null)/push(undefined)/
 * set(i, undefined) 当场 TypeError 抛给变更调用方。与"修复必须覆盖同一语义
 * 的所有入口"（AGENTS §3.1 第 3 问）同构：守卫语义 = "行值域 × entry 存在性"，
 * 入口 = {全量, patch 删除, patch 插入(splice), patch 替换(EKC 双侧)}。
 *
 * 缺陷类 R4-3（RangeError 抛给变更调用方）：slice patch 的 ucHead+ucTail
 * 中间段替换用 spread 传参（`this.splice(..., ...stateNow.slice(...))`），
 * 中间段与源 splice 插入量同量级——大批量插入（spliceMany 的存在动机，
 * 十万行 replaceData 场景）直接 Maximum call stack size exceeded。
 * 等价类 = "patch 端 spread 不定长数组进函数调用"，修复为 spliceArray。
 */

describe('R4-1 map(mapFn 读 index()) × reorder：非 batch 触发序', () => {
    test('swap：mapped ≡ 全量重算', () => {
        const source = new RxList(['a', 'b', 'c'])
        const mapped = source.map((item, index) => `${item}@${index()}`)
        try {
            source.swap(0, 2)
            expect(source.data).toEqual(['c', 'b', 'a'])
            expect(mapped.data).toEqual(source.data.map((x, i) => `${x}@${i}`))
        } finally {
            mapped.destroy()
            source.destroy()
        }
    })

    test('sortSelf：mapped ≡ 全量重算', () => {
        const source = new RxList([3, 1, 2])
        const mapped = source.map((item, index) => `${item}@${index()}`)
        try {
            source.sortSelf((a, b) => a - b)
            expect(mapped.data).toEqual(source.data.map((x, i) => `${x}@${i}`))
        } finally {
            mapped.destroy()
            source.destroy()
        }
    })

    test('reposition：mapped ≡ 全量重算', () => {
        const source = new RxList(['a', 'b', 'c', 'd'])
        const mapped = source.map((item, index) => `${item}@${index()}`)
        try {
            source.reposition(0, 2)
            expect(mapped.data).toEqual(source.data.map((x, i) => `${x}@${i}`))
        } finally {
            mapped.destroy()
            source.destroy()
        }
    })

    test('batch 内 reorder：与非 batch 语义一致', () => {
        const source = new RxList(['a', 'b', 'c'])
        const mapped = source.map((item, index) => `${item}@${index()}`)
        try {
            batch(() => {
                source.swap(0, 2)
            })
            expect(mapped.data).toEqual(source.data.map((x, i) => `${x}@${i}`))
        } finally {
            mapped.destroy()
            source.destroy()
        }
    })

    test('链式：mapped 再派生 filter 仍与全量一致', () => {
        const source = new RxList([1, 2, 3, 4])
        const mapped = source.map((item, index) => item * 10 + index())
        const filtered = mapped.filter(x => x % 2 === 0)
        try {
            source.sortSelf((a, b) => b - a)
            const model = source.data.map((x, i) => x * 10 + i)
            expect(mapped.data).toEqual(model)
            expect(filtered.data).toEqual(model.filter(x => x % 2 === 0))
        } finally {
            filtered.destroy()
            mapped.destroy()
            source.destroy()
        }
    })

    // 等价类横扫：读 index 的 map × 全操作域差分（固定 seed）。
    // 行形态生成器已入 fuzzKit（indexReadingMapFn），broadOperatorsFuzz 的
    // map 列同步继承；本列额外覆盖 swap 与 batch 混排。
    for (const seed of [61, 62, 63]) {
        test(`差分 fuzz（读 index 行）seed=${seed}`, () => {
            const rand = mulberry32(seed)
            let counter = 0
            const source = new RxList<number>([counter++, counter++, counter++])
            const mapped = source.map(indexReadingMapFn)
            try {
                for (let step = 0; step < 120; step++) {
                    const r = rand()
                    const len = source.data.length
                    if (r < 0.3) {
                        const start = Math.floor(rand() * (len + 1))
                        const dc = Math.floor(rand() * 3)
                        const items = Array.from({length: Math.floor(rand() * 3)}, () => counter++)
                        source.splice(start, dc, ...items)
                    } else if (r < 0.45 && len > 0) {
                        source.set(Math.floor(rand() * len), counter++)
                    } else if (r < 0.6) {
                        source.sortSelf((a, b) => b - a)
                    } else if (r < 0.72 && len >= 2) {
                        source.reposition(Math.floor(rand() * len), Math.floor(rand() * len), 1)
                    } else if (r < 0.84 && len >= 2) {
                        const a = Math.floor(rand() * len)
                        const b = (a + 1 + Math.floor(rand() * (len - 1))) % len
                        source.swap(Math.min(a, b), Math.max(a, b), 1)
                    } else if (r < 0.92) {
                        source.push(counter++)
                    } else {
                        batch(() => {
                            source.push(counter++)
                            if (source.data.length >= 2) source.swap(0, source.data.length - 1, 1)
                        })
                    }
                    expect(mapped.data, `seed=${seed} step=${step} src=${JSON.stringify(source.data)}`)
                        .toEqual(indexReadingModel(source.data))
                }
            } finally {
                mapped.destroy()
                source.destroy()
            }
        })
    }
})

describe('R4-2 indexBy/toMap patch 插入侧 × null/undefined 行', () => {
    test('indexBy(属性形式)：push(null/undefined) 不抛错且 ≡ 全量（跳过）', () => {
        const source = new RxList<{id: number} | null | undefined>([{id: 1}, null])
        const byId = source.indexBy('id')
        try {
            expect([...byId.data.keys()]).toEqual([1])
            expect(() => source.push(null)).not.toThrow()
            expect(() => source.push(undefined)).not.toThrow()
            expect(() => source.push({id: 2})).not.toThrow()
            expect([...byId.data.keys()]).toEqual([1, 2])
        } finally {
            byId.destroy()
            source.destroy()
        }
    })

    test('indexBy(函数形式)：getKey 不会收到 null/undefined（与全量语义对称）', () => {
        const seen: unknown[] = []
        const source = new RxList<{id: number} | null>([{id: 1}])
        const byId = source.indexBy(item => {
            seen.push(item)
            return item!.id
        })
        try {
            source.push(null)
            source.splice(1, 1)
            expect(seen.every(x => x != null)).toBe(true)
            expect([...byId.data.keys()]).toEqual([1])
        } finally {
            byId.destroy()
            source.destroy()
        }
    })

    test('indexBy：set 替换 null 行 / 用 null 替换实体行（EKC 双侧守卫）', () => {
        const source = new RxList<{id: number} | null>([{id: 1}, null])
        const byId = source.indexBy('id')
        try {
            // null 旧行 → 实体新行：无旧 entry 可删，添加新 entry
            expect(() => source.set(1, {id: 9})).not.toThrow()
            expect([...byId.data.keys()].sort()).toEqual([1, 9])
            // 实体旧行 → null 新行：删旧 entry，不加新 entry
            expect(() => source.set(0, null)).not.toThrow()
            expect([...byId.data.keys()]).toEqual([9])
        } finally {
            byId.destroy()
            source.destroy()
        }
    })

    test('toMap：push(undefined)/set(i, undefined) 不抛错且 ≡ 全量（跳过）', () => {
        const source = new RxList<[string, number] | undefined>([['a', 1], undefined])
        const asMap = source.toMap()
        try {
            expect([...asMap.data.keys()]).toEqual(['a'])
            expect(() => source.push(undefined)).not.toThrow()
            expect([...asMap.data.keys()]).toEqual(['a'])
            expect(() => source.push(['b', 2])).not.toThrow()
            expect([...asMap.data.keys()]).toEqual(['a', 'b'])
            // 实体行 → undefined：删旧 entry，不加新 entry
            expect(() => source.set(0, undefined)).not.toThrow()
            expect([...asMap.data.keys()]).toEqual(['b'])
            // undefined 行 → 实体行：无旧 entry，加新 entry
            expect(() => source.set(1, ['c', 3])).not.toThrow()
            expect([...asMap.data.keys()].sort()).toEqual(['b', 'c'])
        } finally {
            asMap.destroy()
            source.destroy()
        }
    })

    test('恢复性：抛错修复后派生结构与全量重算保持一致（含 batch 多 info）', () => {
        const source = new RxList<{id: number} | null>([{id: 1}])
        const byId = source.indexBy('id')
        try {
            batch(() => {
                source.push(null)
                source.push({id: 5})
                source.splice(0, 1)
            })
            const model = new Map(
                source.data.filter((x): x is {id: number} => x != null).map(x => [x.id, x])
            )
            expect(new Map(byId.data)).toEqual(model)
        } finally {
            byId.destroy()
            source.destroy()
        }
    })
})

describe('R4 顺带补杀：indexBy/toMap × 非稠密 key set 的幽灵 EKC 守卫（mutation 幸存盲区）', () => {
    test('indexBy：set(-1, x) 不产生幽灵 entry（≡ 全量只扫 [0, length)）', () => {
        const source = new RxList<{id: number}>([{id: 1}])
        const byId = source.indexBy('id')
        try {
            source.set(-1 as never, {id: 9})
            expect([...byId.data.keys()]).toEqual([1])
        } finally {
            byId.destroy()
            source.destroy()
        }
    })

    test('toMap：set(-1, entry) 不产生幽灵 entry', () => {
        const source = new RxList<[string, number]>([['a', 1]])
        const asMap = source.toMap()
        try {
            source.set(-1 as never, ['ghost', 9])
            expect([...asMap.data.keys()]).toEqual(['a'])
        } finally {
            asMap.destroy()
            source.destroy()
        }
    })
})

/**
 * R4 规模债务清偿（groupBy 单遍批量 + toSorted 阈值回退）。
 *
 * groupBy：逐项 insertInSourceOrder/removeAtSourcePosition 对每项各扫一遍前缀
 * → O(k×n)；reorder 分支每组全扫 stateNow → O(组数×n)。修复为单遍分桶：
 * 批量路径下每组恰 ≤1 次删除 splice + ≤1 次插入 splice（依据"同一变更块的
 * 同 key 项在组内必然连续"），reorder 一次 O(n) 分桶。单项操作保留零分配
 * 快路径（与旧实现逐字节等价）。
 *
 * toSorted：批量变更（bulk > 64 且 bulk×4 > m 或 bulk > 4096）回退全量重算
 * （实测交叉点 m=1k/10k/100k 时 k*≈200/2400/5300；排序列表的批量增量没有
 * 廉价下游形态可保，回退与 tie/undefined 家族同语义）。
 */
describe('R4 规模债务清偿：groupBy 批量单遍路径', () => {
    test('批量插入/删除/替换 ≡ 全量模型（含新键组/清空组/重复 key）', () => {
        const source = new RxList<number>([1, 2, 3, 4, 5, 6])
        const getKey = (x: number) => x % 3
        const grouped = source.groupBy(getKey)
        try {
            // 批量插入（含新组键 + 既有组，重复 key 交错）
            source.spliceArray(2, 0, [7, 8, 9, 10, 11])
            expectGroupByEqualsModel(grouped, source.data, getKey, 'bulk insert')
            // 批量替换（删插同时，部分组清空）
            source.spliceArray(0, 8, [30, 31])
            expectGroupByEqualsModel(grouped, source.data, getKey, 'bulk replace')
            // 批量删除（清空整组 → 键删除）
            source.spliceArray(0, source.data.length - 1)
            expectGroupByEqualsModel(grouped, source.data, getKey, 'bulk delete')
            // replaceData 全量换血
            source.replaceData([100, 101, 102, 103, 104, 105, 106])
            expectGroupByEqualsModel(grouped, source.data, getKey, 'replaceData')
        } finally {
            for (const g of grouped.data.values()) g.destroy()
            grouped.destroy(); source.destroy()
        }
    })

    test('部分批量变更下幸存组保持引用稳定（增量路径的语义承诺）', () => {
        const source = new RxList<number>([0, 1, 2, 3, 4, 5])
        const grouped = source.groupBy(x => x % 2)
        try {
            const evenBefore = grouped.data.get(0)
            const oddBefore = grouped.data.get(1)
            source.spliceArray(1, 2, [10, 11, 12, 13])  // 批量替换,两组都幸存
            expect(grouped.data.get(0)).toBe(evenBefore)
            expect(grouped.data.get(1)).toBe(oddBefore)
            expectGroupByEqualsModel(grouped, source.data, x => x % 2, 'identity kept')
        } finally {
            for (const g of grouped.data.values()) g.destroy()
            grouped.destroy(); source.destroy()
        }
    })

    test('触发形状：一次批量 splice 每组收到 ≤2 条 splice info（曾为逐项 k 条）', () => {
        const source = new RxList<number>([0, 1, 2, 3, 4, 5, 6, 7])
        const grouped = source.groupBy(x => x % 2)
        try {
            const evenGroup = grouped.data.get(0)!
            const infos: any[] = []
            const stop = onChange(evenGroup, (batchInfos: any[]) => infos.push(...batchInfos))
            // 批量替换:偶数组同时有删有插
            source.spliceArray(0, 6, [20, 22, 24, 21, 23])
            stop()
            const spliceInfos = infos.filter(i => i.method === 'splice')
            expect(spliceInfos.length).toBeLessThanOrEqual(2)
            expect(spliceInfos.length).toBeGreaterThanOrEqual(1)
            expectGroupByEqualsModel(grouped, source.data, x => x % 2, 'shape')
        } finally {
            for (const g of grouped.data.values()) g.destroy()
            grouped.destroy(); source.destroy()
        }
    })

    test('reorder × 多组（单遍分桶路径）≡ 全量模型且引用稳定', () => {
        const source = new RxList<number>(Array.from({length: 300}, (_, i) => i))
        const getKey = (x: number) => x % 100  // 100 个组
        const grouped = source.groupBy(getKey)
        try {
            const refBefore = grouped.data.get(7)
            source.sortSelf((a, b) => b - a)
            expectGroupByEqualsModel(grouped, source.data, getKey, 'reorder many groups')
            expect(grouped.data.get(7)).toBe(refBefore)
        } finally {
            for (const g of grouped.data.values()) g.destroy()
            grouped.destroy(); source.destroy()
        }
    })

    // 差分 fuzz：批量操作域（大 k splice/replaceData/reorder 混排,含 NaN key 与
    // 单项快路径/批量路径交替）,每步与朴素模型全可观察比对
    for (const seed of [71, 72, 73]) {
        test(`批量差分 fuzz seed=${seed}`, () => {
            const rand = mulberry32(seed)
            let counter = 0
            const getKey = (x: number) => (Number.isNaN(x) ? NaN : x % 5)
            const source = new RxList<number>(Array.from({length: 30}, () => counter++))
            const grouped = source.groupBy(getKey)
            const history: string[] = []
            try {
                for (let step = 0; step < 80; step++) {
                    const r = rand()
                    const len = source.data.length
                    if (r < 0.3) {
                        // 批量插入(有时含 NaN)
                        const k = 2 + Math.floor(rand() * 20)
                        const items = Array.from({length: k}, () => (rand() < 0.1 ? NaN : counter++))
                        const start = Math.floor(rand() * (len + 1))
                        source.spliceArray(start, 0, items)
                        history.push(`bulkIns(${start},${k})`)
                    } else if (r < 0.5 && len > 2) {
                        const start = Math.floor(rand() * len)
                        const dc = 2 + Math.floor(rand() * Math.min(len - start, 15))
                        const ins = Array.from({length: Math.floor(rand() * 6)}, () => counter++)
                        source.spliceArray(start, dc, ins)
                        history.push(`bulkRepl(${start},${dc},${ins.length})`)
                    } else if (r < 0.62 && len > 0) {
                        source.splice(Math.floor(rand() * len), 1)
                        history.push('single-del')
                    } else if (r < 0.74) {
                        source.push(counter++)
                        history.push('push')
                    } else if (r < 0.86 && len >= 2) {
                        source.sortSelf((a, b) => (a ?? 0) - (b ?? 0))
                        history.push('sortSelf')
                    } else if (len > 0) {
                        source.set(Math.floor(rand() * len), counter++)
                        history.push('set')
                    }
                    expectGroupByEqualsModel(grouped, source.data, getKey,
                        `seed=${seed} step=${step} recent=${history.slice(-4).join(',')}`)
                }
            } finally {
                for (const g of grouped.data.values()) g.destroy()
                grouped.destroy(); source.destroy()
            }
        })
    }
})

describe('R4 规模债务清偿：toSorted 批量阈值回退', () => {
    function witness(target: any): () => number {
        const internal: Computed = target instanceof Computed ? target : getComputedInternal(target)!
        let count = 0
        internal.on('fullRecompute', () => count++)
        return () => count
    }

    test('阈值边界：小批量保持增量,大批量回退且结果 ≡ 全量 sort', () => {
        let n = 0
        const mk = (count: number) => Array.from({length: count}, () => (n += 2))
        const source = new RxList<number>(mk(1000))
        const sorted = source.toSorted((a, b) => a - b)
        const fulls = witness(sorted)
        try {
            // bulk=65 > 64 但 65×4=260 ≤ 1000 且 ≤4096 → 增量
            source.spliceArray(500, 0, mk(65))
            expect(fulls()).toBe(0)
            expect(sorted.data).toEqual(source.data.slice().sort((a, b) => a - b))
            // bulk=300:300×4=1200 > 1065 → 回退全量
            source.spliceArray(0, 0, mk(300))
            expect(fulls()).toBe(1)
            expect(sorted.data).toEqual(source.data.slice().sort((a, b) => a - b))
            // 批量删除同样计入 bulk
            source.spliceArray(0, 600)
            expect(fulls()).toBe(2)
            expect(sorted.data).toEqual(source.data.slice().sort((a, b) => a - b))
            // 单项操作仍增量
            source.push(n += 2)
            source.splice(0, 1)
            expect(fulls()).toBe(2)
            expect(sorted.data).toEqual(source.data.slice().sort((a, b) => a - b))
        } finally {
            sorted.destroy(); source.destroy()
        }
    })

    test('绝对上限门：长列表上超 4096 的批量也回退（k×m 悬崖的另一半）', () => {
        let n = 0
        const mk = (count: number) => Array.from({length: count}, () => (n += 2))
        const source = new RxList<number>(mk(30000))
        const sorted = source.toSorted((a, b) => a - b)
        const fulls = witness(sorted)
        try {
            // bulk=5000:5000×4=20000 ≤ 30000 但 > 4096 → 回退
            source.spliceArray(100, 0, mk(5000))
            expect(fulls()).toBe(1)
            expect(sorted.data.length).toBe(source.data.length)
            expect(sorted.data[0]).toBe(2)
        } finally {
            sorted.destroy(); source.destroy()
        }
    })

    test('batch 多 info 内批量 + 单项混排 ≡ 全量 sort', () => {
        let n = 0
        const mk = (count: number) => Array.from({length: count}, () => (n += 2))
        const source = new RxList<number>(mk(200))
        const sorted = source.toSorted((a, b) => a - b)
        try {
            batch(() => {
                source.spliceArray(50, 0, mk(120))  // bulk 回退
                source.push(n += 2)                  // 单项
            })
            expect(sorted.data).toEqual(source.data.slice().sort((a, b) => a - b))
        } finally {
            sorted.destroy(); source.destroy()
        }
    })
})

describe('R4-3 slice patch 中间段大批量替换不受 spread 实参上限约束', () => {
    test('70k 窗口 × 150k 中段插入不 RangeError 且 ≡ 全量语义', () => {
        const N = 70000
        const source = new RxList<number>(Array.from({length: N}, (_, i) => i))
        const sliced = source.slice(0, N + 100000)
        try {
            const big = Array.from({length: 150000}, (_, i) => 1000000 + i)
            // 中段替换（头尾都留存），命中 ucHead+ucTail 的中间段路径
            expect(() => source.spliceArray(10, 5, big)).not.toThrow()
            expect(sliced.data.length).toBe(source.data.slice(0, N + 100000).length)
            expect(sliced.data[10]).toBe(1000000)
            expect(sliced.data[9]).toBe(9)
        } finally {
            sliced.destroy()
            source.destroy()
        }
    })
})
