import {describe, expect, test} from 'vitest'
import {RxList} from '../src/RxList.js'
import {batch} from '../src/notify.js'
import {mulberry32} from './fuzzKit.js'

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
    // 既有 broadOperatorsFuzz 的 map(item, index) 列只存 atom 不读值，
    // 本列覆盖"行升级为带 index 依赖的 rowComputed"的形态。
    for (const seed of [61, 62, 63]) {
        test(`差分 fuzz（读 index 行）seed=${seed}`, () => {
            const rand = mulberry32(seed)
            let counter = 0
            const source = new RxList<number>([counter++, counter++, counter++])
            const mapped = source.map((item, index) => `${item}#${index()}`)
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
                        .toEqual(source.data.map((x, i) => `${x}#${i}`))
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
        const byId = source.indexBy('id' as never)
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
        const byId = source.indexBy('id' as never)
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
        const byId = source.indexBy('id' as never)
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
