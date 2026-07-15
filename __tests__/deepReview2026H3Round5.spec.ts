/**
 * 2026-H3 第五轮深度 review 资产(方法 21:结构级自别名差分 + 下标数值上界域)。
 *
 * 攻击轴 a——结构级自别名:方法 17 的身份别名把**元素**放进多个位置,本轮把
 * **Rx 结构本身**放进一个派生的多个操作数位置(a.concat(a) 跑马灯复制、
 * s.union(s) 等)。按段位置增量的派生(concat)在"同一源占多段"下只对
 * indexOf 命中的首段应用 patch,其余段静默漏更新(动态复现:增量
 * [1,2,3,1,2] vs 全量 [1,2,3,1,2,3])。修复:重复源构造性回退全量重算。
 * RxSet 代数族按终态成员判定,构造性自别名安全,以特征测试钉住。
 *
 * 攻击轴 b——EKC key 的数值上界:合法数组下标域是 [0, 2^32-2],此前
 * isDenseIndexKey 只挡负/小数——key ≥ 2^32-1 的正整数(data[k]=v 是属性赋值,
 * length 不变、全量 computation 不可见)穿透守卫:filter 物化幽灵行、groupBy
 * 物化幽灵成员且前缀循环按 key 迭代 ~2^32 次(单次 set 卡整分钟)、
 * ensureAtomIndex 撑长 atomIndexes 当场 RangeError 抛给 set() 调用方
 * (data 已写、info 未发,状态撕裂)。修复:isDenseIndexKey 补数值上界,
 * set() 的 atomIndexes 门用同一谓词;上界值进入 fuzzKit.adversarialSetIndex
 * 生成器常驻组合空间。
 */
import {describe, expect, test} from 'vitest'
import {RxList} from '../src/RxList.js'
import {RxSet} from '../src/RxSet.js'
import {batch} from '../src/notify.js'
import {TrackOpTypes, TriggerOpTypes} from '../src/operations.js'
import {attachRecomputeCounter} from './fuzzKit.js'

const NON_INDEX_KEY = 2 ** 32 + 5   // 非下标正整数(属性赋值)
const MAX_PLUS_KEYS = [2 ** 32 - 1, 2 ** 32, 2 ** 32 + 5] // 上界邻域

describe('R5-1 concat × 结构级自别名:重复源回退全量,增量恒 ≡ 全量', () => {
    test('a.concat(a):push 后增量结果与全量重算一致(修复前第二段漏 patch)', () => {
        const a = new RxList<number>([1, 2])
        const both = a.concat(a)
        const counter = attachRecomputeCounter(both)
        expect(both.data).toEqual([1, 2, 1, 2])

        a.push(3)
        expect(both.data).toEqual([1, 2, 3, 1, 2, 3])
        // 重复源必须走全量回退(而不是不完整的增量)
        expect(counter.fulls()).toBeGreaterThan(0)

        a.set(0, 9)
        expect(both.data).toEqual([9, 2, 3, 9, 2, 3])
        a.splice(1, 1)
        expect(both.data).toEqual([9, 3, 9, 3])
        counter.detach()
        both.destroy(); a.destroy()
    })

    test('b.concat(a, a):同一 other 占两段', () => {
        const a = new RxList<number>([1])
        const b = new RxList<number>([9])
        const c = b.concat(a, a)
        expect(c.data).toEqual([9, 1, 1])
        a.push(2)
        expect(c.data).toEqual([9, 1, 2, 1, 2])
        b.unshift(8)
        expect(c.data).toEqual([8, 9, 1, 2, 1, 2])
        c.destroy(); b.destroy(); a.destroy()
    })

    test('batch 内变更 × 自别名 concat 同样收敛', () => {
        const a = new RxList<number>([1, 2])
        const both = a.concat(a)
        batch(() => {
            a.push(3)
            a.set(0, 7)
        })
        expect(both.data).toEqual([7, 2, 3, 7, 2, 3])
        both.destroy(); a.destroy()
    })

    test('无重复源的 concat 保持增量(回退门不误伤)', () => {
        const a = new RxList<number>([1])
        const b = new RxList<number>([2])
        const cat = a.concat(b)
        const counter = attachRecomputeCounter(cat)
        a.push(3)
        b.push(4)
        a.set(0, 5)
        expect(counter.fulls()).toBe(0)
        expect(cat.data).toEqual([5, 3, 2, 4])
        counter.detach()
        cat.destroy(); a.destroy(); b.destroy()
    })
})

describe('R5-1b RxSet 代数族 × 自别名:按终态成员判定,构造性安全(特征钉扎)', () => {
    test('union/difference/symmetricDifference/intersection(self) 增量 ≡ 全量', () => {
        const s = new RxSet<number>([1, 2])
        const u = s.union(s)
        const d = s.difference(s)
        const sd = s.symmetricDifference(s)
        const it = s.intersection(s)

        s.add(3)
        s.delete(1)
        batch(() => { s.add(4); s.delete(2) })
        s.replace([7, 8])

        expect([...u.data].sort()).toEqual([7, 8])
        expect([...d.data]).toEqual([])
        expect([...sd.data]).toEqual([])
        expect([...it.data].sort()).toEqual([7, 8])
        u.destroy(); d.destroy(); sd.destroy(); it.destroy(); s.destroy()
    })
})

describe('R5-2 EKC key 数值上界:≥ 2^32-1 的正整数是属性赋值,不得物化/不得崩溃', () => {
    test('map(index) × set(2^32-1):不抛 RangeError,且派生保持一致(修复前状态撕裂)', () => {
        const list = new RxList<number>([1, 2, 3])
        const mapped = list.map((item, index) => item * 10 + index.raw)
        expect(mapped.data).toEqual([10, 21, 32])

        expect(() => list.set(2 ** 32 - 1, 99)).not.toThrow()
        // length 不变、派生不变(与全量 computation 的可见域一致)
        expect(list.data.length).toBe(3)
        expect(mapped.data).toEqual([10, 21, 32])

        // 恢复性:后续契约内操作仍增量自洽
        list.push(4)
        expect(mapped.data).toEqual([10, 21, 32, 43])
        mapped.destroy(); list.destroy()
    })

    test('filter × 上界 key:不物化幽灵行(修复前 [2,4] 变 [2,4,6])', () => {
        for (const key of MAX_PLUS_KEYS) {
            const list = new RxList<number>([1, 2, 3, 4])
            const evens = list.filter(x => typeof x === 'number' && x % 2 === 0)
            list.set(key, 6)
            expect([...evens.data], `key=${key}`).toEqual([2, 4])
            evens.destroy(); list.destroy()
        }
    })

    test('groupBy × 上界 key:不物化幽灵成员,且不进 ~2^32 次前缀循环(修复前单次 set 卡 60s)', () => {
        const list = new RxList<number>([1, 2, 3])
        const groups = list.groupBy(x => (typeof x === 'number' ? x % 2 : 'other'))
        const t0 = Date.now()
        list.set(NON_INDEX_KEY, 8)
        expect(Date.now() - t0).toBeLessThan(1000)
        expect([...groups.data.get(0)!.data]).toEqual([2])
        expect([...groups.data.get(1)!.data]).toEqual([1, 3])
        groups.destroy(); list.destroy()
    })

    test('indexBy/toMap/toSet/slice/createSelection × 上界 key:全家族忽略,与全量一致', () => {
        const list = new RxList<{id: number} | null>([{id: 1}, {id: 2}])
        const byId = list.indexBy('id')
        list.set(NON_INDEX_KEY, {id: 99})
        expect([...byId.data.keys()].sort()).toEqual([1, 2])
        byId.destroy(); list.destroy()

        const pairs = new RxList<[string, number]>([['a', 1]])
        const asMap = pairs.toMap()
        pairs.set(NON_INDEX_KEY, ['ghost', 9])
        expect([...asMap.data.keys()]).toEqual(['a'])
        asMap.destroy(); pairs.destroy()

        const nums = new RxList<number>([1, 2])
        const asSet = nums.toSet()
        const sliced = nums.slice(0, 5)
        const sel = nums.createSelection(new RxSet<number>([1]))
        nums.set(NON_INDEX_KEY, 7)
        expect([...asSet.data].sort()).toEqual([1, 2])
        expect([...sliced.data]).toEqual([1, 2])
        expect(sel.data.length).toBe(2)
        sel.destroy(); sliced.destroy(); asSet.destroy(); nums.destroy()
    })

    test('协议透传不变:上界 key 的 EKC info 原样派发(内部忽略是消费侧行为)', () => {
        const list = new RxList<number>([1])
        const seen: unknown[] = []
        const probe = new RxList<number>(
            function computation(this: RxList<number>) {
                this.manualTrack(list, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE)
                return []
            },
            function applyPatch(_d: unknown, infos: any[]) {
                for (const info of infos) seen.push(info.key)
            }
        )
        list.set(NON_INDEX_KEY, 5)
        expect(seen).toEqual([NON_INDEX_KEY])
        probe.destroy(); list.destroy()
    })
})
