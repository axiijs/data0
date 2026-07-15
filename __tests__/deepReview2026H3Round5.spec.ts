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
import {describe, expect, test, vi} from 'vitest'
import {RxList} from '../src/RxList.js'
import {RxSet} from '../src/RxSet.js'
import {batch} from '../src/notify.js'
import {TrackOpTypes, TriggerOpTypes} from '../src/operations.js'
import {autorun, onChange} from '../src/common.js'
import {atom} from '../src/atom.js'
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

// ---- 未探测清单清偿(round5 补充横扫):自引用回调 × 值域/键域特征 ----

describe('R5-3 自引用回调:谓词/mapFn 经 at(0) 读列表自身(收敛钉扎)', () => {
    // 攻击轴:row indicator/rowComputed 经 at(0) 订阅列表自身——set(0)/splice/swap
    // 同时走「行级重算」与「结构 patch」两路,是 R4-1 触发序窗口的跨行依赖变体
    // ("大于首元素/阈值行"是现实需求)。探针未发现反例;此前没有任何测试构造过
    // 自引用谓词,本组把收敛特征钉为常驻回归。
    const ref = (src: number[]) => src.filter(x => x > src[0])

    test('filter(x => x > at(0)) × set(0)/头部 splice/swap/batch 全收敛', () => {
        const list = new RxList<number>([5, 1, 10, 3])
        const filtered = list.filter(x => x > (list.at(0) ?? -Infinity))
        expect([...filtered.data]).toEqual(ref(list.data))

        list.set(0, 2)                    // 阈值降低:行级重算 × EKC 同 digest
        expect([...filtered.data]).toEqual(ref(list.data))
        list.set(0, 100)                  // 阈值抬高:全部行反选
        expect([...filtered.data]).toEqual(ref(list.data))
        list.splice(0, 1)                 // 删除阈值元素本身(结构 + 阈值同时变)
        expect([...filtered.data]).toEqual(ref(list.data))
        list.unshift(7)                   // 头插新阈值
        expect([...filtered.data]).toEqual(ref(list.data))
        list.swap(0, 2)                   // reorder 路径 × 行级依赖
        expect([...filtered.data]).toEqual(ref(list.data))
        batch(() => {                      // batch 多 info × 自引用
            list.set(0, 2)
            list.splice(1, 1)
        })
        expect([...filtered.data]).toEqual(ref(list.data))
        filtered.destroy(); list.destroy()
    })

    test('map(x => x * at(0)) × set(0)/shift 全收敛(行级 rowComputed 读自身)', () => {
        const list = new RxList<number>([2, 3, 4])
        const scaled = list.map(x => x * (list.at(0) ?? 1))
        const refMap = (src: number[]) => src.map(x => x * src[0])
        expect([...scaled.data]).toEqual(refMap(list.data))
        list.set(0, 10)
        expect([...scaled.data]).toEqual(refMap(list.data))
        list.splice(0, 1)
        expect([...scaled.data]).toEqual(refMap(list.data))
        scaled.destroy(); list.destroy()
    })
})

// ---- 三项裁决落地(D1 防御拷贝 / D2 入口归一化 / D3 纯度契约 + dev 探测) ----

describe('R5-D1 载荷所有权:返回数组归调用方,协议载荷持独立副本', () => {
    test('batch 内改写 splice 返回数组:groupBy/toSorted 不再被毒化', () => {
        const list = new RxList<number>([3, 1, 2, 4])
        const groups = list.groupBy(x => x % 2)
        const sorted = list.toSorted((a, b) => a - b)
        batch(() => {
            const removed = list.splice(0, 2) // 删 3,1
            removed.length = 0                 // 调用方回收返回数组(原生 splice 预期)
            removed.push(999 as any)
        })
        expect([...groups.data.get(0)!.data]).toEqual([2, 4])
        expect(groups.data.get(1)).toBeUndefined()
        expect([...sorted.data]).toEqual([2, 4])
        sorted.destroy(); groups.destroy(); list.destroy()
    })

    test('batch 内改写 clear 返回数组:派生不被毒化', () => {
        const list = new RxList<number>([1, 2, 3])
        const asSet = list.toSet()
        batch(() => {
            const removed = list.clear()
            removed.length = 0
        })
        expect([...asSet.data]).toEqual([])
        asSet.destroy(); list.destroy()
    })

    test('改写 RxSet.replace 返回的 [newItems, deletedItems]:toList 不被毒化', () => {
        const set = new RxSet<number>([1, 2])
        const asList = set.toList()
        batch(() => {
            const [added, removed] = set.replace([2, 3])
            added.length = 0
            removed.length = 0
        })
        expect([...asList.data].sort()).toEqual([2, 3])
        asList.destroy(); set.destroy()
    })

    test('batch 窗口改写 reorder 的 newOrder 数组:派生不被毒化', () => {
        const list = new RxList<number>([1, 2, 3])
        const mapped = list.map(x => x * 10)
        const order: [number, number][] = [[0, 2], [1, 0], [2, 1]]
        batch(() => {
            list.reorder(order)
            order.length = 0 // 调用方复用自己的 order 数组
        })
        expect(list.data).toEqual([2, 3, 1])
        expect([...mapped.data]).toEqual([20, 30, 10])
        mapped.destroy(); list.destroy()
    })

    test('async applyPatch 跨 await 窗口:返回数组改写不毒化挂起的 patch', async () => {
        const list = new RxList<number>([1, 2, 3, 4])
        let patched: number[] = []
        const consumer = new RxList<number>(
            function computation(this: RxList<number>) {
                this.manualTrack(list, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
                return []
            },
            async function applyPatch(_d: unknown, infos: any[]) {
                await Promise.resolve() // 挂起:调用方的同步代码在此窗口执行
                patched = infos.flatMap(info => [...(info.methodResult as number[] ?? [])])
            }
        )
        const removed = list.splice(0, 2)
        removed.length = 0 // patch 尚未消费,同步改写返回数组
        await Promise.resolve(); await Promise.resolve(); await Promise.resolve()
        expect(patched).toEqual([1, 2])
        consumer.destroy(); list.destroy()
    })

    test('dev 构建:onChange handler 改写广播载荷当场 TypeError(冻结)', () => {
        if (!__DEV__) return
        const list = new RxList<number>([1, 2, 3])
        const disposeFn = onChange(list, (infos: any[]) => {
            const mr = infos[0]?.methodResult as number[] | undefined
            if (Array.isArray(mr)) mr.length = 0 // 改写共享广播 → dev 冻结应抛错
        })
        expect(() => list.splice(0, 1)).toThrow(TypeError)
        disposeFn()
        list.destroy()
    })
})

describe('R5-D2 字符串规范下标归一化:set("2")/at("2") ≡ set(2)/at(2)', () => {
    test('set("2", v):真实写行 2,派生与订阅者全部跟上(修复前静默全家分叉)', () => {
        const list = new RxList<number>([1, 2, 3, 4])
        const mapped = list.map(x => x * 10)
        let seen: number | undefined
        const stop = autorun(() => { seen = list.at(2) }, true)
        list.set("2" as any, 99)
        expect(list.data).toEqual([1, 2, 99, 4])
        expect([...mapped.data]).toEqual([10, 20, 990, 40])
        expect(seen).toBe(99)
        stop(); mapped.destroy(); list.destroy()
    })

    test('at("2") 订阅与 set(2) 触发落在同一 dep', () => {
        const list = new RxList<number>([1, 2, 3])
        let seen: number | undefined
        const stop = autorun(() => { seen = list.at("2" as any) }, true)
        expect(seen).toBe(3)
        list.set(2, 42)
        expect(seen).toBe(42)
        stop(); list.destroy()
    })

    test('平台下标域性质横扫:任意 key 的 set 后增量 ≡ 全量重算', () => {
        // 守卫定义域必须与平台下标定义域一致(两个方向:R5-2 上界 / D2 字符串)。
        // 对每个对抗 key:set(k) 后派生的增量结果必须等于强制全量重算——
        // 平台视为下标的 key 派生必须跟上,视为属性的 key 派生必须忽略。
        const DOMAIN: any[] = [2, "2", "02", "2.5", "-0", 5, "5", 5.5, -1, "abc", NaN, 2 ** 32 - 1, 2 ** 32 + 5]
        for (const key of DOMAIN) {
            const list = new RxList<number>([10, 20, 30])
            const mapped = list.map(x => (x as number) * 2)
            list.set(key, 99)
            const incremental = [...mapped.data]
            mapped.recompute(true)
            expect(incremental, `key=${String(key)} (typeof ${typeof key})`).toEqual([...mapped.data])
            mapped.destroy(); list.destroy()
        }
    })

    test('非规范字符串("02"/"2.5"/"-0")保持属性赋值语义,不归一化', () => {
        const list = new RxList<number>([1, 2, 3])
        const mapped = list.map(x => x * 10)
        list.set("02" as any, 99)
        list.set("2.5" as any, 99)
        list.set("-0" as any, 99)
        // 三个都是属性赋值:行元素与 length 不变(属性留在数组对象上,与平台一致)
        expect([...list.data]).toEqual([1, 2, 3])
        expect(list.data.length).toBe(3)
        expect([...mapped.data]).toEqual([10, 20, 30])
        mapped.destroy(); list.destroy()
    })
})

describe('R5-D3 回调纯度契约:dev 探测警告(groupBy/indexBy getKey、toSorted comparator)', () => {
    test('getKey 读响应式数据 → dev 警告一次;纯 getKey 零警告', () => {
        if (!__DEV__) return
        const spy = vi.spyOn(console, 'warn').mockImplementation(() => {})
        try {
            const pure = new RxList([{k: 1}, {k: 2}])
            const pureGroups = pure.groupBy(i => i.k)
            expect(spy.mock.calls.some(c => String(c[0]).includes('groupBy getKey'))).toBe(false)
            pureGroups.destroy(); pure.destroy()

            const st = atom('x')
            const list = new RxList([{id: 1, st}])
            const groups = list.groupBy(i => i.st())
            expect(spy.mock.calls.some(c => String(c[0]).includes('groupBy getKey read reactive data'))).toBe(true)
            groups.destroy(); list.destroy()
        } finally {
            spy.mockRestore()
        }
    })

    test('不稳定 getKey(每次新对象) → dev 警告', () => {
        if (!__DEV__) return
        const spy = vi.spyOn(console, 'warn').mockImplementation(() => {})
        try {
            const list = new RxList([{k: 'a'}])
            const groups = list.groupBy(i => ({key: i.k})) // 每次新引用
            expect(spy.mock.calls.some(c => String(c[0]).includes('unstable key'))).toBe(true)
            groups.destroy(); list.destroy()
        } finally {
            spy.mockRestore()
        }
    })

    test('indexBy 函数形式读响应式 → dev 警告;toSorted comparator 读响应式 → dev 警告', () => {
        if (!__DEV__) return
        const spy = vi.spyOn(console, 'warn').mockImplementation(() => {})
        try {
            const st = atom(1)
            const list = new RxList([{id: 1, st}, {id: 2, st}])
            const byId = list.indexBy(i => `${i.id}:${i.st()}`)
            expect(spy.mock.calls.some(c => String(c[0]).includes('indexBy getKey read reactive data'))).toBe(true)
            byId.destroy()

            const pri = atom(1)
            const sorted = list.toSorted((a, b) => (a.id + pri()) - (b.id + pri()))
            expect(spy.mock.calls.some(c => String(c[0]).includes('toSorted comparator read reactive data'))).toBe(true)
            sorted.destroy(); list.destroy()
        } finally {
            spy.mockRestore()
        }
    })

    test('探测本身不残留订阅:探测后 atom 变化零重算', () => {
        const st = atom('x')
        const list = new RxList([{id: 1, st}])
        const groups = list.groupBy(i => i.st())
        const counter = attachRecomputeCounter(groups)
        st('y') // 契约:不追踪 → 不重算(探测的 probe 订阅必须已随 probe 销毁)
        expect(counter.rounds()).toBe(0)
        expect(counter.fulls()).toBe(0)
        counter.detach(); groups.destroy(); list.destroy()
    })
})

describe('R5-4 值域/键域特征钉扎(补充横扫)', () => {
    test('对象引用作 groupBy 组键:引用身份下增量 ≡ 全量', () => {
        const catA = {name: 'a'}, catB = {name: 'b'}
        const mk = (id: number, cat: object) => ({id, cat})
        const list = new RxList([mk(1, catA), mk(2, catB), mk(3, catA)])
        const groups = list.groupBy(i => i.cat)
        const snap = () => [...groups.data.entries()].map(([k, g]) => [(k as any).name, g.data.map((i: any) => i.id)])
        expect(snap()).toEqual([['a', [1, 3]], ['b', [2]]])
        list.push(mk(4, catB))
        list.splice(0, 1)
        list.set(0, mk(5, catA))
        const inc = snap()
        groups.recompute(true)
        expect(inc).toEqual(snap())
        groups.destroy(); list.destroy()
    })

    test('set(-0) ≡ set(0):at(0) 订阅触发、数据写入、派生全链一致', () => {
        const list = new RxList<number>([1, 2, 3])
        const mapped = list.map(x => x * 10)
        let seen: number | undefined
        const stop = autorun(() => { seen = list.at(0) }, true)
        list.set(-0 as any, 9)
        expect(seen).toBe(9)
        expect(list.data).toEqual([9, 2, 3])
        const inc = [...mapped.data]
        mapped.recompute(true)
        expect(inc).toEqual([...mapped.data])
        stop(); mapped.destroy(); list.destroy()
    })
})
