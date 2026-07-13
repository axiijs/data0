import {describe, expect, test} from 'vitest'
import {RxList} from '../src/RxList.js'
import {RxMap} from '../src/RxMap.js'
import {RxSet} from '../src/RxSet.js'
import {createSelection} from '../src/RxList.js'
import {computed, destroyComputed, getComputedInternal, Computed} from '../src/computed.js'
import {atom} from '../src/atom.js'
import {batch, notifier} from '../src/notify.js'

const tick = () => new Promise<void>(r => setTimeout(r, 0))

/**
 * 2026-H3 第二轮深度 review(方法 18:协议命名空间碰撞审计 + GC 可达性/记账残留审计)
 * 动态复现的缺陷类回归 + 等价类常驻资产。
 *
 * 缺陷类 8(协议命名空间碰撞):manualTrack(METHOD/EXPLICIT_KEY_CHANGE) 的 depsMap
 * key 曾直接使用 TriggerOpTypes.METHOD('method')/EXPLICIT_KEY_CHANGE(
 * 'explicit_key_change')字符串,与用户数据 key 共享同一个 keyToDepMap 命名空间。
 * RxMap key / RxSet 成员 / groupBy 组键 / indexBy 键恰为这两个字符串时,
 * SET/ADD/DELETE 的 key dep 与内部 METHOD 订阅者是同一个 dep——派生结构的
 * applyPatch 收到非协议形状的 info:
 *   - RxMap.keys 的 assert(false, 'unreachable') 直接抛给 map.set 调用方;
 *   - RxSet.toList/difference/... 的 `[a, b] = methodResult` 对 undefined 解构
 *     TypeError,派生列表静默分叉直到下一次全量重算。
 * 修复:track/trigger 双侧把 METHOD/EKC 的 track key 归一化为内部 Symbol,
 * 用户数据 key 与协议 key 从构造上隔离。
 */
describe('缺陷类 8:协议命名空间碰撞(用户 key 撞 "method"/"explicit_key_change")', () => {
    const PROTOCOL_STRINGS = ['method', 'explicit_key_change']

    test('RxMap:以协议字符串为 key 的 set(新增/更新)/delete 不再打崩 keys/values/entries', () => {
        for (const evil of PROTOCOL_STRINGS) {
            const map = new RxMap<string, number>([['a', 1]])
            const keys = map.keys()
            const values = map.values()
            const entries = map.entries()
            const size = map.size
            try {
                map.set(evil, 5)          // ADD 路径
                expect(keys.data).toEqual(['a', evil])
                expect(values.data).toEqual([1, 5])
                map.set(evil, 6)          // SET(更新)路径
                expect(values.data).toEqual([1, 6])
                expect(entries.data).toEqual([['a', 1], [evil, 6]])
                map.delete(evil)          // DELETE 路径
                expect(keys.data).toEqual(['a'])
                expect(size.raw).toBe(1)
            } finally {
                map.destroy()
            }
        }
    })

    test('RxSet:以协议字符串为成员的 add/delete 不再打崩 toList/代数派生', () => {
        for (const evil of PROTOCOL_STRINGS) {
            const s = new RxSet<string>(['a'])
            const other = new RxSet<string>(['b'])
            const list = s.toList()
            const uni = s.union(other)
            const diff = s.difference(other)
            const inter = s.intersection(other)
            const sym = s.symmetricDifference(other)
            try {
                s.add(evil)
                expect(list.data).toEqual(['a', evil])
                expect([...uni.data].sort()).toEqual(['a', 'b', evil].sort())
                expect([...diff.data].sort()).toEqual(['a', evil].sort())
                expect([...inter.data]).toEqual([])
                expect([...sym.data].sort()).toEqual(['a', 'b', evil].sort())
                other.add(evil)
                expect([...inter.data]).toEqual([evil])
                s.delete(evil)
                expect(list.data).toEqual(['a'])
                expect([...diff.data]).toEqual(['a'])
                expect([...inter.data]).toEqual([])
                expect([...sym.data].sort()).toEqual(['a', 'b', evil].sort())
            } finally {
                list.destroy(); uni.destroy(); diff.destroy(); inter.destroy(); sym.destroy()
                s.destroy(); other.destroy()
            }
        }
    })

    test('toMap:元组 key 为协议字符串(内部 RxMap.set/delete 路径)', () => {
        for (const evil of PROTOCOL_STRINGS) {
            const l = new RxList<[string, number]>([['a', 1]])
            const asMap = l.toMap()
            const keys = asMap.keys()
            try {
                l.push([evil, 2])
                expect([...asMap.data.entries()]).toEqual([['a', 1], [evil, 2]])
                expect(keys.data).toEqual(['a', evil])
                l.splice(1, 1)
                expect(keys.data).toEqual(['a'])
            } finally {
                asMap.destroy(); l.destroy()
            }
        }
    })

    test('RxList 元素为协议字符串:toSet/createSelection(RxSet currentValues)不再打崩', () => {
        const l = new RxList<string>(['a'])
        const asSet = l.toSet()
        const cur = new RxSet<string>([])
        const sel = createSelection(l, cur)
        try {
            l.push('method')
            expect([...asSet.data].sort()).toEqual(['a', 'method'])
            cur.add('method')   // currentValues 的 ADD 撞协议 key
            expect(sel.data.map(([item, ind]) => [item, ind.raw])).toEqual([['a', false], ['method', true]])
            cur.delete('method')
            expect(sel.data.map(([, ind]) => ind.raw)).toEqual([false, false])
        } finally {
            sel.destroy(); asSet.destroy(); cur.destroy(); l.destroy()
        }
    })

    test('groupBy/indexBy 组键为协议字符串:含 keys() 派生的全链路', () => {
        const l = new RxList<string>(['method', 'a'])
        const grouped = l.groupBy(x => x)
        const groupKeys = grouped.keys()
        try {
            l.push('method')
            expect(grouped.data.get('method')!.data).toEqual(['method', 'method'])
            expect(groupKeys.data.sort()).toEqual(['a', 'method'])
            l.splice(0, 1) // 删一个 'method'
            expect(grouped.data.get('method')!.data).toEqual(['method'])
        } finally {
            for (const g of grouped.data.values()) g.destroy()
            grouped.destroy(); l.destroy()
        }
    })

    test('用户 key "method" 的 get() 细粒度依赖仍然正常触发(隔离不能牺牲正常 key 语义)', () => {
        const map = new RxMap<string, number>([['method', 1]])
        const seen: (number | undefined)[] = []
        const c = computed(() => {
            const v = map.get('method')
            seen.push(v)
            return v
        })
        try {
            map.set('method', 2)
            expect(c.raw).toBe(2)
            map.delete('method')
            expect(c.raw).toBe(undefined)
        } finally {
            destroyComputed(c)
            map.destroy()
        }
    })

    test('隔离后 METHOD 订阅者不再被同名用户 key 的 SET 无谓触发(过度触发钉扎)', () => {
        const map = new RxMap<string, number>([['method', 1], ['x', 2]])
        let patchInfos: any[] = []
        const probe = new Computed(function (this: Computed) {
            this.manualTrack(map, 'method' as any, 'method' as any) // TrackOpTypes.METHOD, TriggerOpTypes.METHOD
        }, function applyPatch(_data, infos: any[]) {
            patchInfos.push(...infos)
        })
        probe.run([], true)
        try {
            patchInfos = []
            map.set('method', 9)
            // METHOD dep 只应收到协议 METHOD info(method:'set'),不应收到 SET-type 的 key info
            expect(patchInfos.every(i => i.type === 'method')).toBe(true)
            expect(patchInfos.length).toBe(1)
        } finally {
            probe.destroy()
            map.destroy()
        }
    })
})

/**
 * 缺陷类 9:undefined 作为 RxMap 的合法 key 时,get(undefined) 的订阅者漏触发。
 * trigger 的 `key !== void 0` 守卫把"未提供 key"与"key 恰为 undefined"混为一谈,
 * SET/ADD/DELETE 携带的 key=undefined 被静默丢弃,依赖 get(undefined) 的 computed
 * 永久陈旧(静默错误)。修复:用 `'key' in inputInfo` 区分带内 undefined。
 */
describe('缺陷类 9:RxMap 的 undefined key 细粒度依赖', () => {
    test('get(undefined) 随 set/delete(undefined) 更新', () => {
        const map = new RxMap<any, number>([])
        const c = computed(() => map.get(undefined))
        try {
            expect(c.raw).toBe(undefined)
            map.set(undefined, 42)
            expect(c.raw).toBe(42)
            map.set(undefined, 43)
            expect(c.raw).toBe(43)
            map.delete(undefined)
            expect(c.raw).toBe(undefined)
        } finally {
            destroyComputed(c)
            map.destroy()
        }
    })

    test('undefined key 不产生对无关变更的过度触发', () => {
        const map = new RxMap<any, number>([['x', 1]])
        let runs = 0
        const c = computed(() => {
            runs++
            return map.get(undefined)
        })
        try {
            const before = runs
            map.set('x', 2)     // 无关 key 的 SET:不应触发 get(undefined)
            map.delete('x')     // 无关 key 的 DELETE
            expect(runs).toBe(before)
        } finally {
            destroyComputed(c)
            map.destroy()
        }
    })
})

/**
 * 缺陷类 10:RxMap.set 用 === 判等去重,与库内既定的 Object.is 身份语义
 * (atom 判等、toSorted 增量定位、stateOracle 键序)不一致:
 *   - 0 → -0:数据已写入(Map value 直接替换)却不触发,values/entries 等派生
 *     与 source 在 Object.is 可观察面上静默分叉;
 *   - NaN → NaN:无变化却重复触发两次(浪费,幂等性破坏)。
 */
describe('缺陷类 10:RxMap.set 判等采用 Object.is', () => {
    test('0 -> -0 触发派生更新(不再静默分叉)', () => {
        const map = new RxMap<string, number>([['k', 0]])
        const values = map.values()
        try {
            map.set('k', -0)
            expect(Object.is(map.data.get('k'), -0)).toBe(true)
            expect(Object.is(values.data[0], -0)).toBe(true)
        } finally {
            values.destroy(); map.destroy()
        }
    })

    test('NaN -> NaN 不再重复触发(幂等)', () => {
        const map = new RxMap<string, number>([['k', NaN]])
        let patches = 0
        const probe = new Computed(function (this: Computed) {
            this.manualTrack(map, 'get' as any, 'k')
        }, function applyPatch() { patches++ })
        probe.run([], true)
        try {
            map.set('k', NaN)
            map.set('k', NaN)
            expect(patches).toBe(0)
        } finally {
            probe.destroy(); map.destroy()
        }
    })
})

/**
 * 缺陷类 11(慢性泄漏,方法 18 的记账残留审计动态复现):
 * "订阅不同 key → 退订"循环下,notifier.targetMap 的 keyToDepMap 空 Dep 条目
 * 无界残留:10000 次 `computed(() => list.at(i))` 创建/销毁后,depsMap 留存
 * 10000 个空 Dep(每个是 Set + 标记位,长驻 store 在虚拟滚动/分页场景缓慢增长,
 * 直到 source 销毁才释放)。RxList.pruneIndexKeyDeps 只清 RxList 自己的
 * _indexKeyDeps,清不到 notifier 侧的记账。
 * 修复:dep 记录宿主(host/hostKey),退订到空时从 depsMap 摘除;
 * restoreEffectDeps(错误恢复)负责把被摘除的 dep 重新挂回/合并。
 */
/**
 * 缺陷类 12:reduce/reduceToAtom 的"纯尾插"增量判定按**重放时终态**长度回推
 * (argv[0] === source.data.length - 插入数),多 info 重放下把"越界 clamp 到尾部"
 * 的 splice 误判为尾插——喂给 reduceFn 的 index 与全量重算分叉(操作时位置 ×
 * 重放终态的缺陷类,与 findIndex/groupBy/slice 同源)。修复:判定与应用都经
 * digestReplay 内核取逐条操作时长度;顺带让 batch 内连续 push 保持增量
 * (旧判定下多 info 必然回退)。
 */
describe('缺陷类 12:reduce 多 info 尾插判定按操作时长度', () => {
    test('batch 内越界 clamp 尾插:index 与全量一致', () => {
        const source = new RxList<string>(['x', 'y'])
        const collected = source.reduce<RxList<string>>((last, item, index) => {
            last.push(`${item}@${index}`)
        })
        const joined = source.reduceToAtom((acc: string, item, index) => `${acc},${item}@${index}`, '')
        try {
            // 两条 info 的 argv[0] 都恰等于 终态长(4) - 插入数(1) = 3:旧判定误入增量,
            // 但 info1 操作时长度是 2(splice(3) 被 clamp 到 2),index 应为 2 而不是 3。
            batch(() => {
                source.splice(3, 0, 'a')
                source.splice(3, 0, 'b')
            })
            expect(collected.data).toEqual(source.data.map((item, i) => `${item}@${i}`))
            expect(joined.raw).toBe(source.data.reduce((acc, item, i) => `${acc},${item}@${i}`, ''))
        } finally {
            collected.destroy()
            destroyComputed(joined)
            source.destroy()
        }
    })

    test('batch 内连续 push 保持增量(经操作时长度逐条对齐)', () => {
        const source = new RxList<number>([1, 2])
        const collected = source.reduce<RxList<number>>((last, item, index) => {
            last.push(item * 100 + index)
        })
        let fullRecomputes = 0
        ;(collected as unknown as Computed).on('fullRecompute', () => fullRecomputes++)
        try {
            batch(() => {
                source.push(3)
                source.push(4)
            })
            expect(collected.data).toEqual(source.data.map((x, i) => x * 100 + i))
            expect(fullRecomputes).toBe(0)
            // 混合操作(非尾插)仍回退全量,结果一致
            batch(() => {
                source.push(5)
                source.unshift(0)
            })
            expect(collected.data).toEqual(source.data.map((x, i) => x * 100 + i))
            expect(fullRecomputes).toBeGreaterThan(0)
        } finally {
            collected.destroy()
            source.destroy()
        }
    })
})

describe('缺陷类 11:depsMap 空 Dep 条目随订阅退订循环无界残留', () => {
    function depsMapSize(target: object): number {
        const m = (notifier as any).targetMap.get(target)
        return m ? m.size : 0
    }

    test('RxList.at(i) 订阅退订循环后 depsMap 有界', () => {
        const list = new RxList<number>(Array.from({length: 500}, (_, i) => i))
        try {
            for (let i = 0; i < 500; i++) {
                const c = computed(() => list.at(i))
                destroyComputed(c)
            }
            // 修复前:500(每个 index 一个空 dep 永久残留);修复后:0
            expect(depsMapSize(list)).toBeLessThanOrEqual(4)
        } finally {
            list.destroy()
        }
    })

    test('RxMap.get(key) 订阅退订循环后 depsMap 有界', () => {
        const map = new RxMap<string, number>([])
        try {
            for (let i = 0; i < 500; i++) map.set(`k${i}`, i)
            for (let i = 0; i < 500; i++) {
                const c = computed(() => map.get(`k${i}`))
                destroyComputed(c)
            }
            expect(depsMapSize(map)).toBeLessThanOrEqual(4)
        } finally {
            map.destroy()
        }
    })

    test('退订到空再重订阅:依赖关系仍然正确(摘除/重建不破坏响应性)', () => {
        const list = new RxList<number>([10, 20, 30])
        const c1 = computed(() => list.at(1))
        destroyComputed(c1)   // dep 变空,应被摘除
        const c2 = computed(() => list.at(1))  // 同 key 重新订阅(新 dep)
        try {
            expect(c2.raw).toBe(20)
            list.set(1, 99)
            expect(c2.raw).toBe(99)
            list.splice(0, 1) // 结构变更也要触发 at(1)
            expect(c2.raw).toBe(30)
        } finally {
            destroyComputed(c2)
            list.destroy()
        }
    })

    test('getter 抛错的错误恢复:被摘除的 dep 重新挂回,依赖不静默失联', () => {
        const list = new RxList<number>([1, 2, 3])
        const shouldThrow = atom(false)
        let runs = 0
        // manualTracking=false 的普通 computed:重算走 cleanup(全退订)→ 重 track。
        // 若退订摘除 depsMap entry 且错误恢复不挂回,后续 source 变更将永久丢失。
        const c = computed(() => {
            runs++
            const v = list.at(0)
            if (shouldThrow()) throw new Error('boom')
            return v
        })
        try {
            expect(c.raw).toBe(1)
            // 写入触发重算:getter 抛错,错误同步传播给写入方
            expect(() => shouldThrow(true)).toThrow('boom')
            // 抛错期间 source 变更(此轮重算也抛,错误传给 set 调用方)
            expect(() => list.set(0, 5)).toThrow('boom')
            shouldThrow(false) // 恢复(这次触发重算成功)
            expect(c.raw).toBe(5)
            // 错误恢复后的依赖必须仍然存活
            const before = runs
            list.set(0, 7)
            expect(runs).toBeGreaterThan(before)
            expect(c.raw).toBe(7)
        } finally {
            destroyComputed(c)
            list.destroy()
        }
    })
})

/**
 * 缺陷类 13(方法 18 的 GC 审计衍生):挂起的 async/generator getter 在 destroy
 * 之后完成时,completeTracking(isLast) 无条件重放 asyncTracks,把已销毁 effect
 * 重新订阅回各 dep——dep 对僵尸保持强引用(泄漏),每次源触发都白走一遍调度。
 * 修复:active === false 时丢弃重放(与"destroy 取消在途 async patch"同一语义)。
 */
describe('缺陷类 13:destroy 后完成的 async getter 不再重放订阅', () => {
    test('generator getter 挂起期间 destroy,完成后 deps 保持空', async () => {
        const a = atom(1)
        let release!: () => void
        const gate = new Promise<void>(r => { release = r })
        const c = computed(function* () {
            const v = a()          // 挂起前读:track 排队进 asyncTracks
            yield gate
            return v * 2
        } as any)
        await tick()               // 首轮 getter 启动并挂起
        const internal = getComputedInternal(c)!
        destroyComputed(c)
        expect(internal.deps.length).toBe(0)
        release()
        await tick(); await tick() // getter 完成,asyncTracks 重放点
        expect(internal.deps.length).toBe(0)
        const dep = notifier.getPrimitiveAtomDep(a) as any
        const subscribers = dep ? (dep.overflow ? dep.overflow.size : (dep.single !== undefined ? 1 : 0)) : 0
        expect(subscribers).toBe(0)
        a(5)                       // 触发无僵尸接收方,不抛错
        await tick()
    })

    test('存活的 async getter 的 asyncTracks 重放不受影响(阴性对照)', async () => {
        const a = atom(1)
        const c = computed(async () => a() * 2)
        await tick()
        expect(c.raw).toBe(2)
        a(3)
        await tick()
        expect(c.raw).toBe(6)      // 重订阅存活,写入仍驱动重算
        destroyComputed(c)
    })
})
