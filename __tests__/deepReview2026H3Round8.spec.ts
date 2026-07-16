/**
 * 2026-H3 第八轮深度 review(方法 24:闭世界度量的补集挖掘)的实例回归 + 等价类横扫。
 *
 * 方法:既有三大机械防线(差分 fuzz/覆盖账本/mutation 审计)全部度量「已存在的
 * 代码 × 已枚举的输入 × 已登记的条款」这个闭世界,对**缺失物**构造性无感——
 * 缺失的输入维度、缺失的守卫、缺失的条款行、缺失的观察列。本轮沿五条正交轴
 * 系统挖掘补集:
 *   ① 回调返回值形态(TS 签名是无人执法的承诺,typed 测试天然避开违约输入);
 *   ② 观察出口副本的行使方向(grant 的行使空间 = 行使者 × 对象 × 窗口 叉乘,
 *      既有资产只覆盖"调用方改原始数组"一个点);
 *   ③ 同名 API 跨实现点的语义一致性(onCleanup/forEach/replace/toArray);
 *   ④ 守卫对称性(全量侧 assert 的关系不变量 × patch 全部插入入口——参考实现
 *      抛错的输入域是差分 oracle 的结构性禁区,fuzz 永远进不去);
 *   ⑤ 代数不变量与可诊断性(置换保多重集、digest 有界、warn 信噪比)。
 *
 * 缺陷清单(全部动态复现后修复):
 *   R8-1 filter × truthy 非布尔谓词:isFirstRun 的 typeof 判定把每次行级重跑误判
 *        为首跑——重复插行/永不移除/插错位置三形态静默分叉(修复:存储点布尔化)。
 *   R8-2 观察出口(onChange handler/自定义调度器 infos)的副本对 reorder 只拷到
 *        Order[] 一层,pair 与广播共享——handler 行使「可自由处置」即毒化兄弟
 *        patch 消费者;reorderInfo 整对象共享同理(修复:按协议形状深拷)。
 *   R8-3 同一拷贝的反向缺陷:数组型**用户值**(splice 删除项/插入项、RxMap set 的
 *        oldValue 等)被无差别 slice,引用身份破坏,按身份记账的 handler 静默失配
 *        (修复:用户值保引用,协议容器换新)。
 *   R8-4 indexBy 的 EKC 插入侧、toMap 的 splice/EKC 双插入侧缺重复 key 守卫:
 *        全量侧 assert、patch 侧静默覆盖,派生与全量重算永久分叉(修复:全入口同 assert)。
 *   R8-5 map 行级重算不执行上一轮 context.onCleanup(新注册静默顶掉旧注册),
 *        mapFn 每轮分配的资源逐轮泄漏——与 computed 同名 API 语义分叉(修复:
 *        重算前先执行,prepareRecompute 同款"先复位再调用")。
 *   R8-6 RxSet.forEach 的 track 在迭代之后:handler 抛错(含"throw 打断迭代"惯用法
 *        + getter 自 catch)时订阅静默丢失,源变更永不重算(修复:track 先行,
 *        与 RxList/RxMap 对齐)。
 *   R8-7 swap 重叠区间静默丢数据(多重集不保持);公开 reorder 对越界/重复/非置换
 *        pair 静默出洞/错位(修复:swap 重叠 assert;reorder dev 构建校验子集置换)。
 *   R8-8 batch 内非收敛效应环静默死循环(非 batch 同形态 loud assert)——不可测
 *        语义先给 bound 才有可断言性(修复:dev 构建越阈值 console.error 一次)。
 *   R8-9 async getter 的形态提示 warn 每次重算都打(修复:每实例一次)。
 */
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest'
import {atom, batch, computed, Computed, destroyComputed, onChange, once, RxList, RxMap, RxSet, TrackOpTypes, TriggerOpTypes, type Atom, type TriggerInfo} from '../src/index.js'
import {createIndexKeySelection} from '../src/rxListSelection.js'
import {mulberry32, truthyPredicateForms, truthyPredicateModel} from './fuzzKit.js'

// ---------------------------------------------------------------------------
// R8-1 回调返回值形态:filter 布尔化 + 谓词族横扫 + 兄弟 API 宽容性钉扎
// ---------------------------------------------------------------------------

describe('R8-1 filter × 回调返回值形态(truthy 非布尔谓词)', () => {
    type Row = {id: number, v: Atom<number>}

    test('实例回归:truthy→truthy 不重复插行、truthy→falsy 移除、falsy→truthy 插对位置', () => {
        const items: Row[] = [
            {id: 1, v: atom(0)},
            {id: 2, v: atom(2)},
            {id: 3, v: atom(5)},
        ]
        const list = new RxList(items)
        // 平台 Array#filter 惯用形态:返回 number(0 falsy / 非 0 truthy)
        const filtered = list.filter(r => r.v() as unknown as boolean)
        const model = () => list.data.filter(r => r.v.raw !== 0).map(r => r.id)
        const got = () => filtered.data.map(r => r.id)
        expect(got()).toEqual([2, 3])

        items[1].v(3)          // truthy → truthy:曾重复插行([2,3,2])
        expect(got()).toEqual(model())
        items[1].v(0)          // truthy → falsy:曾永不移除
        expect(got()).toEqual(model())
        items[0].v(9)          // falsy → truthy:曾插到尾部([3,1])
        expect(got()).toEqual(model())
        expect(got()).toEqual([1, 3])
        filtered.destroy(); list.destroy()
    })

    test('返回值形态族差分 fuzz:boolean/number/string/nullable 四形态 × 随机操作序列全程 ≡ 模型', () => {
        for (const seed of [8101, 8102, 8103]) {
            for (const [formName, pred] of Object.entries(truthyPredicateForms)) {
                const rand = mulberry32(seed)
                let nextId = 0
                const mkRow = (val: number): Row => ({id: nextId++, v: atom(val)})
                const list = new RxList<Row>([mkRow(1), mkRow(0), mkRow(2), mkRow(0)])
                const filtered = list.filter(r => pred(r.v()))
                const history: string[] = []
                try {
                    for (let step = 0; step < 100; step++) {
                        const op = Math.floor(rand() * 4)
                        if (op === 0 && list.data.length) {
                            const i = Math.floor(rand() * list.data.length)
                            const next = list.data[i].v.raw === 0 ? Math.floor(rand() * 5) + 1 : 0
                            history.push(`toggle(${i}→${next})`)
                            list.data[i].v(next)
                        } else if (op === 1) {
                            const start = Math.floor(rand() * (list.data.length + 1))
                            const del = Math.floor(rand() * 2)
                            const ins = Math.floor(rand() * 2)
                            history.push(`splice(${start},${del},+${ins})`)
                            list.splice(start, del, ...Array.from({length: ins}, () => mkRow(Math.floor(rand() * 3))))
                        } else if (op === 2 && list.data.length) {
                            const i = Math.floor(rand() * list.data.length)
                            history.push(`set(${i})`)
                            list.set(i, mkRow(Math.floor(rand() * 3)))
                        } else if (op === 3 && list.data.length >= 2) {
                            const i = Math.floor(rand() * list.data.length)
                            let j = Math.floor(rand() * list.data.length)
                            if (j === i) j = (j + 1) % list.data.length
                            history.push(`swap(${Math.min(i, j)},${Math.max(i, j)})`)
                            list.swap(Math.min(i, j), Math.max(i, j))
                        }
                        const want = list.data.filter(r => truthyPredicateModel(r.v.raw)).map(r => r.id)
                        expect(filtered.data.map(r => r.id),
                            `form=${formName} seed=${seed} step=${step} history=${history.slice(-6).join(' ')}`
                        ).toEqual(want)
                    }
                } finally {
                    filtered.destroy(); list.destroy()
                }
            }
        }
    })

    test('非响应式 truthy 谓词(纯字段):EKC/splice 差分', () => {
        const list = new RxList([{name: 'x'}, {name: ''}, {name: 'y'}])
        const filtered = list.filter(item => item.name as unknown as boolean)
        expect(filtered.data.map(i => i.name)).toEqual(['x', 'y'])
        list.set(1, {name: 'z'})
        expect(filtered.data.map(i => i.name)).toEqual(['x', 'z', 'y'])
        list.splice(1, 0, {name: ''})
        expect(filtered.data.map(i => i.name)).toEqual(['x', 'z', 'y'])
        filtered.destroy(); list.destroy()
    })

    test('兄弟 API 宽容性钉扎:findIndex/find/some/every/once 只做真值判断,构造性免疫', () => {
        const list = new RxList([0, 3, 0, 7])
        const idx = list.findIndex(x => (x % 2) as unknown as boolean)   // 3 是首个奇数
        const found = list.find(x => (x % 2) as unknown as boolean)
        const any = list.some(x => (x % 2) as unknown as boolean)
        const all = list.every(x => x as unknown as boolean)
        expect(idx()).toBe(1)
        expect(found()).toBe(3)
        expect(any()).toBe(true)
        expect(all()).toBe(false)
        list.set(1, 0)  // 移除首个奇数
        expect(idx()).toBe(3)
        expect(found()).toBe(7)
        list.splice(0, Infinity, 2, 4)
        expect(any()).toBe(false)
        expect(all()).toBe(true)
        destroyComputed(idx); destroyComputed(found); destroyComputed(any); destroyComputed(all)
        list.destroy()

        // once:truthy 非布尔返回值同样停止监听
        const counter = atom(0)
        let runs = 0
        once(() => {
            runs++
            return (counter() >= 2 ? 1 : 0) as unknown as boolean
        }, true)
        counter(1)
        counter(2)
        const runsAtStop = runs
        counter(3)
        expect(runs).toBe(runsAtStop) // 已停止
    })
})

// ---------------------------------------------------------------------------
// R8-2 观察出口敌意 handler:行使「可自由处置」不得毒化兄弟订阅者
// ---------------------------------------------------------------------------

// 深改写 handler 收到的 info 副本:外层数组、嵌套数组(reorder pair/replace 内层)、
// reorderInfo 的 Map 与区间、以及全部标量字段。任何共享都会毒化兄弟消费者。
function deepClobberInfos(infos: any[]) {
    for (const info of infos) {
        if (Array.isArray(info.argv)) {
            for (const el of info.argv) {
                if (Array.isArray(el)) {
                    for (const inner of el) {
                        if (Array.isArray(inner)) { inner[0] = -777; inner[1] = -777; inner.length = 0 }
                    }
                    el.length = 0
                }
            }
            info.argv.length = 0
        }
        if (Array.isArray(info.methodResult)) {
            for (const el of info.methodResult) {
                if (Array.isArray(el)) el.length = 0
            }
            info.methodResult.length = 0
        }
        const ri = info.reorderInfo
        if (ri && typeof ri === 'object') {
            if (ri.oldIndexToNewIndex instanceof Map) ri.oldIndexToNewIndex.clear()
            if (Array.isArray(ri.affectedRange)) { ri.affectedRange[0] = 999; ri.affectedRange[1] = -999 }
            ri.movedCount = -1
        }
        info.key = '__clobbered__'
        info.newValue = '__clobbered__'
        info.oldValue = '__clobbered__'
        info.method = '__clobbered__'
        info.type = '__clobbered__'
    }
    infos.length = 0
}

describe('R8-2 观察出口敌意 handler(onChange 先注册,深改写自己的副本)', () => {
    test('RxList:splice/set/reorder(swap+公开 reorder) × map + createIndexKeySelection', () => {
        const list = new RxList([10, 20, 30, 40])
        const stop = onChange(list, (infos: TriggerInfo[]) => deepClobberInfos(infos as any[]))
        const mapped = list.map(x => x * 2)                        // 消费 argv/methodResult/reorderInfo
        const selected = new RxSet<number>([1])
        const indexSel = createIndexKeySelection(list, selected)   // 消费 reorderInfo.affectedRange

        const assertAll = (ctx: string) => {
            expect(mapped.data, `map ${ctx}`).toEqual(list.data.map(x => x * 2))
            expect(indexSel.data.map(r => r[0]), `indexSel items ${ctx}`).toEqual(list.data)
            indexSel.data.forEach((row, i) => {
                expect(row[1].raw, `indexSel indicator@${i} ${ctx}`).toBe(selected.data.has(i))
            })
        }

        list.push(50); assertAll('push')
        list.splice(1, 2, 99); assertAll('splice')
        list.set(0, 7); assertAll('set')
        list.swap(0, 2); assertAll('swap')
        list.reorder([[0, 1], [1, 0]]); assertAll('public reorder')
        batch(() => { list.push(61); list.swap(0, 1); list.set(2, 8) }); assertAll('batch mixed')
        batch(() => { list.reorder([[0, 2], [1, 0], [2, 1]]); list.push(70) }); assertAll('batch reorder+push')

        stop(); indexSel.destroy(); selected.destroy(); mapped.destroy(); list.destroy()
    })

    test('RxSet:add/delete/replace × toList + union', () => {
        const setA = new RxSet<number>([1, 2])
        const stop = onChange(setA, (infos: TriggerInfo[]) => deepClobberInfos(infos as any[]))
        const listD = setA.toList()
        const other = new RxSet<number>([9])
        const uni = setA.union(other)

        const assertAll = (ctx: string) => {
            expect(listD.data, `toList ${ctx}`).toEqual([...setA.data])
            expect([...uni.data].sort((a, b) => a - b), `union ${ctx}`)
                .toEqual([...new Set([...setA.data, ...other.data])].sort((a, b) => a - b))
        }

        setA.add(3); assertAll('add')
        setA.delete(1); assertAll('delete')
        setA.replace(new Set([5, 6])); assertAll('replace')
        batch(() => { setA.add(7); setA.delete(5) }); assertAll('batch')

        stop(); uni.destroy(); other.destroy(); listD.destroy(); setA.destroy()
    })

    test('RxMap:set/delete/clear/replace × keys + values', () => {
        const map = new RxMap<string, number>([['a', 1], ['b', 2]])
        const stop = onChange(map, (infos: TriggerInfo[]) => deepClobberInfos(infos as any[]))
        const keys = map.keys()
        const values = map.values()

        const assertAll = (ctx: string) => {
            expect(keys.data, `keys ${ctx}`).toEqual([...map.data.keys()])
            expect(values.data, `values ${ctx}`).toEqual([...map.data.values()])
        }

        map.set('c', 3); assertAll('add-set')
        map.set('a', 10); assertAll('update-set')
        map.delete('b'); assertAll('delete')
        map.replace({x: 7, y: 8}); assertAll('replace')
        map.clear(); assertAll('clear')

        stop(); values.destroy(); keys.destroy(); map.destroy()
    })

    test('自定义三参调度器(另一观察出口):深改写 infos 不毒化 patch 消费者', () => {
        const list = new RxList(['a', 'b', 'c'])
        const hostile = new Computed(function (this: Computed) {
            this.manualTrack(list, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
            this.manualTrack(list, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE)
        }, function applyPatch() {}, (recompute, _markDirty, infos?: any[]) => {
            if (infos) deepClobberInfos(infos)
            recompute()
        })
        hostile.run([], true)

        const mapped = list.map(x => x + '!')
        list.swap(0, 2)
        expect(mapped.data).toEqual(list.data.map(x => x + '!'))
        list.splice(1, 1, 'z')
        expect(mapped.data).toEqual(list.data.map(x => x + '!'))

        hostile.destroy(); mapped.destroy(); list.destroy()
    })

    test('先注册 handler 的改写对后注册 handler 不可见(副本独立)', () => {
        const list = new RxList([1, 2, 3])
        const stop1 = onChange(list, (infos: TriggerInfo[]) => deepClobberInfos(infos as any[]))
        const seen: TriggerInfo[][] = []
        const stop2 = onChange(list, (infos: TriggerInfo[]) => { seen.push(infos) })

        list.swap(0, 2)
        const reorderInfo = seen.flat().find(info => info.method === 'reorder')!
        expect(reorderInfo.argv![0]).toEqual([[0, 2], [2, 0]])
        expect((reorderInfo.reorderInfo as any).movedCount).toBe(2)

        stop1(); stop2(); list.destroy()
    })
})

// ---------------------------------------------------------------------------
// R8-3 观察出口的用户值身份保持(拷贝深度的反方向)
// ---------------------------------------------------------------------------

describe('R8-3 观察出口副本:协议容器换新,用户值保引用身份', () => {
    test('RxList<数组元素>:splice 的删除项/插入项经 onChange 保身份', () => {
        const rowA = [1, 2]
        const rowB = [3, 4]
        const list = new RxList<number[]>([rowA])
        let spliceInfo: TriggerInfo | undefined
        const stop = onChange(list, (infos: TriggerInfo[]) => {
            spliceInfo = infos.find(info => info.method === 'splice') ?? spliceInfo
        })

        list.push(rowB)
        expect((spliceInfo!.argv as any[])[2]).toBe(rowB)              // 插入项:同一引用

        list.splice(0, 1)
        expect((spliceInfo!.methodResult as unknown[])[0]).toBe(rowA)  // 删除项:同一引用

        stop(); list.destroy()
    })

    test('RxMap:set 的 oldValue 与 delete 的 methodResult(数组型用户值)保身份', () => {
        const v1 = [1]
        const v2 = [2]
        const map = new RxMap<string, number[]>([['k', v1]])
        const seen: TriggerInfo[] = []
        const stop = onChange(map, (infos: TriggerInfo[]) => { seen.push(...infos) })

        map.set('k', v2)
        const setInfo = seen.find(info => info.method === 'set')!
        expect((setInfo.methodResult as unknown[])[1]).toBe(v1)   // [hasValue, oldValue] 的 oldValue

        map.delete('k')
        const deleteInfo = seen.find(info => info.method === 'delete')!
        expect(deleteInfo.methodResult).toBe(v2)                  // 用户值本身,不动

        stop(); map.destroy()
    })

    test('RxSet:delete 的 argv(数组型成员)保身份;replace 的内层协议容器换新', () => {
        const member = [7]
        const setA = new RxSet<number[]>([member])
        const seen: TriggerInfo[] = []
        const stop = onChange(setA, (infos: TriggerInfo[]) => { seen.push(...infos) })

        setA.delete(member)
        const deleteInfo = seen.find(info => info.method === 'delete')!
        expect(deleteInfo.argv![0]).toBe(member)

        const newMember = [8]
        const [newItems] = setA.replace(new Set([newMember]))
        const replaceInfo = seen.find(info => info.method === 'replace')!
        // 内层数组是协议容器(独立副本,改写不毒化),元素是用户值(保身份)
        expect(replaceInfo.methodResult).not.toBe(newItems)
        expect((replaceInfo.methodResult as unknown[][])[0][0]).toBe(newMember)

        stop(); setA.destroy()
    })
})

// ---------------------------------------------------------------------------
// R8-4 守卫对称性:全量侧 assert 的关系不变量 × patch 全部插入入口
// ---------------------------------------------------------------------------

describe('R8-4 守卫对称性(重复 key × indexBy/toMap 全入口)', () => {
    test('indexBy:EKC 插入撞已有 key 与全量侧同样 assert(曾静默覆盖)', () => {
        const list = new RxList([{k: 'a', v: 1}, {k: 'b', v: 2}])
        const indexed = list.indexBy('k')
        expect(() => list.set(1, {k: 'a', v: 99})).toThrow('indexBy key is already exist')
        indexed.destroy(); list.destroy()
    })

    test('indexBy:同 key 原位替换(先删旧 entry 再插)仍然合法', () => {
        const list = new RxList([{k: 'a', v: 1}, {k: 'b', v: 2}])
        const indexed = list.indexBy('k')
        list.set(0, {k: 'a', v: 100})
        expect((indexed.data.get('a') as any).v).toBe(100)
        // 换 key 替换同样合法(旧 key 让位)
        list.set(0, {k: 'c', v: 5})
        expect(indexed.data.has('a')).toBe(false)
        expect((indexed.data.get('c') as any).v).toBe(5)
        indexed.destroy(); list.destroy()
    })

    test('toMap:splice 插入与 EKC 插入撞 key 与全量侧同样 assert(双入口曾静默覆盖)', () => {
        // 与 indexBy 的既有 loud-fail 语义一致:assert 抛给变更调用方时源数据已写入
        // (触发在数据变更之后),源被重复 key 毒化后后续全量重算继续拒绝——
        // 每个 throw 场景用独立实例,避免跨场景的毒化耦合。
        const listA = new RxList<[string, number]>([['a', 1], ['b', 2]])
        const mapA = listA.toMap()
        expect(() => listA.push(['a', 99])).toThrow('toMap key is already exist')
        mapA.destroy(); listA.destroy()

        const listB = new RxList<[string, number]>([['a', 1], ['b', 2]])
        const mapB = listB.toMap()
        expect(() => listB.set(1, ['a', 99])).toThrow('toMap key is already exist')
        mapB.destroy(); listB.destroy()

        // 同 key 原位替换合法(先删旧 entry 再插)
        const listC = new RxList<[string, number]>([['a', 1], ['b', 2]])
        const mapC = listC.toMap()
        listC.set(0, ['a', 100])
        expect(mapC.data.get('a')).toBe(100)
        mapC.destroy(); listC.destroy()
    })

    test('对称性见证:全量重算侧对同一输入同样拒绝(增量与全量行为一致)', () => {
        expect(() => new RxList([{k: 'a'}, {k: 'a'}]).indexBy('k')).toThrow('indexBy key is already exist')
        expect(() => new RxList<[string, number]>([['a', 1], ['a', 2]]).toMap()).toThrow('toMap key is already exist')
    })
})

// ---------------------------------------------------------------------------
// R8-5 map 行级重算 × context.onCleanup(同名 API 语义对齐)
// ---------------------------------------------------------------------------

describe('R8-5 map 行级重算前执行上一轮 context.onCleanup', () => {
    test('每轮重算恰好执行上一轮注册一次;删除行执行最后一轮注册', () => {
        const dep = atom(0)
        const cleanups: string[] = []
        const source = new RxList(['a'])
        const mapped = source.map((item, _idx, ctx) => {
            const run = dep()
            ctx.onCleanup(() => cleanups.push(`${item}@${run}`))
            return `${item}:${run}`
        })
        expect(mapped.data).toEqual(['a:0'])
        expect(cleanups).toEqual([])

        dep(1)
        expect(mapped.data).toEqual(['a:1'])
        expect(cleanups).toEqual(['a@0'])
        dep(2)
        expect(cleanups).toEqual(['a@0', 'a@1'])

        source.splice(0, 1)
        expect(cleanups).toEqual(['a@0', 'a@1', 'a@2'])
        mapped.destroy(); source.destroy()
    })

    test('条件注册:上一轮注册被执行且复位,后续重算/删除不重复执行(无 double-free)', () => {
        const dep = atom(0)
        const cleanups: string[] = []
        const source = new RxList(['a'])
        const mapped = source.map((item, _idx, ctx) => {
            const run = dep()
            if (run === 0) ctx.onCleanup(() => cleanups.push('once'))
            return `${item}:${run}`
        })
        dep(1)
        expect(cleanups).toEqual(['once'])
        dep(2)
        expect(cleanups).toEqual(['once'])
        source.splice(0, 1)
        expect(cleanups).toEqual(['once'])
        mapped.destroy(); source.destroy()
    })

    test('与 computed 的 context.onCleanup 语义一致(兄弟实现点差分)', () => {
        const dep = atom(0)
        const rowCleanups: number[] = []
        const computedCleanups: number[] = []

        const source = new RxList(['x'])
        const mapped = source.map((_item, _idx, ctx) => {
            const run = dep()
            ctx.onCleanup(() => rowCleanups.push(run))
            return run
        })
        const c = computed(({onCleanup}) => {
            const run = dep()
            onCleanup(() => computedCleanups.push(run))
            return run
        })

        dep(1); dep(2)
        // 两个实现点的执行序列必须一致:每轮重算前执行上一轮
        expect(rowCleanups).toEqual(computedCleanups)
        expect(rowCleanups).toEqual([0, 1])

        destroyComputed(c); mapped.destroy(); source.destroy()
    })
})

// ---------------------------------------------------------------------------
// R8-6 forEach 的 track 序(同名 API 兄弟一致性)
// ---------------------------------------------------------------------------

describe('R8-6 forEach:track 先于迭代(handler 抛错不丢订阅)', () => {
    test('RxSet:getter 内 try/catch 包住 throw 打断迭代(Set.forEach 无 break 的惯用法),订阅仍建立', () => {
        const setA = new RxSet<number>([1, 2, 3])
        const BREAK = new Error('break')
        const c = computed(() => {
            let found: number | undefined
            try {
                setA.forEach(v => {
                    if (v >= 2) { found = v; throw BREAK }
                })
            } catch (e) {
                if (e !== BREAK) throw e
            }
            return found
        })
        expect(c()).toBe(2)
        // track 若在迭代后,这次"成功"的运行不含 setA 依赖 → 永久静默陈旧
        setA.replace(new Set([9]))
        expect(c()).toBe(9)
        destroyComputed(c); setA.destroy()
    })

    test('兄弟钉扎:RxList/RxMap/RxSet 的 forEach 在同形态下行为一致', () => {
        const BREAK = new Error('break')
        const list = new RxList([1, 2])
        const map = new RxMap<string, number>([['a', 1]])
        const setA = new RxSet([1])
        const runsBySibling = {list: 0, map: 0, set: 0}

        const cList = computed(() => {
            runsBySibling.list++
            try { list.forEach(() => { throw BREAK }) } catch (e) { if (e !== BREAK) throw e }
            return list.data.length
        })
        const cMap = computed(() => {
            runsBySibling.map++
            try { map.forEach(() => { throw BREAK }) } catch (e) { if (e !== BREAK) throw e }
            return map.data.size
        })
        const cSet = computed(() => {
            runsBySibling.set++
            try { setA.forEach(() => { throw BREAK }) } catch (e) { if (e !== BREAK) throw e }
            return setA.data.size
        })

        list.push(3); map.set('b', 2); setA.add(2)
        expect(cList()).toBe(3)
        expect(cMap()).toBe(2)
        expect(cSet()).toBe(2)
        expect(runsBySibling).toEqual({list: 2, map: 2, set: 2})

        destroyComputed(cList); destroyComputed(cMap); destroyComputed(cSet)
        list.destroy(); map.destroy(); setA.destroy()
    })
})

// ---------------------------------------------------------------------------
// R8-7 未定义语义域的守卫:swap 重叠 + 公开 reorder 的子集置换校验
// ---------------------------------------------------------------------------

describe('R8-7 swap 重叠 assert 与公开 reorder 校验', () => {
    test('swap 重叠区间 loud-fail(曾静默丢数据:[a,b,c] → [b,c,b],a 丢失)', () => {
        const list = new RxList(['a', 'b', 'c'])
        expect(() => list.swap(0, 1, 2)).toThrow('swap ranges must not overlap')
        // 数据未被触碰(assert 在任何写入之前)
        expect(list.data).toEqual(['a', 'b', 'c'])
        list.destroy()
    })

    test('swap 合法形态不受影响:相邻/远距/自交换(no-op)', () => {
        const list = new RxList(['a', 'b', 'c', 'd'])
        list.swap(0, 1)
        expect(list.data).toEqual(['b', 'a', 'c', 'd'])
        list.swap(0, 2, 2)
        expect(list.data).toEqual(['c', 'd', 'b', 'a'])
        list.swap(1, 1)
        expect(list.data).toEqual(['c', 'd', 'b', 'a'])
        list.destroy()
    })

    test('公开 reorder:dev 校验子集置换(越界/重复/非置换全部 loud-fail)', () => {
        if (!__DEV__) return
        const make = () => new RxList(['a', 'b', 'c'])
        let list = make()
        expect(() => list.reorder([[0, 5]])).toThrow('reorder pair out of range')
        list.destroy(); list = make()
        expect(() => list.reorder([[0, 1], [2, 1]])).toThrow('reorder pairs must not duplicate positions')
        list.destroy(); list = make()
        expect(() => list.reorder([[0, 1]])).toThrow('from/to position sets must match')
        list.destroy(); list = make()
        expect(() => list.reorder([[0.5, 1] as any])).toThrow('reorder pair out of range')
        list.destroy()
    })

    test('公开 reorder:合法子集置换照常工作,内部调用方(sortSelf/reposition/map patch)不受校验影响', () => {
        const list = new RxList([3, 1, 2])
        const mapped = list.map(x => x * 10)
        list.reorder([[0, 1], [1, 2], [2, 0]])       // 轮换
        expect(list.data).toEqual([2, 3, 1])
        expect(mapped.data).toEqual(list.data.map(x => x * 10))
        list.sortSelf((a, b) => a - b)
        expect(list.data).toEqual([1, 2, 3])
        expect(mapped.data).toEqual([10, 20, 30])
        list.reposition(0, 2)
        expect(mapped.data).toEqual(list.data.map(x => x * 10))
        mapped.destroy(); list.destroy()
    })
})

// ---------------------------------------------------------------------------
// R8-8 batch digest 循环诊断(dev)
// ---------------------------------------------------------------------------

describe('R8-8 batch 内非收敛效应环的可诊断性', () => {
    let errSpy: ReturnType<typeof vi.spyOn>
    beforeEach(() => { errSpy = vi.spyOn(console, 'error').mockImplementation(() => {}) })
    afterEach(() => { errSpy.mockRestore() })

    test('越过阈值的互触发环:console.error 一次(不中断 digest,终态仍正确)', () => {
        if (!__DEV__) return
        const a = atom(0)
        const b = atom(0)
        const LIMIT = 100_100
        const eff1 = new Computed(function (this: Computed) {
            const v = a()
            if (v > 0 && v < LIMIT) b(v + 1)
        }, undefined, true)
        eff1.run([], true)
        const eff2 = new Computed(function (this: Computed) {
            const v = b()
            if (v > 0 && v < LIMIT) a(v + 1)
        }, undefined, true)
        eff2.run([], true)

        batch(() => { a(1) })
        const cycleWarnings = errSpy.mock.calls.filter(call => String(call[0]).includes('non-converging effect cycle'))
        expect(cycleWarnings.length).toBe(1)
        expect(a.raw).toBeGreaterThanOrEqual(LIMIT - 1) // digest 未被中断,收敛照常完成

        eff1.destroy(); eff2.destroy()
    })

    test('正常规模的 batch(含级联):零误报', () => {
        if (!__DEV__) return
        const source = new RxList<number>([])
        const mapped = source.map(x => x * 2)
        const filtered = source.filter(x => x % 2 === 0)
        batch(() => {
            for (let i = 0; i < 2000; i++) source.push(i)
        })
        expect(mapped.data.length).toBe(2000)
        const cycleWarnings = errSpy.mock.calls.filter(call => String(call[0]).includes('non-converging effect cycle'))
        expect(cycleWarnings.length).toBe(0)
        filtered.destroy(); mapped.destroy(); source.destroy()
    })
})

// ---------------------------------------------------------------------------
// R8-9 async getter 形态提示的 warn 信噪比(每实例一次)
// ---------------------------------------------------------------------------

describe('R8-9 async getter warn:每实例一次', () => {
    test('多次重算只警告一次;新实例独立计数', async () => {
        if (!__DEV__) return
        const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
        try {
            const countWarns = () => warnSpy.mock.calls.filter(call => String(call[0]).includes('async getter')).length
            const dep = atom(1)
            const c1 = computed(async () => dep() * 2)
            await new Promise(r => setTimeout(r, 10))
            expect(countWarns()).toBe(1)
            dep(2)
            await new Promise(r => setTimeout(r, 10))
            dep(3)
            await new Promise(r => setTimeout(r, 10))
            expect(countWarns()).toBe(1)      // 重算不再刷屏

            const c2 = computed(async () => dep() * 3)
            await new Promise(r => setTimeout(r, 10))
            expect(countWarns()).toBe(2)      // 每实例一次

            destroyComputed(c1); destroyComputed(c2)
        } finally {
            warnSpy.mockRestore()
        }
    })
})

// ---------------------------------------------------------------------------
// R8-10 同名 API 兄弟语义盘点(特征钉扎:分叉至少可见)
// ---------------------------------------------------------------------------

describe('R8-10 同名 API 兄弟语义特征钉扎', () => {
    test('replace/replaceData:RxList/RxMap 就地更新,RxSet 采纳入参容器(A3)', () => {
        const list = new RxList<number>([1])
        const listContainer = list.data
        list.replaceData([2, 3])
        expect(list.data).toBe(listContainer)         // 就地(splice 语义)

        const map = new RxMap<string, number>([['a', 1]])
        const mapContainer = map.data
        map.replace({b: 2})
        expect(map.data).toBe(mapContainer)           // 就地(逐 key set/delete)

        const setA = new RxSet<number>([1])
        const nextContainer = new Set([2])
        setA.replace(nextContainer)
        expect(setA.data).toBe(nextContainer)         // 采纳(所有权移交,A3)

        list.destroy(); map.destroy(); setA.destroy()
    })

    test('toArray:RxList 返回内部数组(只读视图),RxSet 返回快照副本', () => {
        const list = new RxList([1, 2])
        expect(list.toArray()).toBe(list.data)

        const setA = new RxSet([1, 2])
        const snapshot = setA.toArray()
        expect(snapshot).toEqual([1, 2])
        setA.add(3)
        expect(snapshot).toEqual([1, 2])              // 副本不随源变化
        list.destroy(); setA.destroy()
    })
})
