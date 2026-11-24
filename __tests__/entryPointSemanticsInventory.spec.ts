/**
 * 变更语义 × 入口 账本(2026-H3 round3 根因 4 的机械化闭合)。
 *
 * 事故形状(已发生两次):等价类修复沿"被攻击的轴"泛化,漏掉"携带缺陷的轴"——
 *   - 「替换型 splice 的重复值错位」修复未覆盖纯插入/set 入口(AGENTS.md §3.1 记录);
 *   - 「RxMap.set 的 === 判等」修复(方法 18)未覆盖 replace 入口(方法 19 命中:
 *     整表 replace 幽灵触发全部 get(key) 订阅者)。
 * 根因:「同一变更语义还有哪些入口」由修复者口头回答,没有机械过账。
 *
 * 本账本把它变成数据 + 双重强制:
 *   1. 完整性:每个集合变更方法(surfaceClassification.MUTATIONS,与
 *      coverageInventory 共享同一清单)必须归入一个语义组;新增变更方法而不
 *      归组,测试当场失败。
 *   2. 组内一致性:语义组的行为性质写成**参数化探针**,对组内每个入口执行——
 *      给组加入口自动继承探针;豁免必须显式声明理由并附特征钉扎。
 *
 * 语义组:
 *   equalityGatedWrite  值写入,Object.is 判等门(same→零触发,0→-0 触发,NaN→NaN 零触发)
 *   equalityExempt      值写入但**显式豁免**判等门(RxList.set:EKC 协议消费者可能依赖
 *                       set(i, sameItem) 强制重建行,单方面加门属协议变更,须与 axii/axle 同步)
 *   structural          结构变更(增删/换序/清空):判等无意义,已由差分 fuzz 族覆盖
 *   wholesaleReplace    整体替换:destroyed no-op + methodResult 协议形状(consumerContractReplay)
 *
 * 正交轴「key 载荷透传 × 内部消费者归一化」(-0,见文件末):RxMap/RxSet 每个带 key/成员参数的
 * 变更入口,载荷(argv / key)透传调用方原值(AGENTS「argv 原始参数契约不得改写,内部消费者
 * 独立归一化」),而按载荷重放的内部消费者(keys/values/entries/toList)与全量重算在 Object.is
 * 下一致。曾只在 set/delete 写入口改写 -0、漏掉 replace——同一事故形状,故机械过账。
 */
import {describe, expect, test} from 'vitest'
import {atom} from '../src/atom.js'
import {RxList} from '../src/RxList.js'
import {RxMap} from '../src/RxMap.js'
import {RxSet} from '../src/RxSet.js'
import {autorun, onChange} from '../src/common.js'
import {Computed} from '../src/computed.js'
import {ITERATE_KEY} from '../src/notify.js'
import {TrackOpTypes} from '../src/operations.js'
import {MUTATIONS} from './surfaceClassification.js'

// ---- 语义分组(账本本体) ----
const SEMANTIC_GROUPS: Record<string, Record<string, string>> = {
    RxList: {
        push: 'structural', pop: 'structural', shift: 'structural', unshift: 'structural',
        splice: 'structural', spliceArray: 'structural', clear: 'structural',
        reorder: 'structural', reposition: 'structural', swap: 'structural', sortSelf: 'structural',
        replaceData: 'wholesaleReplace',
        set: 'equalityExempt',
    },
    RxMap: {
        set: 'equalityGatedWrite',
        replace: 'equalityGatedWrite', // set 的批量形态:同一语义,同一判等门(方法 19 修复)
        replaceData: 'equalityGatedWrite',
        delete: 'structural', clear: 'structural',
    },
    RxSet: {
        // Set 成员写入天然幂等(has 判等即 SameValueZero 门):add 重复成员零触发
        add: 'equalityGatedWrite',
        delete: 'structural', clear: 'structural',
        replace: 'wholesaleReplace', replaceData: 'wholesaleReplace',
    },
}

// ---- 判等门探针(组内每个入口执行同一套性质) ----
type EqualityProbe = {
    entry: string
    // 建立一个被订阅的可写点;返回写入函数与订阅重跑计数
    make: () => {write: (v: number) => void, runs: () => number, done: () => void}
    // Set 语义(SameValueZero)下 0/-0 不可区分,豁免 0→-0 触发断言
    sameValueZero?: boolean
}

const EQUALITY_PROBES: EqualityProbe[] = [
    {
        entry: 'atom(primitive)',
        make: () => {
            const a = atom<number>(0)
            let n = 0
            const stop = autorun(() => { n++; a() }, true)
            return {write: v => a(v), runs: () => n, done: stop}
        },
    },
    {
        entry: 'atom(objectProxy property set)',
        make: () => {
            const a = atom<any>({v: 0})
            let n = 0
            const stop = autorun(() => { n++; a.v }, true)
            return {write: v => { a.v = v }, runs: () => n, done: stop}
        },
    },
    {
        entry: 'RxMap.set',
        make: () => {
            const m = new RxMap<string, number>([['k', 0]])
            let n = 0
            const stop = autorun(() => { n++; m.get('k') }, true)
            return {write: v => m.set('k', v), runs: () => n, done: () => { stop(); m.destroy() }}
        },
    },
    {
        entry: 'RxMap.replace',
        make: () => {
            const m = new RxMap<string, number>([['k', 0], ['other', 1]])
            let n = 0
            const stop = autorun(() => { n++; m.get('k') }, true)
            return {write: v => m.replace([['k', v], ['other', 1]]), runs: () => n, done: () => { stop(); m.destroy() }}
        },
    },
    {
        entry: 'RxMap.replaceData',
        make: () => {
            const m = new RxMap<string, number>([['k', 0]])
            let n = 0
            const stop = autorun(() => { n++; m.get('k') }, true)
            return {write: v => m.replaceData([['k', v]]), runs: () => n, done: () => { stop(); m.destroy() }}
        },
    },
    {
        entry: 'RxSet.add',
        sameValueZero: true, // Set 成员语义:0 与 -0 同一成员,add(-0) 不触发是正确语义
        make: () => {
            const s = new RxSet<number>([0])
            let n = 0
            const stop = autorun(() => { n++; s.toArray() }, true)
            return {write: v => { s.add(v) }, runs: () => n, done: () => { stop(); s.destroy() }}
        },
    },
]

describe('账本完整性:每个变更方法必须归入语义组', () => {
    test('MUTATIONS ⊆ SEMANTIC_GROUPS(新增变更方法必须归组)', () => {
        const missing: string[] = []
        for (const [cls, methods] of Object.entries(MUTATIONS)) {
            for (const m of methods) {
                if (!SEMANTIC_GROUPS[cls]?.[m]) missing.push(`${cls}.${m}`)
            }
        }
        expect(missing, `以下变更方法未归入语义组(equalityGatedWrite/equalityExempt/structural/wholesaleReplace):\n${missing.join('\n')}`).toEqual([])
    })
    test('SEMANTIC_GROUPS ⊆ MUTATIONS(账本不得引用不存在的方法)', () => {
        const stale: string[] = []
        for (const [cls, methods] of Object.entries(SEMANTIC_GROUPS)) {
            for (const m of Object.keys(methods)) {
                if (!MUTATIONS[cls]?.includes(m)) stale.push(`${cls}.${m}`)
            }
        }
        expect(stale, `账本引用了未登记的方法:\n${stale.join('\n')}`).toEqual([])
    })
    test('equalityGatedWrite 组的每个入口都有判等探针(给组加入口必须补探针)', () => {
        const gated: string[] = []
        for (const [cls, methods] of Object.entries(SEMANTIC_GROUPS)) {
            for (const [m, group] of Object.entries(methods)) {
                if (group === 'equalityGatedWrite') gated.push(`${cls}.${m}`)
            }
        }
        // atom 的两个形态不在集合 MUTATIONS 里,是探针面的补充项
        const probed = new Set(EQUALITY_PROBES.map(p => p.entry))
        const missing = gated.filter(g => {
            const [cls, m] = g.split('.')
            return !probed.has(`${cls}.${m}`)
        })
        expect(missing, `equalityGatedWrite 组缺探针:\n${missing.join('\n')}`).toEqual([])
    })
})

describe('组内一致性:判等门性质对所有入口成立(Object.is 语义)', () => {
    for (const probe of EQUALITY_PROBES) {
        test(`${probe.entry}:same→零触发 / 0→-0 ${probe.sameValueZero ? '(SameValueZero 豁免)' : '→触发'} / NaN→NaN→零触发`, () => {
            const p1 = probe.make()
            try {
                const before = p1.runs()
                p1.write(0)                    // 初值即 0:Object.is 相同
                expect(p1.runs(), `${probe.entry} same-value`).toBe(before)
                if (!probe.sameValueZero) {
                    p1.write(-0)               // 0 → -0:可观察变化
                    expect(p1.runs(), `${probe.entry} 0→-0`).toBe(before + 1)
                } else {
                    p1.write(-0)               // SameValueZero:同一成员,零触发
                    expect(p1.runs(), `${probe.entry} 0→-0 (SVZ)`).toBe(before)
                }
            } finally { p1.done() }

            const p2 = probe.make()
            try {
                p2.write(NaN)
                const afterFirstNaN = p2.runs()
                p2.write(NaN)                  // NaN → NaN:Object.is 相同,零触发
                expect(p2.runs(), `${probe.entry} NaN→NaN`).toBe(afterFirstNaN)
            } finally { p2.done() }
        })
    }
})

describe('equalityExempt 豁免的特征钉扎(变更即协议决策,失败先问方向)', () => {
    test('RxList.set(i, sameItem) 现状:仍触发 EKC(下游可能依赖强制行重建)', () => {
        const list = new RxList<number>([7])
        let ekcCount = 0
        const stop = autorun(() => { list.at(0) }, true)
        const mapped = list.map(x => { ekcCount++; return x })
        expect(ekcCount).toBe(1)
        list.set(0, 7) // Object.is 相同的值
        // 现状特征:EKC 照发,派生 map 重跑该行(若未来加判等门,此断言变 1,
        // 必须与 axii/axle 的 RxListHost 消费语义同步决策后才能改)
        expect(ekcCount).toBe(2)
        stop(); mapped.destroy(); list.destroy()
    })
})

// ---- key 载荷透传 × 内部消费者归一化(-0) ----
// 每个 RxMap/RxSet 变更入口要么有探针,要么显式豁免(无 key 参数)。探针断言两件事:
//   payloadRaw  载荷里是调用方传入的原值(-0 原样;replace 的 argv 是调用方的容器本身)
//   derivedOk   按载荷重放的内部消费者 ≡ 全量重算(Object.is 逐元素,含顺序)
type KeyProbe = {entry: string, run: () => {payloadRaw: boolean, derivedOk: boolean}}
const sameSeq = (a: readonly unknown[], b: readonly unknown[]) => a.length === b.length && a.every((x, i) => Object.is(x, b[i]))
const captureIterate = (target: object) => {
    const infos: any[] = []
    const c = new Computed(function (this: Computed) { this.manualTrack(target, TrackOpTypes.ITERATE, ITERATE_KEY) },
        function (_d: unknown, triggerInfos: any[]) { infos.push(...triggerInfos) })
    c.run([], true)
    return {infos, stop: () => c.destroy()}
}
const mapDerivedOk = (m: RxMap<any, any>, keys: RxList<any>, values: RxList<any>, entries: RxList<[any, any]>) =>
    sameSeq(keys.data, [...m.data.keys()]) && sameSeq(values.data, [...m.data.values()])
    && sameSeq(entries.data.map(e => e[0]), [...m.data.keys()])
const mapProbe = (entry: string, initial: [any, any][], write: (m: RxMap<any, any>) => void, payloadRaw: (methodInfos: any[], keyInfos: any[]) => boolean): KeyProbe => ({
    entry,
    run: () => {
        const m = new RxMap<any, any>(initial)
        const keys = m.keys(), values = m.values(), entries = m.entries()
        const methodInfos: any[] = []
        const stopMethod = onChange(m, (infos: any[]) => { methodInfos.push(...infos) })
        const keyLevel = captureIterate(m)
        write(m)
        stopMethod(); keyLevel.stop()
        const result = {payloadRaw: payloadRaw(methodInfos, keyLevel.infos), derivedOk: mapDerivedOk(m, keys, values, entries)}
        m.destroy()
        return result
    },
})
const setProbe = (entry: string, initial: any[], write: (s: RxSet<any>) => unknown, payloadRaw: (methodInfos: any[], written: unknown) => boolean): KeyProbe => ({
    entry,
    run: () => {
        const s = new RxSet<any>(initial)
        const list = s.toList()
        const methodInfos: any[] = []
        const stop = onChange(s, (infos: any[]) => { methodInfos.push(...infos) })
        const written = write(s)
        stop()
        const result = {payloadRaw: payloadRaw(methodInfos, written), derivedOk: sameSeq(list.data, [...s.data])}
        list.destroy(); s.destroy()
        return result
    },
})
const isMinusZero = (x: unknown) => Object.is(x, -0)
const KEY_PROBES: KeyProbe[] = [
    mapProbe('RxMap.set', [[1, 'w']], m => m.set(-0, 'v'), method => method.length === 1 && isMinusZero(method[0].argv[0])),
    mapProbe('RxMap.delete', [[0, 'v'], [1, 'w']], m => m.delete(-0), method => method.length === 1 && isMinusZero(method[0].argv[0])),
    mapProbe('RxMap.replace', [[1, 'w']], m => m.replace([[-0, 'v']]), (_m, keyLevel) => keyLevel.some(i => i.type === 'add' && isMinusZero(i.key))),
    mapProbe('RxMap.replaceData', [[1, 'w']], m => m.replaceData([[-0, 'v']]), (_m, keyLevel) => keyLevel.some(i => i.type === 'add' && isMinusZero(i.key))),
    setProbe('RxSet.add', [1], s => s.add(-0), method => method.length === 1 && isMinusZero(method[0].argv[0])),
    setProbe('RxSet.delete', [0, 1], s => s.delete(-0), method => method.length === 1 && isMinusZero(method[0].argv[0])),
    // replace 的 argv 是调用方传入的容器本身(采纳/读取都不改写它);成员由 Set 去重归一化
    setProbe('RxSet.replace', [1], s => { const arr = [-0, 2]; s.replace(arr); return arr }, (method, arr) => method.length === 1 && method[0].argv[0] === arr),
    setProbe('RxSet.replaceData', [1], s => { const arr = [-0, 2]; s.replaceData(arr); return arr }, (method, arr) => method.length === 1 && method[0].argv[0] === arr),
]
// 无 key/成员参数的入口
const KEY_EXEMPT: Record<string, string> = {
    'RxMap.clear': '无 key 参数',
    'RxSet.clear': '无成员参数(委托 replace([]))',
}

describe('key 载荷透传 × 内部消费者归一化(-0)', () => {
    test('RxMap/RxSet 的每个变更入口都有 -0 探针或显式豁免', () => {
        const probed = new Set(KEY_PROBES.map(p => p.entry))
        const missing: string[] = []
        for (const cls of ['RxMap', 'RxSet']) {
            for (const m of MUTATIONS[cls]) {
                const entry = `${cls}.${m}`
                if (!probed.has(entry) && !KEY_EXEMPT[entry]) missing.push(entry)
            }
        }
        expect(missing, `以下入口缺 -0 载荷探针:\n${missing.join('\n')}`).toEqual([])
    })
    for (const probe of KEY_PROBES) {
        test(`${probe.entry}(-0):载荷透传原值,内部消费者 ≡ 全量重算`, () => {
            expect(probe.run(), probe.entry).toEqual({payloadRaw: true, derivedOk: true})
        })
    }
})
