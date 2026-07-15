/**
 * 差分 fuzz 共享工具库。
 *
 * 动机(2026-H2 review 教训):对抗值域曾散落在各 fuzz 文件里各写各的,
 * "新维度加一次、全算子生效"无从谈起——undefined 作为合法元素值、重复值 ×
 * selection 家族两个盲区因此存活多轮。规则:
 *   1. 新增对抗值域维度必须加在本文件(而不是某个 fuzz 的局部),
 *      并在 coverageInventory 中为相关算子登记覆盖或显式标注 UNCOVERED;
 *   2. 所有 fuzz 的 PRNG 从这里导入(固定 seed 可复现,失败信息输出 seed 与操作史)。
 */

// mulberry32:固定 seed 的确定性 PRNG
export function mulberry32(seed: number) {
    let a = seed >>> 0
    return function () {
        a |= 0; a = (a + 0x6D2B79F5) | 0
        let t = Math.imul(a ^ (a >>> 15), 1 | a)
        t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296
    }
}

export type Rand = () => number

// ---- 对抗值域(元素值维度) ----
// 每个域是 (rand) => 取值函数。fuzz 按需组合;新维度(如 NaN、-0、共享引用)加在这里。

/** 唯一值域:自增计数,重复不可能出现(基线差分) */
export function uniqueInts(start = 100) {
    let counter = start
    return () => counter++
}

/** 重复值域:窄整数域,高频撞值(重复值定位缺陷类) */
export function duplicateInts(rand: Rand, width = 5) {
    return () => Math.floor(rand() * width)
}

/** 含 undefined 的值域:undefined 是 RxList 合法元素值(哨兵混用缺陷类) */
export function withUndefined<T>(rand: Rand, inner: () => T, probability = 0.25) {
    return (): T | undefined => (rand() < probability ? undefined : inner())
}

/** 含 NaN/-0 的数值域(Object.is 与 SameValueZero 分歧的等价类) */
export function withWeirdNumbers(rand: Rand, inner: () => number, probability = 0.15) {
    return (): number => {
        const r = rand()
        if (r < probability / 2) return NaN
        if (r < probability) return -0
        return inner()
    }
}

// ---- 对抗参数域(splice start 维度):负数/越界/小数/NaN ----
export function adversarialSpliceStart(rand: Rand, len: number): number {
    const r = rand()
    if (r < 0.15) return -Math.floor(rand() * (len + 2))
    if (r < 0.25) return len + Math.floor(rand() * 3)
    if (r < 0.32) return rand() * len
    if (r < 0.35) return NaN
    return Math.floor(rand() * (len + 1))
}

// ---- 对抗参数域(splice deleteCount 维度):负/NaN/Infinity/越界/小数 ----
// 2026-H3 round5:R5-2 教训的同构盲区——deleteCount 此前只在一次性单测里对抗,
// 各生成器恒为良性 0..3,"脏 deleteCount × batch 重放 × 派生族"的组合空间为零
// (与"越界代表值只有 len+1..len+3"同一种生成器分域窄化)。归一化语义:
// ToIntegerOrInfinity 后 clamp 到 [0, len-start](与 Array#splice 一致)。
export function adversarialSpliceDeleteCount(rand: Rand, len: number): number {
    const r = rand()
    if (r < 0.08) return -1 - Math.floor(rand() * 2)   // 负 → 0
    if (r < 0.14) return NaN                             // NaN → 0
    if (r < 0.20) return Infinity                        // Infinity → 删到尾
    if (r < 0.28) return len + Math.floor(rand() * 3)   // 越界 → clamp
    if (r < 0.36) return Math.floor(rand() * 3) + 0.5   // 小数 → trunc
    return Math.floor(rand() * 3)                        // 良性
}

// ---- 对抗参数域(set key 维度):越界(产生稀疏洞)/负/小数/数值上界 ----
// 2026-H3 round3 教训:稀疏(OOB set)此前以"一次性 sweep"进入体系而不是生成器,
// 该维度因此从不与操作序列组合——"先 OOB set、再不等长 splice"这种两步组合
// 任何随机搜索都生成不出来,createIndexKeySelection 的校正循环崩溃因此存活。
// 规则:契约外形态操作也必须以生成器进入组合空间(oracle 由调用方按二级契约降级,
// 见 sparseOpsFuzz 的"不崩溃 + 强制全量重算后收敛"两级断言)。
// 2026-H3 round5:key ≥ 2^32-1 的正整数不是数组下标(属性赋值、length 不变),
// 曾穿透 isDenseIndexKey 物化幽灵行/成员并在 groupBy 前缀循环卡 ~2^32 次迭代;
// 数值上界与负/小数同属"非下标 key"等价类,进入生成器常驻组合空间。
export function adversarialSetIndex(rand: Rand, len: number): number {
    const r = rand()
    if (r < 0.45) return len + 1 + Math.floor(rand() * 3)  // 越界:产生洞
    if (r < 0.55) return -1 - Math.floor(rand() * 2)         // 负:数组属性赋值
    if (r < 0.62) return Math.floor(rand() * len) + 0.5      // 小数:属性赋值
    if (r < 0.7) return 2 ** 32 - 1 + Math.floor(rand() * 3) // 数值上界:非下标正整数(属性赋值)
    return Math.floor(rand() * Math.max(len, 1))             // 区间内
}

// ---- 共享操作序列生成器(形态操作域) ----
// 对一个 RxList 源执行一步随机操作(含契约内结构操作与契约外 OOB/负/小数 set),
// 返回操作描述(用于失败信息回放)。所有形态类 fuzz 从这里取操作,
// 保证"新形态操作加一次、全 fuzz 生效"。
import type {RxList} from '../src/RxList.js'

export function performRandomListOp(
    rand: Rand,
    list: RxList<number>,
    nextValue: () => number,
): string {
    const len = list.data.length
    const r = rand()
    if (r < 0.22) {
        const start = adversarialSpliceStart(rand, len)
        const deleteCount = adversarialSpliceDeleteCount(rand, len)
        const items = Array.from({length: Math.floor(rand() * 3)}, () => nextValue())
        list.splice(start, deleteCount, ...items)
        return `splice(${start},${deleteCount},[${items}])`
    }
    if (r < 0.34) {
        const v = nextValue()
        list.push(v)
        return `push(${v})`
    }
    if (r < 0.40 && len > 0) {
        list.pop()
        return `pop()`
    }
    if (r < 0.48 && len > 0) {
        list.shift()
        return `shift()`
    }
    if (r < 0.60 && len > 0) {
        const index = Math.floor(rand() * len)
        const v = nextValue()
        list.set(index, v)
        return `set(${index},${v})`
    }
    if (r < 0.75) {
        // 形态迁移操作:越界/负/小数 set(契约外,透传;可能产生稀疏洞)
        const index = adversarialSetIndex(rand, len)
        const v = nextValue()
        list.set(index, v)
        return `set*(${index},${v})`
    }
    if (r < 0.83 && len >= 2) {
        const i = Math.floor(rand() * len)
        let j = Math.floor(rand() * len)
        if (j === i) j = (j + 1) % len
        list.swap(Math.min(i, j), Math.max(i, j))
        return `swap(${Math.min(i, j)},${Math.max(i, j)})`
    }
    if (r < 0.90 && len >= 2) {
        const start = Math.floor(rand() * len)
        const newStart = Math.floor(rand() * len)
        if (start !== newStart) {
            list.reposition(start, newStart)
            return `reposition(${start},${newStart})`
        }
        list.push(nextValue())
        return `push(fallback)`
    }
    if (r < 0.95 && len >= 2) {
        // 洞位安全的比较器:sortSelf 会把洞读成 undefined 传给 compare
        list.sortSelf((a, b) => ((a ?? -Infinity) as number) - ((b ?? -Infinity) as number))
        return `sortSelf()`
    }
    const v = nextValue()
    list.unshift(v)
    return `unshift(${v})`
}

// ---- 行形态维度(map 行的 mapFn 运行时行为;2026-H3 round4) ----
// R4-1 教训:"传了 index 参数"≠"读了 index 值"。map 的行按 mapFn **执行期**是否
// 读取响应式数据分成三种形态,走完全不同的实现路径:
//   storesValue   —— 只存值/存 atom 引用:行级探测捕获零依赖,行不升级(纯数据路径);
//   readsIndex    —— 执行期读 index():行升级为带 index atom 依赖的 rowComputed,
//                    "结构搬移 × 行级重算"的触发序交错路径只在此形态可达;
//   readsItemAtom —— 执行期读 item 内部 atom:行升级为带 item 依赖的 rowComputed,
//                    hasPendingStructuralInfos 守卫的 frame 定位分支只在
//                    此形态 × batch(行依赖先于结构 info 入队)可达。
// 所有 map 形态类 fuzz/killer 从这里取 mapFn 与对照模型,禁止在单个文件里私藏形态。
import type {Atom} from '../src/atom.js'
import {atom as createAtom} from '../src/atom.js'

/** readsIndex 形态:mapFn 执行期读 index()(行升级为带 index 依赖的 rowComputed) */
export const indexReadingMapFn = (item: number, index: Atom<number>) => `${item}#${index()}`
/** readsIndex 形态的全量对照模型 */
export const indexReadingModel = (src: number[]) => src.map((x, i) => `${x}#${i}`)

/** storesValue 形态:只存 atom 引用不读值(行不升级;既有 fuzz 的默认形态,显式命名) */
export const valueStoringMapFn = (item: number, index: Atom<number>) => ({item, index})

/** readsItemAtom 形态:行内含 atom 的元素工厂 + 执行期读 atom 的 mapFn */
export type AtomRow = {id: number, label: Atom<string>}
export function atomRowFactory(): (label: string) => AtomRow {
    let id = 0
    return (label: string) => ({id: id++, label: createAtom(label)})
}
export const itemAtomReadingMapFn = (row: AtomRow) => `${row.id}:${row.label()}`
export const itemAtomReadingModel = (src: AtomRow[]) => src.map(row => `${row.id}:${row.label.raw}`)

// ---- 触发精确度计数器(观察面:谁被重算了几次) ----
// 2026-H3 round3 教训:全部差分 fuzz 只对比终值,"结果正确但多做了工作"类缺陷
// (值未变的幽灵触发、无关源的误触发)对值 oracle 天然不可见。本计数器把
// 「每次 digest 每个派生至多 1 轮 patch + 至多 1 轮全量回退」与「无关源零触发」
// 变成可断言列;形态/差分 fuzz 按需挂载。
export type RecomputeCounter = {
    rounds: () => number
    fulls: () => number
    reset: () => void
    detach: () => void
}

// host 是任何 ReactiveEffect 派生(RxList/RxMap/RxSet 实例,或 computed 的 internal)
export function attachRecomputeCounter(host: {on: Function, off: Function}): RecomputeCounter {
    let rounds = 0
    let fulls = 0
    const onRecompute = () => { rounds++ }
    const onFull = () => { fulls++ }
    host.on('recompute', onRecompute)
    host.on('fullRecompute', onFull)
    return {
        rounds: () => rounds,
        fulls: () => fulls,
        reset: () => { rounds = 0; fulls = 0 },
        detach: () => { host.off('recompute', onRecompute); host.off('fullRecompute', onFull) },
    }
}
