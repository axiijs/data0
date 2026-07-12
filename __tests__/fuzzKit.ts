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
