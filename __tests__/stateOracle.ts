/**
 * 规范化状态 oracle（2026-H3 深度反思的制度化产物）。
 *
 * 教训：五个差分 fuzz 曾共享同一份不完备投影——`for (const [k, g] of grouped.data)`
 * 只遍历**已存在的键**逐个比内容，空组键残留（幽灵键）在"空 ≡ 空"下恒真，
 * groupBy 空组缺陷因此存活多轮；方法 11 的朴素参考模型本身是对的，却被同一
 * 有偏投影消费掉。oracle 的强度决定差分测试的上界，且 oracle 形状会随复制
 * 传播——资产数量不等于 oracle 多样性。
 *
 * 规则（对偶于 coverageInventory 的 UNCOVERED 纪律，见 AGENTS.md §3.3）：
 *   派生结构的差分比较默认走本模块的**全可观察状态**规范化比对
 *   （键集 + 逐键内容 + size），任何弱化（只比部分面）必须在断言处显式注释理由。
 *   新增派生结构类型时先在这里补规范化器，再写 fuzz。
 */
import {expect} from 'vitest'
import type {RxList} from '../src/RxList.js'
import type {RxMap} from '../src/RxMap.js'

/** Object.is 语义的 key 稳定序标签：NaN 可定位、-0/0 可区分、跨类型稳定 */
function keyTag(k: unknown): string {
    if (typeof k === 'number') {
        if (Number.isNaN(k)) return 'num:NaN'
        if (Object.is(k, -0)) return 'num:-0'
        return `num:${k}`
    }
    return `${typeof k}:${String(k)}`
}

export function sortKeysCanonical<K>(keys: Iterable<K>): K[] {
    return [...keys].sort((a, b) => keyTag(a) < keyTag(b) ? -1 : keyTag(a) > keyTag(b) ? 1 : 0)
}

/** 朴素参考模型：与 groupBy 的全量 computation 同语义（保序、SameValueZero 键） */
export function modelGroupBy<T, K>(src: readonly T[], getKey: (item: T) => K): Map<K, T[]> {
    const groups = new Map<K, T[]>()
    for (let i = 0; i < src.length; i++) {
        const item = src[i]
        const key = getKey(item)
        const group = groups.get(key)
        if (group) {
            group.push(item)
        } else {
            groups.set(key, [item])
        }
    }
    return groups
}

/**
 * groupBy 全可观察状态比对：键集（无幽灵键/缺键）、size、逐键内容与顺序。
 * 键序按 Object.is 语义规范化，NaN/-0/字符串键都可稳定参与。
 */
export function expectGroupByEqualsModel<T, K>(
    grouped: RxMap<K, RxList<T>>,
    src: readonly T[],
    getKey: (item: T) => K,
    ctx: string,
) {
    const model = modelGroupBy(src, getKey)
    expect(
        sortKeysCanonical(grouped.data.keys()).map(keyTag),
        `groupBy keys ${ctx}`,
    ).toEqual(sortKeysCanonical(model.keys()).map(keyTag))
    expect(grouped.data.size, `groupBy size ${ctx}`).toBe(model.size)
    for (const [k, expected] of model) {
        expect(grouped.data.get(k)!.data, `group[${keyTag(k)}] ${ctx}`).toEqual(expected)
    }
}
