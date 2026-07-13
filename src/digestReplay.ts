import {TriggerInfo} from "./notify.js";
import {TriggerOpTypes} from "./operations.js";
import {normalizeSpliceStart, spliceMany} from "./util.js";

/**
 * digest 内「操作时源状态」重建内核。
 *
 * 背景（缺陷类：操作时位置 × 重放时终态）：triggerInfo 的 key/argv 是**操作时**
 * 位置，而 applyPatch 重放时 `source.data` 已是本次 digest 的**终态**。单 info 时
 * 两者一致；batch/延迟调度积累多条 info 时，凡是在重放中读取 source.data 做
 * 前缀计数/区间算术/长度回推的 patch 都会错位——findIndex/groupBy/slice 曾
 * 因此各自出过缺陷，历史修复是"多 info 一律回退全量重算"。
 *
 * 本内核把该等价类一次性关死：从终态逐条**逆向**应用 info 还原出每条 info
 * 应用后的源状态快照，patch 端对第 i 条 info 读 `after(i)`，与单 info 语义
 * 逐条对齐，多 info 因此可以安全增量而不再回退。
 *
 * 逆操作定义（info 按 RxList 的协议形状）：
 * - splice：after 在 [start, start+insertedCount) 处是插入项 → 逆 = 删插入、
 *   还原 methodResult（真实删除项）。start 用操作时长度归一化（负/越界/小数），
 *   操作时长度 = after.length - insertedCount + deletedCount。
 * - EXPLICIT_KEY_CHANGE：逆 = after[key] 写回 oldValue。**oldValue 为 undefined
 *   时带内不可区分**"替换了合法 undefined 元素"与"越界 set 扩长/洞位"（协议
 *   已知限制，同 toSorted 先例），一律判不可重建。
 * - reorder：pairs 语义 data[to] = old[from] → 逆 = before[from] = after[to]。
 *
 * 不可重建（返回 null）时调用方回退全量重算——保守方向，正确性不变。
 *
 * 性能契约：单 info 快路径零拷贝零分配（调用方通常在 length > 1 时才调用）；
 * 多 info 重建 O(n × infos) 拷贝，只发生在此前必然全量重算的路径上，通常
 * 仍远小于全量重算派生结构的成本（如 groupBy 重建全部分组 RxList）。
 *
 * 稀疏语义说明：逆向 splice 还原的删除项来自 methodResult（native splice 已把
 * 洞物化为 undefined），重建快照因此是稠密的。patch 消费者只按值读取快照，
 * 不做 hasOwnProperty 区分，两者等价；产生洞的 set 本身已被 EKC 歧义规则拦截。
 */
export type DigestSourceStates<T> = {
    /** infos[0..index] 应用后的源内容；index === infos.length-1 时即 finalData 活引用 */
    after(index: number): T[]
    /**
     * infos[index] 应用前的长度（操作时长度，用于归一化 splice start）。
     * 仅对 splice info 有定义良好语义；EKC 越界扩长（契约外）在 index 0 上
     * 无需求逆因而无法检出，其 lengthBefore 不可信——消费者只应对 splice 使用。
     */
    lengthBefore(index: number): number
}

// info 造成的长度变化（after.length - before.length）；无法解码返回 null
function lengthDelta(info: TriggerInfo): number | null {
    if (info.method === 'splice') {
        const inserted = info.argv!.length - 2
        const deleted = (info.methodResult as unknown[] | undefined)?.length ?? 0
        return inserted - deleted
    }
    if (info.method === 'reorder') return 0
    if (info.type === TriggerOpTypes.EXPLICIT_KEY_CHANGE) return 0
    return null
}

// 从 after 还原 before；不可逆返回 null。不得修改 after。
function inverseApply<T>(after: T[], info: TriggerInfo): T[] | null {
    if (info.method === 'splice') {
        const inserted = info.argv!.length - 2
        const deleted = (info.methodResult as T[] | undefined) ?? []
        const lengthBefore = after.length - inserted + deleted.length
        if (lengthBefore < 0) return null
        const start = normalizeSpliceStart(info.argv![0], lengthBefore)
        const before = after.slice()
        spliceMany(before, start, inserted, deleted)
        return before
    }
    if (info.method === 'reorder') {
        const pairs = info.argv?.[0] as [number, number][] | undefined
        if (!pairs) return null
        const before = after.slice()
        for (const [from, to] of pairs) {
            if (!Number.isInteger(from) || !Number.isInteger(to)
                || from < 0 || to < 0 || from >= before.length || to >= after.length) return null
            before[from] = after[to]
        }
        return before
    }
    if (info.type === TriggerOpTypes.EXPLICIT_KEY_CHANGE) {
        const key = info.key
        // 非稠密下标（负/小数/字符串/越界）与 oldValue === undefined（合法 undefined
        // 元素 vs 越界扩长带内不可区分）都不可安全求逆。
        if (typeof key !== 'number' || !Number.isInteger(key) || key < 0 || key >= after.length) return null
        if (info.oldValue === undefined) return null
        const before = after.slice()
        before[key] = info.oldValue as T
        return before
    }
    return null
}

export function reconstructDigestStates<T>(finalData: T[], infos: TriggerInfo[]): DigestSourceStates<T> | null {
    const n = infos.length
    const deltas = new Array<number>(n)
    for (let i = 0; i < n; i++) {
        const d = lengthDelta(infos[i])
        if (d === null) return null
        deltas[i] = d
    }
    if (n === 1) {
        // 快路径：不拷贝。after(0) 即终态；lengthBefore 由长度差回推（单 info 下成立）。
        return {
            after: () => finalData,
            lengthBefore: () => finalData.length - deltas[0],
        }
    }
    const states = new Array<T[]>(n)
    states[n - 1] = finalData
    for (let i = n - 1; i >= 1; i--) {
        const before = inverseApply(states[i], infos[i])
        if (before === null) return null
        states[i - 1] = before
    }
    // 首条 info 虽无需求逆，但仍必须可逆才放行：歧义 info（EKC 旧值 undefined、
    // 越界 key）在旧实现里走"多 info 一律全量重算"，保守语义必须逐字保留——
    // 否则增量路径会把 undefined 交给用户回调（如 groupBy 的 getKey），
    // 在旧实现不抛错的输入上抛错。
    if (inverseApply(states[0], infos[0]) === null) return null
    return {
        after: (index: number) => states[index],
        lengthBefore: (index: number) => states[index].length - deltas[index],
    }
}
