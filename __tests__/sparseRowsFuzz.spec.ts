import {describe, expect, test} from 'vitest'
import {RxList} from '../src/RxList.js'

/**
 * 稀疏行等价类的差分 fuzz(源自 axle test/fuzz-invariants.test.tsx 的操作分布,
 * 剥离 DOM 层)。触发链:越界 set(契约内透传)让数据与行级记账产生洞 →
 * reorder(sortSelf/swap/reposition)的搬移把洞物化为显式 undefined(forEach
 * 不再跳过)→ 后续 splice 删除/整体销毁迭代行记账时崩溃。
 *
 * 不变量:任意操作序列下不抛错;已定义元素的 map 派生保持位置一致;
 * filter 派生等于"已定义且匹配"的子序列;destroy 不抛错。
 */

// 与 axle 相同的确定性 LCG(Numerical Recipes 参数)
function makeRandom(seed: number) {
    let state = seed >>> 0
    return () => {
        state = (state * 1664525 + 1013904223) >>> 0
        return state / 0x100000000
    }
}

describe('sparse rows fuzz (axle operation mix)', () => {
    for (let seed = 1; seed <= 24; seed++) {
        test(`seed=${seed}`, () => {
            const rand = makeRandom(seed * 7919)
            let counter = 0
            const items = new RxList<number>(
                Array.from({length: 5 + Math.floor(rand() * 10)}, () => counter++),
            )
            const mapped = items.map((v) => ({v}))
            const filtered = items.filter((x) => typeof x === 'number' && x % 2 === 0)
            const ops: string[] = []
            try {
                for (let step = 0; step < 100; step++) {
                    const op = Math.floor(rand() * 10)
                    const len = items.length()
                    if (op === 0) { ops.push(`push(${counter})`); items.push(counter++) }
                    else if (op === 1 && len) { ops.push('pop'); items.pop() }
                    else if (op === 2) { ops.push(`unshift(${counter})`); items.unshift(counter++) }
                    else if (op === 3 && len) { ops.push('shift'); items.shift() }
                    else if (op === 4) {
                        const styles = [
                            Math.floor(rand() * (len + 1)),
                            Math.floor(rand() * (len + 1)) - len - 1,
                            NaN,
                            undefined as unknown as number,
                            rand() * len,
                        ]
                        const start = styles[Math.floor(rand() * styles.length)]!
                        const del = Math.floor(rand() * 4)
                        const ins = Array.from({length: Math.floor(rand() * 3)}, () => counter++)
                        ops.push(`splice(${start},${del},[${ins}])`)
                        items.splice(start, del, ...ins)
                    } else if (op === 5 && len) {
                        const i = Math.floor(rand() * len)
                        ops.push(`set(${i},${counter})`); items.set(i, counter++)
                    } else if (op === 6) {
                        // 越界 set:稀疏洞的唯一入口(契约内透传行为)
                        const i = len + Math.floor(rand() * 3)
                        ops.push(`set-oor(${i},${counter})`); items.set(i, counter++)
                    } else if (op === 7 && len > 1) {
                        ops.push('sortSelf')
                        items.sortSelf((a, b) => (rand() < 0.5 ? a - b : b - a))
                    } else if (op === 8 && len > 2) {
                        const a = Math.floor(rand() * len)
                        const b = Math.floor(rand() * len)
                        if (a !== b) { ops.push(`swap(${Math.min(a, b)},${Math.max(a, b)})`); items.swap(Math.min(a, b), Math.max(a, b)) }
                    } else if (op === 9 && len > 2) {
                        const a = Math.floor(rand() * len)
                        const b = Math.floor(rand() * len)
                        ops.push(`reposition(${a},${b})`); items.reposition(a, b)
                    }

                    const ctx = () => `seed=${seed} step=${step} recent=${ops.slice(-6).join(';')}`
                    // map 不变量:已定义元素位置一致(洞的行为不做约定,只要求不崩)
                    for (let i = 0; i < items.data.length; i++) {
                        const v = items.data[i]
                        if (v !== null && v !== undefined) {
                            expect(mapped.data[i]?.v, `map ${ctx()} at ${i}`).toBe(v)
                        }
                    }
                    // filter 不变量:等于"已定义且匹配"的子序列
                    const expectedFiltered: number[] = []
                    for (let i = 0; i < items.data.length; i++) {
                        const v = items.data[i]
                        if (typeof v === 'number' && v % 2 === 0) expectedFiltered.push(v)
                    }
                    expect(filtered.data, `filter ${ctx()}`).toEqual(expectedFiltered)
                }
            } finally {
                // destroy 不变量:稀疏记账不允许把销毁路径炸掉
                mapped.destroy()
                filtered.destroy()
                items.destroy()
            }
        })
    }
})
