/**
 * digestReplay 内核的差分地面真值测试。
 *
 * 性质：batch 内逐操作记录真实中间状态（写立即生效，只有订阅者被推迟），
 * digest 后把捕获的 triggerInfos + 终态喂给内核，重建快照必须与记录逐条相等。
 * 固定 seed；失败信息输出 seed 与操作史。
 */
import {describe, expect, test} from 'vitest'
import {RxList} from '../src/RxList.js'
import {batch} from '../src/notify.js'
import type {TriggerInfo} from '../src/notify.js'
import {computed, Computed, destroyComputed} from '../src/computed.js'
import {TrackOpTypes, TriggerOpTypes} from '../src/operations.js'
import {reconstructDigestStates} from '../src/digestReplay.js'
import {adversarialSpliceStart, mulberry32} from './fuzzKit.js'

function captureInfos<T>(list: RxList<T>) {
    const infos: TriggerInfo[] = []
    const c = computed(
        function computation(this: Computed) {
            this.manualTrack(list, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
            this.manualTrack(list, TrackOpTypes.EXPLICIT_KEY_CHANGE, TriggerOpTypes.EXPLICIT_KEY_CHANGE)
            return null
        },
        function applyPatch(this: Computed, _d, triggerInfos: TriggerInfo[]) {
            infos.push(...triggerInfos)
        },
        true,
    )
    return {infos, destroy: () => destroyComputed(c)}
}

describe('digestReplay: 单条 info 逆操作', () => {
    test('splice：负/越界/小数/NaN start 与操作时长度归一化', () => {
        for (const start of [-1, -99, 99, 1.5, NaN, 0]) {
            const list = new RxList<number>([1, 2, 3, 4])
            const cap = captureInfos(list)
            const before = list.data.slice()
            list.splice(start as number, 2, 100, 200, 300)
            const states = reconstructDigestStates(list.data, cap.infos)!
            expect(states, `start=${start}`).not.toBeNull()
            expect(states.lengthBefore(0), `start=${start}`).toBe(before.length)
            expect(states.after(0), `start=${start}`).toBe(list.data)
            cap.destroy(); list.destroy()
        }
    })

    test('EKC：需要求逆的位置（index ≥ 1）上 oldValue undefined 判不可重建', () => {
        const list = new RxList<number | undefined>([undefined, 2])
        const cap = captureInfos(list)
        batch(() => {
            list.splice(1, 1)   // 先有一条，让 set 落在需要求逆的位置
            list.set(0, 9)      // oldValue === undefined → 歧义
        })
        expect(reconstructDigestStates(list.data, cap.infos)).toBeNull()
        cap.destroy(); list.destroy()
    })

    test('EKC：需要求逆的位置上越界 set（扩长产生洞）判不可重建', () => {
        const list = new RxList<number>([1])
        const cap = captureInfos(list)
        batch(() => {
            list.push(10)
            list.set(5, 9)      // OOB：oldValue undefined → 歧义
        })
        expect(reconstructDigestStates(list.data, cap.infos)).toBeNull()
        cap.destroy(); list.destroy()
    })

    test('EKC：首条 info 歧义（oldValue undefined）同样判不可重建（保守回退语义）', () => {
        // after(0) 虽可由后续 info 的逆推出，但歧义 info 在旧实现下走全量回退；
        // 内核保守放行会让增量路径把 undefined 交给用户回调，行为漂移。
        const list = new RxList<number | undefined>([undefined, 2])
        const cap = captureInfos(list)
        batch(() => {
            list.set(0, 9)      // oldValue === undefined，位于首条
            list.splice(1, 1)
        })
        expect(reconstructDigestStates(list.data, cap.infos)).toBeNull()
        expect(list.data).toEqual([9])
        cap.destroy(); list.destroy()
    })

    test('reorder：swap/reposition/sortSelf 的 pairs 可逆', () => {
        const ops: ((l: RxList<number>) => void)[] = [
            l => l.swap(0, 2),
            l => l.reposition(3, 0, 2),
            l => l.sortSelf((a, b) => b - a),
        ]
        for (const [i, op] of ops.entries()) {
            const list = new RxList<number>([3, 1, 4, 1, 5])
            const cap = captureInfos(list)
            const before = list.data.slice()
            batch(() => {
                op(list)
                list.push(999) // 凑成多 info，强制走重建路径
            })
            const states = reconstructDigestStates(list.data, cap.infos)!
            expect(states, `op#${i}`).not.toBeNull()
            // 重建的 after(0) = reorder 之后、push 之前
            expect(states.after(0).slice(0, before.length).length, `op#${i}`).toBe(before.length)
            expect([...states.after(0)].sort((a, b) => a - b), `op#${i}`)
                .toEqual([...before].sort((a, b) => a - b))
            expect(states.after(1)).toBe(list.data)
            cap.destroy(); list.destroy()
        }
    })
})

describe('digestReplay: 协议外 info 的守卫可达性（合成 info 直喂内核）', () => {
    const mk = (partial: Partial<TriggerInfo>): TriggerInfo =>
        ({source: null, type: TriggerOpTypes.METHOD, ...partial}) as TriggerInfo

    test('未知方法：单条即判不可重建（delta 前置判断，不依赖 inverse 兜底）', () => {
        expect(reconstructDigestStates([1, 2], [mk({method: 'mystery'})])).toBeNull()
    })

    test('reorder 缺 pairs（argv 空）判不可重建', () => {
        const infos = [
            mk({method: 'reorder'}),
            mk({method: 'splice', argv: [0, 0, 9], methodResult: []}),
        ]
        expect(reconstructDigestStates([9, 1, 2], infos)).toBeNull()
    })

    test('reorder pairs 越界/非整数判不可重建', () => {
        for (const pairs of [[[0, 5]], [[5, 0]], [[0.5, 1]], [[-1, 0]]]) {
            const infos = [
                mk({method: 'reorder', argv: [pairs]}),
                mk({method: 'splice', argv: [0, 0, 9], methodResult: []}),
            ]
            expect(reconstructDigestStates([9, 1, 2], infos), JSON.stringify(pairs)).toBeNull()
        }
    })

    test('splice 的操作时长度回推为负（methodResult 与 argv 不自洽）判不可重建', () => {
        const infos = [
            mk({method: 'splice', argv: [0, 0, 9], methodResult: []}),
            mk({method: 'splice', argv: [0, 0, 7, 7, 7], methodResult: []}),
        ]
        // after(1) = [x]，第二条声称插入 3 删除 0 → lengthBefore = -2
        expect(reconstructDigestStates([7], infos)).toBeNull()
    })
})

describe('digestReplay: 多 info 差分地面真值（固定 seed fuzz）', () => {
    for (const seed of [7, 41, 2026]) {
        test(`seed=${seed}: 重建快照 ≡ batch 内逐操作实录`, () => {
            const rand = mulberry32(seed)
            let nextVal = 100
            for (let round = 0; round < 60; round++) {
                const list = new RxList<number>([1, 2, 3, 4, 5].map(x => x * (round + 1)))
                const cap = captureInfos(list)
                const recorded: number[][] = []
                const history: string[] = []
                const opsCount = 2 + Math.floor(rand() * 3)
                batch(() => {
                    for (let i = 0; i < opsCount; i++) {
                        const r = rand()
                        const len = list.data.length
                        if (r < 0.45) {
                            const start = adversarialSpliceStart(rand, len)
                            const deleteCount = Math.floor(rand() * 3)
                            const inserts = Array.from({length: Math.floor(rand() * 3)}, () => nextVal++)
                            history.push(`splice(${start},${deleteCount},[${inserts}])`)
                            list.splice(start, deleteCount, ...inserts)
                        } else if (r < 0.7 && len > 0) {
                            const idx = Math.floor(rand() * len)
                            history.push(`set(${idx},${nextVal})`)
                            list.set(idx, nextVal++)
                        } else if (r < 0.85 && len > 1) {
                            const a = Math.floor(rand() * len)
                            const b = Math.floor(rand() * len)
                            if (a !== b) {
                                history.push(`swap(${a},${b})`)
                                list.swap(a, b)
                            } else {
                                history.push(`push(${nextVal})`)
                                list.push(nextVal++)
                            }
                        } else {
                            history.push(`push(${nextVal})`)
                            list.push(nextVal++)
                        }
                        recorded.push(list.data.slice())
                    }
                })
                const ctx = `seed=${seed} round=${round} ops=${history.join(';')}`
                const states = reconstructDigestStates(list.data, cap.infos)
                // 值域全为非 undefined 且 set 只打稠密下标 → 必可重建
                expect(states, ctx).not.toBeNull()
                expect(cap.infos.length, ctx).toBe(recorded.length)
                for (let i = 0; i < recorded.length; i++) {
                    expect(states!.after(i), `${ctx} after(${i})`).toEqual(recorded[i])
                    const beforeLen = i === 0
                        ? [1, 2, 3, 4, 5].length
                        : recorded[i - 1].length
                    expect(states!.lengthBefore(i), `${ctx} lengthBefore(${i})`).toBe(beforeLen)
                }
                cap.destroy(); list.destroy()
            }
        })
    }
})
