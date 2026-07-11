import {describe, expect, test} from 'vitest'
import {RxList} from '../src/RxList.js'
import {ReactiveEffect} from '../src/reactiveEffect.js'
import {notifier} from '../src/notify.js'
import {TrackOpTypes, TriggerOpTypes} from '../src/operations.js'

/**
 * async applyPatch 的调度交错枚举。
 *
 * F1 类缺陷(挂起的 patch 霸占 activeScopes → 幽灵依赖/写入丢失/弹错栈)的
 * 触发条件是特定的完成顺序与并发写入时机。靠灵感构造敌意场景不可持续,
 * 这里把"两个 async patch + 中途写入"的事件序全排列逐一执行,每种交错都
 * 断言:最终数据与源一致(无静默丢失)、全局作用域栈与追踪栈完全复原。
 */

const tick = () => new Promise<void>(resolve => setTimeout(resolve, 0))

function permutations<T>(items: T[]): T[][] {
    if (items.length <= 1) return [items.slice()]
    const result: T[][] = []
    for (let i = 0; i < items.length; i++) {
        const rest = [...items.slice(0, i), ...items.slice(i + 1)]
        for (const perm of permutations(rest)) {
            result.push([items[i], ...perm])
        }
    }
    return result
}

function createControlledAsyncDerived(source: RxList<number>) {
    let pendingResolve: (() => void) | undefined
    const derived = new RxList<number>(
        function computation(this: RxList<number>) {
            this.manualTrack(source, TrackOpTypes.METHOD, TriggerOpTypes.METHOD)
            return source.data.slice()
        },
        async function applyPatch(this: RxList<number>, _data, triggerInfos) {
            await new Promise<void>(resolve => { pendingResolve = resolve })
            for (const info of triggerInfos) {
                if (info.method === 'splice') {
                    this.spliceArray(this.data.length, 0, info.argv!.slice(2) as number[])
                }
            }
        }
    )
    return {
        derived,
        hasPending: () => pendingResolve !== undefined,
        resolve: () => {
            const resolve = pendingResolve
            pendingResolve = undefined
            resolve?.()
        },
    }
}

type Step = 'writeA' | 'writeB' | 'resolveA' | 'resolveB'

describe('async patch interleaving enumeration', () => {
    const steps: Step[] = ['writeA', 'writeB', 'resolveA', 'resolveB']
    const allOrders = permutations(steps) // 4! = 24 种交错

    for (const order of allOrders) {
        test(order.join(' -> '), async () => {
            const scopesDepthBefore = ReactiveEffect.activeScopes.length
            const trackStackDepthBefore = notifier.trackStack.length
            const sourceA = new RxList([1])
            const sourceB = new RxList([100])
            const a = createControlledAsyncDerived(sourceA)
            const b = createControlledAsyncDerived(sourceB)
            let next = 2
            try {
                // 启动两个挂起的 patch
                sourceA.push(next++)
                sourceB.push(next++)
                await tick()

                for (const step of order) {
                    if (step === 'writeA') sourceA.push(next++)
                    else if (step === 'writeB') sourceB.push(next++)
                    else if (step === 'resolveA') a.resolve()
                    else b.resolve()
                    await tick()
                    // 任何交错点上,同步世界都必须是干净的
                    expect(ReactiveEffect.activeScopes.length, `scopes after ${step}`).toBe(scopesDepthBefore)
                }

                // 排干所有后续 patch 轮次
                for (let i = 0; i < 10 && (a.hasPending() || b.hasPending()); i++) {
                    a.resolve()
                    b.resolve()
                    await tick()
                }

                // 无论何种交错:数据不丢失、不重复,与源完全一致
                expect(a.derived.data, `derivedA for ${order.join('->')}`).toEqual(sourceA.data)
                expect(b.derived.data, `derivedB for ${order.join('->')}`).toEqual(sourceB.data)
                expect(ReactiveEffect.activeScopes.length).toBe(scopesDepthBefore)
                expect(notifier.trackStack.length).toBe(trackStackDepthBefore)
            } finally {
                a.derived.destroy()
                b.derived.destroy()
                sourceA.destroy()
                sourceB.destroy()
            }
        })
    }
})
