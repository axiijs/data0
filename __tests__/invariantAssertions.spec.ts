import {describe, expect, test} from 'vitest'
import {atom} from '../src/atom.js'
import {computed, Computed, destroyComputed} from '../src/computed.js'
import {batch, notifier} from '../src/notify.js'
import {ReactiveEffect} from '../src/reactiveEffect.js'
import {RxList} from '../src/RxList.js'

/**
 * L2 防线自检:验证 dev-mode 全局不变量断言在违约时真的会开火。
 * (被动路径——现有全部测试在断言开启下零误报——由整个套件本身覆盖。)
 */
describe('dev-mode 不变量断言', () => {
    test('batch 内泄漏 activeScopes 会在 batch 边界被当场检出', () => {
        const fake = new Computed(function (this: Computed) { return 1 })
        const depthBefore = ReactiveEffect.activeScopes.length
        try {
            expect(() => {
                batch(() => {
                    // 模拟某个订阅者/用户代码遗留 scope 未弹出
                    ReactiveEffect.activeScopes.push(fake)
                })
            }).toThrow('activeScopes depth not restored after batch')
        } finally {
            // 恢复全局状态,不污染其他用例
            ReactiveEffect.activeScopes.length = depthBefore
            fake.destroy()
        }
    })

    test('batch 内 pause/reset 不配平会在 batch 边界被当场检出', () => {
        const depthBefore = notifier.trackStack.length
        try {
            expect(() => {
                batch(() => {
                    notifier.pauseTracking() // 只 pause 不 reset
                })
            }).toThrow('trackStack depth not restored after batch')
        } finally {
            notifier.trackStack.length = depthBefore
            notifier.shouldTrack = true
        }
    })

    test('平衡的 batch 正常通过,digest 后 session 完全静止', () => {
        const a = atom(1)
        const double = computed(() => a() * 2)
        try {
            batch(() => {
                a(2)
                a(3)
            })
            expect(double()).toBe(6)
            expect(notifier.sessionQueue.length).toBe(0)
            expect(notifier.inEffectSession).toBe(false)
            expect(notifier.isDigesting).toBe(false)
        } finally {
            destroyComputed(double)
        }
    })
})

describe('RxList 行级记账不变量的开火自检', () => {
    // 这些断言由全套测试被动执行（零误报），这里主动违约验证它们真的会开火：
    // 变异掉断言条件/守卫（mutation 审计的幸存类之一）会让本组测试当场失败。
    test('atomIndexes 值漂移在下一次结构变更时被检出', () => {
        const source = new RxList([1, 2, 3])
        const mapped = source.map((x, idx) => x + idx.raw)
        try {
            // 破坏 index atom 的值（模拟记账错位）
            source.atomIndexes![1]!(5)
            expect(() => source.push(4)).toThrow('atomIndex value drift at 1')
        } finally {
            mapped.destroy()
            source.destroy()
        }
    })

    test('atomIndexes 长于 data 在下一次结构变更时被检出', () => {
        const source = new RxList([1, 2])
        const mapped = source.map((x, idx) => x + idx.raw)
        try {
            source.atomIndexes!.push(atom(2), atom(3))
            expect(() => source.push(9)).toThrow('atomIndexes longer than data')
        } finally {
            mapped.destroy()
            source.destroy()
        }
    })

    test('map 行级 effect frame 与数据错位在 patch 边界被检出', () => {
        const source = new RxList([1, 2])
        const mapped = source.map(x => x * 2)
        try {
            mapped.effectFramesArray.push([])   // 人为错位
            expect(() => source.push(3)).toThrow('map effectFramesArray misaligned with data')
        } finally {
            mapped.destroy()
            source.destroy()
        }
    })
})

describe('身份式 scope 出栈(completeTracking)', () => {
    test('completeTracking 只移除自己,不误弹栈顶的其他 effect', () => {
        const self = new Computed(function (this: Computed) { return 1 })
        const other = new Computed(function (this: Computed) { return 2 })
        const depthBefore = ReactiveEffect.activeScopes.length
        try {
            // 构造交错:self 先入栈,other 后入栈(位于栈顶)
            self.prepareTracking(false, true)
            other.prepareTracking(false, true)
            // self 先完成:必须移除 self 自己,而不是弹掉栈顶的 other
            self.completeTracking(false, true)
            const scopes = ReactiveEffect.activeScopes
            expect(scopes.includes(self)).toBe(false)
            expect(scopes[scopes.length - 1]).toBe(other)
            other.completeTracking(false, true)
            expect(ReactiveEffect.activeScopes.length).toBe(depthBefore)
        } finally {
            ReactiveEffect.activeScopes.length = depthBefore
            self.destroy()
            other.destroy()
        }
    })
})
