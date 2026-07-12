import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest'
import {atom} from '../src/atom.js'
import {RxTime} from '../src/RxTime.js'

/**
 * RxTime 的确定性时钟审计(fake timers)。外围模块此前只有 4 条真实时钟测试
 * (不可控、慢);本资产用假时钟钉住:阈值翻转的 timeout 语义、参数 atom 变化的
 * 重算+重设、以及 2026-H2 修复的清理泄漏缺陷类(多入口 disposer 覆盖)。
 */
describe('RxTime deterministic (fake timers)', () => {
    beforeEach(() => {
        vi.useFakeTimers({toFake: ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'Date']})
    })
    afterEach(() => {
        vi.useRealTimers()
    })

    test('gt(未来时刻):到点自动翻转为 true', () => {
        const now = Date.now()
        const t = new RxTime()
        const passed = t.gt(now + 1000)
        expect(passed.raw).toBe(false)
        vi.advanceTimersByTime(1100)
        expect(passed.raw).toBe(true)
        t.destroy()
    })

    test('参数 atom 变化立即重算并重设 timeout', () => {
        const now = Date.now()
        const threshold = atom(now + 1000)
        const t = new RxTime()
        const passed = t.gt(threshold)
        expect(passed.raw).toBe(false)

        threshold(now + 5000) // 阈值推远:1.1s 后仍应为 false
        vi.advanceTimersByTime(1100)
        expect(passed.raw).toBe(false)
        vi.advanceTimersByTime(4000) // 越过新阈值
        expect(passed.raw).toBe(true)
        t.destroy()
    })

    test('destroy 后不再翻转(timeout 与 autorun 全部清理,幂等)', () => {
        const now = Date.now()
        const threshold = atom(now + 1000)
        const t = new RxTime()
        const passed = t.gt(threshold)
        t.destroy()
        t.destroy() // 幂等
        threshold(now - 1) // autorun 已停,不得重算
        vi.advanceTimersByTime(2000) // timeout 已清,不得翻转
        expect(passed.raw).toBe(false)
    })

    test('缺陷回归:resolve 与 subscribe 混用后 destroy 清理全部副作用', () => {
        const t = new RxTime()
        const passed = t.gt(Date.now() + 1000)
        const ticker = t.subscribe(100) // 旧实现:覆盖 stopAutorun → resolve 的 autorun 泄漏
        const tickerAtStop = ticker.raw
        t.destroy()

        vi.advanceTimersByTime(2000)
        expect(passed.raw).toBe(false)       // autorun+timeout 已停
        expect(ticker.raw).toBe(tickerAtStop) // interval 已停
    })

    test('stopAutorun 公开入口停止全部已注册副作用', () => {
        const t = new RxTime()
        const passed = t.gt(Date.now() + 1000)
        const ticker = t.subscribe(100)
        const tickerAtStop = ticker.raw
        t.stopAutorun!()
        vi.advanceTimersByTime(2000)
        expect(passed.raw).toBe(false)
        expect(ticker.raw).toBe(tickerAtStop)
    })

    test('链式算术 + lt/eq 语义', () => {
        const now = Date.now()
        const t = new RxTime()
        // (now+offset) 与当前时间比较:add 之后 lt
        const before = t.add(1000).lt(now + 500) // now+1000 < now+500 → false
        expect(before.raw).toBe(false)
        t.destroy()
    })
})
