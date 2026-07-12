import {describe, expect, test} from 'vitest'
import {RxList} from '../src/RxList.js'

/**
 * RxList.reduce 的首个专项资产(2026-H2 覆盖清单曾暴露该公开算子处于零测试状态)。
 * 契约(README 支持矩阵):尾部追加增量,其余操作回退全量重算。
 * 差分基准:从当前 source.data 用朴素 reduce 全量重算。
 */
describe('RxList.reduce', () => {
    const recompute = (src: number[]) => src.map(x => x * 2)

    test('全量计算 + 尾部追加增量 ≡ 全量重算(含重复值)', () => {
        const source = new RxList<number>([1, 2, 2, 3])
        const doubled = source.reduce<RxList<number>>((last, item) => last.push(item * 2))
        try {
            expect(doubled.data).toEqual(recompute(source.data))

            // 尾部追加走增量路径
            source.push(4)
            expect(doubled.data).toEqual(recompute(source.data))
            source.push(2, 5)
            expect(doubled.data).toEqual(recompute(source.data))
        } finally {
            doubled.destroy()
            source.destroy()
        }
    })

    test('非尾部操作回退全量重算 ≡ 全量重算', () => {
        const source = new RxList<number>([1, 2, 3])
        const doubled = source.reduce<RxList<number>>((last, item) => last.push(item * 2))
        try {
            source.unshift(0)
            expect(doubled.data).toEqual(recompute(source.data))
            source.splice(1, 1, 9, 9)
            expect(doubled.data).toEqual(recompute(source.data))
            source.set(0, 7)
            expect(doubled.data).toEqual(recompute(source.data))
            source.sortSelf((a, b) => a - b)
            expect(doubled.data).toEqual(recompute(source.data))
        } finally {
            doubled.destroy()
            source.destroy()
        }
    })

    test('destroy 后不再接收更新(僵尸检查)', () => {
        const source = new RxList<number>([1, 2])
        const doubled = source.reduce<RxList<number>>((last, item) => last.push(item * 2))
        const snapshot = [...doubled.data]
        doubled.destroy()
        source.push(3)
        expect(doubled.data).toEqual(snapshot)
        source.destroy()
    })
})
