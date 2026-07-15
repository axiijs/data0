/**
 * atom 创建时形态矩阵(2026-H3 round3 根因 6 的特征资产)。
 *
 * 事故:primitive atom 快路径(5f15d2a,2026-05)的 commit message 声称
 * "preserving object atom behavior"——对「生而对象」的 atom 为真,对「生而
 * null、后来写入对象」的 atom 为假(proxy 的 set 陷阱按当前值分派;primitive
 * 形态没有陷阱,属性写静默落在函数对象上)。该窄化没有让任何测试变红,因为
 * "形态选择规则"此前从未被枚举——测试镜像文档,未枚举的语义无从失败。
 *
 * 本矩阵把「initValue 类型 → 形态 → 能力」整表钉死:任何改动 atom 形态选择
 * 或形态能力的变更(尤其性能优化)都会在这里当场显形,由作者对照 README
 * 「传播模型」的形态契约决定是修代码还是改契约。
 *
 * 配套流程规则(AGENTS.md「性能优化的行为特征差分」):性能类 PR 必须先跑本
 * 矩阵与全套特征测试,行为窄化必须显式记录而不是静默通过。
 */
import {describe, expect, test} from 'vitest'
import {atom, isAtom} from '../src/atom.js'
import {isPrimitiveAtom} from '../src/atom.js'
import {autorun} from '../src/common.js'

class Instance { x = 1 }

// initValue → 期望形态(primitive = 轻量函数,无 Proxy;proxy = 完整陷阱)
const FORM_MATRIX: Array<[label: string, make: () => any, primitive: boolean]> = [
    ['null',        () => atom(null),               true],
    ['undefined',   () => atom(undefined),          true],
    ['number',      () => atom(1),                  true],
    ['string',      () => atom('s'),                true],
    ['boolean',     () => atom(true),               true],
    ['NaN',         () => atom(NaN),                true],
    ['plainObject', () => atom({x: 1}),             false],
    ['array',       () => atom([1, 2]),             false],
    ['Map',         () => atom(new Map()),          false],
    ['Set',         () => atom(new Set()),          false],
    ['classInst',   () => atom(new Instance()),     false],
    ['function 值', () => atom((() => 1) as any),   false],
    ['interceptor', () => atom(1, (u, h) => [u, h]), false], // interceptor 强制 proxy
]

describe('形态选择规则:initValue 类型 → primitive/proxy(创建时定型,永不迁移)', () => {
    for (const [label, make, primitive] of FORM_MATRIX) {
        test(`atom(${label}) → ${primitive ? 'primitive' : 'proxy'} 形态`, () => {
            const a = make()
            expect(isAtom(a)).toBe(true)
            expect(isPrimitiveAtom(a), `${label} 形态`).toBe(primitive)
        })
    }
})

describe('形态 × 能力矩阵(整值读写/属性读/属性写触发)', () => {
    test('两种形态的整值读写与依赖追踪等价', () => {
        for (const [label, make] of FORM_MATRIX) {
            const a = make()
            let runs = 0
            const stop = autorun(() => { runs++; a() }, true)
            expect(runs, `${label} 初跑`).toBe(1)
            a({fresh: true})
            expect(runs, `${label} 整值写触发`).toBe(2)
            expect(a.raw, `${label} raw`).toEqual({fresh: true})
            stop()
        }
    })

    test('proxy 形态:属性读转发(plain object)+ 属性写触发,按当前值分派(迁移安全)', () => {
        const a = atom<any>({x: 1})
        let runs = 0
        const stop = autorun(() => { runs++; a() }, true)
        a.x = 2
        expect(runs).toBe(2)         // 属性写触发
        expect(a.raw.x).toBe(2)      // 写穿 value
        expect(a.x).toBe(2)          // 属性读转发
        // 值迁移到 null 再回到对象:proxy 陷阱按当前值分派,能力随值恢复
        a(null)
        expect(runs).toBe(3)
        a({y: 5})
        expect(runs).toBe(4)
        a.y = 6
        expect(runs).toBe(5)
        expect(a.raw.y).toBe(6)
        stop()
    })

    test('primitive 形态:值迁移为对象后属性能力不出现(形态不迁移,R3-C1 契约)', () => {
        const a = atom<any>(null)   // primitive 形态定型
        a({x: 1})                    // 值迁移为对象
        let runs = 0
        const stop = autorun(() => { runs++; a() }, true)
        ;(a as any).x = 9            // 属性写:落函数对象,不写 value、不触发
        expect(runs).toBe(1)
        expect(a.raw.x).toBe(1)
        stop()
    })

    test('proxy 形态 × class 实例:属性写触发但属性读不转发(R3-C2 不对称)', () => {
        const inst = new Instance()
        const a = atom<any>(inst)
        let runs = 0
        const stop = autorun(() => { runs++; a() }, true)
        a.x = 2
        expect(runs).toBe(2)
        expect(inst.x).toBe(2)
        expect(a.x).toBeUndefined()
        stop()
    })
})
