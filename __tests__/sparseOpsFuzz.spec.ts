/**
 * 形态操作差分 fuzz(2026-H3 round3 根因 2/5 的机械化闭合)。
 *
 * 背景:稀疏(OOB set)此前以"一次性 sweep"进入体系而不是操作生成器,该维度
 * 从不与操作序列组合——"先 OOB set、再不等长 splice"任何随机搜索都生成不出,
 * createIndexKeySelection 校正循环崩溃、concat 段错位因此在专用 sweep 的绿格子
 * 下存活。本资产把契约外形态操作(越界/负/小数 set)放进共享操作生成器
 * (fuzzKit.performRandomListOp),与契约内结构操作、batch 分组自由组合。
 *
 * 二级 oracle(契约外序列的承诺面,弱于差分 fuzz 的"每步 ≡ 全量"):
 *   T1 无崩溃:任何一步操作不得向调用方抛错;
 *   T2 强制收敛:序列结束后对每个派生 force full recompute,结果必须等于
 *      "该算子 computation 语义对当前 source.data 的参考实现"(逐算子明确
 *      洞位语义:map/selection 跳洞、groupBy/toSet/concat 物化洞、reduce 原生跳洞);
 *   T3 恢复性:强制重算后再做一步契约内操作(push),派生结构必须增量跟上
 *      (内部记账——行 frame、atomIndexes、indicator——经稀疏+强制重算后仍自洽)。
 *
 * 失败信息输出 seed 与完整操作史(AGENTS.md 随机测试纪律)。
 */
import {describe, expect, test} from 'vitest'
import {RxList, createIndexKeySelection, createSelection} from '../src/RxList.js'
import {RxSet} from '../src/RxSet.js'
import {recompute} from '../src/computed.js'
import {batch} from '../src/notify.js'
import {mulberry32, performRandomListOp, uniqueInts} from './fuzzKit.js'
import {sortKeysCanonical} from './stateOracle.js'

// 洞可被 reorder 物化成显式 undefined(data[newIndex] = 读洞所得),
// 所以所有用户回调都必须 undefined 容忍,参考实现与派生使用同一函数。
const mapFn = (x: number | undefined) => ((x ?? -1) as number) * 2 + 1
const filterPred = (x: number | undefined) => typeof x === 'number' && x % 2 === 0
const findPred = (x: number | undefined) => typeof x === 'number' && x % 7 === 0
// CAUTION comparator 必须把 undefined 排尾(与 Array#sort 的强制规则一致):
//  native sort **从不为 undefined 元素调用 comparator**并强制将其排到尾部,
//  而 toSorted 的增量二分必须咨询 comparator——把 undefined 排前(?? -Infinity)
//  的 comparator 与 native 规则冲突,增量与全量在"含 undefined 的列表插入
//  定义值"时必然分叉(本 fuzz 首跑发现;README toSorted 参数契约已补充该裁定,
//  与 NaN × 裸数值 comparator 的既有契约外裁定同构)。
const holeCompare = (a: number | undefined, b: number | undefined) =>
    ((a ?? Infinity) as number) - ((b ?? Infinity) as number)
const groupKey = (x: number | undefined) => (x === undefined ? 'hole' : (x as number) % 3)
const reduceFn = (acc: number, x: number | undefined) => acc + (typeof x === 'number' ? x : 0)

const densify = <T,>(arr: readonly T[]) => Array.from(arr)

function buildFamily(source: RxList<number>, other: RxList<number>, selSet: RxSet<number>, idxSet: RxSet<number>) {
    return {
        mapped: source.map(mapFn),
        mappedIdx: source.map((x, i) => `${i.raw}:${x ?? 'u'}`),
        filtered: source.filter(filterPred),
        sorted: source.toSorted(holeCompare),
        sliced: source.slice(1, 5),
        merged: source.concat(other),
        grouped: source.groupBy(groupKey),
        byKey: source.indexBy(x => x),
        asSet: source.toSet(),
        fi: source.findIndex(findPred),
        len: source.length,
        total: source.reduceToAtom(reduceFn, 0),
        selv: createSelection(source, selSet),
        seli: createIndexKeySelection(source, idxSet),
    }
}
type Family = ReturnType<typeof buildFamily>

// 各算子 computation 语义的参考实现(洞位语义逐算子成文,这就是"洞的物化语义"清单)
const refs = {
    mapped(src: readonly (number | undefined)[]) { // map:跳洞(hasOwnProperty)
        const out = new Array(src.length)
        for (let i = 0; i < src.length; i++) {
            if (Object.prototype.hasOwnProperty.call(src, i)) out[i] = mapFn(src[i])
        }
        return densify(out)
    },
    filtered: (src: readonly (number | undefined)[]) => src.filter(filterPred), // native filter 跳洞
    sorted: (src: readonly (number | undefined)[]) => densify(src.slice().sort(holeCompare)), // sort 洞排尾
    sliced: (src: readonly (number | undefined)[]) => densify(src.slice(1, 5)),
    merged(src: readonly (number | undefined)[], other: readonly number[]) { // for..of 物化洞
        const out: (number | undefined)[] = []
        for (const x of src) out.push(x)
        for (const x of other) out.push(x)
        return out
    },
    grouped(src: readonly (number | undefined)[]) { // groupBy:全下标扫描,物化洞
        const m = new Map<any, (number | undefined)[]>()
        for (let i = 0; i < src.length; i++) {
            const k = groupKey(src[i])
            if (!m.has(k)) m.set(k, [])
            m.get(k)!.push(src[i])
        }
        return m
    },
    byKey(src: readonly (number | undefined)[]) { // indexBy:null/undefined 行跳过
        const m = new Map<any, number>()
        for (let i = 0; i < src.length; i++) {
            const item = src[i]
            if (item == null) continue
            m.set(item, item)
        }
        return m
    },
    asSet: (src: readonly (number | undefined)[]) => new Set(src), // Set 构造物化洞
    fi(src: readonly (number | undefined)[]) {
        for (let i = 0; i < src.length; i++) if (findPred(src[i])) return i
        return -1
    },
    total: (src: readonly (number | undefined)[]) => src.reduce(reduceFn, 0), // native reduce 跳洞
    selv: (src: readonly (number | undefined)[], sel: Set<number>) =>
        densify(src.map(item => [item, sel.has(item as number)])), // map 跳洞
    seli: (src: readonly (number | undefined)[], sel: Set<number>) =>
        densify(src.map((item, i) => [item, sel.has(i)])),
}

function expectFamilyConverged(f: Family, source: RxList<number>, other: RxList<number>, selSet: RxSet<number>, idxSet: RxSet<number>, ctx: string) {
    const src = source.data
    expect(densify(f.mapped.data), `mapped ${ctx}`).toEqual(refs.mapped(src))
    expect(densify(f.filtered.data), `filtered ${ctx}`).toEqual(refs.filtered(src))
    expect(densify(f.sorted.data), `sorted ${ctx}`).toEqual(refs.sorted(src))
    expect(densify(f.sliced.data), `sliced ${ctx}`).toEqual(refs.sliced(src))
    expect(densify(f.merged.data), `merged ${ctx}`).toEqual(refs.merged(src, other.data))
    const gRef = refs.grouped(src)
    expect(sortKeysCanonical(f.grouped.data.keys()), `grouped keys ${ctx}`).toEqual(sortKeysCanonical(gRef.keys()))
    for (const [k, items] of gRef) {
        expect(densify(f.grouped.data.get(k)!.data), `grouped[${String(k)}] ${ctx}`).toEqual(densify(items))
    }
    const bRef = refs.byKey(src)
    expect(sortKeysCanonical(f.byKey.data.keys()), `byKey keys ${ctx}`).toEqual(sortKeysCanonical(bRef.keys()))
    expect([...f.asSet.data].sort(holeCompare), `asSet ${ctx}`).toEqual([...refs.asSet(src)].sort(holeCompare))
    expect(f.fi.raw, `findIndex ${ctx}`).toBe(refs.fi(src))
    expect(f.total.raw, `reduceToAtom ${ctx}`).toBe(refs.total(src))
    expect(densify(f.selv.data.map(r => r && [r[0], r[1].raw])), `selv ${ctx}`)
        .toEqual(refs.selv(src, new Set(selSet.data)))
    expect(densify(f.seli.data.map(r => r && [r[0], r[1].raw])), `seli ${ctx}`)
        .toEqual(refs.seli(src, new Set(idxSet.data)))
}

function forceFullRecompute(f: Family) {
    f.mapped.recompute(true)
    f.mappedIdx.recompute(true)
    // filtered 是源模式外观(无 getter),无法 force——它的收敛由逐步维护保证,
    // 直接参与 T2 比较(若分叉即为真实缺陷,不豁免)。
    f.sorted.recompute(true)
    f.sliced.recompute(true)
    f.merged.recompute(true)
    f.grouped.recompute(true)
    f.byKey.recompute(true)
    f.asSet.recompute(true)
    recompute(f.fi, true)
    recompute(f.len, true)
    recompute(f.total, true)
    f.selv.recompute(true)
    f.seli.recompute(true)
}

function destroyFamily(f: Family) {
    f.selv.destroy(); f.seli.destroy()
    f.mapped.destroy(); f.mappedIdx.destroy(); f.filtered.destroy(); f.sorted.destroy()
    f.sliced.destroy(); f.merged.destroy(); f.grouped.destroy(); f.byKey.destroy(); f.asSet.destroy()
}

describe('形态操作差分 fuzz:契约外 OOB/负/小数 set × 结构操作 × batch 的组合空间', () => {
    const SEEDS = 24
    const STEPS = 18

    for (let seed = 1; seed <= SEEDS; seed++) {
        test(`seed=${seed}: T1 无崩溃 / T2 强制收敛 / T3 恢复性`, () => {
            const rand = mulberry32(seed * 7919)
            const nextValue = uniqueInts(seed * 1000)
            const source = new RxList<number>([nextValue(), nextValue(), nextValue(), nextValue(), nextValue(), nextValue()])
            const other = new RxList<number>([nextValue(), nextValue()])
            const selSet = new RxSet<number>([source.data[1]])
            const idxSet = new RxSet<number>([0, 2])
            const f = buildFamily(source, other, selSet, idxSet)
            const history: string[] = []

            try {
                for (let step = 0; step < STEPS; step++) {
                    const r = rand()
                    if (r < 0.08) {
                        // 侧源/选中集操作:concat 的 other 段、selection 的 currentValues
                        const which = rand()
                        if (which < 0.4) { const v = nextValue(); other.push(v); history.push(`other.push(${v})`) }
                        else if (which < 0.6 && other.data.length) { other.pop(); history.push('other.pop()') }
                        else if (which < 0.8) { const v = source.data[Math.floor(rand() * Math.max(source.data.length, 1))]; if (v !== undefined) { selSet.add(v); history.push(`selSet.add(${v})`) } }
                        else { const i = Math.floor(rand() * (source.data.length + 2)); idxSet.data.has(i) ? idxSet.delete(i) : idxSet.add(i); history.push(`idxSet.toggle(${i})`) }
                    } else if (r < 0.38) {
                        // batch 分组:2-3 个操作单次 digest 重放
                        const k = 2 + Math.floor(rand() * 2)
                        const ops: string[] = []
                        batch(() => {
                            for (let i = 0; i < k; i++) ops.push(performRandomListOp(rand, source, nextValue))
                        })
                        history.push(`batch{${ops.join('; ')}}`)
                    } else {
                        history.push(performRandomListOp(rand, source, nextValue))
                    }
                }

                // T2:强制全量重算后必须与各算子 computation 语义的参考实现一致
                forceFullRecompute(f)
                expectFamilyConverged(f, source, other, selSet, idxSet, `after force (seed=${seed})`)
                expect(f.len.raw, `len after force (seed=${seed})`).toBe(source.data.length)

                // T3:强制重算后内部记账必须仍支持增量(稀疏 + 重建后的一步契约内操作)
                const v = nextValue()
                source.push(v)
                history.push(`push(${v}) [recovery]`)
                expectFamilyConverged(f, source, other, selSet, idxSet, `after recovery push (seed=${seed})`)
                expect(f.len.raw, `len after recovery (seed=${seed})`).toBe(source.data.length)
            } catch (e) {
                // 失败时附带 seed 与操作史(可复现)
                throw new Error(`sparseOpsFuzz seed=${seed} 失败\n操作史:\n  ${history.join('\n  ')}\n原始错误: ${(e as Error).stack ?? e}`)
            } finally {
                destroyFamily(f)
                source.destroy(); other.destroy(); selSet.destroy(); idxSet.destroy()
            }
        })
    }
})
