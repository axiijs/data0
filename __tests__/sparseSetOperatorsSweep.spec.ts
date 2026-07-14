/**
 * 合并前等价类横扫：OOB set 之后，各派生结构不得抛错，且后续契约内操作可恢复。
 * 只断言"无崩溃 + 后续 push 仍工作"，不断言稀疏语义细节（契约外）。
 *
 * 2026-H3 round3 扩展(方法 19 教训):本 sweep 原来只走"稀疏 × 纯尾插"——尾插
 * 不进任何按区间遍历的校正/搬移路径,createIndexKeySelection 的 index 校正循环
 * 撞洞崩溃因此存活。追加第二横扫:稀疏之后走**不等长 splice(头删) + swap(reorder)**,
 * 这两类操作会驱动派生结构的平移校正区/affectedRange 区间遍历跨过洞位。
 */
import {describe, expect, test} from 'vitest'
import {atom, batch, RxList, RxSet} from '../src'

function makeSparse(list: RxList<number>) {
  list.set(6, 99)
}

describe('OOB set × derived structures: no crash, recoverable', () => {
  const build = () => new RxList([1, 2, 3])

  test('map(index)', () => {
    const l = build(); const d = l.map((x, i) => `${i()}:${x}`)
    expect(() => makeSparse(l)).not.toThrow()
    expect(() => l.push(7)).not.toThrow()
    expect(d.data[7]).toBe('7:7')
    l.destroy(); d.destroy()
  })
  test('map(no index)', () => {
    const l = build(); const d = l.map(x => x * 2)
    expect(() => makeSparse(l)).not.toThrow()
    expect(() => l.push(7)).not.toThrow()
    expect(d.data[7]).toBe(14)
    l.destroy(); d.destroy()
  })
  test('filter', () => {
    const l = build(); const d = l.filter(x => (x ?? 0) % 2 === 1)
    expect(() => makeSparse(l)).not.toThrow()
    expect(() => l.push(7)).not.toThrow()
    expect(d.data).toContain(7)
    l.destroy(); d.destroy()
  })
  test('toSorted', () => {
    const l = build(); const d = l.toSorted((a, b) => (a ?? 0) - (b ?? 0))
    expect(() => makeSparse(l)).not.toThrow()
    expect(() => l.push(7)).not.toThrow()
    l.destroy(); d.destroy()
  })
  test('slice', () => {
    const l = build(); const d = l.slice(0, 5)
    expect(() => makeSparse(l)).not.toThrow()
    expect(() => l.push(7)).not.toThrow()
    l.destroy(); d.destroy()
  })
  test('concat', () => {
    const l = build(); const other = new RxList([100]); const d = l.concat(other)
    expect(() => makeSparse(l)).not.toThrow()
    expect(() => l.push(7)).not.toThrow()
    l.destroy(); other.destroy(); d.destroy()
  })
  test('groupBy', () => {
    const l = build(); const d = l.groupBy(x => (x ?? 0) % 2)
    expect(() => makeSparse(l)).not.toThrow()
    expect(() => l.push(7)).not.toThrow()
    l.destroy(); d.destroy()
  })
  test('indexBy', () => {
    const l = build(); const d = l.indexBy(x => x)
    expect(() => makeSparse(l)).not.toThrow()
    expect(() => l.push(7)).not.toThrow()
    l.destroy(); d.destroy()
  })
  test('toSet', () => {
    const l = build(); const d = l.toSet()
    expect(() => makeSparse(l)).not.toThrow()
    expect(() => l.push(7)).not.toThrow()
    l.destroy(); d.destroy()
  })
  test('reduce (toAtom)', () => {
    const l = build(); const total = l.reduceToAtom((acc, x) => acc + (x ?? 0), 0)
    expect(() => makeSparse(l)).not.toThrow()
    expect(() => l.push(7)).not.toThrow()
    expect(typeof total.raw).toBe('number')
    l.destroy()
  })
  test('find / findIndex', () => {
    const l = build()
    const found = l.find(x => x === 7)
    const fi = l.findIndex(x => x === 7)
    expect(() => makeSparse(l)).not.toThrow()
    expect(() => l.push(7)).not.toThrow()
    expect(fi.raw).toBe(l.data.indexOf(7))
    expect(found()).toBe(7)
    l.destroy()
  })
  test('length', () => {
    const l = build(); const len = l.length
    expect(() => makeSparse(l)).not.toThrow()
    expect(() => l.push(7)).not.toThrow()
    expect(len.raw).toBe(l.data.length)
    l.destroy()
  })
  test('createSelection (value)', () => {
    const l = build(); const sel = new RxSet<number>([])
    const d = l.createSelection(sel)
    expect(() => makeSparse(l)).not.toThrow()
    expect(() => l.push(7)).not.toThrow()
    l.destroy(); d.destroy(); sel.destroy()
  })
  test('createIndexKeySelection', () => {
    const l = build(); const sel = new RxSet<number>([1])
    const d = l.createIndexKeySelection(sel)
    expect(() => makeSparse(l)).not.toThrow()
    expect(() => l.push(7)).not.toThrow()
    l.destroy(); d.destroy(); sel.destroy()
  })
  test('batch: OOB set + push single digest (map index multi-info fallback)', () => {
    const l = build(); const d = l.map((x, i) => `${i()}:${x}`)
    expect(() => batch(() => { l.set(6, 99); l.push(7) })).not.toThrow()
    expect(d.data[7]).toBe('7:7')
    expect(d.data[6]).toBe('6:99')
    l.destroy(); d.destroy()
  })
})

// 校正/搬移路径横扫:洞位行 × 不等长 splice(区间平移) + swap(affectedRange 遍历)。
// 等价类:凡按 index 区间逐行校正/搬移的派生结构,循环体必须容忍洞位行
// (R3-1 缺陷类;map/filter/createSelections 既有 ?. 守卫,createIndexKeySelection 曾缺失)。
describe('OOB set × correction paths: uneven splice + reorder, no crash, recoverable', () => {
  const build = () => new RxList([1, 2, 3])
  const exercise = (l: RxList<number>) => {
    makeSparse(l)
    l.splice(0, 1)      // 不等长:后续行整体平移,校正区跨洞
    l.swap(0, l.data.length - 1) // reorder:affectedRange 遍历跨洞
    l.push(7)           // 可恢复
  }

  test('createIndexKeySelection (RxSet)', () => {
    const l = build(); const sel = new RxSet<number>([1])
    const d = l.createIndexKeySelection(sel)
    expect(() => exercise(l)).not.toThrow()
    l.destroy(); d.destroy(); sel.destroy()
  })
  test('createIndexKeySelection (Atom)', () => {
    const l = build(); const sel = atom<number|null>(1)
    const d = l.createIndexKeySelection(sel)
    expect(() => exercise(l)).not.toThrow()
    l.destroy(); d.destroy()
  })
  test('createSelection (value)', () => {
    const l = build(); const sel = new RxSet<number>([2])
    const d = l.createSelection(sel)
    expect(() => exercise(l)).not.toThrow()
    l.destroy(); d.destroy(); sel.destroy()
  })
  test('createSelections (multi inner)', () => {
    const l = build(); const s1 = new RxSet<number>([1]); const s2 = atom<number|null>(2)
    const d = l.createSelections([s1], [s2])
    expect(() => exercise(l)).not.toThrow()
    l.destroy(); d.destroy(); s1.destroy()
  })
  test('map(index)', () => {
    const l = build(); const d = l.map((x, i) => `${i()}:${x}`)
    expect(() => exercise(l)).not.toThrow()
    l.destroy(); d.destroy()
  })
  test('filter', () => {
    const l = build(); const d = l.filter(x => (x ?? 0) % 2 === 1)
    expect(() => exercise(l)).not.toThrow()
    l.destroy(); d.destroy()
  })
  test('slice', () => {
    const l = build(); const d = l.slice(0, 5)
    expect(() => exercise(l)).not.toThrow()
    l.destroy(); d.destroy()
  })
  test('groupBy', () => {
    const l = build(); const d = l.groupBy(x => (x ?? 0) % 2)
    expect(() => exercise(l)).not.toThrow()
    l.destroy(); d.destroy()
  })
  test('toSorted', () => {
    const l = build(); const d = l.toSorted((a, b) => (a ?? 0) - (b ?? 0))
    expect(() => exercise(l)).not.toThrow()
    l.destroy(); d.destroy()
  })
  test('concat(前段稀疏,后续段不错位)', () => {
    const l = build(); const other = new RxList([100]); const d = l.concat(other)
    expect(() => exercise(l)).not.toThrow()
    // R3-2 回归:任何时刻增量结果 ≡ 全量重算(段结构不可错乱)
    const full: number[] = []
    for (const item of l.data) full.push(item)
    full.push(100)
    expect([...d.data]).toEqual(full)
    l.destroy(); other.destroy(); d.destroy()
  })
  test('findIndex', () => {
    const l = build(); const fi = l.findIndex(x => x === 7)
    expect(() => exercise(l)).not.toThrow()
    expect(fi.raw).toBe(l.data.indexOf(7))
    l.destroy()
  })
})
