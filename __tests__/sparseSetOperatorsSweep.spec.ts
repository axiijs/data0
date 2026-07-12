/**
 * 合并前等价类横扫：OOB set 之后，各派生结构不得抛错，且后续契约内操作可恢复。
 * 只断言"无崩溃 + 后续 push 仍工作"，不断言稀疏语义细节（契约外）。
 */
import {describe, expect, test} from 'vitest'
import {batch, RxList, RxSet} from '../src'

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
