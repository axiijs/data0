import {describe, expect, test} from 'vitest'
import {AsyncRxSlice} from '../src/AsyncRxSlice.js'
import {LinkedList} from '../src/LinkedList.js'
import {autorun} from '../src/common.js'

/**
 * 外围模块 2026-H2 燃尽:AsyncRxSlice 此前 1 条测试(receipt 交错/错误路径/销毁
 * 全部未覆盖),LinkedList 1 条(removeBetween 记账/迭代追踪未覆盖)。
 */

type Deferred<T> = {promise: Promise<T>, resolve: (v: T) => void, reject: (e: any) => void}
function deferred<T>(): Deferred<T> {
    let resolve!: (v: T) => void
    let reject!: (e: any) => void
    const promise = new Promise<T>((res, rej) => { resolve = res; reject = rej })
    return {promise, resolve, reject}
}
const tick = () => new Promise<void>(r => setTimeout(r, 0))

describe('AsyncRxSlice: fetchReceipt 交错与错误路径', () => {
    test('旧请求晚到:结果被 receipt 丢弃,终态为新请求', async () => {
        const pending: Deferred<number[]>[] = []
        const slice = new AsyncRxSlice<number>([], () => {
            const d = deferred<number[]>()
            pending.push(d)
            return d.promise
        })
        const p1 = slice.fetch()          // 请求 1
        slice.fetchFullRemoteData()       // 请求 2(推进 receipt)
        expect(pending.length).toBe(2)

        pending[1].resolve([7, 8])        // 新请求先完成
        await tick()
        expect(slice.data).toEqual([7, 8])
        expect(slice.isLoading.raw).toBe(false)

        pending[0].resolve([1, 2])        // 旧请求晚到:必须被丢弃
        await tick()
        expect(slice.data).toEqual([7, 8])
        expect(slice.isLoading.raw).toBe(false)
        await p1
        slice.destroy()
    })

    test('fetch 失败:loadError 置位、isLoading 复位;下次成功后 loadError 清空', async () => {
        const pending: Deferred<number[]>[] = []
        const slice = new AsyncRxSlice<number>([], () => {
            const d = deferred<number[]>()
            pending.push(d)
            return d.promise
        })
        const p = slice.fetchFullRemoteData()
        pending[0].reject(new Error('net down'))
        await p
        expect(slice.loadError.raw?.message).toBe('net down')
        expect(slice.isLoading.raw).toBe(false)
        expect(slice.data).toEqual([])

        const p2 = slice.fetchFullRemoteData()
        pending[1].resolve([1])
        await p2
        expect(slice.loadError.raw).toBe(null)
        expect(slice.data).toEqual([1])
        slice.destroy()
    })

    test('update 家族:append/prepend/moveForward/moveBackward 的 cursor 与拼接方向', async () => {
        const calls: any[][] = []
        const slice = new AsyncRxSlice<{id: number}>(
            [{id: 10}, {id: 11}],
            (...args: any[]) => {
                calls.push(args)
                const fetchBeforeCursor = args[3]
                return Promise.resolve(fetchBeforeCursor ? [{id: 9}] : [{id: 12}])
            },
            item => item?.id,
        )
        await slice.append(1)
        expect(calls[0][0]).toBe(11) // 尾 cursor
        expect(slice.data.map(x => x.id)).toEqual([10, 11, 12])

        await slice.prepend(1)
        expect(calls[1][0]).toBe(10) // 头 cursor
        expect(calls[1][3]).toBe(true)
        expect(slice.data.map(x => x.id)).toEqual([9, 10, 11, 12])

        await slice.moveForward(2)
        expect(slice.data.map(x => x.id)).toEqual([12]) // replace 模式
        slice.destroy()
    })

    test('update 抛错(reject)后 receipt 守卫:loading 不被旧请求错误复位', async () => {
        const pending: Deferred<number[]>[] = []
        const slice = new AsyncRxSlice<number>([1], () => {
            const d = deferred<number[]>()
            pending.push(d)
            return d.promise
        }, x => x)
        const u1 = slice.update(1, 1)      // 请求 1
        const u2 = slice.update(1, 1)      // 请求 2(推进 receipt)
        pending[0].reject(new Error('old boom')) // 旧请求失败:全部忽略
        await u1
        expect(slice.isLoading.raw).toBe(true)   // 新请求仍在途
        expect(slice.loadError.raw).toBe(null)
        pending[1].resolve([2])
        await u2
        expect(slice.isLoading.raw).toBe(false)
        expect(slice.data).toEqual([1, 2])
        slice.destroy()
    })

    test('destroy 释放惰性 autoFetchPromise computed(幂等)', async () => {
        const slice = new AsyncRxSlice<number>([], () => Promise.resolve([1]))
        await slice.fetch()
        expect(slice.data).toEqual([1])
        slice.destroy()
        slice.destroy()
        // 销毁后变更是 no-op(继承 RxList 防线)
        slice.push(9)
        expect(slice.data).toEqual([1])
    })
})

describe('LinkedList: 结构操作、记账与响应式迭代', () => {
    const items = () => [{id: 1}, {id: 2}, {id: 3}]

    test('insertBefore:头/中/尾(缺省 ref)三形态与链完整性', () => {
        const [a, b, c] = items()
        const list = new LinkedList<{id: number}>([a, c])
        list.insertBefore(b, list.getNodeByItem(c))            // 中
        const head = {id: 0}
        list.insertBefore(head, list.getNodeByItem(a))         // 头
        const tail = {id: 4}
        list.insertBefore(tail)                                 // 尾(缺省)
        expect(list.map(n => n.item.id)).toEqual([0, 1, 2, 3, 4])
        expect(list.head!.item).toBe(head)
        expect(list.tail!.item).toBe(tail)
        // 双向指针完整性
        let cur = list.head
        let prev: any = undefined
        while (cur) { expect(cur.prev).toBe(prev); prev = cur; cur = cur.next }
        expect(list.tail).toBe(prev)
    })

    test('removeBetween:部分区间/全量,itemToNode 记账同步清理', () => {
        const [a, b, c] = items()
        const list = new LinkedList<{id: number}>([a, b, c])
        list.removeBetween(list.getNodeByItem(b), list.getNodeByItem(b))
        expect(list.map(n => n.item.id)).toEqual([1, 3])
        expect(list.getNodeByItem(b)).toBe(undefined) // 记账已清
        expect(list.getNodeByItem(a)!.next).toBe(list.getNodeByItem(c))

        list.removeBetween() // 全量(缺省 head..tail)
        expect(list.map(n => n.item.id)).toEqual([])
        expect(list.head).toBe(undefined)
        expect(list.tail).toBe(undefined)
        expect(list.getNodeByItem(a)).toBe(undefined)
    })

    test('at:正向定位与 -1 取尾', () => {
        const [a, b, c] = items()
        const list = new LinkedList<{id: number}>([a, b, c])
        expect(list.at(0)!.item).toBe(a)
        expect(list.at(2)!.item).toBe(c)
        expect(list.at(-1)!.item).toBe(c)
        expect(list.at(5)).toBe(undefined)
    })

    test('迭代建立 ITERATE 依赖:insertBefore/removeBetween 触发重跑', () => {
        const [a, b] = items()
        const list = new LinkedList<{id: number}>([a])
        const snapshots: number[][] = []
        const stop = autorun(() => {
            snapshots.push(list.map(n => n.item.id))
        }, true)
        expect(snapshots).toEqual([[1]])
        list.insertBefore(b)
        expect(snapshots.length).toBe(2)
        expect(snapshots[1]).toEqual([1, 2])
        list.removeBetween(list.getNodeByItem(a), list.getNodeByItem(a))
        expect(snapshots.length).toBe(3)
        expect(snapshots[2]).toEqual([2])
        stop()
    })
})
