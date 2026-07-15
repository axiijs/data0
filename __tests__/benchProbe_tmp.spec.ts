// 临时 ABBA 微基准(不入库):D1 防御拷贝的热路径成本
import {test} from 'vitest'
import {RxList} from '../src/RxList.js'

function median(xs: number[]) {
    const s = [...xs].sort((a, b) => a - b)
    return s[Math.floor(s.length / 2)]
}

function timeIt(fn: () => void, rounds = 5): number {
    const times: number[] = []
    for (let i = 0; i < rounds; i++) {
        const t0 = performance.now()
        fn()
        times.push(performance.now() - t0)
    }
    return median(times)
}

test('bench probe', () => {
    // 预热
    {
        const w = new RxList<number>(Array.from({length: 100}, (_, i) => i))
        const wm = w.map(x => x + 1)
        for (let i = 0; i < 2000; i++) w.push(i)
        wm.destroy(); w.destroy()
    }

    // col1: push 热路径(纯尾插,期望零变化——冻结空载荷)
    const push = timeIt(() => {
        const list = new RxList<number>([])
        const mapped = list.map(x => x * 2)
        for (let i = 0; i < 100_000; i++) list.push(i)
        mapped.destroy(); list.destroy()
    })

    // col2: 单元素删除 splice(载荷副本 = 1 元素)
    const spliceOne = timeIt(() => {
        const list = new RxList<number>(Array.from({length: 2000}, (_, i) => i))
        const mapped = list.map(x => x * 2)
        for (let i = 0; i < 30_000; i++) {
            list.splice(i % 1500, 1, i)
        }
        mapped.destroy(); list.destroy()
    })

    // col3: replaceData 20k(载荷副本 = 20k 数组)
    const replaceBig = timeIt(() => {
        const list = new RxList<number>(Array.from({length: 20_000}, (_, i) => i))
        const mapped = list.map(x => x * 2)
        for (let i = 0; i < 10; i++) {
            list.replaceData(Array.from({length: 20_000}, (_, j) => j + i))
        }
        mapped.destroy(); list.destroy()
    })

    // col4: swap 重载(reorder argv 副本)
    const swapHeavy = timeIt(() => {
        const list = new RxList<number>(Array.from({length: 50}, (_, i) => i))
        const mapped = list.map(x => x * 2)
        for (let i = 0; i < 20_000; i++) list.swap(i % 25, 25 + (i % 25))
        mapped.destroy(); list.destroy()
    })

    console.log(JSON.stringify({push, spliceOne, replaceBig, swapHeavy}))
})
