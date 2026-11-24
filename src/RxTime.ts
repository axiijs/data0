import {Atom, isAtom, atom} from "./atom.js";
import {autorun} from "./common";
import {ReactiveEffect} from "./reactiveEffect.js";
import {assert} from "./util.js";

// CAUTION value 的合法域与 simplifying 的实现严格对齐:add/sub 支持 RxTime 嵌套,
//  mul/div 只支持标量/atom(旧类型里的 Operation[] 成员从无产出方,已删除)。
type Operation = {
    type: 'add' | 'sub'
    value: number | Atom<number> | RxTime
} | {
    type: 'mul' | 'div'
    value: number | Atom<number>
}
// setTimeout 的延时上限（32 位有符号整数，约 24.8 天）。超过它 Node 打
// TimeoutOverflowWarning 并改成 1ms、浏览器按 0 处理——都是立即触发的虚假唤醒。
const MAX_TIMEOUT = 2 ** 31 - 1
// 唤醒时 Date.now() 还停在排定时的刻度上（粗精度时钟）时的退避续排：延时 2^n ms、上限 1s，
// 连续 16 次（累计约 8s）仍在同一刻度才放弃——那只能是被冻结的时钟（见 resolve 的说明第 4 条）。
const MAX_STALLS = 16
const MAX_STALL_DELAY = 1000
/**
 * @category Basic
 */
export class RxTime {
    public operations: Operation[] = []
    public interval?: number
    public timeoutId: any = null
    add(value: number|RxTime|Atom<number>): RxTime {
        assert(!this.data, 'RxTime can not be modified after resolved')
        this.operations.push({type: 'add', value})
        return this
    }
    sub(value: number|Atom<number>|RxTime): RxTime {
        assert(!this.data, 'RxTime can not be modified after resolved')
        this.operations.push({type: 'sub', value})
        return this
    }
    mul(value: number|Atom<number>): RxTime {
        assert(!this.data, 'RxTime can not be modified after resolved')
        this.operations.push({type: 'mul', value})
        return this
    }
    div(value: number|Atom<number>): RxTime {
        assert(!this.data, 'RxTime can not be modified after resolved')
        this.operations.push({type: 'div', value})
        return this
    }
    // 整理
    simplifying() {
        let coefficient = 1
        let constant = 0
        for(let operation of this.operations) {
            switch(operation.type) {
                case 'add':
                    if (typeof operation.value === 'number') {
                        constant += operation.value
                    } else if(isAtom(operation.value)) {
                        constant += operation.value()
                    } else {
                        const other = operation.value as RxTime
                        const [targetCoefficient, targetConstant] = other.simplifying()
                        coefficient += targetCoefficient
                        constant += targetConstant
                    }
                    break
                case 'sub':
                    if (typeof operation.value === 'number') {
                        constant -= operation.value
                    } else if(isAtom(operation.value)) {
                        constant -= operation.value()
                    } else {
                        const other = operation.value as RxTime
                        const [targetCoefficient, targetConstant] = other.simplifying()
                        coefficient -= targetCoefficient
                        constant -= targetConstant
                    }
                    break
                case 'mul':
                    if (typeof operation.value === 'number') {
                        coefficient *= operation.value
                        constant *= operation.value
                    } else if(isAtom(operation.value)) {
                        coefficient *= operation.value()
                        constant *= operation.value()
                    }
                    break
                case 'div':
                    if (typeof operation.value === 'number') {
                        coefficient /= operation.value
                        constant /= operation.value
                    } else if(isAtom(operation.value)) {
                        coefficient /= operation.value()
                        constant /= operation.value()
                    }
                    break
            }
        }
        return [coefficient, constant]
    }
    public data?: Atom<any>
    // CAUTION 多入口清理:resolve 与 subscribe 都会注册常驻副作用(autorun/interval)。
    //  旧实现用单个 stopAutorun 字段,后注册的覆盖先注册的——先 resolve 再 subscribe
    //  时 resolve 的 autorun 永久泄漏。现在所有清理进 disposers,stopAutorun 保留为
    //  "停止全部"的公开入口(语义收严,不再只停最后一个)。
    private disposers: Array<() => void> = []
    public stopAutorun?: () => void
    private addDisposer(dispose: () => void) {
        this.disposers.push(dispose)
        this.stopAutorun = () => this.disposeAll()
    }
    private disposeAll() {
        if (this.timeoutId) {
            clearTimeout(this.timeoutId)
            this.timeoutId = null
        }
        const disposers = this.disposers
        this.disposers = []
        disposers.forEach(dispose => dispose())
    }
    resolve(compare: (v:number) => boolean): Atom<boolean> {
        const result = atom(false)
        // 本次 resolve 是否已被清理（destroy/stopAutorun）、以及求值代次：唤醒回调写 result 时
        // 订阅者同步执行，可能销毁本实例或改写参数 atom（autorun 立即重算并排定新的唤醒）——
        // 回调随后的续排必须能看见这两件事。
        let disposed = false
        let generation = 0
        // CAUTION 唤醒的排定与续排（任一时刻至多一个 timer，清理后为零）：
        //  1. `>=` 而不是 `>`：恰在越点时刻求值（deadline(Date.now()) 让它"立刻过期"）时
        //     表达式值还是 0，gt 要到下一毫秒才成立——不排唤醒就永远停在 false。
        //  2. 延时钳到 MAX_TIMEOUT：更远的越点会被平台改成立即触发，唤醒时 compare 仍是
        //     旧结果，之后再无唤醒，到点永不翻转。
        //  3. 唤醒时时钟还没越过越点（延时被钳、系统时钟被回拨）就按剩余时间续排——旧实现
        //     只唤醒一次，没越点也不再管。
        //  4. 唤醒时 Date.now() 与排定时相同（firedAt === armedAt）不等于「时钟冻结」：
        //     Date.now() 的精度可能粗于 +2ms（精度被刻意降低的运行环境、粗粒度的平台时钟），
        //     唤醒落在同一个刻度里，曾因此直接放弃续排——恰在期限求值的 gt(deadline) 又永远
        //     是 false（上面第 1 条要修的症状）。改为有界退避续排：延时 2、4、8…ms（上限
        //     1s），连续 MAX_STALLS 次读到同一刻度才放弃。真冻结的时钟（只 fake 了 timer 没
        //     fake Date）因此至多多排 MAX_STALLS 个 timer 就停，runAllTimers 不会死循环。
        const arm = (coefficient: number, constant: number, armedAt: number, armedGeneration: number, stalls = 0) => {
            // 如果还有 timeout，说明没到计算时间，计算中的参数变化了引发的重新计算，先清空
            if (this.timeoutId) {
                clearTimeout(this.timeoutId)
                this.timeoutId = null
            }
            // CAUTION isFinite 守卫：系数相消（t1.gt(t2) 这类 RxTime 差,coefficient
            //  为 0）时 -constant/0 是 ±Infinity——表达式值与时间无关,不存在"下次
            //  变化时刻"。不加守卫会 setTimeout(Infinity)：Node 打
            //  TimeoutOverflowWarning 并 clamp 成 1ms 的虚假唤醒,浏览器 clamp 成 0。
            const nextChangeTimestamp = - constant / coefficient
            if (!Number.isFinite(nextChangeTimestamp) || nextChangeTimestamp < armedAt) return
            // CAUTION 这里的 +2 非常重要，因为系数常数可能有浮点，或者 Date.now() 算出来的时候可能并没有达到真正的变化时间。
            //  所以这里使用 +2 来保证在是真正已经产生变化了。
            const backoff = stalls === 0 ? 0 : Math.min(2 ** stalls, MAX_STALL_DELAY)
            const timeoutTime = Math.min(Math.max(nextChangeTimestamp - armedAt + 2, backoff), MAX_TIMEOUT)
            const timeoutId = this.timeoutId = setTimeout(() => {
                // timeoutId 非空 ⇔ 有一个尚未触发的 timer
                if (this.timeoutId === timeoutId) this.timeoutId = null
                const firedAt = Date.now()
                // CAUTION 这里是可以复用计算结果的，因为如果计算中有 atom 变化了，那么 autorun 整个都会重算，不会走到这里。
                //  走到这里说明没有 atom 变化。
                result(compare(firedAt*coefficient + constant))
                if (disposed || generation !== armedGeneration) return
                if (firedAt <= nextChangeTimestamp) {
                    if (firedAt !== armedAt) {
                        arm(coefficient, constant, firedAt, armedGeneration)
                    } else if (stalls < MAX_STALLS) {
                        arm(coefficient, constant, armedAt, armedGeneration, stalls + 1)
                    }
                }
            }, timeoutTime)
        }
        // CAUTION createDetached(2026-H3 round6,R6-2 等价类):resolve 的结果缓存在
        //  实例(this.data)、清理归实例(disposers/destroy)。内部 autorun 若被创建
        //  时所在的 autorun/computed 收集为 child,宿主一重算它就被销毁——data 从此
        //  不再随时间推进,而实例的 disposers 还持着已死引用(半销毁脑裂)。
        //  生命周期归属实例的内部 effect 必须与创建作用域隔离(同 AsyncRxSlice.fetch)。
        const stop = ReactiveEffect.createDetached(() => autorun(() => {
            const armedGeneration = ++generation
            const currentTimestamp = Date.now()
            const [coefficient, constant] = this.simplifying()
            // CAUTION 先排唤醒、后写结果：写 result 会同步执行订阅者，订阅者里 destroy 本实例
            //  （"一旦变回 false 就不再监听"）时 disposeAll 必须已经能看到并清掉这个 timer。
            //  旧顺序（先写后排）下销毁发生在排定之前，求值的后半段给已销毁的实例留下一个
            //  无人持有的 timer，到点还会翻转它的 result。
            arm(coefficient, constant, currentTimestamp, armedGeneration)
            // 立刻计算结果
            result(compare(currentTimestamp*coefficient + constant))
        },true))
        this.addDisposer(() => {
            disposed = true
            stop()
        })

        this.data = result

        return result
    }
    gt(value: number|Atom<number>|RxTime): Atom<boolean> {
        this.sub(value)
        return this.resolve((v) => v > 0)
    }
    lt(value: number|Atom<number>|RxTime): Atom<boolean> {
        this.sub(value)
        return this.resolve((v) => v < 0)

    }
    eq(value: number|Atom<number>|RxTime): Atom<boolean> {
        this.sub(value)
        return this.resolve((v) => v === 0)
    }
    subscribe(interval: number) {
        this.interval = interval
        const data = atom(Date.now())
        const intervalId = setInterval(() => {
            data(Date.now())
        }, interval)
        this.addDisposer(() => clearInterval(intervalId))

        return data
    }
    destroy(): void {
        this.disposeAll()
    }
}
