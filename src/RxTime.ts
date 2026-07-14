import {Atom, isAtom, atom} from "./atom.js";
import {autorun} from "./common";
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
         const stop = autorun(() => {
             // 立刻计算结果
             const currentTimestamp = Date.now()
             const [coefficient, constant] = this.simplifying()
             result(compare(currentTimestamp*coefficient + constant))

             // 如果还有 timeout，说明没到计算时间，计算中的参数变化了引发的重新计算，先清空
             if (this.timeoutId) {
                 clearTimeout(this.timeoutId)
                 this.timeoutId = null
             }
             // 下次变化的时候重新计算
             // CAUTION isFinite 守卫：系数相消（t1.gt(t2) 这类 RxTime 差,coefficient
             //  为 0）时 -constant/0 是 ±Infinity——表达式值与时间无关,不存在"下次
             //  变化时刻"。不加守卫会 setTimeout(Infinity)：Node 打
             //  TimeoutOverflowWarning 并 clamp 成 1ms 的虚假唤醒,浏览器 clamp 成 0。
             const nextChangeTimestamp = - constant / coefficient
             if (Number.isFinite(nextChangeTimestamp) && nextChangeTimestamp > currentTimestamp)  {
                 // CAUTION 这里的 +2 非常重要，因为系数常数可能有浮点，或者 Date.now() 算出来的时候可能病名有达到真正的变化时间。
                 //  所以这里使用 +2 来保证在是真正已经产生变化了。
                 const timeoutTime = nextChangeTimestamp - currentTimestamp + 2
                 this.timeoutId = setTimeout(() => {
                     // CAUTION 这里是可以复用计算结果的，因为如果计算中有 atom 变化了，那么 autorun 整个都会重算，不会走到这里。
                     //  走到这里说明没有 atom 变化。
                     result(compare(Date.now()*coefficient + constant))
                 }, timeoutTime)

             }
        },true)
        this.addDisposer(stop)

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
