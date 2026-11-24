import {RxList} from "./RxList.js";
import {Atom, atom} from "./atom.js";
import {computed, destroyComputed} from "./computed.js";
import {ReactiveEffect, reportTeardownError} from "./reactiveEffect.js";
import {warn} from "./util.js";

type GetRemoteData<T> = (cursor?: number, length?: number, stop?: number, fetchBeforeCursor?: boolean) => Promise<T[]>

type GetCursor = (item: any) => number

// 应该继承 RxList, 这样基于 AsyncRxSlice 的其他数据后面可以继续保持 reactive
/**
 * @internal
 */
export class AsyncRxSlice<T> extends RxList<T>{
    isLoading: Atom<boolean> = atom(false)
    loadError: Atom<any>= atom(null)
    fetchReceipt: number = 0
    autoFetchPromise?: Atom<Promise<any>>
    constructor(cached: T[], public getRemoteData: GetRemoteData<T>, public getCursor?: GetCursor ) {
        super(cached)
    }
    fetch(): Promise<any>{
        // CAUTION 已销毁实例不再发起拉取（destroy 后结构只读，README §6）：destroyResources
        //  只能清理当时已存在的 autoFetchPromise，销毁后首次 fetch() 会新建一个**活的**
        //  computed——立刻发一次远程请求、订阅 getRemoteData 读到的参数，此后参数每变一次
        //  再发一次，且逃过一切销毁路径（destroy 幂等，已无人能销毁它）。与 RxList.length
        //  的"已销毁结构的 meta 首读"同一等价类；这里连创建都不做：创建即发请求。
        //  返回销毁前最后一次拉取的 promise（其结果已被 receipt 作废），没有则返回已完成的 promise。
        if (!this.active) {
            warn('fetching a destroyed AsyncRxSlice is a no-op')
            return this.autoFetchPromise ? this.autoFetchPromise.raw : Promise.resolve()
        }
        if (!this.autoFetchPromise) {
            // CAUTION createDetached(2026-H3 round6 R6-2):实例缓存的惰性结构,
            //  生命周期归实例(destroyResources)。不隔离的话,首次 fetch 发生在
            //  autorun/computed 内(条件驱动拉取)时会被收集为宿主 child,宿主
            //  重算即销毁——autoFetchPromise 字段仍指向已销毁 computed,此后
            //  getRemoteData 的响应式参数变化不再触发重新拉取,fetch() 永远
            //  返回旧 promise(静默陈旧)。与 RxList.length/RxMap.keys 同一
            //  等价类(实例缓存惰性结构 × 创建作用域生命周期)。
            this.autoFetchPromise = ReactiveEffect.createDetached(() =>
                computed(this.fetchFullRemoteData) as Atom<Promise<any>>
            )
        }
        return this.autoFetchPromise()
    }
    fetchFullRemoteData = () => {
        let thisReceipt = ++this.fetchReceipt
        this.isLoading(true)
        this.loadError(null)
        // CAUTION getRemoteData 同步抛错（参数校验、离线短路等，TS 的 `=> Promise<T[]>` 管不住）
        //  与 reject 走同一条收尾：否则异常从 computed getter 里直接飞出，上面刚写的
        //  isLoading(true) 再无人复位（receipt 已推进，旧请求的收尾也不认）。update() 的
        //  try/await 早已兜住同一输入，兄弟实现点必须一致。调用仍在 getter 的同步段内，
        //  抛错前读到的响应式参数照常建立依赖，参数变化后会重新拉取。
        let request: Promise<T[]>
        try {
            request = this.getRemoteData()
        } catch (err) {
            request = Promise.reject(err)
        }
        return request.then((data) => {
            if (this.fetchReceipt !== thisReceipt) return
            this.spliceArray(0, Infinity, data)
        }).catch(err => {
            if (this.fetchReceipt !== thisReceipt) return
            this.loadError(err)
        }).finally(() => {
            // 旧请求完成时不能提前关闭较新请求的 loading。
            if (this.fetchReceipt === thisReceipt) this.isLoading(false)
        })
    }
    async update(cursor:number, length? : number, stop?: number, fetchBeforeCursor?: boolean, replace?: boolean) {
        // 已销毁实例不再发请求、不再翻动 isLoading/loadError（理由同 fetch）
        if (!this.active) {
            warn('updating a destroyed AsyncRxSlice is a no-op')
            return
        }
        const thisReceipt = ++this.fetchReceipt
        this.isLoading(true)
        this.loadError(null)
        let newData
        try {
            newData = await this.getRemoteData(cursor, length, stop,fetchBeforeCursor)
        }catch(err) {
            if (this.fetchReceipt !== thisReceipt) return
            this.isLoading(false)
            this.loadError(err)
            return
        }
        if (this.fetchReceipt !== thisReceipt) return
        try {
            if (replace) {
                this.spliceArray(0, Infinity, newData)
            } else {
                if (fetchBeforeCursor) {
                    this.spliceArray(0, 0, newData)
                }else {
                    this.spliceArray(this.data.length, 0, newData)
                }
            }
        } finally {
            // 即使下游 patch 抛错，也不能把 loading 永久卡住。
            if (this.fetchReceipt === thisReceipt) this.isLoading(false)
        }
    }
    async append(length? : number, end?: number) {
        return this.update(this.getCursor!(this.data.at(-1)), length, end,false)
    }
    async prepend(length? : number, start?: number) {
        return this.update(this.getCursor!(this.data.at(0)), length, start,true)
    }
    async moveForward(length? : number, end?: number) {
        return this.update(this.getCursor!(this.data.at(-1)), length, end,false, true)
    }
    async moveBackward(length? : number, start?: number) {
        return this.update(this.getCursor!(this.data.at(0)), length, start,true, true)
    }
    /**
     * @internal
     * 统一资源清理钩子（见 ReactiveEffect.destroyResources）。
     */
    destroyResources() {
        // 作废一切在途 fetch/update：回调只认 receipt 匹配。不 bump 的话，
        // destroy 后仍可能写入 loadError / isLoading（僵尸控制态）。
        this.fetchReceipt++
        // CAUTION 下面每一步都会执行用户代码（isLoading/loadError 的订阅者、autoFetchPromise 的
        //  destroy 监听者），抛错不能跳过其余步骤（「销毁必须做完」，README §6）：曾是顺序执行，
        //  一处抛错，autoFetchPromise 就还活着、还订阅着 getRemoteData 读到的参数（参数一变就
        //  再请求远端），RxList 的基类钩子（length、行级 effect）也整个跳过。第一个错误抛给调用方，
        //  其后的走 reportTeardownError（同 RxList.destroyResources）。
        let failed = false
        let first: unknown
        const record = (error: unknown) => {
            if (failed) {
                reportTeardownError(error)
            } else {
                failed = true
                first = error
            }
        }
        try {
            this.isLoading(false)
        } catch (error) {
            record(error)
        }
        try {
            this.loadError(null)
        } catch (error) {
            record(error)
        }
        if (this.autoFetchPromise) {
            try {
                destroyComputed(this.autoFetchPromise)
            } catch (error) {
                record(error)
            }
        }
        try {
            super.destroyResources()
        } catch (error) {
            record(error)
        }
        if (failed) throw first
    }
}
