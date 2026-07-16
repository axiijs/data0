import {RxList} from "./RxList.js";
import {Atom, atom} from "./atom.js";
import {computed, destroyComputed} from "./computed.js";
import {ReactiveEffect} from "./reactiveEffect.js";

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
        return this.getRemoteData().then((data) => {
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
        this.isLoading(false)
        this.loadError(null)
        if (this.autoFetchPromise) {
            destroyComputed(this.autoFetchPromise)
        }
        super.destroyResources()
    }
}
