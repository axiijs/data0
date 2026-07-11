import {RxList} from "./RxList.js";
import {Atom, atom} from "./atom.js";
import {computed, destroyComputed} from "./computed.js";

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
            this.autoFetchPromise = computed(this.fetchFullRemoteData) as Atom<Promise<any>>
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
    destroy() {
        if (this.autoFetchPromise) {
            destroyComputed(this.autoFetchPromise)
        }
        super.destroy()
    }
}
