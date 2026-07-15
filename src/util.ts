export const extend = Object.assign

export const isArray = Array.isArray
// FIXME 支持自定义的 Map 和 Set
export const isMap = (val: unknown): val is Map<any, any> =>
    toTypeString(val) === '[object Map]'
export const isSet = (val: unknown): val is Set<any> =>
    toTypeString(val) === '[object Set]'

export const objectToString = Object.prototype.toString
export const toTypeString = (value: unknown): string =>
    objectToString.call(value)

// CAUTION 用原型链而非 constructor 字段：
//  - Object.create(null) 无 constructor，旧实现会误判为非 plain，导致 object-atom
//    的 get 转发到 updater 函数、属性读永远 undefined 且不 track。
//  - 用户可写 `obj.constructor = null` 同样绕过 constructor===Object 门闩。
//  - class 实例的原型不是 Object/Array/null，仍被排除（与 atom 浅包装契约一致）。
export const isPlainObject = (val: unknown): val is object => {
    if (val === null || typeof val !== 'object') return false
    const proto = Object.getPrototypeOf(val)
    return proto === Object.prototype || proto === Array.prototype || proto === null
}

export const def = (obj: object, key: string | symbol, value: any) => {
    Object.defineProperty(obj, key, {
        configurable: true,
        enumerable: false,
        value
    })
}

export function isStringOrNumber(target: any) {
    return typeof target === 'string' || typeof  target === 'number'
}

export function isReactivableType( data: any ) {
    return isPlainObject(data) || isArray(data)  || isMap(data) || isSet(data)
}

export function assert(condition: boolean, message: string ) {
    if (!condition) {
        if (__DEV__) debugger
        throw new Error(message)
    }
}

export function warn(message: string ) {
    if (__DEV__) {
        console.warn(message)
    }
}

export function isAsync(fn: Function) {
    return fn.constructor.name === 'AsyncFunction'
}

export function isGenerator(fn: Function) {
    return fn.constructor.name === 'GeneratorFunction'
}

// CAUTION 为了一般场景中的新能，不深度 replace!
//  用户可以通过 computed 的再封装实现对某个 computed 结果的深度监听。
export function replace(source: any, nextSourceValue: any) {
    const rawSource = source
    if (Array.isArray(source)) {
        spliceMany(source, 0, Infinity, nextSourceValue)
    } else if (isPlainObject(source)) {
        // Set 查找而不是 includes：对象 key 多时 filter+includes 是 O(n²)
        const nextKeys = new Set(Object.keys(nextSourceValue))
        for (const k of Object.keys(rawSource)) {
            if (!nextKeys.has(k)) delete (source as { [k: string]: any })[k]
        }
        Object.assign(source, nextSourceValue)
    } else if (source instanceof Map) {

        for (const key of rawSource.keys()) {
            if (nextSourceValue.has(key)) {
                source.set(key, nextSourceValue.get(key))
            } else {
                source.delete(key)
            }
        }

        for (const key of nextSourceValue.keys()) {
            if (!rawSource.has(key)) {
                source.set(key, nextSourceValue.get(key))
            }
        }

    } else if (source instanceof Set) {
        rawSource.forEach((item: any) => {
            if (!nextSourceValue.has(item)) source.delete(item)
        })

        nextSourceValue.forEach((item: any) => {
            if (!rawSource.has(item)) source.add(item)
        })
    } else {
        assert(false, 'unknown source type to replace data')
    }
}

export function nextTick(fn: () => any) {
    Promise.resolve().then(fn)
}

// CAUTION 协议载荷所有权(2026-H3 round5 裁定,README「参数契约」):变更方法的
//  返回数组归调用方所有(原生 Array#splice 预期),协议载荷(info.methodResult、
//  reorder 的 argv[0]、RxSet.replace 的内层数组)持独立副本——否则 batch /
//  async applyPatch 跨 await / onChange handler / 自定义调度器四类延迟消费窗口
//  里,调用方改写返回数组会静默毒化全部 patch 消费者与 digestReplay 重建。
//  CAUTION 只拷贝、不冻结:曾对副本(及 trigger 汇聚点的 argv)做 dev
//  Object.freeze 让订阅者改写当场抛错,ABBA 实测 dev 下 push +20%/单删
//  splice +30%/swap +52%(freeze 调用本身 + 冻结数组的元素种类转换拖慢后续
//  消费),热路径不可接受。订阅者侧的只读性由契约(README)+ onChange 给
//  handler 发防御副本(见 common.ts)承载。空数组共享冻结单例:纯尾插
//  (push)的删除项恒空,最热变更路径零额外分配(冻结单例只读,不在热读路径)。
const EMPTY_PROTOCOL_PAYLOAD = Object.freeze([]) as unknown as unknown[]
export function toProtocolPayload<T>(items: T[]): T[] {
    if (items.length === 0) return EMPTY_PROTOCOL_PAYLOAD as T[]
    return items.slice()
}

// Array#splice 对 start/deleteCount 的 ToIntegerOrInfinity 语义：
// NaN/undefined → 0，小数截断，-0 → +0，±Infinity 保留（由后续 clamp 处理）。
export function toIntegerOrInfinity(value: unknown): number {
    const n = Number(value)
    if (Number.isNaN(n)) return 0
    const i = Math.trunc(n)
    // CAUTION 规范要求 -0 归一化为 +0：Math.trunc(-0.5) 是 -0，若不归一化会
    //  顺着 normalizeSpliceStart 流进派生结构（例如 findIndex 返回 -0，
    //  atom 的 Object.is 判等会把 0 → -0 当成变化反复触发订阅者）。
    return i === 0 ? 0 : i
}

// 归一化 splice 的 start：负数从末尾回退，最终 clamp 到 [0, length]。
// CAUTION triggerInfo.argv 按契约透传用户原始参数（axii/axle 锁定该行为并自行归一化），
//  但 data0 内部消费 argv 的地方（元数据维护、派生列表的 patch）必须先用它归一化，
//  否则负/越界/小数 start 会算错受影响区间或直接用负 index 访问数组。
export function normalizeSpliceStart(start: unknown, length: number): number {
    const n = toIntegerOrInfinity(start)
    return n < 0 ? Math.max(length + n, 0) : Math.min(n, length)
}

// 归一化 splice 的 deleteCount：clamp 到 [0, length - normalizedStart]
export function normalizeSpliceDeleteCount(deleteCount: unknown, length: number, normalizedStart: number): number {
    return Math.min(Math.max(toIntegerOrInfinity(deleteCount), 0), length - normalizedStart)
}

// 超过该数量的插入不再走 native splice 的 spread 传参。
// V8 的实参上限约 65k，留足余量；小批量仍用 native splice（快于手动搬移）。
const SPLICE_SPREAD_LIMIT = 8192

/**
 * 语义等同 `arr.splice(start, deleteCount, ...items)`，但 items 以数组传入：
 * 大批量插入（如 10 万行 replaceData）用 spread 传实参会直接 RangeError 爆栈，
 * 且实参传递本身是一次 O(n) 拷贝。大批量走 copyWithin 手动搬移。
 */
export function spliceMany<T>(arr: T[], start: number, deleteCount: number, items?: T[]): T[] {
    const insertCount = items ? items.length : 0
    if (insertCount === 0) {
        return arr.splice(start, deleteCount)
    }
    if (insertCount <= SPLICE_SPREAD_LIMIT) {
        return arr.splice(start, deleteCount, ...items!)
    }

    // 按 Array.prototype.splice 规范归一化 start/deleteCount
    const len = arr.length
    const s = start < 0 ? Math.max(len + start, 0) : Math.min(start, len)
    const dc = Math.min(Math.max(deleteCount, 0), len - s)

    const removed = arr.slice(s, s + dc)
    const diff = insertCount - dc
    if (diff > 0) {
        arr.length = len + diff
        arr.copyWithin(s + insertCount, s + dc, len)
    } else if (diff < 0) {
        arr.copyWithin(s + insertCount, s + dc, len)
        arr.length = len + diff
    }
    for (let i = 0; i < insertCount; i++) {
        arr[s + i] = items![i]
    }
    return removed
}
