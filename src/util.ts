export function makeMap(
    str: string,
    expectsLowerCase?: boolean
): (key: string) => boolean {
    const map: Record<string, boolean> = Object.create(null)
    const list: Array<string> = str.split(',')
    for (let i = 0; i < list.length; i++) {
        map[list[i]] = true
    }
    return expectsLowerCase ? val => !!map[val.toLowerCase()] : val => !!map[val]
}


/**
 * Always return false.
 */
export const NO = () => false

const onRE = /^on[^a-z]/
export const isOn = (key: string) => onRE.test(key)

export const isModelListener = (key: string) => key.startsWith('onUpdate:')

export const extend = Object.assign

export const remove = <T>(arr: T[], el: T) => {
    const i = arr.indexOf(el)
    if (i > -1) {
        arr.splice(i, 1)
    }
}


const arrayProperties = Object.getOwnPropertyNames(Array.prototype);
const arrayMethods = new Set(arrayProperties.filter(prop => typeof Array.prototype[prop as keyof typeof Array.prototype] === 'function'))
export const isArrayMethod = (key: string) => arrayMethods.has(key)


const hasOwnProperty = Object.prototype.hasOwnProperty
export const hasOwn = (
    val: object,
    key: string | symbol
) => hasOwnProperty.call(val, key)

export const isArray = Array.isArray
// FIXME 支持自定义的 Map 和 Set
export const isMap = (val: unknown): val is Map<any, any> =>
    toTypeString(val) === '[object Map]'
export const isSet = (val: unknown): val is Set<any> =>
    toTypeString(val) === '[object Set]'

export const isDate = (val: unknown): val is Date =>
    toTypeString(val) === '[object Date]'
export const isRegExp = (val: unknown): val is RegExp =>
    toTypeString(val) === '[object RegExp]'
export const isFunction = (val: unknown): val is Function =>
    typeof val === 'function'
export const isString = (val: unknown): val is string => typeof val === 'string'
export const isSymbol = (val: unknown): val is symbol => typeof val === 'symbol'
export const isObject = (val: unknown): val is Record<any, any> =>
    val !== null && typeof val === 'object'

export const isPromise = <T = any>(val: unknown): val is Promise<T> => {
    return isObject(val) && isFunction(val.then) && isFunction(val.catch)
}

export const objectToString = Object.prototype.toString
export const toTypeString = (value: unknown): string =>
    objectToString.call(value)

export const toRawType = (value: unknown): string => {
    // extract "RawType" from strings like "[object RawType]"
    return toTypeString(value).slice(8, -1)
}

// CAUTION 这个判断不能识别是不是自己创建的对象
// export const isPlainObject = (val: unknown): val is object => toTypeString(val) === '[object Object]'
export const isPlainObject = (val: unknown): val is object => (val?.constructor === Object || val?.constructor === Array )

export const isIntegerKeyQuick = (key: unknown) =>
    isString(key) && (
        key[0] === '0'|| key[0] === '1' || key[0] === '2' || key[0] === '3' || key[0] === '4' || key[0] === '5' || key[0] === '6' || key[0] === '7' || key[0] === '8' || key[0] === '9'
    )

export const isIntegerKey = (key: unknown) =>
    isString(key) &&
    key !== 'NaN' &&
    key[0] !== '-' &&
    '' + parseInt(key, 10) === key

export const isReservedProp = /*#__PURE__*/ makeMap(
    // the leading comma is intentional so empty string "" is also included
    ',key,ref,ref_for,ref_key,' +
    'onVnodeBeforeMount,onVnodeMounted,' +
    'onVnodeBeforeUpdate,onVnodeUpdated,' +
    'onVnodeBeforeUnmount,onVnodeUnmounted'
)

export const isBuiltInDirective = /*#__PURE__*/ makeMap(
    'bind,cloak,else-if,else,for,html,if,model,on,once,pre,show,slot,text,memo'
)

const cacheStringFunction = <T extends (str: string) => string>(fn: T): T => {
    // @ts-ignore
    const cache: Record<string, string> = Object.create(null)
    return ((str: string) => {
        const hit = cache[str]
        return hit || (cache[str] = fn(str))
    }) as T
}

const camelizeRE = /-(\w)/g
/**
 * @private
 */
export const camelize = cacheStringFunction((str: string): string => {
    return str.replace(camelizeRE, (_, c) => (c ? c.toUpperCase() : ''))
})

const hyphenateRE = /\B([A-Z])/g
/**
 * @private
 */
export const hyphenate = cacheStringFunction((str: string) =>
    str.replace(hyphenateRE, '-$1').toLowerCase()
)

/**
 * @private
 */
export const capitalize = cacheStringFunction(
    (str: string) => str.charAt(0).toUpperCase() + str.slice(1)
)

/**
 * @private
 */
export const toHandlerKey = cacheStringFunction((str: string) =>
    str ? `on${capitalize(str)}` : ``
)

// compare whether a value has changed, accounting for NaN.
export const hasChanged = (value: any, oldValue: any): boolean =>
    !Object.is(value, oldValue)

export const invokeArrayFns = (fns: Function[], arg?: any) => {
    for (let i = 0; i < fns.length; i++) {
        fns[i](arg)
    }
}

export const def = (obj: object, key: string | symbol, value: any) => {
    Object.defineProperty(obj, key, {
        configurable: true,
        enumerable: false,
        value
    })
}

/**
 * "123-foo" will be parsed to 123
 * This is used for the .number modifier in v-model
 */
export const looseToNumber = (val: any): any => {
    const n = parseFloat(val)
    return isNaN(n) ? val : n
}

/**
 * Only concerns number-like strings
 * "123-foo" will be returned as-is
 */
export const toNumber = (val: any): any => {
    const n = isString(val) ? Number(val) : NaN
    return isNaN(n) ? val : n
}


export function isStringOrNumber(target: any) {
    return typeof target === 'string' || typeof  target === 'number'
}

export function isReactivableType( data: any ) {
    return isPlainObject(data) || isArray(data)  || isMap(data) || isSet(data)
}

export const getStackTrace = function() {
    const obj = {};
    //@ts-ignore
    Error.captureStackTrace(obj, getStackTrace);
    //@ts-ignore
    return obj.stack.split('\n').slice(1, Infinity).map(line => {
        const nameAndLoc =  line.replace(/^\s+at\s/, '').split(' ')
        if (nameAndLoc.length === 1) nameAndLoc.unshift('anonymous')
        nameAndLoc[1] = nameAndLoc[1].slice(1, nameAndLoc[1].length -1)
        return nameAndLoc
    });
};


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

export function uuid() {
    return Math.random().toString(36).substring(2)
} // CAUTION 为了一般场景中的新能，不深度 replace!
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

// Array#splice 对 start/deleteCount 的 ToIntegerOrInfinity 语义：
// NaN/undefined → 0，小数截断，±Infinity 保留（由后续 clamp 处理）。
export function toIntegerOrInfinity(value: unknown): number {
    const n = Number(value)
    if (Number.isNaN(n)) return 0
    return Math.trunc(n)
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
