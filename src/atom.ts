import {Notifier} from "./notify";
import {TrackOpTypes, TriggerOpTypes} from './operations'
import {def, isPlainObject, isStringOrNumber} from "./util";
import {ReactiveFlags} from "./flags";
import {setDebugName} from "./debug";
import {ReactiveEffect} from "./reactiveEffect.js";

export type UpdateFn<T> = (prev: T) => T

export interface AtomBase<T> {
  [ReactiveFlags.IS_ATOM]: true,
  raw: T,
  (newValue?: any): T
}

export type Atom<T = any> = T extends object ? (AtomBase<T> & T) : AtomBase<T>

export type AtomInitialType = any


export type AtomInterceptor<T>  = (updater: Updater<T>, h: Handler) => [Updater<T>, Handler]

type Updater<T> = (newValue?: T | UpdateFn<T>) => any
type Handler = ProxyHandler<object>
const PRIMITIVE_ATOM_VALUE = Symbol('primitive atom value')

type PrimitiveAtomUpdater<T> = ((newValue?: T) => T | void) & {
    [PRIMITIVE_ATOM_VALUE]: T
    [Symbol.toPrimitive]: (hint: string) => string | number | null | unknown
}

/**
 * @category Basic
 */
export function atom<T>(initValue: T, interceptor? : AtomInterceptor<typeof initValue>, name?: string): Atom<T>
export function atom<T>(initValue: null, interceptor? : AtomInterceptor<typeof initValue>, name?: string): Atom<T|null>
export function atom<T>(initValue?: T | undefined, interceptor? : AtomInterceptor<typeof initValue>, name?: string): Atom<T|undefined>
export function atom(initValue: AtomInitialType, interceptor? : AtomInterceptor<typeof initValue>, name?: string)  {
    if (!interceptor && isPrimitiveAtomValue(initValue)) {
        return createPrimitiveAtom(initValue, name)
    }

    let value: typeof initValue|undefined  = initValue

    // CAUTION 只能这样写才能支持 arguments.length === 0 ，否则就永远不会 为 0
    function updater (newValue?: typeof initValue) {
        if (arguments.length === 0) {
            trackAtomValue(finalProxy)
            return value
        }

        // CAUTION 不再支持 newValue 为 function 的方式，因为 atom 中可以包装 atom，就像指针可以指向另一个指针一样。
        // if(typeof newValue === 'function') {
        //     value = newValue!(value)
        // } else {
        //     value = newValue
        // }
        // CAUTION Object.is 而不是 ===：NaN 重复写入不应该反复触发
        if (Object.is(value, newValue)) return
        const oldValue = value
        value = newValue
        Notifier.instance.trigger(finalProxy, TriggerOpTypes.ATOM, { key: 'value', newValue, oldValue})
    }

    const handler:Handler = {
        get(target, key) {
            // 对外提供一种获取 value，但是不触发 track 的方式。在一些框架里面会用到
            if (key === 'raw'||key ===ReactiveFlags.RAW) return value

            if (key === ReactiveFlags.IS_ATOM) return true
            if (key === 'call') return function(_this:any, newValue?: typeof initValue) {
                return arguments.length > 1 ? finalUpdater.call(_this, newValue): finalUpdater.call(_this)
            }

            // TODO 是不是也要像 reactive 一样层层包装才行？？？，不然当把这个值传给 dom 元素的时候，它就已经不能被识别出来，也就不能 reactive 了。
            if (isPlainObject(value)) {
                trackAtomValue(finalProxy)
            }
            // CAUTION 针对非  class 的对象提供深度的获取的能力
            return Reflect.get(isPlainObject(value) ? value : finalUpdater, key)
        },
        set(target, key, newValue) {
            // CAUTION 注意这里是不 trigger 的
            if (typeof value === 'object') {
                return Reflect.set(value, key, newValue)
            }

            return false
        },
        // TODO 有必要要吗？？？
        getPrototypeOf(): object | null {
            if (value && typeof value === 'object') return Reflect.getPrototypeOf(value as object)
            return null
        }
    }



    const [finalUpdater, finalHandler] = interceptor ? interceptor(updater, handler) : [updater, handler]


    Object.assign( finalUpdater, {
        [Symbol.toPrimitive](hint: string) {
            trackAtomValue(finalProxy)
            if ((!hint || hint === 'default') && isStringOrNumber(value)) {
                return value
            } else if (hint === 'number' && typeof value === 'number' ) {
                // CAUTION 不支持 string 隐式转 number
                return value;
            } else if (hint === 'string'){
                return isStringOrNumber(value) ? value.toString() : Object.prototype.toString.call(value)
            }

            return null;
        }
    })

    if (name) {
        setDebugName(finalUpdater, name)
    }

    def(finalUpdater, ReactiveFlags.IS_ATOM, true)
    const finalProxy = new Proxy(finalUpdater, finalHandler) as Atom<typeof initValue>
    return finalProxy
}

function isPrimitiveAtomValue(value: unknown) {
    return value === null || (typeof value !== 'object' && typeof value !== 'function')
}

export function isPrimitiveAtom(r: unknown) {
    return typeof r === 'function' && Object.prototype.hasOwnProperty.call(r, PRIMITIVE_ATOM_VALUE)
}

// CAUTION 所有 primitive atom 共享同一个原型：raw 访问器、Symbol.toPrimitive、IS_ATOM 标记
//  都放在原型上，每个 atom 实例只保留一个自有的值属性。
//  旧实现对每个 atom 函数做 2 次 defineProperty + 2 次动态属性赋值，会把函数对象推进
//  字典属性模式，每个 atom 多花约 3 倍内存；长列表里每行一个 atom 时是主要常驻开销之一。
const primitiveAtomProto = Object.create(Function.prototype, {
    raw: {
        configurable: true,
        enumerable: false,
        get: getPrimitiveAtomRaw,
    },
    [Symbol.toPrimitive]: {
        configurable: true,
        writable: true,
        enumerable: false,
        value: primitiveAtomToPrimitive,
    },
    [ReactiveFlags.IS_ATOM]: {
        configurable: false,
        writable: false,
        enumerable: false,
        value: true,
    },
})

function createPrimitiveAtom<T>(initValue: T, name?: string) {
    // CAUTION 只能这样写才能支持 arguments.length === 0 ，否则就永远不会 为 0
    const updater = function(newValue?: T): T | void {
        if (arguments.length === 0) {
            trackAtomValue(updater, true)
            return updater[PRIMITIVE_ATOM_VALUE]
        }

        // CAUTION 和 Proxy atom 保持一致，不再支持 newValue 为 function 的 updater 语义。
        // Object.is 而不是 ===：NaN 重复写入不应该反复触发
        if (Object.is(updater[PRIMITIVE_ATOM_VALUE], newValue)) return
        const oldValue = updater[PRIMITIVE_ATOM_VALUE]
        updater[PRIMITIVE_ATOM_VALUE] = newValue as T
        Notifier.instance.triggerPrimitiveAtomValue(updater, { key: 'value', newValue, oldValue})
    } as PrimitiveAtomUpdater<T>

    // CAUTION setPrototypeOf 要在添加自有属性之前做，V8 对"先改原型再加属性"的对象
    //  能保持 fast properties；值属性用普通赋值（symbol key，不污染 for...in/Object.keys）。
    Object.setPrototypeOf(updater, primitiveAtomProto)
    updater[PRIMITIVE_ATOM_VALUE] = initValue

    if (name) {
        setDebugName(updater, name)
    }

    return updater as unknown as Atom<T>
}

function getPrimitiveAtomRaw<T>(this: PrimitiveAtomUpdater<T>) {
    return this[PRIMITIVE_ATOM_VALUE]
}

function primitiveAtomToPrimitive(this: PrimitiveAtomUpdater<unknown>, hint: string) {
    trackAtomValue(this, true)
    const value = this[PRIMITIVE_ATOM_VALUE]
    if ((!hint || hint === 'default') && isStringOrNumber(value)) {
        return value
    } else if (hint === 'number' && typeof value === 'number' ) {
        // CAUTION 不支持 string 隐式转 number
        return value;
    } else if (hint === 'string'){
        return isStringOrNumber(value) ? value.toString() : Object.prototype.toString.call(value)
    }

    return null;
}

function trackAtomValue(target: object, primitive = false) {
    const notifier = Notifier.instance
    if (!notifier.shouldTrack || !ReactiveEffect.activeScopes.length) return
    if (primitive) {
        notifier.trackPrimitiveAtomValue(target)
        return
    }
    notifier.track(target, TrackOpTypes.ATOM, 'value')
}

atom.fixed = function<T>(initValue: T) {
    function getValue() {
        return initValue
    }
    def(getValue, ReactiveFlags.IS_ATOM, true)
    return getValue as Atom<T>
}

atom.lazy = function<T>(getter: () => T) {
    def(getter, ReactiveFlags.IS_ATOM, true)
    return getter as Atom<T>
}


atom.as = new Proxy({}, {
    get(p, name: string) {
        return (initialValue: Parameters<typeof atom>[0], interceptor: Parameters<typeof atom>[1]) => {
            return atom(initialValue, interceptor, name)
        }
    }
})

/**
 * @category Basic
 */
export function isAtom<T>(r: Atom<T> | unknown): r is Atom<T>
export function isAtom(r: any): r is Atom<any> {
    return !!(r && r[ReactiveFlags.IS_ATOM])
}
