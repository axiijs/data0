// @ts-ignore
import {ITERATE_KEY, notifier} from './notify'
import {TrackOpTypes, TriggerOpTypes} from "./operations";


class ListNode<T> {
    prev?: ListNode<T>
    next?: ListNode<T>
    constructor(public item: T) {
    }
}

/**
 * 响应式双向链表(手动 notifier.trigger 协议的示例级结构)。
 *
 * CAUTION 维护状态(2026-H3 round6 工程面盘点):axii/axle 两个下游均未使用
 * (axii 有自己的 LinkedList 实现),功能面为最小集。保留导出以维持 API 兼容,
 * 但新代码优先使用 RxList;如未来确认无外部使用者,可在 major 版本移除。
 */
export class LinkedList<T extends object> implements Iterable<ListNode<T>>{
    head?: ListNode<T>
    tail?: ListNode<T>
    itemToNode = new WeakMap<T, ListNode<T>>()
    constructor(source: T[])  {
        let prev: ListNode<T>|undefined = undefined
        source.forEach(item => {
            const node = this.createNode(item)
            if (prev) {
                prev.next = node
                node.prev = prev
            } else {
                this.head = node
            }
            prev = node
        })
        this.tail = prev
    }
    createNode(item:T) {
        const node = new ListNode(item)
        this.itemToNode.set(item, node)
        return node
    }
    insertBefore(newItem: T, refNode?: ListNode<T>) {
        const newNode = this.createNode(newItem)
        if (!this.head) {
            this.head = this.tail = newNode
        } else {
            // 没有 ref ，insert 在尾部，和 dom API 保持一致
            if (!refNode) {
                this.tail!.next = newNode
                newNode.prev = this.tail
                this.tail = newNode
            } else {
                newNode.prev = refNode.prev
                if (newNode.prev) newNode.prev.next = newNode

                newNode.next = refNode
                refNode.prev = newNode
                if (this.head === refNode) {
                    this.head = newNode
                }
            }
        }

        notifier.trigger(this, TriggerOpTypes.METHOD, { method:'insertBefore', argv: [newItem, refNode], result: { add:[{newValue:newNode}]}})
        notifier.trigger(this, TriggerOpTypes.ADD, { key: ITERATE_KEY })
        return newNode
    }
    // removeBetween 移除的部分包含 startNode 和 endNode
    // TODO 支持 startNode, endNode 缺省的情况，说明删到末尾
    removeBetween(startNode: ListNode<T>|undefined = this.head, endNode:ListNode<T>|undefined = this.tail) {
        const prev = startNode?.prev
        const next = endNode?.next
        if (prev) {
            prev.next = next
        } else {
            this.head = next
        }

        if (next) {
            next.prev = prev
        } else {
            this.tail = prev
        }


        // 清理 itemToNode 中被移除的映射，避免 getNodeByItem 返回已脱链的节点。
        // 仅当 WeakMap 指向本节点时才删：同一 item 身份出现多次时，constructor
        // 只登记最后一个节点；删掉非登记节点不应抹掉幸存者的查找入口。
        let current: ListNode<T>|undefined = startNode
        while (current) {
            if (this.itemToNode.get(current.item) === current) {
                this.itemToNode.delete(current.item)
            }
            if (current === endNode) break
            current = current.next
        }

        notifier.trigger(this, TriggerOpTypes.METHOD, { method:'removeBetween', argv: [startNode, endNode]})
        notifier.trigger(this, TriggerOpTypes.DELETE, { key: ITERATE_KEY})
    }

    getNodeByItem(item: T){
        return this.itemToNode.get(item)
    }
    *[Symbol.iterator]() {
        notifier.track(this, TrackOpTypes.ITERATE,  ITERATE_KEY)
        let current = this.head
        while(current) {
            yield current
            current = current.next
        }
    }
    map(mapFn: (node: ListNode<T>) => any) {
        const result = []
        for(let node of this) {
            result.push(mapFn(node))
        }
        return result
    }
    at(index: number) {
        if (index === -1) return this.tail
        let count = 0
        let current = this.head
        while(current && count < index) {
            current = current.next
            count++
        }
        return current
    }
}
