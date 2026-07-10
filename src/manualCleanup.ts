import {assert} from "./util.js";

export type CleanupFrame = ManualCleanup[]


export class ManualCleanup {

    static collectFrames: CleanupFrame[] = []

    static collectEffect() {
        const frame: CleanupFrame = []
        ManualCleanup.collectFrames.push(frame)
        return () => {
            const frames = ManualCleanup.collectFrames
            assert(frames[frames.length - 1] === frame, 'collect effect frame error')
            return frames.pop()!
        }
    }

    constructor() {
        // CAUTION 每个 effect/computed 的构造都会经过这里，用索引访问而不是 .at(-1)
        const frames = ManualCleanup.collectFrames
        if (frames.length) {
            frames[frames.length - 1].push(this)
        }
    }

    destroy() {
        // should be override
    }
}