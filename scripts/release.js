import { execFileSync } from 'child_process'

const version = process.argv[2]
if (!version) {
  throw new Error('Missing version argument')
}


const gitStatus = execFileSync('git', ['status', './', '--porcelain'], {encoding: 'utf8'}).trim()
const isClean = gitStatus  === ''
if (!isClean) {
  throw new Error('Working tree is not clean')
}

try {
  // 参数数组不会经过 shell 解析；version 即使来自外部输入也不能追加第二条命令。
  execFileSync('pnpm', ['install', '--frozen-lockfile'], {stdio: 'inherit'})
  execFileSync('pnpm', ['run', 'build'], {stdio: 'inherit'})
  execFileSync('pnpm', ['version', version], {stdio: 'inherit'})
  // CAUTION 必须把 version 产生的 tag 推上远端(2026-H3 round6 工程面静态确认:
  //  裸 `git push` 不推 tags,v2.10.0-v2.12.0 三个已发布版本远端无对应 tag,
  //  发布不可审计溯源)。--follow-tags 推随提交可达的 annotated tag,
  //  --tags 兜底 lightweight 形态(pnpm/npm version 的 tag 形态因版本而异)。
  execFileSync('git', ['push', '--follow-tags'], {stdio: 'inherit'})
  execFileSync('git', ['push', '--tags'], {stdio: 'inherit'})
  execFileSync('pnpm', ['publish', './'], {stdio: 'inherit'})
} catch (e) {
  console.error(e)
  process.exit(1)
}
