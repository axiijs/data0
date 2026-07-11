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
  execFileSync('git', ['push'], {stdio: 'inherit'})
  execFileSync('pnpm', ['publish', './'], {stdio: 'inherit'})
} catch (e) {
  console.error(e)
  process.exit(1)
}
