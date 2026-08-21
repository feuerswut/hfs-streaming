/* ---------------------------------------------------------------------------
   build.mjs -- src/ to dist/public/.

   Plain Node ESM. Calls the esbuild and sass JS APIs directly: no bundler
   framework, no gulp/grunt, no webpack/vite/rollup.

   What it produces:

     dist/public/player.html   copy of src/frontend/player.html (slim shell:
                                markup + the {{STREAM_FLV_PATH}}/{{STREAM_APP}}/
                                {{STREAM_KEY}} bootstrap script + <script src>
                                / <link> tags to the two files below)
     dist/public/player.js     esbuild IIFE bundle of src/frontend/player.ts
     dist/public/player.css    src/styles/player.scss via sass + postcss

   dist/plugin.js and dist/node_modules/ (the vendored node-media-server) are
   untouched by this build -- only dist/public/ is generated here.

   The {{STREAM_FLV_PATH}}/{{STREAM_APP}}/{{STREAM_KEY}} placeholders live in
   src/frontend/player.html and must survive into dist/public/player.html
   byte-for-byte: plugin.js does a plain string .replace() over the served
   file's text at startup, so those exact tokens have to still be there for
   the substitution to find.

   Usage:
     node build/build.mjs            one-shot build
     node build/build.mjs --watch    build, then rebuild on any src/ change
   --------------------------------------------------------------------------- */

import fs from 'node:fs'
import fsp from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import * as esbuild from 'esbuild'
import * as sass from 'sass'
import postcss from 'postcss'
import autoprefixer from 'autoprefixer'
import cssnano from 'cssnano'

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

const ROOT     = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const SRC      = path.join(ROOT, 'src')
const FRONTEND = path.join(SRC, 'frontend')
const STYLES   = path.join(SRC, 'styles')
const DIST     = path.join(ROOT, 'dist')
const PUBLIC   = path.join(DIST, 'public')

// The placeholders plugin.js substitutes at serve-time -- verified present in
// the built player.html at the end of the build so a future edit that
// accidentally drops one of them fails the build instead of shipping quietly.
const REQUIRED_PLACEHOLDERS = ['{{STREAM_FLV_PATH}}', '{{STREAM_APP}}', '{{STREAM_KEY}}']

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

const rel = p => path.relative(ROOT, p).replaceAll('\\', '/')
const log = (...a) => console.log(...a)

function kb(bytes) {
  return `${(bytes / 1024).toFixed(1)} kB`
}

// ---------------------------------------------------------------------------
// 1. Clean -- dist/public/ only. dist/plugin.js and dist/node_modules/ (the
//    vendored node-media-server) are never touched here.
// ---------------------------------------------------------------------------

async function cleanPublic() {
  const resolved = path.resolve(PUBLIC)
  if (resolved === path.resolve(DIST) || !resolved.startsWith(path.resolve(DIST) + path.sep))
    throw new Error(`refusing to clean ${resolved}: not a subdirectory of dist/`)

  await fsp.rm(resolved, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 })
  await fsp.mkdir(resolved, { recursive: true })
  log(`  clean          ${rel(resolved)}/`)
}

// ---------------------------------------------------------------------------
// 2. player.js (esbuild)
// ---------------------------------------------------------------------------
// Not minified, on purpose: this is a small page served from a local HFS box,
// and readable stack traces beat a few saved kilobytes for a plugin nobody
// else's build tooling ever touches.

async function buildPlayerJs() {
  const outfile = path.join(PUBLIC, 'player.js')
  const result = await esbuild.build({
    absWorkingDir: ROOT,
    entryPoints:   [path.join(FRONTEND, 'player.ts')],
    outfile,
    bundle:        true,
    format:        'iife',
    platform:      'browser',
    target:        ['es2022'],
    charset:       'utf8',
    minify:        false,
    sourcemap:     false,
    legalComments: 'inline',
    logLevel:      'warning',
    metafile:      true,
  })
  const size = result.metafile.outputs[rel(outfile)]?.bytes ?? (await fsp.stat(outfile)).size
  log(`  esbuild        ${rel(outfile)}  (${kb(size)})`)
}

// ---------------------------------------------------------------------------
// 3. player.css -- sass, then postcss (autoprefixer + cssnano)
// ---------------------------------------------------------------------------

async function buildPlayerCss() {
  const entry   = path.join(STYLES, 'player.scss')
  const outfile = path.join(PUBLIC, 'player.css')

  const compiled = sass.compile(entry, {
    style:     'expanded',
    loadPaths: [STYLES],
    sourceMap: false,
  })

  const processed = await postcss([
    autoprefixer(),
    cssnano({ preset: 'default' }),
  ]).process(compiled.css, { from: entry, to: outfile })

  processed.warnings().forEach(w => console.warn(`  [postcss] ${w.toString()}`))

  await fsp.mkdir(path.dirname(outfile), { recursive: true })
  await fsp.writeFile(outfile, processed.css, 'utf8')
  log(`  sass+postcss   ${rel(outfile)}  (${kb(Buffer.byteLength(processed.css))})`)
}

// ---------------------------------------------------------------------------
// 4. player.html -- copied verbatim (it's already the slim shell)
// ---------------------------------------------------------------------------

async function copyPlayerHtml() {
  const from = path.join(FRONTEND, 'player.html')
  const to   = path.join(PUBLIC, 'player.html')
  await fsp.mkdir(path.dirname(to), { recursive: true })
  await fsp.copyFile(from, to)
  log(`  copy           ${rel(to)}`)
}

// ---------------------------------------------------------------------------
// Verification
// ---------------------------------------------------------------------------

const REQUIRED_OUTPUTS = [
  'dist/public/player.html',
  'dist/public/player.js',
  'dist/public/player.css',
]

async function verifyOutputs() {
  const missing = REQUIRED_OUTPUTS.filter(p => !fs.existsSync(path.join(ROOT, p)))
  if (missing.length) throw new Error(`build produced an incomplete tree, missing:\n  ${missing.join('\n  ')}`)

  const html = await fsp.readFile(path.join(ROOT, 'dist/public/player.html'), 'utf8')
  const missingPlaceholders = REQUIRED_PLACEHOLDERS.filter(p => !html.includes(p))
  if (missingPlaceholders.length)
    throw new Error(`dist/public/player.html is missing placeholder(s) plugin.js substitutes at runtime:\n  ${missingPlaceholders.join('\n  ')}`)

  log(`  verify         ${REQUIRED_OUTPUTS.length} required paths present, all placeholders intact`)
}

// ---------------------------------------------------------------------------
// Build
// ---------------------------------------------------------------------------

async function build() {
  const t0 = Date.now()
  log('build: src/ -> dist/public/')

  await cleanPublic()
  await buildPlayerJs()
  await buildPlayerCss()
  await copyPlayerHtml()
  await verifyOutputs()

  log(`build: done in ${Date.now() - t0} ms`)
}

// ---------------------------------------------------------------------------
// Watch
// ---------------------------------------------------------------------------

const WATCH_DIRS = [FRONTEND, STYLES]

async function watch() {
  await build().catch(reportBuildError)
  log(`\nwatching ${WATCH_DIRS.map(rel).join(', ')} -- Ctrl+C to stop`)

  let timer = null
  let running = false
  let queued = false

  const rebuild = async () => {
    if (running) { queued = true; return }
    running = true
    try {
      log(`\n[${new Date().toLocaleTimeString()}] change detected`)
      await build()
    } catch (err) {
      reportBuildError(err)
    } finally {
      running = false
      if (queued) { queued = false; setTimeout(rebuild, 0) }
    }
  }

  for (const dir of WATCH_DIRS) {
    fs.watch(dir, { recursive: true }, () => {
      clearTimeout(timer)
      timer = setTimeout(rebuild, 150)
    })
  }
}

function reportBuildError(err) {
  if (err && Array.isArray(err.errors)) console.error(err.message)
  else console.error(err)
}

// ---------------------------------------------------------------------------

const isWatch = process.argv.includes('--watch')

try {
  if (isWatch) await watch()
  else await build()
} catch (err) {
  reportBuildError(err)
  process.exit(1)
}
