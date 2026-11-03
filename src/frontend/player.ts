// ---------------------------------------------------------------------------
// player.ts -- HTTP-FLV playback via mpegts.js (loaded globally from a CDN
// <script> tag in player.html), with auto-reconnect and a small status UI.
//
// Config is not baked into this bundle at build time: player.html sets
// window.__STREAM_CONFIG__ from the {{STREAM_FLV_PATH}}/{{STREAM_APP}}/
// {{STREAM_KEY}} placeholders that plugin.js string-replaces at serve-time,
// and this script reads that object at boot. Keeping the substitution in the
// HTML (not in this compiled bundle) means the plugin's replace() calls keep
// working unchanged regardless of how the TS is bundled/minified.
// ---------------------------------------------------------------------------

// Minimal surface of the mpegts.js global we actually use -- the real
// library ships its own (much larger) type definitions we don't need here.
interface MpegtsPlayer {
    attachMediaElement(el: HTMLMediaElement): void
    on(event: string, cb: (...args: any[]) => void): void
    load(): void
    pause(): void
    unload(): void
    detachMediaElement(): void
    destroy(): void
}

interface MpegtsStatic {
    isSupported(): boolean
    createPlayer(mediaDataSource: Record<string, unknown>, config?: Record<string, unknown>): MpegtsPlayer
    Events: { ERROR: string; MEDIA_INFO: string }
}

declare const mpegts: MpegtsStatic

interface StreamConfig {
    flvPath: string
    app: string
    key: string
}

declare global {
    interface Window {
        __STREAM_CONFIG__: StreamConfig
    }
}

// The `declare global` augmentation above only applies inside a module; this
// empty export is what makes TypeScript treat the file as one (it has no
// other import/export -- esbuild still bundles it as a plain IIFE).
export {}

// ── DOM refs ──────────────────────────────────────────────────────────────
const video        = document.getElementById('video') as HTMLVideoElement
const overlay      = document.getElementById('overlay') as HTMLElement
const overlayIcon  = document.getElementById('overlay-icon') as HTMLElement
const overlayTitle = document.getElementById('overlay-title') as HTMLElement
const overlaySub   = document.getElementById('overlay-sub') as HTMLElement
const dot          = document.getElementById('dot') as HTMLElement
const statusText   = document.getElementById('status-text') as HTMLElement
const retryBadge   = document.getElementById('retry-badge') as HTMLElement
const streamUrlEl  = document.getElementById('stream-url') as HTMLElement

// Build the absolute stream URL from the injected path
const streamUrl = window.location.origin + window.__STREAM_CONFIG__.flvPath
streamUrlEl.textContent = streamUrl

// ── State ─────────────────────────────────────────────────────────────────
let player: MpegtsPlayer | null = null
let retryCount = 0
let retryTimer: ReturnType<typeof setTimeout> | undefined
let stalledTimer: ReturnType<typeof setTimeout> | undefined
const destroyed = false
const MAX_DELAY = 16000 // cap retry back-off at 16 s

// ── Helpers ───────────────────────────────────────────────────────────────
function setStatus(state: 'live' | 'waiting' | 'error' | '', text: string, sub?: string): void {
    dot.className = 'dot ' + state
    statusText.textContent = text
    if (sub !== undefined) overlaySub.textContent = sub
}

function showOverlay(icon: string, title: string, sub?: string): void {
    overlayIcon.textContent = icon
    overlayTitle.textContent = title
    if (sub !== undefined) overlaySub.textContent = sub
    overlay.classList.remove('hidden')
}

function hideOverlay(): void {
    overlay.classList.add('hidden')
}

function setRetry(n: number): void {
    if (n > 0) {
        retryBadge.textContent = `retry #${n}`
        retryBadge.classList.add('visible')
    } else {
        retryBadge.classList.remove('visible')
    }
}

// ── mpegts player lifecycle ─────────────────────────────────────────────
function destroyPlayer(): void {
    if (!player) return
    try {
        player.pause()
        player.unload()
        player.detachMediaElement()
        player.destroy()
    } catch (_) { /* ignore */ }
    player = null
}

function connect(): void {
    if (destroyed) return
    destroyPlayer()

    if (!mpegts.isSupported()) {
        showOverlay('❌', 'Browser not supported', 'mpegts.js requires MSE — try Chrome or Edge.')
        setStatus('error', 'Unsupported browser')
        return
    }

    setStatus('waiting', retryCount === 0 ? 'Connecting…' : `Reconnecting… (attempt ${retryCount})`)
    setRetry(retryCount)

    player = mpegts.createPlayer(
        {
            type:   'flv',
            isLive: true,
            url:    streamUrl,
        },
        {
            enableWorker:                true,
            liveBufferLatencyChasing:    true,
            liveBufferLatencyMaxLatency: 2.0,
            liveBufferLatencyMinRemain:  0.5,
            fixAudioTimestampGap:        true,
        },
    )

    player.attachMediaElement(video)

    // ── mpegts events ────────────────────────────────────────────────────
    player.on(mpegts.Events.ERROR, (errType, errDetail, errInfo) => {
        console.warn('[player] error', errType, errDetail, errInfo)
        scheduleRetry()
    })

    player.on(mpegts.Events.MEDIA_INFO, () => {
        // Media info received means NMS accepted the FLV play request
        dbgLog('media info received')
    })

    // ── HTML5 video events ───────────────────────────────────────────────
    video.addEventListener('playing', onPlaying)
    video.addEventListener('waiting', onWaiting)
    video.addEventListener('pause',   onPause)
    video.addEventListener('error',   onVideoError)
    video.addEventListener('ended',   () => scheduleRetry())
    video.addEventListener('stalled', onStalled)

    player.load()
    video.play().catch(() => {
        setStatus('', '▶ Click to play')
        showOverlay('▶', 'Click to play', 'Autoplay was blocked by the browser')
        overlay.style.cursor = 'pointer'
        overlay.addEventListener('click', () => {
            overlay.style.cursor = ''
            video.play().catch(() => { /* ignore */ })
        }, { once: true })
    })
}

function onPlaying(): void {
    hideOverlay()
    setStatus('live', '🔴 LIVE')
    setRetry(0)
    retryCount = 0
    clearTimeout(retryTimer)
}

function onWaiting(): void {
    setStatus('waiting', '⏳ Buffering…')
}

function onPause(): void {
    if (!video.ended) setStatus('', '⏸ Paused')
}

function onVideoError(): void {
    console.warn('[player] video element error', video.error)
    scheduleRetry()
}

function onStalled(): void {
    clearTimeout(stalledTimer)
    stalledTimer = setTimeout(() => {
        if (video.paused || video.ended) return
        dbgLog('stalled too long, reconnecting')
        scheduleRetry(true)
    }, 8000)
}

function scheduleRetry(immediate?: boolean): void {
    clearTimeout(retryTimer)
    clearTimeout(stalledTimer)
    if (destroyed) return

    destroyPlayer()
    retryCount++
    const delay = immediate ? 1500 : Math.min(1500 * Math.pow(1.6, retryCount - 1), MAX_DELAY)

    showOverlay('📡', 'Stream offline or interrupted', `Retrying in ${Math.round(delay / 1000)}s… (attempt ${retryCount})`)
    setStatus('error', `Waiting for stream… (retry #${retryCount} in ${Math.round(delay / 1000)}s)`)

    retryTimer = setTimeout(connect, delay)
}

function dbgLog(..._args: unknown[]): void {
    // Uncomment to enable client-side debug output:
    // console.log('[streaming]', ..._args)
}

// ── Boot ──────────────────────────────────────────────────────────────────
connect()
