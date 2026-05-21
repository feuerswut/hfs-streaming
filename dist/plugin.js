exports.version = 0.10;
exports.description = "RTMP Live Streaming - ingest via RTMP (OBS etc.), serve HTTP-FLV with HTML5 player";
exports.apiRequired = 13;
exports.repo = "feuerswut/hfs-streaming";

exports.config = {
    rtmpPort: {
        type: 'number', defaultValue: 1935,
        helperText: "RTMP ingest port (point OBS / ffmpeg here)", xs: 6,
    },
    internalHttpPort: {
        type: 'number', defaultValue: 8979,
        helperText: "Internal NMS HTTP port (not exposed publicly – pick any free port)", xs: 6,
    },
    streamKey: {
        type: 'string', defaultValue: 'stream',
        helperText: "Stream key. In OBS set Stream Key to this value.", xs: 6,
    },
    gopCache: {
        type: 'boolean', defaultValue: true,
        helperText: "Cache last keyframe group so new viewers get a frame instantly", xs: 6,
    },
};

exports.changelog = [
    { version: 0.10, message: "Switched ingest to node-media-server (RTMP). HTTP-FLV output via mpegts.js player." },
];

// ─────────────────────────────────────────────────────────────────────────────
const http = require('http');

let _nms  = null;   // NodeMediaServer instance
let _port = 8979;   // kept in sync so proxy always knows where NMS listens

// ─────────────────────────────────────────────────────────────────────────────
exports.init = api => {

    const NodeMediaServer = require('node-media-server');

    const rtmpPort  = api.getConfig('rtmpPort')        || 1935;
    const httpPort  = api.getConfig('internalHttpPort')|| 8979;
    const streamKey = api.getConfig('streamKey')       || 'stream';
    const gopCache  = api.getConfig('gopCache')        ?? true;

    _port = httpPort;

    // ── Start NMS ────────────────────────────────────────────────────────────
    _nms = new NodeMediaServer({
        logType: 1,   // 1 = errors only; set 3 for verbose
        rtmp: {
            port:         rtmpPort,
            chunk_size:   60000,
            gop_cache:    gopCache,
            ping:         30,
            ping_timeout: 60,
        },
        http: {
            port:         httpPort,
            allow_origin: '*',
        },
    });

    // Enforce stream key on every publish attempt
    _nms.on('prePublish', (id, streamPath /*, args */) => {
        const incomingKey = streamPath.split('/').pop();
        if (incomingKey !== streamKey) {
            api.log(`[streaming] rejected publish: bad key "${incomingKey}"`);
            const session = _nms.getSession(id);
            if (session) session.reject();
        }
    });

    _nms.run();
    api.log(`[streaming] RTMP listening on :${rtmpPort}  |  NMS HTTP on :${httpPort}`);
    api.log(`[streaming] OBS → rtmp://THIS_SERVER:${rtmpPort}/live/${streamKey}`);

    // ── Player HTML (inlined – no public/ file needed) ───────────────────────
    const playerHtml = buildPlayerHtml(streamKey);

    // ── HFS middleware ────────────────────────────────────────────────────────
    exports.middleware = async ctx => {

        // Match any path that contains /live (works behind reverse-proxies)
        const liveIdx = ctx.path.indexOf('/live');
        if (liveIdx === -1) return;

        const sub    = ctx.path.slice(liveIdx + 5);  // e.g. "" | "/stream"
        const method = ctx.method.toUpperCase();

        // GET /live  →  player page
        if (method === 'GET' && (sub === '' || sub === '/')) {
            ctx.status = 200;
            ctx.type   = 'text/html';
            ctx.body   = playerHtml;
            ctx.stop();
            return;
        }

        // GET /live/stream  →  proxy HTTP-FLV from NMS
        if (method === 'GET' && sub === '/stream') {
            const nmsUrl = `http://127.0.0.1:${_port}/live/${streamKey}.flv`;
            await proxyFlv(ctx, nmsUrl);
            ctx.stop();
            return;
        }

        // GET /live/health  →  quick JSON status
        if (method === 'GET' && sub === '/health') {
            ctx.type = 'application/json';
            ctx.set('Cache-Control', 'no-cache');
            ctx.body = JSON.stringify({ ok: true, rtmpPort, httpPort, streamKey });
            ctx.stop();
        }
    };

    return {
        unload() {
            if (_nms) {
                _nms.stop();
                _nms = null;
                api.log('[streaming] NMS stopped');
            }
        },
    };
};

// ─────────────────────────────────────────────────────────────────────────────
// Proxy NMS's HTTP-FLV response straight through to the HFS client.
// ctx.body = the NMS response stream; Koa will pipe it.
// ─────────────────────────────────────────────────────────────────────────────
function proxyFlv(ctx, url) {
    return new Promise(resolve => {
        const req = http.get(url, res => {
            ctx.status = res.statusCode || 200;
            ctx.type   = 'video/x-flv';
            ctx.set('Cache-Control',     'no-cache, no-store');
            ctx.set('Connection',        'keep-alive');
            ctx.set('X-Accel-Buffering', 'no');
            ctx.set('Access-Control-Allow-Origin', '*');

            ctx.body = res;                  // Koa pipes the stream
            ctx.req.on('close', resolve);    // client disconnected
            res.on('end', resolve);
        });

        req.on('error', () => {
            ctx.status = 503;
            ctx.type   = 'application/json';
            ctx.body   = JSON.stringify({ error: 'Stream not available – is someone ingesting?' });
            resolve();
        });
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// Build the viewer page.  mpegts.js decodes HTTP-FLV natively in the browser.
// ─────────────────────────────────────────────────────────────────────────────
function buildPlayerHtml(streamKey) {
    return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Live Stream</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            background: #1a1a1a;
            font-family: Arial, sans-serif;
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 100vh;
            color: #fff;
        }
        .container { width: 90%; max-width: 1000px; }
        video {
            width: 100%;
            background: #000;
            border-radius: 4px;
            display: block;
        }
        .info {
            margin-top: 20px;
            padding: 15px;
            background: #222;
            border-radius: 4px;
        }
        .info h2 { margin-bottom: 10px; font-size: 16px; }
        .info p  { font-size: 13px; color: #aaa; margin: 5px 0; }
    </style>
</head>
<body>
    <div class="container">
        <video id="player" controls muted></video>
        <div class="info">
            <h2>📺 Live Stream</h2>
            <p id="status">Connecting…</p>
            <p id="stream-url"></p>
        </div>
    </div>

    <!-- mpegts.js: browser-side HTTP-FLV / MPEG-TS demuxer -->
    <script src="https://cdn.jsdelivr.net/npm/mpegts.js@1/dist/mpegts.min.js"></script>
    <script>
        const video     = document.getElementById('player');
        const statusEl  = document.getElementById('status');
        const urlEl     = document.getElementById('stream-url');

        // Derive stream URL from current page location
        const streamUrl = window.location.pathname.replace(/\\/$/, '') + '/stream';
        urlEl.textContent = 'URL: ' + window.location.origin + streamUrl;

        if (!mpegts.isSupported()) {
            statusEl.textContent = '❌ mpegts.js is not supported in this browser.';
        } else {
            const player = mpegts.createPlayer({
                type: 'flv',
                isLive: true,
                url:  streamUrl,
            }, {
                enableWorker:          true,
                liveBufferLatencyChasing: true,
                liveBufferLatencyMaxLatency: 1.5,
                liveBufferLatencyMinRemain:  0.5,
            });

            player.attachMediaElement(video);
            player.load();

            player.on(mpegts.Events.ERROR, (type, detail) => {
                statusEl.textContent = '❌ Error – ' + detail + ' (is someone streaming?)';
            });

            video.addEventListener('playing', () => { statusEl.textContent = '🔴 LIVE'; });
            video.addEventListener('pause',   () => { statusEl.textContent = '⏸ PAUSED'; });
            video.addEventListener('waiting', () => { statusEl.textContent = '⏳ Buffering…'; });

            video.play().catch(() => {
                // Autoplay blocked – user must click play
                statusEl.textContent = '▶ Click play to start';
            });
        }
    </script>
</body>
</html>`;
}