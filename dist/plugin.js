exports.version = 0.6;
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
    debug: {
        type: 'boolean', defaultValue: false,
        helperText: "Log ingest events (accepted / rejected publishes)", xs: 6,
    },
};

exports.changelog = [
    { version: 0.6, message: "Switched ingest to node-media-server (RTMP). HTTP-FLV output via mpegts.js player." },
];

// ─────────────────────────────────────────────────────────────────────────────
const http = require('http');
const path = require('path');
const fs   = require('fs');

let _nms   = null;   // NodeMediaServer instance
let _port  = 8979;   // kept in sync so proxy always knows where NMS listens
let _debug = false;  // toggled from config

function dbg(api, ...args) {
    if (_debug) api.log('[streaming]', ...args);
}

// ─────────────────────────────────────────────────────────────────────────────
exports.init = api => {

    const NodeMediaServer = require('node-media-server');

    const rtmpPort  = api.getConfig('rtmpPort')         || 1935;
    const httpPort  = api.getConfig('internalHttpPort') || 8979;
    const streamKey = api.getConfig('streamKey')        || 'stream';
    const gopCache  = api.getConfig('gopCache')         ?? true;
    _debug          = api.getConfig('debug')            ?? false;
    _port           = httpPort;

    // ── Load player.html from public/ ────────────────────────────────────────
    const playerHtmlPath = path.join(__dirname, 'public', 'player.html');
    let playerHtml = null;
    try {
        playerHtml = fs.readFileSync(playerHtmlPath, 'utf8');
        api.log('[streaming] player.html loaded from public/player.html');
    } catch (e) {
        api.log('[streaming] WARNING: public/player.html not found – a notice will be served instead');
    }

    // ── Start NMS ────────────────────────────────────────────────────────────
    _nms = new NodeMediaServer({
        logType: 1,   // 1 = errors only
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

    // Enforce stream key; debug-log accepted ingests
    _nms.on('prePublish', (id, streamPath /*, args */) => {
        const incomingKey = streamPath.split('/').pop();
        if (incomingKey !== streamKey) {
            api.log(`[streaming] rejected publish: bad key "${incomingKey}"`);
            const session = _nms.getSession(id);
            if (session) session.reject();
        } else {
            dbg(api, `ingest accepted – stream path: ${streamPath}`);
        }
    });

    _nms.on('postPublish', (id, streamPath /*, args */) => {
        dbg(api, `ingest live (postPublish) – stream path: ${streamPath}`);
    });

    _nms.on('donePublish', (id, streamPath /*, args */) => {
        dbg(api, `ingest ended (donePublish) – stream path: ${streamPath}`);
    });

    _nms.run();
    api.log(`[streaming] RTMP listening on :${rtmpPort}  |  NMS HTTP on :${httpPort}`);
    api.log(`[streaming] OBS → rtmp://THIS_SERVER:${rtmpPort}/live/${streamKey}`);

    // ── HFS middleware ────────────────────────────────────────────────────────
    exports.middleware = async ctx => {

        // Match any path containing /live (works behind reverse-proxies)
        const liveIdx = ctx.path.indexOf('/live');
        if (liveIdx === -1) return;

        const sub    = ctx.path.slice(liveIdx + 5);  // e.g. "" | "/stream"
        const method = ctx.method.toUpperCase();

        // GET /live  →  player page (or missing-file notice)
        if (method === 'GET' && (sub === '' || sub === '/')) {
            ctx.status = 200;
            ctx.type   = 'text/html';
            ctx.body   = playerHtml !== null ? playerHtml : missingPlayerHtml();
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
// ─────────────────────────────────────────────────────────────────────────────
function proxyFlv(ctx, url) {
    return new Promise(resolve => {
        const req = http.get(url, res => {
            ctx.status = res.statusCode || 200;
            ctx.type   = 'video/x-flv';
            ctx.set('Cache-Control',              'no-cache, no-store');
            ctx.set('Connection',                 'keep-alive');
            ctx.set('X-Accel-Buffering',          'no');
            ctx.set('Access-Control-Allow-Origin','*');

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
// Served when public/player.html is absent.
// ─────────────────────────────────────────────────────────────────────────────
function missingPlayerHtml() {
    return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Player missing</title>
    <style>
        body { background:#1a1a1a; color:#e0e0e0; font-family:Arial,sans-serif;
               display:flex; justify-content:center; align-items:center; min-height:100vh; }
        .box { max-width:480px; padding:2rem; background:#222; border-radius:6px; text-align:center; }
        h2   { margin-bottom:1rem; color:#f08030; }
        code { background:#333; padding:2px 6px; border-radius:3px; font-size:0.9em; }
        p    { margin:0.6rem 0; line-height:1.5; color:#aaa; font-size:0.9em; }
    </style>
</head>
<body>
    <div class="box">
        <h2>⚠️ Player not found</h2>
        <p><code>public/player.html</code> is missing from the plugin folder.</p>
        <p>Create that file to serve your custom player here.<br>
           The stream itself is still available at <code>/live/stream</code>.</p>
    </div>
</body>
</html>`;
}