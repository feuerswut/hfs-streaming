exports.version = 0.9;
exports.description = "RTMP Live Streaming — ingest via RTMP (OBS / ffmpeg), serve HTTP-FLV with HTML5 player";
exports.apiRequired = 13;
exports.repo = "feuerswut/hfs-streaming";

exports.config = {
    rtmpPort: {
        type: 'number', defaultValue: 1935, label: 'RTMP Port',
        helperText: "TCP port OBS / ffmpeg pushes RTMP to. Restart plugin after changing.",
    },
    streamApp: {
        type: 'string', defaultValue: 'live', label: 'Stream App',
        helperText: "RTMP application name. In OBS → Stream → Server: rtmp://YOUR_HOST:PORT/<app>",
    },
    streamKey: {
        type: 'string', defaultValue: 'stream', label: 'Stream Key',
        helperText: "Only this key is allowed to publish. In OBS → Stream → Stream Key: <key>",
    },
    nmsLogLevel: {
        type: 'select', defaultValue: 'error', label: 'NMS Log Level',
        options: { 'Error (default)': 'error', 'Warn': 'warn', 'Info': 'info', 'Debug': 'debug', 'Trace': 'trace' },
        helperText: "Node-Media-Server internal log verbosity. Lines are captured to the HFS console.",
    },
    debug: {
        type: 'boolean', defaultValue: false, label: 'Plugin Debug Logging',
        helperText: "Log plugin-level events (connects, disconnects, key checks) to the HFS console.",
    },
};

exports.changelog = [
    { "version": 0.9, "message": "First WORKING version. Only use NMS code, dont run an instance of it." },
];

// ─────────────────────────────────────────────────────────────────────────────
const path = require('path');
const fs   = require('fs');

// NMS sub-module paths — resolved relative to this plugin's node_modules.
// We deliberately do NOT require the top-level 'node-media-server' package
// because its NodeHttpServer constructor unconditionally pulls in 'express',
// 'cors', and 'ws' which are not available in the HFS runtime.
// Instead we use only the RTMP server + Context, which depend purely on
// Node built-ins (net, crypto, querystring, …).
const NMS_BASE = path.join(__dirname, 'node_modules', 'node-media-server', 'src');

let _rtmpServer     = null;  // NodeRtmpServer instance
let _Context        = null;  // NMS Context singleton reference
let _debug          = false;
let _restoreConsole = null;  // cleanup fn for console intercept

// ─────────────────────────────────────────────────────────────────────────────
function dbg(api, ...args) {
    if (_debug) api.log('[streaming]', ...args);
}

// Intercept Node's console so NMS's internal log lines appear in the HFS
// output panel.  NMS writes via console.log directly.
// We wrap it, prepend [nms], forward to api.log, then call the original.
// Returns a restore function to be called on unload.
//
// NMS 4.x log format:  [<locale date string>] [LEVEL] message
// e.g. on de-DE:       [22.5.2026, 10:30:45] [INFO] Rtmp Server listening …
// The old regex /^\[\d{2}\/\d{2}\/\d{4}/ only matched en-GB/en-AU style dates.
// New regex matches any NMS log line regardless of locale.
function interceptConsole(api) {
    const origLog   = console.log;
    const origError = console.error;
    const origWarn  = console.warn;

    let isIntercepting = false;

    const NMS_LINE = /^\[.*?\] \[(?:TRACE|DEBUG|INFO|WARN|ERROR)\] /;

    const wrap = (orig, level) => (...args) => {
        orig(...args); // Always output to the real terminal

        if (isIntercepting) return;

        const line = args.map(a =>
            (typeof a === 'object' ? JSON.stringify(a) : String(a))
        ).join(' ');

        // Only intercept lines that look like NMS log output
        if (NMS_LINE.test(line)) {
            isIntercepting = true;
            try {
                api.log(`[nms${level}] ${line}`);
            } catch (_) {
            } finally {
                isIntercepting = false;
            }
        }
    };

    console.log   = wrap(origLog,   '');
    console.error = wrap(origError, ':error');
    console.warn  = wrap(origWarn,  ':warn');

    return () => {
        console.log   = origLog;
        console.error = origError;
        console.warn  = origWarn;
    };
}

// ─────────────────────────────────────────────────────────────────────────────
exports.init = api => {

    // ── Pull in only what we need from NMS (all use Node built-ins only) ─────
    const Context        = require(path.join(NMS_BASE, 'core',   'context.js'));
    const NodeRtmpServer = require(path.join(NMS_BASE, 'server', 'rtmp_server.js'));
    const logger         = require(path.join(NMS_BASE, 'core',   'logger.js'));

    _Context = Context;

    const rtmpPort    = api.getConfig('rtmpPort')    || 1935;
    const streamApp   = api.getConfig('streamApp')   || 'live';
    const streamKey   = api.getConfig('streamKey')   || 'stream';
    const nmsLogLevel = api.getConfig('nmsLogLevel') || 'error';
    _debug            = api.getConfig('debug')       ?? false;

    // ── Console intercept for NMS logs ────────────────────────────────────────
    _restoreConsole = interceptConsole(api);

    // ── Configure NMS Context (replaces old NodeMediaServer constructor) ──────
    logger.level    = nmsLogLevel;
    Context.config  = {
        rtmp: { port: rtmpPort },
        bind: '0.0.0.0',
        // HTTP intentionally omitted — we serve FLV directly from the HFS
        // middleware, so we never need NMS's express-based HTTP server.
    };

    // ── Start RTMP server ─────────────────────────────────────────────────────
    _rtmpServer = new NodeRtmpServer();
    _rtmpServer.run();
    api.log(`[streaming] RTMP listening on :${rtmpPort}`);
    api.log(`[streaming] OBS → rtmp://THIS_SERVER:${rtmpPort}/${streamApp}   Key: ${streamKey}`);

    // ── NMS event handlers (NMS 4.x always passes the session object) ─────────
    Context.eventEmitter.on('prePublish', session => {
        // session.streamApp and session.streamName are set in RtmpSession.onConnect
        if (session.streamApp !== streamApp || session.streamName !== streamKey) {
            api.log(`[streaming] rejected publish: bad app/key "${session.streamPath}" from ${session.ip}`);
            // session.close() ends the socket; NMS cleans up via the 'close' event
            try { session.close(); } catch (_) {}
        } else {
            dbg(api, `ingest accepted – ${session.streamPath} from ${session.ip}`);
        }
    });

    Context.eventEmitter.on('postPublish', session => {
        dbg(api, `ingest live – ${session.streamPath}`);
    });

    Context.eventEmitter.on('donePublish', session => {
        dbg(api, `ingest ended – ${session.streamPath}`);
    });

    // ── Load & prepare player.html ────────────────────────────────────────────
    // The file contains {{STREAM_FLV_PATH}}, {{STREAM_APP}}, {{STREAM_KEY}}
    // placeholders that we replace at startup so the player knows where to
    // connect. STREAM_FLV_PATH is the public HFS path (/live/stream).
    const playerHtmlPath = path.join(__dirname, 'public', 'player.html');
    let playerHtml = null;
    try {
        playerHtml = fs.readFileSync(playerHtmlPath, 'utf8')
            .replace('{{STREAM_FLV_PATH}}', '/live/stream')
            .replace('{{STREAM_APP}}',      streamApp)
            .replace('{{STREAM_KEY}}',      streamKey);
        api.log('[streaming] player.html loaded from public/player.html');
    } catch (e) {
        api.log('[streaming] WARNING: public/player.html not found — a notice will be served instead');
    }

    // ── HFS middleware ────────────────────────────────────────────────────────
    exports.middleware = async ctx => {

        // Match any path containing /live (works behind reverse-proxies)
        const liveIdx = ctx.path.indexOf('/live');
        if (liveIdx === -1) return;

        const sub    = ctx.path.slice(liveIdx + 5);  // e.g. "" | "/stream" | "/health"
        const method = ctx.method.toUpperCase();

        // ── GET /live  →  player page ─────────────────────────────────────────
        if (method === 'GET' && (sub === '' || sub === '/')) {
            ctx.status = 200;
            ctx.type   = 'text/html';
            ctx.body   = playerHtml !== null ? playerHtml : missingPlayerHtml();
            ctx.stop();
            return;
        }

        // ── GET /live/stream  →  direct HTTP-FLV from NMS BroadcastServer ────
        if (method === 'GET' && sub === '/stream') {
            const broadcastKey = `/${streamApp}/${streamKey}`;
            const broadcast    = Context.broadcasts.get(broadcastKey);

            if (!broadcast || !broadcast.publisher) {
                // Stream is offline — let the player retry
                ctx.status = 503;
                ctx.type   = 'application/json';
                ctx.set('Cache-Control', 'no-cache');
                ctx.body   = JSON.stringify({ error: 'Stream offline', key: broadcastKey });
                ctx.stop();
                return;
            }

            await serveFLV(ctx, broadcast, api);
            ctx.stop();
            return;
        }

        // ── GET /live/health  →  quick JSON status ────────────────────────────
        if (method === 'GET' && sub === '/health') {
            const broadcastKey = `/${streamApp}/${streamKey}`;
            const broadcast    = Context.broadcasts.get(broadcastKey);
            const live         = !!(broadcast && broadcast.publisher);
            ctx.type           = 'application/json';
            ctx.set('Cache-Control', 'no-cache');
            ctx.body = JSON.stringify({ ok: true, live, rtmpPort, streamApp, streamKey });
            ctx.stop();
        }
    };

    // ── Unload / cleanup ──────────────────────────────────────────────────────
    return {
        unload() {
            // 1. Remove all event listeners we registered so they don't fire
            //    on a stale api object after reload.
            Context.eventEmitter.removeAllListeners('prePublish');
            Context.eventEmitter.removeAllListeners('postPublish');
            Context.eventEmitter.removeAllListeners('donePublish');

            // 2. Gracefully close every active session (RTMP + any FLV subscribers
            //    that are internal NMS sessions — our custom HFS subscribers are
            //    cleaned up by their own 'close' event handler below).
            for (const session of Context.sessions.values()) {
                try { session.close(); } catch (_) {}
            }

            // 3. Shut down the RTMP TCP server.
            //    closeAllConnections() (Node 18.2+) immediately destroys open
            //    sockets so the port is released without waiting for keep-alive.
            if (_rtmpServer) {
                try { _rtmpServer.tcpServer?.close(); }           catch (_) {}
                try { _rtmpServer.tcpServer?.closeAllConnections?.(); } catch (_) {}
                try { _rtmpServer.tlsServer?.close(); }           catch (_) {}
                try { _rtmpServer.tlsServer?.closeAllConnections?.(); } catch (_) {}
                _rtmpServer = null;
            }

            // 4. Reset NMS Context so a future reload starts with a clean slate.
            Context.sessions.clear();
            Context.broadcasts.clear();
            _Context = null;

            // 5. Purge every NMS module from the require cache so the next
            //    require() call gets a fresh copy (including the Context singleton).
            const nmsRoot = path.join(__dirname, 'node_modules', 'node-media-server');
            for (const key of Object.keys(require.cache)) {
                if (key.startsWith(nmsRoot)) {
                    delete require.cache[key];
                }
            }

            // 6. Restore console methods.
            if (_restoreConsole) {
                _restoreConsole();
                _restoreConsole = null;
            }
        },
    };
};

// ─────────────────────────────────────────────────────────────────────────────
// Serve an HTTP-FLV live stream directly from the NMS BroadcastServer.
//
// Instead of reverse-proxying NMS's own HTTP server (which requires express /
// cors / ws), we register the HFS response as a lightweight FLV "subscriber"
// inside NMS's internal BroadcastServer.  postPlay() immediately sends the
// cached FLV header + GOP frames so late-joiners see a frame instantly; all
// subsequent packets are pushed via broadcastMessage → subscriber.sendBuffer().
//
// ctx.respond = false tells Koa not to touch the response after the middleware
// returns, leaving the connection open for the lifetime of the stream.
// ─────────────────────────────────────────────────────────────────────────────
function serveFLV(ctx, broadcast, api) {
    return new Promise(resolve => {
        const id = 'hfs-' + Date.now().toString(36) + Math.random().toString(36).slice(2, 6);

        // Minimal session object that satisfies BroadcastServer's subscriber API.
        const subscriber = {
            id,
            ip:          ctx.ip || '127.0.0.1',
            isPublisher: false,
            protocol:    'flv',
            endTime:     0,
            streamApp:   broadcast.publisher.streamApp,
            streamName:  broadcast.publisher.streamName,
            streamPath:  broadcast.publisher.streamPath,

            sendBuffer(buffer) {
                try {
                    if (!ctx.res.writableEnded && !ctx.res.destroyed) {
                        ctx.res.write(buffer);
                    }
                } catch (_) {
                    // Socket already gone; the 'close' event below handles cleanup.
                }
            },
        };

        // Take over the response — Koa must not try to finalise it after us.
        ctx.respond = false;
        ctx.res.writeHead(200, {
            'Content-Type':            'video/x-flv',
            'Cache-Control':           'no-cache, no-store',
            'Connection':              'keep-alive',
            'Transfer-Encoding':       'chunked',
            'X-Accel-Buffering':       'no',
            'Access-Control-Allow-Origin': '*',
        });

        // postPlay() synchronously writes the FLV header + any cached GOP frames
        // to our subscriber (via sendBuffer above), then adds us to subscribers map.
        const err = broadcast.postPlay(subscriber);
        if (err) {
            dbg(api, `postPlay rejected subscriber ${id}: ${err}`);
            try { ctx.res.end(); } catch (_) {}
            resolve();
            return;
        }

        dbg(api, `FLV subscriber ${id} connected for ${subscriber.streamPath} (ip: ${subscriber.ip})`);

        // Clean up when the client closes the connection.
        let cleaned = false;
        const cleanup = () => {
            if (cleaned) return;
            cleaned = true;
            dbg(api, `FLV subscriber ${id} disconnected`);
            try { broadcast.donePlay(subscriber); } catch (_) {}
        };

        ctx.req.on('close', cleanup);
        ctx.req.on('error', cleanup);
        ctx.res.on('error', cleanup);

        // Resolve now — the connection stays open via the subscriber until cleanup().
        resolve();
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
