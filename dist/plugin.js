// HFS v3 Streaming Plugin - MPEG-TS Multicast Streaming
exports.version = 0.4;
exports.description = "(BETA) MPEG-TS Streaming - multicast with RAM buffer, HTML5 player";
exports.apiRequired = 8.65;

exports.author = "feuerswut";
exports.repo = "feuerswut/hfs-streaming";

exports.config = {
    maxBufferSize: {
        type: 'number',
        defaultValue: 200,
        helperText: "Max RAM buffer in MB (default 200). Older data discarded when full.",
        xs: 6,
    },
    streamPath: {
        type: 'string',
        defaultValue: '/live',
        helperText: "Base path: /live or /",
        xs: 6,
    },
    useSubhost: {
        type: 'boolean',
        defaultValue: false,
        helperText: "Use subdomain mode (live.example.com) instead of path",
        xs: 6,
    },
    subhostName: {
        type: 'string',
        defaultValue: 'live',
        helperText: "Subdomain name (e.g., 'live' for live.example.com)",
        xs: 6,
    },
    streamingKey: {
        type: 'string',
        defaultValue: '',
        helperText: "Optional: require ?key=VALUE for ingest. Leave empty to disable.",
        xs: 6,
    },
    allowPublicIngest: {
        type: 'boolean',
        defaultValue: false,
        helperText: "Allow anonymous ingest (when no streaming key set)",
        xs: 6,
    },
    allowedIngestUsers: {
        type: 'string',
        defaultValue: '',
        helperText: "Comma-separated users/groups allowed to ingest (empty = all authenticated)",
        xs: 6,
    },
    maxConnectedClients: {
        type: 'number',
        defaultValue: 100,
        helperText: "Max viewers. 0 = unlimited.",
        xs: 6,
    },
    debug: {
        type: 'boolean',
        defaultValue: false,
        helperText: "Enable debug logging",
        xs: 6,
    },
};

exports.changelog = [
    { version: 0.3, message: "Fix ingest/stream routing, correct HFS API usage" },
    { version: 0.1, message: "BETA: HTML player, streaming keys, debug logging" },
];

const path = require('path');
const fs   = require('fs');
const { EventEmitter } = require('events');

let DEBUG = false;
function log(...args) {
    if (DEBUG) console.log('[HFS-Streaming]', new Date().toISOString().split('T')[1], ...args);
}

// ── JSON helpers ──────────────────────────────────────────────────────────────
function jsonOk(ctx, payload) {
    ctx.type = 'application/json';
    ctx.set('Cache-Control', 'no-cache');
    ctx.body = JSON.stringify(payload);
    ctx.stop();
}
function jsonErr(ctx, status, msg) {
    ctx.status = status;
    ctx.type   = 'application/json';
    ctx.body   = JSON.stringify({ success: false, error: msg });
    ctx.stop();
}

// ── Static file server ────────────────────────────────────────────────────────
function serveStatic(ctx, filePath) {
    try {
        const resolved = path.resolve(filePath || path.join(__dirname, 'public', 'player.html'));

        // Prevent directory traversal
        const pub = path.resolve(path.join(__dirname, 'public'));
        if (!resolved.startsWith(pub + path.sep) && resolved !== pub) {
            return jsonErr(ctx, 403, 'Forbidden');
        }
        if (!fs.existsSync(resolved)) {
            return jsonErr(ctx, 404, 'Not found');
        }

        const MIME = {
            '.html': 'text/html; charset=utf-8',
            '.css':  'text/css',
            '.js':   'application/javascript',
            '.json': 'application/json',
            '.png':  'image/png',
            '.svg':  'image/svg+xml',
            '.jpg':  'image/jpeg',
            '.jpeg': 'image/jpeg',
        };

        ctx.type = MIME[path.extname(resolved)] || 'application/octet-stream';
        ctx.set('Cache-Control', 'no-cache');
        ctx.body = ctx.type.startsWith('image/') || ctx.type === 'application/octet-stream'
            ? fs.createReadStream(resolved)
            : fs.readFileSync(resolved, 'utf8');
        ctx.stop();
    } catch (err) {
        jsonErr(ctx, 500, err.message);
    }
}

// ── Circular buffer ───────────────────────────────────────────────────────────
class CircularStreamBuffer extends EventEmitter {
    constructor(maxSizeBytes) {
        super();
        this.maxSize       = maxSizeBytes;
        this.buffer        = Buffer.allocUnsafe(maxSizeBytes);
        this.chunks        = [];         // [{writeStart, size, sequence}]
        this.writeHead     = 0;          // next write position in this.buffer
        this.totalWritten  = 0;
        log('Buffer created:', (maxSizeBytes / 1024 / 1024).toFixed(0) + 'MB');
    }

    write(data) {
        const size = data.length;
        if (size === 0) return;

        // If single chunk is bigger than the whole buffer, trim to tail
        if (size > this.maxSize) {
            return this.write(data.slice(size - this.maxSize));
        }

        const start = this.writeHead;

        if (start + size <= this.maxSize) {
            data.copy(this.buffer, start);
        } else {
            // Wrap around
            const firstPart = this.maxSize - start;
            data.copy(this.buffer, start, 0, firstPart);
            data.copy(this.buffer, 0,     firstPart);
        }

        this.writeHead    = (start + size) % this.maxSize;
        this.totalWritten += size;

        this.chunks.push({ writeStart: start, size, sequence: this.totalWritten });

        // Evict chunks that are now overwritten (sequence more than maxSize bytes old)
        const oldest = this.totalWritten - this.maxSize;
        this.chunks = this.chunks.filter(c => c.sequence > oldest);

        this.emit('data', data);
    }

    // Reconstruct all buffered data in order
    getBufferedData() {
        if (this.chunks.length === 0) return Buffer.alloc(0);

        const parts = [];
        for (const chunk of this.chunks) {
            const { writeStart, size } = chunk;
            if (writeStart + size <= this.maxSize) {
                parts.push(this.buffer.slice(writeStart, writeStart + size));
            } else {
                const tail = this.buffer.slice(writeStart);
                const head = this.buffer.slice(0, (writeStart + size) % this.maxSize);
                parts.push(Buffer.concat([tail, head]));
            }
        }
        return Buffer.concat(parts);
    }

    getSize()  { return this.chunks.reduce((s, c) => s + c.size, 0); }

    clear() {
        this.chunks      = [];
        this.writeHead   = 0;
        this.totalWritten = 0;
        log('Buffer cleared');
    }
}

// ── Stream manager ────────────────────────────────────────────────────────────
class StreamManager extends EventEmitter {
    constructor(maxBufferMB, streamName) {
        super();
        this.streamName      = streamName;
        this.buffer          = new CircularStreamBuffer(maxBufferMB * 1024 * 1024);
        this.subscribers     = new Set();
        this.ingestActive    = false;
        this.ingestStartTime = null;
        this.stats = { bytesIngested: 0, bytesStreamed: 0, peakClients: 0, totalClients: 0 };
        log('StreamManager created:', streamName);
    }

    addSubscriber(ctx) {
        this.subscribers.add(ctx);
        this.stats.totalClients++;
        this.stats.peakClients = Math.max(this.stats.peakClients, this.subscribers.size);
        log(`+ Subscriber: ${this.streamName} (total: ${this.subscribers.size})`);
        return () => this.removeSubscriber(ctx);
    }

    removeSubscriber(ctx) {
        this.subscribers.delete(ctx);
        log(`- Subscriber: ${this.streamName} (total: ${this.subscribers.size})`);
    }

    ingestData(chunk) {
        if (!this.ingestActive) {
            this.ingestActive    = true;
            this.ingestStartTime = Date.now();
            log('Ingest started:', this.streamName);
        }
        this.stats.bytesIngested += chunk.length;
        this.buffer.write(chunk);
        this._broadcast(chunk);
    }

    _broadcast(data) {
        for (const sub of this.subscribers) {
            try {
                sub.res.write(data);
                this.stats.bytesStreamed += data.length;
            } catch (err) {
                log('Broadcast error:', err.message);
                this.removeSubscriber(sub);
            }
        }
    }

    stopIngest() {
        if (this.ingestActive) {
            log('Ingest stopped:', this.streamName, `(${Date.now() - this.ingestStartTime}ms)`);
            this.ingestActive = false;
        }
    }

    getStats() {
        return {
            ...this.stats,
            connectedClients: this.subscribers.size,
            bufferUsed:       this.buffer.getSize(),
            bufferPercent:    Math.round((this.buffer.getSize() / this.buffer.maxSize) * 100),
            ingestActive:     this.ingestActive,
            uptime:           this.ingestActive ? Date.now() - this.ingestStartTime : 0,
        };
    }

    clear() {
        this.buffer.clear();
        this.subscribers.clear();
        this.ingestActive = false;
        log('StreamManager cleared:', this.streamName);
    }
}

const streamManagers = new Map();
function getStreamManager(name, mb) {
    if (!streamManagers.has(name)) streamManagers.set(name, new StreamManager(mb, name));
    return streamManagers.get(name);
}

// ── Plugin init ───────────────────────────────────────────────────────────────
exports.init = async api => {
    const { getCurrentUsername, getGroupsForUser } = api.require('./auth');

    DEBUG = api.getConfig('debug') || false;
    log('Plugin loaded');

    return { middleware };

    async function middleware(ctx) {
        // Read config fresh each call so live changes take effect
        const cfg = {
            maxBufferSize:       api.getConfig('maxBufferSize')       ?? 200,
            streamPath:          (api.getConfig('streamPath')         || '/live').replace(/\/+$/, '') || '/',
            useSubhost:          api.getConfig('useSubhost')          ?? false,
            subhostName:         api.getConfig('subhostName')         || 'live',
            streamingKey:        api.getConfig('streamingKey')        || '',
            allowPublicIngest:   api.getConfig('allowPublicIngest')   ?? false,
            allowedIngestUsers:  api.getConfig('allowedIngestUsers')  || '',
            maxConnectedClients: api.getConfig('maxConnectedClients') ?? 100,
        };

        const url    = ctx.req.url.split('?')[0];
        const host   = ctx.get('host') || '';
        const method = ctx.method.toUpperCase();
        const base   = cfg.streamPath;          // e.g. "/live"  (no trailing slash)

        // ── Decide whether this request belongs to the streaming plugin ──────
        let isStreaming = false;
        let streamName  = 'main';

        if (cfg.useSubhost) {
            const subdomain = host.split('.')[0];
            if (subdomain === cfg.subhostName) {
                isStreaming = true;
                // In subhost mode the stream name comes from the path
                const parts = url.split('/').filter(p => p);
                streamName  = parts[0] || 'main';
            }
        } else {
            if (url === base || url.startsWith(base + '/')) {
                isStreaming = true;
                // Strip base, split path  →  /live/mycam/stream  →  ["mycam","stream"]
                const rel   = url.slice(base.length).replace(/^\//, '');
                const parts = rel.split('/').filter(p => p);
                //  /live              →  parts = []         →  streamName = "main"
                //  /live/stream       →  parts = ["stream"] →  streamName = "main"
                //  /live/mycam        →  parts = ["mycam"]  →  streamName = "mycam"
                //  /live/mycam/stream →  parts = ["mycam","stream"]  →  streamName = "mycam"
                streamName  = (parts[0] && parts[0] !== 'stream' && parts[0] !== 'stats' &&
                               parts[0] !== 'health' && parts[0] !== 'clear')
                              ? parts[0]
                              : 'main';
            }
        }

        if (!isStreaming) return;  // not ours – let HFS handle it

        log(`${method} ${url} host=${host} stream=${streamName}`);

        const manager = getStreamManager(streamName, cfg.maxBufferSize);

        // ── Helper: true if the URL tail matches a keyword ───────────────────
        const tail = url.slice(base.length).replace(/^\//, '');   // strip base + leading /
        // tail examples: ""  "stream"  "mycam"  "mycam/stream"  "stats"  "mycam/stats"
        const tailParts  = tail.split('/').filter(p => p);
        const lastSeg    = tailParts[tailParts.length - 1] || '';

        // ── Static HTML player (GET / or GET /live) ──────────────────────────
        if (method === 'GET' && (tail === '' || tail === '/')) {
            return serveStatic(ctx);
        }

        // ── Static assets under /live/assets/... ────────────────────────────
        if (method === 'GET' && tailParts[0] === 'assets') {
            const safePath = path.join(__dirname, 'public', 'assets', ...tailParts.slice(1));
            return serveStatic(ctx, safePath);
        }

        // ════════════════════════════════════════════════════════════════════
        // INGEST  (POST or PUT to /live, /live/mycam, /live/ingest, etc.)
        // ════════════════════════════════════════════════════════════════════
        if (method === 'POST' || method === 'PUT') {
            log('Ingest attempt');

            // ── Auth: streaming key ─────────────────────────────────────────
            if (cfg.streamingKey) {
                const qs  = ctx.req.url.includes('?') ? ctx.req.url.slice(ctx.req.url.indexOf('?') + 1) : '';
                const key = new URLSearchParams(qs).get('key');
                if (key !== cfg.streamingKey) {
                    log('Ingest DENIED: bad key');
                    return jsonErr(ctx, 401, 'Invalid streaming key');
                }
                log('Ingest auth: key OK');
            } else {
                // ── Auth: HFS user ──────────────────────────────────────────
                const user = getCurrentUsername(ctx);

                if (!user && !cfg.allowPublicIngest) {
                    log('Ingest DENIED: no auth, public ingest disabled');
                    return jsonErr(ctx, 403, 'Authentication required');
                }

                if (user && cfg.allowedIngestUsers) {
                    const allowed = cfg.allowedIngestUsers.split(',').map(s => s.trim()).filter(Boolean);
                    if (allowed.length > 0) {
                        const groups = getGroupsForUser ? await getGroupsForUser(user) : [];
                        const ok     = allowed.includes(user) || allowed.some(g => groups.includes(g));
                        if (!ok) {
                            log(`Ingest DENIED: user ${user} not in allowedIngestUsers`);
                            return jsonErr(ctx, 403, 'User not allowed to ingest');
                        }
                    }
                }

                log(`Ingest auth: user=${user || '(anonymous)'}`);
            }

            // ── Accept the raw binary stream ────────────────────────────────
            ctx.status = 200;
            ctx.type   = 'application/octet-stream';
            ctx.set('Cache-Control', 'no-cache');

            ctx.req.on('data',  chunk => manager.ingestData(chunk));
            ctx.req.on('end',   ()    => manager.stopIngest());
            ctx.req.on('error', err   => { log('Ingest req error:', err.message); manager.stopIngest(); });

            // Keep the response open until the sender closes the connection.
            // We write the response manually, so give HFS an unresolved promise
            // as the body so it doesn't finalise the response prematurely.
            ctx.body = new Promise(resolve => {
                ctx.req.on('close', resolve);
                ctx.req.on('end',   resolve);
            });
            ctx.stop();
            return;
        }

        // ════════════════════════════════════════════════════════════════════
        // GET endpoints
        // ════════════════════════════════════════════════════════════════════
        if (method !== 'GET') return;  // nothing else to handle

        // ── /live/[name/]stats ───────────────────────────────────────────────
        if (lastSeg === 'stats') {
            return jsonOk(ctx, manager.getStats());
        }

        // ── /live/[name/]health ──────────────────────────────────────────────
        if (lastSeg === 'health') {
            return jsonOk(ctx, { ok: true, streaming: manager.ingestActive, clients: manager.subscribers.size });
        }

        // ── /live/[name/]clear ───────────────────────────────────────────────
        if (lastSeg === 'clear') {
            manager.clear();
            return jsonOk(ctx, { ok: true });
        }

        // ── /live/[name/]stream  ─OR─  /live  ─OR─  /live/[name] ────────────
        // Everything else at or under this base is treated as "give me the stream".
        // Clients will connect to /live/stream  or  /live/mycam/stream  or even
        // just /live  to receive the MPEG-TS bytes.
        {
            if (cfg.maxConnectedClients > 0 && manager.subscribers.size >= cfg.maxConnectedClients) {
                log('Stream DENIED: max clients reached');
                return jsonErr(ctx, 503, 'Maximum number of viewers reached');
            }

            log(`Stream subscribe: ${streamName} (${manager.subscribers.size + 1} clients)`);

            ctx.status = 200;
            ctx.type   = 'video/mp2t';
            ctx.set('Cache-Control', 'no-cache, no-store, must-revalidate');
            ctx.set('Connection',    'keep-alive');
            ctx.set('X-Accel-Buffering', 'no');   // tell nginx not to buffer this

            // Send whatever is already in the buffer so the player can sync
            const buffered = manager.buffer.getBufferedData();
            if (buffered.length > 0) {
                log(`Sending ${buffered.length} buffered bytes to new subscriber`);
                ctx.res.write(buffered);
            }

            const unsub = manager.addSubscriber(ctx);

            ctx.req.on('close', unsub);
            ctx.req.on('error', err => { log('Subscriber error:', err.message); unsub(); });

            // Keep response alive until the client disconnects
            ctx.body = new Promise(resolve => {
                ctx.req.on('close', resolve);
                ctx.req.on('error', resolve);
            });
            ctx.stop();
        }
    }
};