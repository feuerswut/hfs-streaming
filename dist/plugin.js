exports.version = 0.3;
exports.description = "(BETA) MPEG-TS Streaming - multicast with RAM buffer, HTML5 player";
exports.apiRequired = 13;
exports.author = "feuerswut";
exports.repo = "feuerswut/hfs-streaming";

exports.config = {
    maxBufferSize: {
        type: 'number', defaultValue: 50,
        helperText: "Max RAM buffer in MB.", xs: 6,
    },
    streamingKey: {
        type: 'string', defaultValue: '',
        helperText: "Require ?key=VALUE for ingest. Leave empty to disable.", xs: 6,
    },
    allowPublicIngest: {
        type: 'boolean', defaultValue: false,
        helperText: "Allow anonymous ingest (when no streaming key set)", xs: 6,
    },
    allowedIngestUsers: {
        type: 'string', defaultValue: '',
        helperText: "Comma-separated users allowed to ingest (empty = all authenticated)", xs: 6,
    },
    maxConnectedClients: {
        type: 'number', defaultValue: 100,
        helperText: "Max viewers. 0 = unlimited.", xs: 6,
    },
    debug: {
        type: 'boolean', defaultValue: false,
        helperText: "Enable debug logging", xs: 6,
    },
};

exports.changelog = [
    { version: 0.6, message: "Fix startup crash: replaced pre-allocated circular buffer with dynamic chunk list" },
    { version: 0.5, message: "Simplified routing, working middleware pattern" },
];

const path = require('path');
const fs   = require('fs');

let DEBUG = false;
function dbg(...args) {
    if (DEBUG) console.log('[streaming]', new Date().toISOString().split('T')[1], ...args);
}

function jsonOk(ctx, payload) {
    ctx.type = 'application/json';
    ctx.set('Cache-Control', 'no-cache');
    ctx.body = JSON.stringify(payload);
    return true;
}
function jsonErr(ctx, status, msg) {
    ctx.status = status;
    ctx.type   = 'application/json';
    ctx.body   = JSON.stringify({ error: msg });
    return true;
}

// ── Simple chunk-list buffer (no upfront allocation) ─────────────────────────
class StreamBuffer {
    constructor(maxSizeBytes) {
        this.maxSize  = maxSizeBytes;
        this.chunks   = [];
        this.byteSize = 0;
    }

    write(data) {
        this.chunks.push(data);
        this.byteSize += data.length;
        // Evict oldest chunks when over limit
        while (this.byteSize > this.maxSize && this.chunks.length > 0) {
            this.byteSize -= this.chunks.shift().length;
        }
    }

    getBufferedData() {
        return this.chunks.length ? Buffer.concat(this.chunks) : Buffer.alloc(0);
    }

    clear() {
        this.chunks   = [];
        this.byteSize = 0;
    }
}

// ── Stream manager ────────────────────────────────────────────────────────────
class StreamManager {
    constructor() {
        this.buffer          = null; // created lazily with config value
        this.subscribers     = new Set();
        this.ingestActive    = false;
        this.ingestStartTime = null;
        this.stats           = { bytesIngested: 0, bytesStreamed: 0, peakClients: 0, totalClients: 0 };
    }

    ensureBuffer(maxBufferMB) {
        if (!this.buffer) this.buffer = new StreamBuffer(maxBufferMB * 1024 * 1024);
    }

    addSubscriber(ctx) {
        this.subscribers.add(ctx);
        this.stats.totalClients++;
        this.stats.peakClients = Math.max(this.stats.peakClients, this.subscribers.size);
        dbg(`+ subscriber (total: ${this.subscribers.size})`);
        return () => { this.subscribers.delete(ctx); dbg(`- subscriber (total: ${this.subscribers.size})`); };
    }

    ingestData(chunk) {
        if (!this.ingestActive) {
            this.ingestActive    = true;
            this.ingestStartTime = Date.now();
            dbg('ingest started');
        }
        this.stats.bytesIngested += chunk.length;
        if (this.buffer) this.buffer.write(chunk);
        for (const sub of this.subscribers) {
            try {
                sub.res.write(chunk);
                this.stats.bytesStreamed += chunk.length;
            } catch (e) {
                dbg('broadcast error:', e.message);
                this.subscribers.delete(sub);
            }
        }
    }

    stopIngest() {
        if (this.ingestActive) {
            dbg('ingest stopped');
            this.ingestActive = false;
        }
    }

    getStats() {
        return {
            ...this.stats,
            connectedClients: this.subscribers.size,
            bufferUsed:       this.buffer?.byteSize ?? 0,
            bufferMax:        this.buffer?.maxSize   ?? 0,
            bufferPercent:    this.buffer ? Math.round((this.buffer.byteSize / this.buffer.maxSize) * 100) : 0,
            ingestActive:     this.ingestActive,
            uptime:           this.ingestActive ? Date.now() - this.ingestStartTime : 0,
        };
    }

    clear() {
        this.buffer?.clear();
        this.subscribers.clear();
        this.ingestActive = false;
    }
}

const manager = new StreamManager();

// ── Plugin init ───────────────────────────────────────────────────────────────
exports.init = api => {
    DEBUG = api.getConfig('debug') || false;

    let playerHtml;
    try {
        playerHtml = fs.readFileSync(path.join(__dirname, 'public', 'player.html'), 'utf8');
        dbg('player.html loaded');
    } catch (e) {
        api.log('Error loading player.html:', e.message);
    }

    exports.middleware = async ctx => {
        const liveIdx = ctx.path.indexOf('/live');
        if (liveIdx === -1) return;

        const sub    = ctx.path.slice(liveIdx + 5);
        const method = ctx.method.toUpperCase();

        dbg(`${method} ${ctx.path} sub="${sub}"`);

        // ── GET /live → player ────────────────────────────────────────────────
        if (method === 'GET' && (sub === '' || sub === '/')) {
            if (!playerHtml) return jsonErr(ctx, 500, 'player.html not found');
            ctx.status = 200;
            ctx.type   = 'text/html';
            ctx.body   = playerHtml;
            return true;
        }

        // ── GET /live/stats ───────────────────────────────────────────────────
        if (method === 'GET' && sub === '/stats') return jsonOk(ctx, manager.getStats());

        // ── GET /live/health ──────────────────────────────────────────────────
        if (method === 'GET' && sub === '/health')
            return jsonOk(ctx, { ok: true, streaming: manager.ingestActive, clients: manager.subscribers.size });

        // ── GET /live/clear ───────────────────────────────────────────────────
        if (method === 'GET' && sub === '/clear') {
            manager.clear();
            return jsonOk(ctx, { ok: true });
        }

        // ── POST/PUT → ingest ─────────────────────────────────────────────────
        if (method === 'POST' || method === 'PUT') {
            const streamingKey       = api.getConfig('streamingKey')       || '';
            const allowPublicIngest  = api.getConfig('allowPublicIngest')  ?? false;
            const allowedIngestUsers = api.getConfig('allowedIngestUsers') || '';

            if (streamingKey) {
                const qs  = ctx.req.url.includes('?') ? ctx.req.url.slice(ctx.req.url.indexOf('?') + 1) : '';
                const key = new URLSearchParams(qs).get('key');
                if (key !== streamingKey) { dbg('ingest denied: bad key'); return jsonErr(ctx, 401, 'Invalid streaming key'); }
                dbg('ingest auth: key ok');
            } else {
                const { getCurrentUsername } = api.require('./auth');
                const user = getCurrentUsername(ctx);
                if (!user && !allowPublicIngest) return jsonErr(ctx, 403, 'Authentication required');
                if (user && allowedIngestUsers) {
                    const allowed = allowedIngestUsers.split(',').map(s => s.trim()).filter(Boolean);
                    if (allowed.length && !allowed.includes(user)) return jsonErr(ctx, 403, 'User not allowed to ingest');
                }
                dbg(`ingest auth: user=${user || '(anonymous)'}`);
            }

            manager.ensureBuffer(api.getConfig('maxBufferSize') || 50);

            ctx.status = 200;
            ctx.type   = 'application/octet-stream';
            ctx.set('Cache-Control', 'no-cache');
            ctx.req.on('data',  chunk => manager.ingestData(chunk));
            ctx.req.on('end',   ()    => manager.stopIngest());
            ctx.req.on('error', err   => { dbg('ingest error:', err.message); manager.stopIngest(); });
            ctx.body = new Promise(resolve => { ctx.req.on('close', resolve); ctx.req.on('end', resolve); });
            ctx.stop();
            return;
        }

        // ── GET /live/stream (or anything else) → serve stream ────────────────
        if (method === 'GET') {
            const max = api.getConfig('maxConnectedClients') ?? 100;
            if (max > 0 && manager.subscribers.size >= max) return jsonErr(ctx, 503, 'Maximum viewers reached');

            manager.ensureBuffer(api.getConfig('maxBufferSize') || 50);

            ctx.status = 200;
            ctx.type   = 'video/mp2t';
            ctx.set('Cache-Control',     'no-cache, no-store, must-revalidate');
            ctx.set('Connection',        'keep-alive');
            ctx.set('X-Accel-Buffering', 'no');

            const buffered = manager.buffer.getBufferedData();
            if (buffered.length > 0) {
                dbg(`sending ${buffered.length} buffered bytes`);
                ctx.res.write(buffered);
            }

            const unsub = manager.addSubscriber(ctx);
            ctx.req.on('close', unsub);
            ctx.req.on('error', err => { dbg('subscriber error:', err.message); unsub(); });
            ctx.body = new Promise(resolve => { ctx.req.on('close', resolve); ctx.req.on('error', resolve); });
            ctx.stop();
        }
    };
};