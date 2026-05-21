exports.version = 0.2;
exports.description = "(BETA) MPEG-TS Streaming - multicast with RAM buffer, HTML5 player";
exports.apiRequired = 13;
exports.author = "feuerswut";
exports.repo = "feuerswut/hfs-streaming";

exports.config = {
    maxBufferSize: {
        type: 'number', defaultValue: 200,
        helperText: "Max RAM buffer in MB. Older data discarded when full.", xs: 6,
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
    { version: 0.5, message: "Simplified routing, working middleware pattern" },
    { version: 0.4, message: "Fix ingest/stream routing, correct HFS API usage" },
];

const path = require('path');
const fs   = require('fs');
const { EventEmitter } = require('events');

// ── Logging ───────────────────────────────────────────────────────────────────
let DEBUG = false;
function dbg(...args) {
    if (DEBUG) console.log('[streaming]', new Date().toISOString().split('T')[1], ...args);
}

// ── Response helpers ──────────────────────────────────────────────────────────
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

// ── Circular buffer ───────────────────────────────────────────────────────────
class CircularStreamBuffer extends EventEmitter {
    constructor(maxSizeBytes) {
        super();
        this.maxSize      = maxSizeBytes;
        this.buffer       = Buffer.allocUnsafe(maxSizeBytes);
        this.chunks       = [];
        this.writeHead    = 0;
        this.totalWritten = 0;
    }

    write(data) {
        const size = data.length;
        if (size === 0) return;
        if (size > this.maxSize) return this.write(data.slice(size - this.maxSize));

        const start = this.writeHead;
        if (start + size <= this.maxSize) {
            data.copy(this.buffer, start);
        } else {
            const firstPart = this.maxSize - start;
            data.copy(this.buffer, start, 0, firstPart);
            data.copy(this.buffer, 0, firstPart);
        }

        this.writeHead    = (start + size) % this.maxSize;
        this.totalWritten += size;
        this.chunks.push({ writeStart: start, size, sequence: this.totalWritten });

        const oldest = this.totalWritten - this.maxSize;
        this.chunks = this.chunks.filter(c => c.sequence > oldest);

        this.emit('data', data);
    }

    getBufferedData() {
        if (this.chunks.length === 0) return Buffer.alloc(0);
        const parts = [];
        for (const chunk of this.chunks) {
            const { writeStart, size } = chunk;
            if (writeStart + size <= this.maxSize) {
                parts.push(this.buffer.slice(writeStart, writeStart + size));
            } else {
                parts.push(Buffer.concat([
                    this.buffer.slice(writeStart),
                    this.buffer.slice(0, (writeStart + size) % this.maxSize)
                ]));
            }
        }
        return Buffer.concat(parts);
    }

    getSize() { return this.chunks.reduce((s, c) => s + c.size, 0); }

    clear() {
        this.chunks = [];
        this.writeHead = 0;
        this.totalWritten = 0;
    }
}

// ── Stream manager ────────────────────────────────────────────────────────────
class StreamManager extends EventEmitter {
    constructor(maxBufferMB) {
        super();
        this.buffer          = new CircularStreamBuffer(maxBufferMB * 1024 * 1024);
        this.subscribers     = new Set();
        this.ingestActive    = false;
        this.ingestStartTime = null;
        this.stats           = { bytesIngested: 0, bytesStreamed: 0, peakClients: 0, totalClients: 0 };
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
        this.buffer.write(chunk);
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
    }
}

const manager = new StreamManager(200); // recreated on init with real config

// ── Plugin init ───────────────────────────────────────────────────────────────
exports.init = api => {
    const { getCurrentUsername } = api.require('./auth');

    DEBUG = api.getConfig('debug') || false;

    // Load player HTML once
    let playerHtml;
    try {
        playerHtml = fs.readFileSync(path.join(__dirname, 'public', 'player.html'), 'utf8');
        dbg('player.html loaded');
    } catch (e) {
        api.log('Error loading player.html:', e.message);
    }

    // Reinitialise manager with configured buffer size
    const bufMB = api.getConfig('maxBufferSize') || 200;
    manager.buffer = new CircularStreamBuffer(bufMB * 1024 * 1024);

    exports.middleware = async ctx => {
        // Find /live in the path (handles hostname-prefixed paths like /feuerswut.de/live)
        const liveIdx = ctx.path.indexOf('/live');
        if (liveIdx === -1) return;

        const sub    = ctx.path.slice(liveIdx + 5); // everything after /live  e.g. "" "/stream" "/stats"
        const method = ctx.method.toUpperCase();

        dbg(`${method} ${ctx.path} → sub="${sub}"`);

        // ── GET /live → player ────────────────────────────────────────────────
        if (method === 'GET' && (sub === '' || sub === '/')) {
            if (!playerHtml) return jsonErr(ctx, 500, 'player.html not found');
            ctx.status = 200;
            ctx.type   = 'text/html';
            ctx.body   = playerHtml;
            return true;
        }

        // ── GET /live/stats ───────────────────────────────────────────────────
        if (method === 'GET' && sub === '/stats') {
            return jsonOk(ctx, manager.getStats());
        }

        // ── GET /live/health ──────────────────────────────────────────────────
        if (method === 'GET' && sub === '/health') {
            return jsonOk(ctx, { ok: true, streaming: manager.ingestActive, clients: manager.subscribers.size });
        }

        // ── GET /live/clear ───────────────────────────────────────────────────
        if (method === 'GET' && sub === '/clear') {
            manager.clear();
            return jsonOk(ctx, { ok: true });
        }

        // ── POST/PUT /live → ingest ───────────────────────────────────────────
        if (method === 'POST' || method === 'PUT') {
            const cfg = {
                streamingKey:       api.getConfig('streamingKey')       || '',
                allowPublicIngest:  api.getConfig('allowPublicIngest')  ?? false,
                allowedIngestUsers: api.getConfig('allowedIngestUsers') || '',
            };

            if (cfg.streamingKey) {
                const qs  = ctx.req.url.includes('?') ? ctx.req.url.slice(ctx.req.url.indexOf('?') + 1) : '';
                const key = new URLSearchParams(qs).get('key');
                if (key !== cfg.streamingKey) {
                    dbg('ingest denied: bad key');
                    return jsonErr(ctx, 401, 'Invalid streaming key');
                }
                dbg('ingest auth: key ok');
            } else {
                const user = getCurrentUsername(ctx);
                if (!user && !cfg.allowPublicIngest) {
                    dbg('ingest denied: unauthenticated');
                    return jsonErr(ctx, 403, 'Authentication required');
                }
                if (user && cfg.allowedIngestUsers) {
                    const allowed = cfg.allowedIngestUsers.split(',').map(s => s.trim()).filter(Boolean);
                    if (allowed.length && !allowed.includes(user)) {
                        dbg(`ingest denied: user ${user} not allowed`);
                        return jsonErr(ctx, 403, 'User not allowed to ingest');
                    }
                }
                dbg(`ingest auth: user=${user || '(anonymous)'}`);
            }

            ctx.status = 200;
            ctx.type   = 'application/octet-stream';
            ctx.set('Cache-Control', 'no-cache');

            ctx.req.on('data',  chunk => manager.ingestData(chunk));
            ctx.req.on('end',   ()    => manager.stopIngest());
            ctx.req.on('error', err   => { dbg('ingest error:', err.message); manager.stopIngest(); });

            ctx.body = new Promise(resolve => {
                ctx.req.on('close', resolve);
                ctx.req.on('end',   resolve);
            });
            ctx.stop();
            return;
        }

        // ── GET /live/stream (or anything else under /live) → serve stream ────
        if (method === 'GET') {
            const max = api.getConfig('maxConnectedClients') ?? 100;
            if (max > 0 && manager.subscribers.size >= max) {
                return jsonErr(ctx, 503, 'Maximum viewers reached');
            }

            dbg(`stream subscribe (${manager.subscribers.size + 1} clients)`);

            ctx.status = 200;
            ctx.type   = 'video/mp2t';
            ctx.set('Cache-Control',     'no-cache, no-store, must-revalidate');
            ctx.set('Connection',        'keep-alive');
            ctx.set('X-Accel-Buffering', 'no');

            const buffered = manager.buffer.getBufferedData();
            if (buffered.length > 0) {
                dbg(`sending ${buffered.length} buffered bytes to new subscriber`);
                ctx.res.write(buffered);
            }

            const unsub = manager.addSubscriber(ctx);
            ctx.req.on('close', unsub);
            ctx.req.on('error', err => { dbg('subscriber error:', err.message); unsub(); });

            ctx.body = new Promise(resolve => {
                ctx.req.on('close', resolve);
                ctx.req.on('error', resolve);
            });
            ctx.stop();
        }
    };
};