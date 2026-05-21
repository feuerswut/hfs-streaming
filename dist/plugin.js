// HFS v3 Streaming Plugin - MPEG-TS Multicast Streaming
exports.version = 0.3;
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
    { "version": 0.1, "message": "BETA: HTML player, streaming keys, debug logging" }
];

const path = require('path');
const fs = require('fs');
const { EventEmitter } = require('events');

let DEBUG = false;

function log(...args) {
    if (DEBUG) {
        console.log('[HFS-Streaming]', new Date().toISOString().split('T')[1], ...args);
    }
}

/**
 * Serve static files from public directory
 */
function serveStatic(ctx, filePath) {
    try {
        const full     = filePath || path.join(__dirname, 'public', 'player.html');
        const resolved = path.resolve(full);

        // Security: prevent directory traversal
        if (!resolved.startsWith(path.resolve(__dirname) + path.sep) &&
             resolved !== path.resolve(__dirname)) {
            ctx.status = 403;
            ctx.type = 'text/plain';
            ctx.body = 'Forbidden';
            ctx.stop();
            return;
        }

        if (!fs.existsSync(resolved)) {
            ctx.status = 404;
            ctx.type = 'text/plain';
            ctx.body = 'Not found';
            ctx.stop();
            return;
        }

        const types = {
            '.html': 'text/html; charset=utf-8',
            '.css': 'text/css',
            '.js': 'application/javascript',
            '.json': 'application/json',
            '.png': 'image/png',
            '.svg': 'image/svg+xml',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
        };
        
        ctx.type = types[path.extname(resolved)] || 'text/plain';
        ctx.set('Cache-Control', 'no-cache');
        ctx.body = ctx.type.startsWith('image/') ? fs.createReadStream(resolved) : fs.readFileSync(resolved, 'utf8');
        ctx.stop();
    } catch (err) {
        ctx.status = 500;
        ctx.type = 'text/plain';
        ctx.body = 'Error: ' + err.message;
        ctx.stop();
    }
}

/**
 * Circular buffer for RAM streaming
 */
class CircularStreamBuffer extends EventEmitter {
    constructor(maxSizeBytes) {
        super();
        this.maxSize = maxSizeBytes;
        this.buffer = Buffer.allocUnsafe(maxSizeBytes);
        this.chunks = [];
        this.currentOffset = 0;
        this.totalWritten = 0;
        log('Buffer created:', (maxSizeBytes / 1024 / 1024).toFixed(0) + 'MB');
    }

    write(data) {
        const dataSize = data.length;
        
        if (dataSize > this.maxSize) {
            const offset = dataSize - this.maxSize;
            return this.write(data.slice(offset));
        }

        if (this.currentOffset + dataSize > this.maxSize) {
            const firstPart = this.maxSize - this.currentOffset;
            data.copy(this.buffer, this.currentOffset, 0, firstPart);
            data.copy(this.buffer, 0, firstPart);
            this.currentOffset = dataSize - firstPart;
        } else {
            data.copy(this.buffer, this.currentOffset);
            this.currentOffset = (this.currentOffset + dataSize) % this.maxSize;
        }

        this.totalWritten += dataSize;
        this.chunks.push({
            offset: this.currentOffset,
            size: dataSize,
            timestamp: Date.now(),
            sequence: this.totalWritten
        });

        this.chunks = this.chunks.filter(chunk => 
            chunk.sequence > (this.totalWritten - this.maxSize)
        );

        this.emit('data', data);
    }

    getBufferedData() {
        if (this.chunks.length === 0) return Buffer.alloc(0);
        
        let result = [];
        for (const chunk of this.chunks) {
            if (chunk.offset + chunk.size <= this.buffer.length) {
                result.push(this.buffer.slice(chunk.offset, chunk.offset + chunk.size));
            } else {
                const firstPart = this.buffer.slice(chunk.offset);
                const secondPart = this.buffer.slice(0, (chunk.offset + chunk.size) % this.maxSize);
                result.push(Buffer.concat([firstPart, secondPart]));
            }
        }
        return Buffer.concat(result);
    }

    getSize() {
        return this.chunks.reduce((sum, chunk) => sum + chunk.size, 0);
    }

    clear() {
        this.chunks = [];
        this.currentOffset = 0;
        this.totalWritten = 0;
        log('Buffer cleared');
    }
}

/**
 * Stream manager - handles buffering and subscriber broadcast
 */
class StreamManager extends EventEmitter {
    constructor(maxBufferMB, streamName) {
        super();
        this.streamName = streamName;
        this.buffer = new CircularStreamBuffer(maxBufferMB * 1024 * 1024);
        this.subscribers = new Set();
        this.ingestActive = false;
        this.ingestStartTime = null;
        this.stats = {
            bytesIngested: 0,
            bytesStreamed: 0,
            peakClients: 0,
            totalClients: 0,
        };
        log(`StreamManager: ${streamName}`);
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

    ingestData(data) {
        if (!this.ingestActive) {
            this.ingestActive = true;
            this.ingestStartTime = Date.now();
            log(`>> Ingest started: ${this.streamName}`);
        }
        
        this.stats.bytesIngested += data.length;
        this.buffer.write(data);
        this.broadcastToSubscribers(data);
    }

    broadcastToSubscribers(data) {
        let count = 0;
        for (const sub of this.subscribers) {
            try {
                sub.res.write(data);
                this.stats.bytesStreamed += data.length;
                count++;
            } catch (err) {
                log(`Broadcast error: ${err.message}`);
            }
        }
        return count;
    }

    stopIngest() {
        if (this.ingestActive) {
            const duration = Date.now() - this.ingestStartTime;
            log(`<< Ingest stopped: ${this.streamName} (${duration}ms)`);
            this.ingestActive = false;
        }
    }

    getStats() {
        return {
            ...this.stats,
            connectedClients: this.subscribers.size,
            bufferUsed: this.buffer.getSize(),
            bufferPercent: Math.round((this.buffer.getSize() / this.buffer.maxSize) * 100),
            ingestActive: this.ingestActive,
            uptime: this.ingestActive ? Date.now() - this.ingestStartTime : 0,
        };
    }

    clear() {
        this.buffer.clear();
        this.subscribers.clear();
        this.ingestActive = false;
    }
}

const streamManagers = new Map();

function getStreamManager(name, mb) {
    if (!streamManagers.has(name)) {
        streamManagers.set(name, new StreamManager(mb, name));
    }
    return streamManagers.get(name);
}

exports.init = async api => {
    const auth = api.require('./auth');
    const getCurrentUsername = auth.getCurrentUsername;
    const getGroupsForUser = auth.getGroupsForUser;

    DEBUG = api.getConfig('debug') || false;
    log('Plugin loaded');

    return { middleware };

    async function middleware(ctx) {
        const cfg = {
            maxBufferSize: api.getConfig('maxBufferSize') || 200,
            streamPath: api.getConfig('streamPath') || '/live',
            useSubhost: api.getConfig('useSubhost') || false,
            subhostName: api.getConfig('subhostName') || 'live',
            streamingKey: api.getConfig('streamingKey') || '',
            allowPublicIngest: api.getConfig('allowPublicIngest') || false,
            allowedIngestUsers: api.getConfig('allowedIngestUsers') || '',
            maxConnectedClients: api.getConfig('maxConnectedClients') || 100,
        };

        const url = ctx.req.url.split('?')[0];
        const host = ctx.get('host');
        const method = ctx.method;

        // Determine if this is a streaming request
        let isStreaming = false;
        let streamName = 'main';

        if (cfg.useSubhost) {
            const subdomain = host.split('.')[0];
            if (subdomain === cfg.subhostName) {
                isStreaming = true;
            }
        } else {
            if (url.startsWith(cfg.streamPath)) {
                isStreaming = true;
                const parts = url.substring(cfg.streamPath.length).split('/').filter(p => p);
                streamName = parts[0] || 'main';
            }
        }

        if (!isStreaming) return;

        log(`${method} ${url} | ${streamName}`);

        const manager = getStreamManager(streamName, cfg.maxBufferSize);
        const base = cfg.streamPath;

        // ============ STATIC FILES (HTML) ============
        if (url === base || url === base + '/') {
            return serveStatic(ctx);
        }

        if (url.startsWith(base + '/') && !url.includes('/stream') && !url.includes('/ingest') && method === 'GET') {
            const rel = url.slice(base.length + 1);
            if (rel && !rel.startsWith('api') && !rel.startsWith('stream') && !rel.startsWith('ingest')) {
                const filePath = path.join(__dirname, 'public', rel);
                return serveStatic(ctx, filePath);
            }
        }

        // ============ INGEST (POST/PUT) ============
        if (method === 'POST' || method === 'PUT') {
            log(`Ingest request`);

            // Check streaming key
            if (cfg.streamingKey) {
                if (ctx.query.key !== cfg.streamingKey) {
                    log(`Ingest DENIED: bad key`);
                    ctx.status = 401;
                    ctx.body = JSON.stringify({ error: 'Invalid key' });
                    ctx.type = 'application/json';
                    ctx.stop();
                    return;
                }
                log(`Ingest auth: key OK`);
            } else {
                // No key: check HFS auth
                const user = getCurrentUsername(ctx);
                
                if (!user && !cfg.allowPublicIngest) {
                    log(`Ingest DENIED: no auth`);
                    ctx.status = 403;
                    ctx.body = JSON.stringify({ error: 'Auth required' });
                    ctx.type = 'application/json';
                    ctx.stop();
                    return;
                }

                // Check user whitelist
                if (cfg.allowedIngestUsers && user) {
                    const allowed = cfg.allowedIngestUsers.split(',').map(s => s.trim()).filter(s => s);
                    if (allowed.length > 0) {
                        const groups = getGroupsForUser ? await getGroupsForUser(user) : [];
                        const ok = allowed.includes(user) || allowed.some(g => groups.includes(g));
                        
                        if (!ok) {
                            log(`Ingest DENIED: user ${user} not allowed`);
                            ctx.status = 403;
                            ctx.body = JSON.stringify({ error: 'Not allowed' });
                            ctx.type = 'application/json';
                            ctx.stop();
                            return;
                        }
                    }
                }

                log(`Ingest auth: user=${user}`);
            }

            // Accept ingest
            manager.startIngest();
            ctx.status = 200;
            ctx.type = 'application/octet-stream';

            ctx.req.on('data', chunk => {
                manager.ingestData(chunk);
            });

            ctx.req.on('end', () => manager.stopIngest());
            ctx.req.on('error', err => {
                log(`Ingest error: ${err.message}`);
                manager.stopIngest();
            });

            ctx.body = new Promise(() => {});
            ctx.stop();
            return;
        }

        // ============ STREAMING (GET) ============
        if (method === 'GET') {
            // Stream endpoint
            if (url === base + '/' + streamName + '/stream' || 
                (cfg.useSubhost && (url === '/' || url === ''))) {
                
                log(`Stream request`);
                
                if (cfg.maxConnectedClients > 0 && manager.subscribers.size >= cfg.maxConnectedClients) {
                    log(`Stream DENIED: max clients`);
                    ctx.status = 503;
                    ctx.body = JSON.stringify({ error: 'Max clients reached' });
                    ctx.type = 'application/json';
                    ctx.stop();
                    return;
                }

                ctx.type = 'video/mp2t';
                ctx.set('Cache-Control', 'no-cache, no-store, must-revalidate');
                ctx.set('Connection', 'keep-alive');
                ctx.status = 200;

                const buffered = manager.buffer.getBufferedData();
                if (buffered.length > 0) {
                    ctx.res.write(buffered);
                }

                const unsub = manager.addSubscriber(ctx);

                ctx.req.on('close', unsub);
                ctx.req.on('error', unsub);

                ctx.body = new Promise(() => {});
                ctx.stop();
                return;
            }

            // Stats
            if (url.includes('/stats')) {
                ctx.type = 'application/json';
                ctx.body = JSON.stringify(manager.getStats());
                ctx.stop();
                return;
            }

            // Health
            if (url.includes('/health')) {
                ctx.type = 'application/json';
                ctx.body = JSON.stringify({
                    ok: true,
                    streaming: manager.ingestActive,
                    clients: manager.subscribers.size
                });
                ctx.stop();
                return;
            }

            // Clear buffer
            if (url.includes('/clear')) {
                manager.clear();
                ctx.type = 'application/json';
                ctx.body = JSON.stringify({ ok: true });
                ctx.stop();
                return;
            }
        }
    }
};