// HFS v3 Streaming Plugin - MPEG-TS Multicast Streaming
exports.version = 0.1;
exports.description = "(BETA) MPEG-TS Streaming - Real-time multicast streaming to connected devices with RAM buffering";
exports.apiRequired = 8.65;

exports.author = "feuerswut";
exports.repo = "feuerswut/hfs-streaming";

exports.config = {
    maxBufferSize: {
        type: 'number',
        defaultValue: 200,
        helperText: "Maximum RAM buffer size in MB (default 200MB). Older fragments are discarded.",
        xs: 6,
    },
    streamPath: {
        type: 'string',
        defaultValue: '/live',
        helperText: "Base path for streaming. Use '/live' for /live or '/' for root path streaming.",
        xs: 6,
    },
    useSubhost: {
        type: 'boolean',
        defaultValue: false,
        helperText: "Enable subdomain mode (e.g., live.example.com). Requires DNS setup.",
        xs: 6,
    },
    subhostName: {
        type: 'string',
        defaultValue: 'live',
        helperText: "Subdomain name when useSubhost is enabled (e.g., 'live' for live.example.com).",
        xs: 6,
    },
    allowPublicIngest: {
        type: 'boolean',
        defaultValue: false,
        helperText: "Allow anonymous users to ingest streams. Requires allowPublicAccess for viewers.",
        xs: 6,
    },
    allowedIngestUsers: {
        type: 'string',
        defaultValue: '',
        helperText: "Comma-separated list of users/groups allowed to ingest (empty = all authenticated users).",
        xs: 6,
    },
    chunkDuration: {
        type: 'number',
        defaultValue: 2000,
        helperText: "TS chunk duration in milliseconds for buffering strategy.",
        xs: 6,
    },
    maxConnectedClients: {
        type: 'number',
        defaultValue: 100,
        helperText: "Maximum concurrent stream clients. 0 = unlimited.",
        xs: 6,
    },
};

exports.changelog = [
    { "version": 1.0, "message": "Initial release - MPEG-TS multicast streaming with RAM buffering" }
];

const path = require('path');
const fs = require('fs');
const { EventEmitter } = require('events');

/**
 * Circular buffer for RAM-based stream storage
 */
class CircularStreamBuffer extends EventEmitter {
    constructor(maxSizeBytes) {
        super();
        this.maxSize = maxSizeBytes;
        this.buffer = Buffer.allocUnsafe(maxSizeBytes);
        this.chunks = []; // Array of {offset, size, timestamp, data}
        this.currentOffset = 0;
        this.totalWritten = 0;
        this.startSequence = 0;
    }

    write(data) {
        const dataSize = data.length;
        
        // If data is larger than buffer, only keep the last part
        if (dataSize > this.maxSize) {
            const offset = dataSize - this.maxSize;
            return this.write(data.slice(offset));
        }

        // Check if we need to wrap around
        if (this.currentOffset + dataSize > this.maxSize) {
            // Write to end of buffer
            const firstPart = this.maxSize - this.currentOffset;
            data.copy(this.buffer, this.currentOffset, 0, firstPart);
            // Write remainder to start
            data.copy(this.buffer, 0, firstPart);
            this.currentOffset = dataSize - firstPart;
        } else {
            // Direct write
            data.copy(this.buffer, this.currentOffset);
            this.currentOffset = (this.currentOffset + dataSize) % this.maxSize;
        }

        this.totalWritten += dataSize;
        
        // Store chunk metadata
        this.chunks.push({
            offset: this.currentOffset,
            size: dataSize,
            timestamp: Date.now(),
            sequence: this.totalWritten
        });

        // Cleanup old chunks that are overwritten
        this.chunks = this.chunks.filter(chunk => {
            const chunkEnd = (chunk.offset + chunk.size) % this.maxSize;
            // Keep if chunk is still valid in the buffer
            return chunk.sequence > (this.totalWritten - this.maxSize);
        });

        this.emit('data', data);
    }

    getBufferedData() {
        // Return all valid buffered data in order
        if (this.chunks.length === 0) return Buffer.alloc(0);
        
        let result = [];
        for (const chunk of this.chunks) {
            if (chunk.offset + chunk.size <= this.buffer.length) {
                result.push(this.buffer.slice(chunk.offset, chunk.offset + chunk.size));
            } else {
                // Wrapped chunk
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
    }
}

/**
 * Streaming session manager
 */
class StreamManager extends EventEmitter {
    constructor(maxBufferMB) {
        super();
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
    }

    addSubscriber(ctx) {
        this.subscribers.add(ctx);
        this.stats.totalClients++;
        this.stats.peakClients = Math.max(this.stats.peakClients, this.subscribers.size);
        this.emit('subscriber-added', this.subscribers.size);
        return () => this.removeSubscriber(ctx);
    }

    removeSubscriber(ctx) {
        this.subscribers.delete(ctx);
        this.emit('subscriber-removed', this.subscribers.size);
    }

    ingestData(data) {
        if (!this.ingestActive) {
            this.ingestActive = true;
            this.ingestStartTime = Date.now();
        }
        this.stats.bytesIngested += data.length;
        this.buffer.write(data);
        this.broadcastToSubscribers(data);
    }

    broadcastToSubscribers(data) {
        let activeSubscribers = [];
        for (const subscriber of this.subscribers) {
            try {
                subscriber.res.write(data);
                this.stats.bytesStreamed += data.length;
                activeSubscribers.push(subscriber);
            } catch (err) {
                // Client disconnected, will be cleaned up
            }
        }
        return activeSubscribers.length;
    }

    startIngest() {
        this.ingestActive = true;
        this.ingestStartTime = Date.now();
    }

    stopIngest() {
        this.ingestActive = false;
    }

    getStats() {
        return {
            ...this.stats,
            connectedClients: this.subscribers.size,
            bufferUsed: this.buffer.getSize(),
            bufferUsedPercent: (this.buffer.getSize() / this.buffer.maxSize) * 100,
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

// Global stream managers (one per stream)
const streamManagers = new Map();

function getStreamManager(streamName, maxBufferMB) {
    if (!streamManagers.has(streamName)) {
        streamManagers.set(streamName, new StreamManager(maxBufferMB));
    }
    return streamManagers.get(streamName);
}

exports.init = async api => {
    const auth = api.require('./auth');
    const getCurrentUsername = auth.getCurrentUsername;
    const getGroupsForUser = auth.getGroupsForUser;

    return { middleware };

    async function middleware(ctx) {
        const config = {
            maxBufferSize: api.getConfig('maxBufferSize') || 200,
            streamPath: api.getConfig('streamPath') || '/live',
            useSubhost: api.getConfig('useSubhost') || false,
            subhostName: api.getConfig('subhostName') || 'live',
            allowPublicIngest: api.getConfig('allowPublicIngest') || false,
            allowedIngestUsers: api.getConfig('allowedIngestUsers') || '',
            chunkDuration: api.getConfig('chunkDuration') || 2000,
            maxConnectedClients: api.getConfig('maxConnectedClients') || 100,
        };

        const url = ctx.req.url;
        const host = ctx.get('host');

        // Determine if request is for streaming based on host or path
        let isStreamingRequest = false;
        let streamName = null;

        if (config.useSubhost) {
            // Check for subdomain mode (live.example.com)
            const hostParts = host.split('.');
            if (hostParts[0] === config.subhostName) {
                isStreamingRequest = true;
                streamName = 'main'; // Default stream name for subhost
            }
        } else {
            // Check for path mode (/live or configured path)
            if (url.startsWith(config.streamPath)) {
                isStreamingRequest = true;
                // Extract stream name from path: /live/stream1 -> stream1
                const pathParts = url.substring(config.streamPath.length).split('/').filter(p => p);
                streamName = pathParts[0] || 'main';
            }
        }

        if (!isStreamingRequest) {
            return; // Not a streaming request
        }

        const manager = getStreamManager(streamName, config.maxBufferSize);
        
        // Ingest endpoints (POST/PUT)
        if (ctx.method === 'POST' || ctx.method === 'PUT') {
            // Check ingest authorization
            const username = getCurrentUsername(ctx);
            
            if (!username && !config.allowPublicIngest) {
                ctx.status = 403;
                ctx.body = JSON.stringify({ error: 'Authentication required for ingest' });
                ctx.type = 'application/json';
                ctx.stop();
                return;
            }

            // Check if user is allowed to ingest
            if (config.allowedIngestUsers && username) {
                const allowedList = config.allowedIngestUsers
                    .split(',')
                    .map(s => s.trim())
                    .filter(s => s);
                
                if (allowedList.length > 0) {
                    const userGroups = getGroupsForUser ? await getGroupsForUser(username) : [];
                    const isAllowed = allowedList.includes(username) || 
                                    allowedList.some(allowed => userGroups.includes(allowed));
                    
                    if (!isAllowed) {
                        ctx.status = 403;
                        ctx.body = JSON.stringify({ error: 'User not allowed to ingest' });
                        ctx.type = 'application/json';
                        ctx.stop();
                        return;
                    }
                }
            }

            // Handle ingest endpoint
            if (url.endsWith('/ingest') || 
                (config.streamPath === '/' && url === '/ingest') ||
                (config.useSubhost && url === '/')) {
                
                manager.startIngest();
                ctx.type = 'application/octet-stream';
                ctx.status = 200;

                // Read stream data and buffer it
                ctx.req.on('data', (chunk) => {
                    manager.ingestData(chunk);
                });

                ctx.req.on('end', () => {
                    manager.stopIngest();
                });

                ctx.req.on('error', (err) => {
                    console.error('Ingest error:', err);
                    manager.stopIngest();
                });

                // Keep connection open
                ctx.body = new Promise(() => {});
                ctx.stop();
                return;
            }

            ctx.status = 404;
            ctx.stop();
            return;
        }

        // Stream viewing endpoints (GET)
        if (ctx.method === 'GET') {
            // Check view authorization
            const username = getCurrentUsername(ctx);
            
            // Serve main stream or specific stream
            const serveStream = (url.endsWith('/stream') || 
                               url === config.streamPath || 
                               url === config.streamPath + '/' ||
                               (config.useSubhost && url === '/') ||
                               (config.useSubhost && url === '/'));

            if (serveStream) {
                // Check client limit
                if (config.maxConnectedClients > 0 && manager.subscribers.size >= config.maxConnectedClients) {
                    ctx.status = 503;
                    ctx.body = JSON.stringify({ error: 'Maximum concurrent clients reached' });
                    ctx.type = 'application/json';
                    ctx.stop();
                    return;
                }

                ctx.type = 'video/mp2t';
                ctx.set('Cache-Control', 'no-cache, no-store, must-revalidate');
                ctx.set('Connection', 'keep-alive');
                ctx.set('Content-Type', 'video/mp2t');
                ctx.status = 200;

                // Send buffered data first
                const buffered = manager.buffer.getBufferedData();
                if (buffered.length > 0) {
                    ctx.res.write(buffered);
                }

                // Subscribe to new data
                const unsubscribe = manager.addSubscriber(ctx);

                ctx.req.on('close', () => {
                    unsubscribe();
                });

                ctx.req.on('error', () => {
                    unsubscribe();
                });

                // Keep connection open
                ctx.body = new Promise(() => {});
                ctx.stop();
                return;
            }

            // Stats endpoint
            if (url.endsWith('/stats') || url === config.streamPath + '/stats') {
                const stats = manager.getStats();
                ctx.type = 'application/json';
                ctx.body = JSON.stringify(stats, null, 2);
                ctx.stop();
                return;
            }

            // Health check endpoint
            if (url.endsWith('/health') || url === config.streamPath + '/health') {
                ctx.type = 'application/json';
                ctx.body = JSON.stringify({
                    status: 'ok',
                    streaming: manager.ingestActive,
                    clients: manager.subscribers.size
                });
                ctx.stop();
                return;
            }

            // Clear buffer endpoint (admin only)
            if ((ctx.method === 'DELETE' || ctx.method === 'POST') && url.endsWith('/clear')) {
                // You can add admin check here if needed
                manager.clear();
                ctx.type = 'application/json';
                ctx.body = JSON.stringify({ message: 'Buffer cleared' });
                ctx.stop();
                return;
            }
        }

        // Default stream list endpoint
        if ((url === config.streamPath || url === config.streamPath + '/') && ctx.method === 'GET') {
            const streams = Array.from(streamManagers.entries()).map(([name, mgr]) => ({
                name,
                stats: mgr.getStats()
            }));

            ctx.type = 'application/json';
            ctx.body = JSON.stringify({
                streams,
                config: {
                    maxBufferSize: config.maxBufferSize,
                    streamPath: config.streamPath,
                    useSubhost: config.useSubhost,
                }
            }, null, 2);
            ctx.stop();
            return;
        }
    }
};
