exports.version = 0.1;
exports.description = "(BETA) MPEG-TS Streaming";
exports.apiRequired = 8.65;
exports.author = "feuerswut";
exports.repo = "feuerswut/hfs-streaming";

exports.config = {
    streamPath: { type: 'string', defaultValue: '/live' },
};

const path = require('path');
const fs   = require('fs');

exports.init = async api => {
    return { middleware };

    async function middleware(ctx) {
        const base = (api.getConfig('streamPath') || '/live').replace(/\/+$/, '');
        const url  = ctx.req.url.split('?')[0];

        if (url !== base && !url.startsWith(base + '/')) return;

        const sub = url.slice(base.length);

        if (sub === '' || sub === '/') {
            const file = path.join(__dirname, 'public', 'player.html');
            if (!fs.existsSync(file)) {
                ctx.type = 'text/plain';
                ctx.body = 'File not found';
                ctx.stop();
                return;
            }
            ctx.type = 'text/html; charset=utf-8';
            ctx.set('Cache-Control', 'no-cache');
            ctx.body = fs.readFileSync(file, 'utf8');
            ctx.stop();
        }
    }
};