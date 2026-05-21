exports.version = 0.1
exports.description = "Streaming Player"
exports.apiRequired = 13
exports.author = "feuerswut"
exports.repo = "feuerswut/hfs-streaming"

exports.init = api => {
    const fs = require('fs')
    const path = require('path')

    const html = fs.readFileSync(path.join(__dirname, 'public/player.html'), 'utf8')

    exports.middleware = ctx => {
        if (!ctx.path.endsWith('/live')) return
        ctx.status = 200
        ctx.type = 'text/html'
        ctx.body = html
        return true
    }
}