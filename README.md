# HFS Streaming Plugin - Simple Guide

## What Does This Do?

This plugin lets you **stream live video** through HFS (like Netflix, but you control it).

Think of it like:
- **OBS or your camera** = The person talking
- **HFS Streaming Plugin** = The phone line
- **Viewers** = People listening on the other end

## How to Use It

### Step 1: Put the Files in HFS
Copy the `plugin.js` file to your HFS plugins folder
or install via the plugins panel: feuerswut/hfs-streaming

### Step 2: Turn It On
In HFS admin panel:
1. Go to "Plugins"
2. Find "Streaming"
3. Turn it on

### Step 3: Send Video to It
**From OBS:**
1. Open OBS
2. Settings → Output
3. Set server to: `http://localhost:3000/live/ingest` (use your appropriate server url and port)
4. Click "Start Streaming"

**From FFmpeg (command line):**
```bash
ffmpeg -f dshow -i video="Your Camera" -f mpegts http://localhost:3000/live/ingest
```

### Step 4: Watch the Stream
In VLC or any video player:
```
http://localhost:3000/live/stream
```

## Configuration (Optional)

In HFS admin panel under "Streaming":

| Setting | What it does | Default |
|---------|------------|---------|
| **rtmpPort** | TCP port OBS / ffmpeg pushes RTMP to | 1935 |
| **streamApp** | RTMP application name (the part of the URL right after the host:port) | live |
| **streamKey** | Only this key is allowed to publish | stream |
| **nmsLogLevel** | Node-Media-Server internal log verbosity | error |
| **debug** | Log plugin-level events (connects, disconnects, key checks) | off |

## Check if It's Working

Open this in your browser:
```
http://localhost:3000/live/health
```

You'll see JSON with whether the stream is currently live, plus the active rtmpPort/streamApp/streamKey.

## Simple Example

**Your setup:**
- OBS on your computer
- HFS running on same computer
- A friend on another computer

**What to do:**
1. Start streaming in OBS to: `http://localhost:3000/live/ingest`
2. Send your friend this link: `http://your-computer-ip:3000/live/stream`
3. Your friend opens the link in VLC or web browser
4. They watch your stream! 🎥

## Troubleshooting

### "Connection Failed" in OBS
- Is HFS running?
- Is the port 3000 open?
- Try: `http://localhost:3000` in browser

### No one can connect
- You need to give them your computer's IP, not `localhost`
- Example: `http://192.168.1.100:3000/live/stream`

### Stream keeps stopping
- Buffer might be full → increase `maxBufferSize` to 300 or 400

## That's It!

Really, it's that simple. Video goes in one end, viewers watch on the other end.

Need help? Check the main `README.md` for advanced stuff.
