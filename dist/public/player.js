"use strict";
(() => {
  // src/frontend/player.ts
  var video = document.getElementById("video");
  var overlay = document.getElementById("overlay");
  var overlayIcon = document.getElementById("overlay-icon");
  var overlayTitle = document.getElementById("overlay-title");
  var overlaySub = document.getElementById("overlay-sub");
  var dot = document.getElementById("dot");
  var statusText = document.getElementById("status-text");
  var retryBadge = document.getElementById("retry-badge");
  var streamUrlEl = document.getElementById("stream-url");
  var streamUrl = window.location.origin + window.__STREAM_CONFIG__.flvPath;
  streamUrlEl.textContent = streamUrl;
  var player = null;
  var retryCount = 0;
  var retryTimer;
  var stalledTimer;
  var destroyed = false;
  var MAX_DELAY = 16e3;
  function setStatus(state, text, sub) {
    dot.className = "dot " + state;
    statusText.textContent = text;
    if (sub !== void 0) overlaySub.textContent = sub;
  }
  function showOverlay(icon, title, sub) {
    overlayIcon.textContent = icon;
    overlayTitle.textContent = title;
    if (sub !== void 0) overlaySub.textContent = sub;
    overlay.classList.remove("hidden");
  }
  function hideOverlay() {
    overlay.classList.add("hidden");
  }
  function setRetry(n) {
    if (n > 0) {
      retryBadge.textContent = `retry #${n}`;
      retryBadge.classList.add("visible");
    } else {
      retryBadge.classList.remove("visible");
    }
  }
  function destroyPlayer() {
    if (!player) return;
    try {
      player.pause();
      player.unload();
      player.detachMediaElement();
      player.destroy();
    } catch (_) {
    }
    player = null;
  }
  function connect() {
    if (destroyed) return;
    destroyPlayer();
    if (!mpegts.isSupported()) {
      showOverlay("❌", "Browser not supported", "mpegts.js requires MSE — try Chrome or Edge.");
      setStatus("error", "Unsupported browser");
      return;
    }
    setStatus("waiting", retryCount === 0 ? "Connecting…" : `Reconnecting… (attempt ${retryCount})`);
    setRetry(retryCount);
    player = mpegts.createPlayer(
      {
        type: "flv",
        isLive: true,
        url: streamUrl
      },
      {
        enableWorker: true,
        liveBufferLatencyChasing: true,
        liveBufferLatencyMaxLatency: 2,
        liveBufferLatencyMinRemain: 0.5,
        fixAudioTimestampGap: true
      }
    );
    player.attachMediaElement(video);
    player.on(mpegts.Events.ERROR, (errType, errDetail, errInfo) => {
      console.warn("[player] error", errType, errDetail, errInfo);
      scheduleRetry();
    });
    player.on(mpegts.Events.MEDIA_INFO, () => {
      dbgLog("media info received");
    });
    video.addEventListener("playing", onPlaying);
    video.addEventListener("waiting", onWaiting);
    video.addEventListener("pause", onPause);
    video.addEventListener("error", onVideoError);
    video.addEventListener("ended", () => scheduleRetry());
    video.addEventListener("stalled", onStalled);
    player.load();
    video.play().catch(() => {
      setStatus("", "▶ Click to play");
      showOverlay("▶", "Click to play", "Autoplay was blocked by the browser");
      overlay.style.cursor = "pointer";
      overlay.addEventListener("click", () => {
        overlay.style.cursor = "";
        video.play().catch(() => {
        });
      }, { once: true });
    });
  }
  function onPlaying() {
    hideOverlay();
    setStatus("live", "🔴 LIVE");
    setRetry(0);
    retryCount = 0;
    clearTimeout(retryTimer);
  }
  function onWaiting() {
    setStatus("waiting", "⏳ Buffering…");
  }
  function onPause() {
    if (!video.ended) setStatus("", "⏸ Paused");
  }
  function onVideoError() {
    console.warn("[player] video element error", video.error);
    scheduleRetry();
  }
  function onStalled() {
    clearTimeout(stalledTimer);
    stalledTimer = setTimeout(() => {
      if (video.paused || video.ended) return;
      dbgLog("stalled too long, reconnecting");
      scheduleRetry(true);
    }, 8e3);
  }
  function scheduleRetry(immediate) {
    clearTimeout(retryTimer);
    clearTimeout(stalledTimer);
    if (destroyed) return;
    destroyPlayer();
    retryCount++;
    const delay = immediate ? 1500 : Math.min(1500 * Math.pow(1.6, retryCount - 1), MAX_DELAY);
    showOverlay("📡", "Stream offline or interrupted", `Retrying in ${Math.round(delay / 1e3)}s… (attempt ${retryCount})`);
    setStatus("error", `Waiting for stream… (retry #${retryCount} in ${Math.round(delay / 1e3)}s)`);
    retryTimer = setTimeout(connect, delay);
  }
  function dbgLog(..._args) {
  }
  connect();
})();
