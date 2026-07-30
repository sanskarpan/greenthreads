const PALETTE = {
  bg: '#0a0b0e',
  card: '#14171c',
  cardBorder: 'rgba(255,255,255,0.09)',
  grid: 'rgba(255,255,255,0.05)',
  text: '#e7e9ee',
  dim: '#9aa2ad',
  mute: '#626a76',
  accent: '#6d8bff',
  ready: '#38bdf8',
  running: '#fbbf24',
  blocked: '#fb7185',
  finished: '#34d399',
};

class GreenThreadsVisualizer {
  constructor() {
    this.ws = null;
    this.fibers = [];
    this.metrics = {};
    this.events = [];
    this.schedulerType = '';

    this.fiberCanvas = document.getElementById('fiber-canvas');
    this.fiberCtx = this.fiberCanvas.getContext('2d');
    this.timelineCanvas = document.getElementById('timeline-canvas');
    this.timelineCtx = this.timelineCanvas.getContext('2d');

    // Logical (CSS-pixel) dimensions, kept in sync by updateCanvasSize().
    this.fiberDim = { w: 1200, h: 480 };
    this.timelineDim = { w: 1200, h: 180 };

    this.isRunning = false;
    this.eventHistory = [];

    this.init();
  }

  init() {
    this.connectWebSocket();
    this.setupEventListeners();
    this.updateCanvasSize();
    this.drawEmptyState();
    this.drawTimeline();
  }

  connectWebSocket() {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws`;

    this.ws = new WebSocket(wsUrl);

    this.ws.onopen = () => {
      this.updateStatus('Connected', 'running');
    };

    this.ws.onmessage = (event) => {
      try {
        this.handleMessage(JSON.parse(event.data));
      } catch {
        this.updateStatus('Invalid server message', 'error');
      }
    };

    this.ws.onerror = () => {
      this.updateStatus('Connection error', 'error');
    };

    this.ws.onclose = () => {
      this.updateStatus('Disconnected', 'idle');
      setTimeout(() => this.connectWebSocket(), 3000);
    };
  }

  handleMessage(message) {
    switch (message.type) {
      case 'initSuccess':
        this.updateStatus('Runtime initialized', 'running');
        this.isRunning = true;
        break;
      case 'update':
      case 'stateUpdate':
        this.updateState(message.payload);
        break;
      case 'success':
        break;
      case 'error':
        this.updateStatus(message.payload?.message || 'Request failed', 'error');
        break;
      default:
        this.updateStatus('Unknown server message', 'error');
    }
  }

  updateState(payload) {
    this.fibers = payload.fibers || [];
    this.metrics = payload.metrics || {};
    this.events = payload.events || [];
    this.schedulerType = payload.schedulerType || '';

    if (this.events.length > 0) {
      this.eventHistory = [...this.events, ...this.eventHistory].slice(0, 100);
    }

    this.render();
  }

  render() {
    this.drawFiberVisualization();
    this.drawTimeline();
    this.updateMetrics();
    this.updateFiberList();
    this.updateEventLog();
    this.updateSchedulerInfo();
  }

  /* ---------- Canvas helpers ------------------------------------------ */

  roundRect(ctx, x, y, w, h, r) {
    if (typeof ctx.roundRect === 'function') {
      ctx.beginPath();
      ctx.roundRect(x, y, w, h, r);
      return;
    }
    ctx.beginPath();
    ctx.moveTo(x + r, y);
    ctx.arcTo(x + w, y, x + w, y + h, r);
    ctx.arcTo(x + w, y + h, x, y + h, r);
    ctx.arcTo(x, y + h, x, y, r);
    ctx.arcTo(x, y, x + w, y, r);
    ctx.closePath();
  }

  drawEmptyState() {
    const ctx = this.fiberCtx;
    const { w, h } = this.fiberDim;

    ctx.fillStyle = PALETTE.bg;
    ctx.fillRect(0, 0, w, h);

    ctx.fillStyle = PALETTE.mute;
    ctx.textAlign = 'center';
    ctx.font = '600 15px system-ui, -apple-system, sans-serif';
    ctx.fillText('No active runtime', w / 2, h / 2 - 8);
    ctx.font = '13px ui-monospace, "SF Mono", Menlo, monospace';
    ctx.fillStyle = 'rgba(154,162,173,0.55)';
    ctx.fillText('Initialize a runtime, then spawn fibers to visualize them', w / 2, h / 2 + 16);
  }

  drawFiberVisualization() {
    const ctx = this.fiberCtx;
    const { w, h } = this.fiberDim;

    ctx.fillStyle = PALETTE.bg;
    ctx.fillRect(0, 0, w, h);

    if (this.fibers.length === 0) {
      this.drawEmptyState();
      return;
    }

    const cardW = 152;
    const cardH = 92;
    const pad = 14;
    const cols = Math.max(1, Math.floor((w - pad) / (cardW + pad)));

    this.fibers.forEach((fiber, index) => {
      const row = Math.floor(index / cols);
      const col = index % cols;
      const x = pad + col * (cardW + pad);
      const y = pad + row * (cardH + pad);
      if (y + cardH > h) return; // clip overflow rows

      const accent = this.getFiberColor(fiber.state);

      // Card body
      this.roundRect(ctx, x, y, cardW, cardH, 9);
      ctx.fillStyle = PALETTE.card;
      ctx.fill();
      ctx.lineWidth = 1;
      ctx.strokeStyle = PALETTE.cardBorder;
      ctx.stroke();

      // Left accent bar
      this.roundRect(ctx, x, y, 3, cardH, 2);
      ctx.fillStyle = accent;
      ctx.fill();

      const tx = x + 15;

      // Fiber id (mono)
      ctx.textAlign = 'left';
      ctx.font = '600 14px ui-monospace, "SF Mono", Menlo, monospace';
      ctx.fillStyle = PALETTE.text;
      ctx.fillText(`#${fiber.id}`, tx, y + 24);

      // State pill dot + label (top right)
      ctx.textAlign = 'right';
      ctx.font = '600 10px system-ui, sans-serif';
      ctx.fillStyle = accent;
      const stateLabel = (fiber.state || '').toUpperCase();
      ctx.fillText(stateLabel, x + cardW - 14, y + 23);
      ctx.beginPath();
      ctx.arc(x + cardW - 14 - ctx.measureText(stateLabel).width - 8, y + 20, 3, 0, 2 * Math.PI);
      ctx.fillStyle = accent;
      ctx.fill();

      // Name
      ctx.textAlign = 'left';
      ctx.font = '13px system-ui, sans-serif';
      ctx.fillStyle = PALETTE.dim;
      const name = this.truncate(ctx, fiber.name || '', cardW - 30);
      ctx.fillText(name, tx, y + 46);

      // Footer: priority + schedule count (mono, muted)
      ctx.font = '11px ui-monospace, "SF Mono", Menlo, monospace';
      ctx.fillStyle = PALETTE.mute;
      ctx.fillText(`prio ${fiber.priority}`, tx, y + 74);
      ctx.textAlign = 'right';
      ctx.fillText(`${fiber.scheduleCount}×`, x + cardW - 14, y + 74);
    });
  }

  truncate(ctx, text, maxW) {
    if (ctx.measureText(text).width <= maxW) return text;
    let t = text;
    while (t.length > 1 && ctx.measureText(t + '…').width > maxW) t = t.slice(0, -1);
    return t + '…';
  }

  drawTimeline() {
    const ctx = this.timelineCtx;
    const { w, h } = this.timelineDim;

    ctx.fillStyle = PALETTE.bg;
    ctx.fillRect(0, 0, w, h);

    if (this.eventHistory.length === 0) {
      ctx.fillStyle = 'rgba(154,162,173,0.5)';
      ctx.font = '13px ui-monospace, "SF Mono", Menlo, monospace';
      ctx.textAlign = 'center';
      ctx.fillText('Event timeline will populate as fibers run', w / 2, h / 2);
      return;
    }

    const maxEvents = 60;
    const events = this.eventHistory.slice(0, maxEvents).reverse();
    const startX = 20;
    const endX = w - 20;
    const baselineY = h - 34;
    const slot = (endX - startX) / maxEvents;
    const barW = Math.max(3, slot - 3);
    const maxBar = h - 64;

    // Baseline
    ctx.strokeStyle = PALETTE.grid;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(startX, baselineY + 0.5);
    ctx.lineTo(endX, baselineY + 0.5);
    ctx.stroke();

    events.forEach((event, i) => {
      const x = startX + i * slot;
      const color = this.getEventColor(event.eventType);
      // deterministic-ish height by event index for visual rhythm
      const barH = 14 + ((i * 37) % maxBar);
      const y = baselineY - barH;

      this.roundRect(ctx, x, y, barW, barH, 2);
      ctx.fillStyle = color;
      ctx.globalAlpha = 0.85;
      ctx.fill();
      ctx.globalAlpha = 1;

      // baseline tick
      ctx.beginPath();
      ctx.arc(x + barW / 2, baselineY, 2, 0, 2 * Math.PI);
      ctx.fillStyle = color;
      ctx.fill();
    });

    ctx.fillStyle = PALETTE.mute;
    ctx.font = '11px ui-monospace, "SF Mono", Menlo, monospace';
    ctx.textAlign = 'left';
    ctx.fillText('oldest', startX, h - 12);
    ctx.textAlign = 'right';
    ctx.fillText('newest', endX, h - 12);
  }

  getFiberColor(state) {
    switch (state) {
      case 'Ready': return PALETTE.ready;
      case 'Running': return PALETTE.running;
      case 'Blocked': return PALETTE.blocked;
      case 'Finished': return PALETTE.finished;
      default: return PALETTE.mute;
    }
  }

  getEventColor(eventType) {
    switch (eventType) {
      case 'Created': return PALETTE.ready;
      case 'Scheduled': return PALETTE.accent;
      case 'Running': return PALETTE.running;
      case 'Yielded': return '#f59e0b';
      case 'Blocked': return PALETTE.blocked;
      case 'Unblocked': return PALETTE.finished;
      case 'Completed': return PALETTE.finished;
      case 'ContextSwitch': return '#a78bfa';
      default: return PALETTE.mute;
    }
  }

  /* ---------- DOM panels ---------------------------------------------- */

  updateMetrics() {
    document.getElementById('metric-created').textContent = this.metrics.totalFibersCreated || 0;
    document.getElementById('metric-completed').textContent = this.metrics.totalFibersCompleted || 0;
    document.getElementById('metric-active').textContent = this.metrics.activeFibers || 0;
    document.getElementById('metric-blocked').textContent = this.metrics.blockedFibers || 0;
    document.getElementById('metric-switches').textContent = this.metrics.totalContextSwitches || 0;
    document.getElementById('metric-yields').textContent = this.metrics.totalYields || 0;

    const stealRate = this.metrics.stealSuccessRate || 0;
    document.getElementById('metric-steal-rate').textContent = (stealRate * 100).toFixed(1) + '%';

    document.getElementById('metric-scheduler').textContent = this.schedulerType || '—';
  }

  updateFiberList() {
    const listEl = document.getElementById('fiber-list');
    listEl.replaceChildren();

    if (this.fibers.length === 0) {
      const empty = document.createElement('div');
      empty.className = 'empty-state';
      empty.textContent = 'No fibers';
      listEl.appendChild(empty);
      return;
    }

    this.fibers.forEach(fiber => {
      const item = document.createElement('div');
      const state = (fiber.state || '').toLowerCase();
      // State-based class drives the left accent border via CSS (no inline
      // styles — the page CSP forbids them).
      item.className = `fiber-item state-${state}`;

      const stateClass = `fiber-state fiber-state-${state}`;

      const header = document.createElement('div');
      header.className = 'fiber-item-header';
      const title = document.createElement('span');
      const strong = document.createElement('strong');
      strong.textContent = `#${fiber.id}`;
      title.append(strong, document.createTextNode(` ${fiber.name}`));
      const stateEl = document.createElement('span');
      stateEl.className = stateClass;
      stateEl.textContent = fiber.state;
      header.append(title, stateEl);

      const details = document.createElement('div');
      details.className = 'fiber-item-details';
      details.textContent = `prio ${fiber.priority}  ·  scheduled ${fiber.scheduleCount}×  ·  yields ${fiber.yieldCount}  ·  cpu ${fiber.cpuTime}ms`;
      if (fiber.blockReason) {
        details.append(document.createElement('br'), document.createTextNode(`blocked: ${fiber.blockReason}`));
      }
      item.append(header, details);

      listEl.appendChild(item);
    });
  }

  updateEventLog() {
    const logEl = document.getElementById('event-log');
    logEl.replaceChildren();

    const recentEvents = this.eventHistory.slice(0, 20);

    if (recentEvents.length === 0) {
      const empty = document.createElement('div');
      empty.className = 'empty-state';
      empty.textContent = 'No events yet';
      logEl.appendChild(empty);
      return;
    }

    recentEvents.forEach(event => {
      const item = document.createElement('div');
      item.className = 'event-item';

      const timestamp = new Date(event.timestamp).toLocaleTimeString();

      const timestampEl = document.createElement('span');
      timestampEl.className = 'event-timestamp';
      timestampEl.textContent = timestamp;
      const typeEl = document.createElement('span');
      typeEl.className = 'event-type';
      typeEl.textContent = event.eventType;
      const detailsEl = document.createElement('span');
      detailsEl.className = 'event-details';
      detailsEl.textContent = `#${event.fiberId} ${event.details}`;
      item.append(timestampEl, typeEl, detailsEl);

      logEl.appendChild(item);
    });
  }

  updateSchedulerInfo() {
    const infoEl = document.getElementById('scheduler-info');
    if (this.schedulerType) {
      infoEl.textContent = `scheduler: ${this.schedulerType}`;
    }
  }

  updateStatus(message, type) {
    const statusEl = document.getElementById('status-message');
    statusEl.textContent = message;
    statusEl.className = `status-${type}`;
  }

  updateCanvasSize() {
    const dpr = window.devicePixelRatio || 1;
    const container = document.getElementById('fiber-canvas-container');
    if (!container) return;

    const cssW = container.clientWidth || 1200;

    const size = (canvas, ctx, logicalH) => {
      canvas.width = Math.round(cssW * dpr);
      canvas.height = Math.round(logicalH * dpr);
      canvas.style.height = logicalH + 'px';
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    };

    size(this.fiberCanvas, this.fiberCtx, this.fiberDim.h);
    this.fiberDim.w = cssW;

    size(this.timelineCanvas, this.timelineCtx, this.timelineDim.h);
    this.timelineDim.w = cssW;
  }

  setupEventListeners() {
    document.getElementById('btn-init').addEventListener('click', () => {
      const schedulerType = document.getElementById('scheduler-type').value;
      const numWorkers = parseInt(document.getElementById('num-workers').value);
      this.sendMessage('init', { schedulerType, numWorkers });
    });

    document.getElementById('btn-stop').addEventListener('click', () => {
      this.sendMessage('stop', {});
      this.isRunning = false;
      this.updateStatus('Stopped', 'idle');
    });

    document.getElementById('btn-reset').addEventListener('click', () => {
      this.sendMessage('reset', {});
      this.fibers = [];
      this.metrics = {};
      this.events = [];
      this.eventHistory = [];
      this.isRunning = false;
      this.updateStatus('Reset', 'idle');
      this.render();
    });

    document.getElementById('btn-spawn').addEventListener('click', () => {
      if (!this.isRunning) {
        this.updateStatus('Initialize the runtime first', 'error');
        return;
      }
      const name = document.getElementById('fiber-name').value;
      const priority = parseInt(document.getElementById('fiber-priority').value);
      const duration = parseInt(document.getElementById('fiber-duration').value);
      this.sendMessage('spawn', { name: name || 'worker', priority, duration });
    });

    document.getElementById('btn-spawn-multiple').addEventListener('click', () => {
      if (!this.isRunning) {
        this.updateStatus('Initialize the runtime first', 'error');
        return;
      }
      for (let i = 0; i < 5; i++) {
        const duration = 500 + Math.random() * 2000;
        const priority = Math.floor(Math.random() * 5);
        setTimeout(() => {
          this.sendMessage('spawn', {
            name: `worker-${i + 1}`,
            priority,
            duration: Math.floor(duration),
          });
        }, i * 100);
      }
    });

    window.addEventListener('resize', () => {
      this.updateCanvasSize();
      if (this.fibers.length === 0) { this.drawEmptyState(); } else { this.drawFiberVisualization(); }
      this.drawTimeline();
    });
  }

  sendMessage(type, payload) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ type, payload }));
    } else {
      this.updateStatus('WebSocket not connected', 'error');
    }
  }
}

window.addEventListener('DOMContentLoaded', () => {
  new GreenThreadsVisualizer();
});
