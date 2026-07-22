class GreenThreadsVisualizer {
    constructor() {
        this.ws = null;
        this.fibers = [];
        this.metrics = {};
        this.events = [];
        this.schedulerType = '';

        // Canvas references
        this.fiberCanvas = document.getElementById('fiber-canvas');
        this.fiberCtx = this.fiberCanvas.getContext('2d');
        this.timelineCanvas = document.getElementById('timeline-canvas');
        this.timelineCtx = this.timelineCanvas.getContext('2d');

        // State
        this.isRunning = false;
        this.eventHistory = [];

        this.init();
    }

    init() {
        this.connectWebSocket();
        this.setupEventListeners();
        this.updateCanvasSize();
        this.drawEmptyState();
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

        this.ws.onerror = (error) => {
            this.updateStatus('Connection error', 'error');
        };

        this.ws.onclose = () => {
            this.updateStatus('Disconnected', 'idle');
            // Attempt to reconnect after 3 seconds
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

        // Add events to history
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

    drawEmptyState() {
        const ctx = this.fiberCtx;
        const width = this.fiberCanvas.width;
        const height = this.fiberCanvas.height;

        ctx.fillStyle = '#f8f9fa';
        ctx.fillRect(0, 0, width, height);

        ctx.strokeStyle = '#cbd5e0';
        ctx.lineWidth = 2;
        ctx.strokeRect(10, 10, width - 20, height - 20);

        ctx.fillStyle = '#4a5568';
        ctx.font = 'bold 24px Arial';
        ctx.textAlign = 'center';
        ctx.fillText('Fiber Visualization', width / 2, height / 2 - 40);

        ctx.font = '16px Arial';
        ctx.fillStyle = '#718096';
        ctx.fillText('Click "Initialize Runtime" to get started', width / 2, height / 2);
        ctx.fillText('Then spawn fibers to see them visualized here', width / 2, height / 2 + 30);
    }

    drawFiberVisualization() {
        const ctx = this.fiberCtx;
        const width = this.fiberCanvas.width;
        const height = this.fiberCanvas.height;

        // Clear canvas
        ctx.fillStyle = '#f8f9fa';
        ctx.fillRect(0, 0, width, height);

        if (this.fibers.length === 0) {
            this.drawEmptyState();
            return;
        }

        // Draw fibers as rectangles in a grid
        const fiberWidth = 120;
        const fiberHeight = 80;
        const padding = 15;
        const cols = Math.max(1, Math.floor((width - padding) / (fiberWidth + padding)));

        this.fibers.forEach((fiber, index) => {
            const row = Math.floor(index / cols);
            const col = index % cols;
            const x = padding + col * (fiberWidth + padding);
            const y = padding + row * (fiberHeight + padding);

            // Draw fiber box
            const color = this.getFiberColor(fiber.state);
            ctx.fillStyle = fiber.color || color;
            ctx.globalAlpha = 0.7;
            ctx.fillRect(x, y, fiberWidth, fiberHeight);
            ctx.globalAlpha = 1.0;

            // Draw border
            ctx.strokeStyle = this.getBorderColor(fiber.state);
            ctx.lineWidth = 3;
            ctx.strokeRect(x, y, fiberWidth, fiberHeight);

            // Draw fiber info
            ctx.fillStyle = '#fff';
            ctx.font = 'bold 14px Arial';
            ctx.textAlign = 'left';
            ctx.fillText(`ID: ${fiber.id}`, x + 8, y + 20);

            ctx.font = '12px Arial';
            ctx.fillText(fiber.name, x + 8, y + 38);
            ctx.fillText(fiber.state, x + 8, y + 53);
            ctx.fillText(`P: ${fiber.priority}`, x + 8, y + 68);

            // Draw schedule count
            ctx.textAlign = 'right';
            ctx.fillText(`${fiber.scheduleCount}`, x + fiberWidth - 8, y + 68);
        });
    }

    drawTimeline() {
        const ctx = this.timelineCtx;
        const width = this.timelineCanvas.width;
        const height = this.timelineCanvas.height;

        // Clear canvas
        ctx.fillStyle = '#f8f9fa';
        ctx.fillRect(0, 0, width, height);

        if (this.eventHistory.length === 0) {
            ctx.fillStyle = '#4a5568';
            ctx.font = '16px Arial';
            ctx.textAlign = 'center';
            ctx.fillText('Event timeline will appear here', width / 2, height / 2);
            return;
        }

        // Draw timeline
        const maxEvents = 50;
        const events = this.eventHistory.slice(0, maxEvents);
        const eventWidth = (width - 40) / maxEvents;
        const startX = 20;
        const startY = height - 40;

        // Draw axis
        ctx.strokeStyle = '#cbd5e0';
        ctx.lineWidth = 2;
        ctx.beginPath();
        ctx.moveTo(startX, startY);
        ctx.lineTo(width - 20, startY);
        ctx.stroke();

        // Draw events
        events.forEach((event, index) => {
            const x = startX + index * eventWidth;
            const eventHeight = 30;
            const y = startY - eventHeight;

            // Draw event marker
            const color = this.getEventColor(event.eventType);
            ctx.fillStyle = color;
            ctx.fillRect(x, y, eventWidth - 2, eventHeight);

            // Draw event dot on timeline
            ctx.beginPath();
            ctx.arc(x + eventWidth / 2, startY, 4, 0, 2 * Math.PI);
            ctx.fill();
        });

        // Draw labels
        ctx.fillStyle = '#4a5568';
        ctx.font = '12px Arial';
        ctx.textAlign = 'left';
        ctx.fillText('Recent Events →', startX, height - 10);
    }

    getFiberColor(state) {
        switch (state) {
            case 'Ready':
                return '#4ECDC4';
            case 'Running':
                return '#FFD93D';
            case 'Blocked':
                return '#FF6B6B';
            case 'Finished':
                return '#95E1D3';
            default:
                return '#cbd5e0';
        }
    }

    getBorderColor(state) {
        switch (state) {
            case 'Ready':
                return '#3ba89e';
            case 'Running':
                return '#ddb91a';
            case 'Blocked':
                return '#d44949';
            case 'Finished':
                return '#6dc4b0';
            default:
                return '#a0aec0';
        }
    }

    getEventColor(eventType) {
        switch (eventType) {
            case 'Created':
                return '#4ECDC4';
            case 'Scheduled':
                return '#667eea';
            case 'Running':
                return '#FFD93D';
            case 'Yielded':
                return '#FFA07A';
            case 'Blocked':
                return '#FF6B6B';
            case 'Unblocked':
                return '#98D8C8';
            case 'Completed':
                return '#95E1D3';
            case 'ContextSwitch':
                return '#BB8FCE';
            default:
                return '#cbd5e0';
        }
    }

    updateMetrics() {
        document.getElementById('metric-created').textContent = this.metrics.totalFibersCreated || 0;
        document.getElementById('metric-completed').textContent = this.metrics.totalFibersCompleted || 0;
        document.getElementById('metric-active').textContent = this.metrics.activeFibers || 0;
        document.getElementById('metric-blocked').textContent = this.metrics.blockedFibers || 0;
        document.getElementById('metric-switches').textContent = this.metrics.totalContextSwitches || 0;
        document.getElementById('metric-yields').textContent = this.metrics.totalYields || 0;

        const stealRate = this.metrics.stealSuccessRate || 0;
        document.getElementById('metric-steal-rate').textContent = (stealRate * 100).toFixed(1) + '%';

        document.getElementById('metric-scheduler').textContent = this.schedulerType || '-';
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
            item.className = 'fiber-item';
            item.style.borderLeftColor = fiber.color;

            const stateClass = `fiber-state fiber-state-${fiber.state.toLowerCase()}`;

            const header = document.createElement('div');
            header.className = 'fiber-item-header';
            const title = document.createElement('span');
            const strong = document.createElement('strong');
            strong.textContent = `Fiber ${fiber.id}`;
            title.append(strong, document.createTextNode(` - ${fiber.name}`));
            const state = document.createElement('span');
            state.className = stateClass;
            state.textContent = fiber.state;
            header.append(title, state);

            const details = document.createElement('div');
            details.className = 'fiber-item-details';
            details.textContent = `Priority: ${fiber.priority} | Scheduled: ${fiber.scheduleCount}x | Yields: ${fiber.yieldCount}x | CPU: ${fiber.cpuTime}ms`;
            if (fiber.blockReason) {
                details.append(document.createElement('br'), document.createTextNode(`Blocked: ${fiber.blockReason}`));
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
            timestampEl.textContent = `[${timestamp}]`;
            const typeEl = document.createElement('span');
            typeEl.className = 'event-type';
            typeEl.textContent = event.eventType;
            const detailsEl = document.createElement('span');
            detailsEl.className = 'event-details';
            detailsEl.textContent = `Fiber ${event.fiberId}: ${event.details}`;
            item.append(timestampEl, typeEl, detailsEl);

            logEl.appendChild(item);
        });
    }

    updateSchedulerInfo() {
        const infoEl = document.getElementById('scheduler-info');
        if (this.schedulerType) {
            infoEl.textContent = `Active Scheduler: ${this.schedulerType}`;
        }
    }

    updateStatus(message, type) {
        const statusEl = document.getElementById('status-message');
        statusEl.textContent = message;
        statusEl.className = `status-${type}`;
    }

    updateCanvasSize() {
        // The canvas size is set in HTML, this method can be used for responsive sizing
        const container = document.getElementById('fiber-canvas-container');
        if (container) {
            this.fiberCanvas.width = container.clientWidth;
            this.timelineCanvas.width = container.clientWidth;
        }
    }

    setupEventListeners() {
        // Initialize runtime
        document.getElementById('btn-init').addEventListener('click', () => {
            const schedulerType = document.getElementById('scheduler-type').value;
            const numWorkers = parseInt(document.getElementById('num-workers').value);

            this.sendMessage('init', {
                schedulerType: schedulerType,
                numWorkers: numWorkers
            });
        });

        // Stop runtime
        document.getElementById('btn-stop').addEventListener('click', () => {
            this.sendMessage('stop', {});
            this.isRunning = false;
            this.updateStatus('Stopped', 'idle');
        });

        // Reset runtime
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

        // Spawn fiber
        document.getElementById('btn-spawn').addEventListener('click', () => {
            if (!this.isRunning) {
                this.updateStatus('Please initialize runtime first', 'error');
                return;
            }

            const name = document.getElementById('fiber-name').value;
            const priority = parseInt(document.getElementById('fiber-priority').value);
            const duration = parseInt(document.getElementById('fiber-duration').value);

            this.sendMessage('spawn', {
                name: name || 'Worker',
                priority: priority,
                duration: duration
            });
        });

        // Spawn multiple fibers
        document.getElementById('btn-spawn-multiple').addEventListener('click', () => {
            if (!this.isRunning) {
                this.updateStatus('Please initialize runtime first', 'error');
                return;
            }

            for (let i = 0; i < 5; i++) {
                const duration = 500 + Math.random() * 2000;
                const priority = Math.floor(Math.random() * 5);

                setTimeout(() => {
                    this.sendMessage('spawn', {
                        name: `Worker-${i + 1}`,
                        priority: priority,
                        duration: Math.floor(duration)
                    });
                }, i * 100);
            }
        });

        // Window resize
        window.addEventListener('resize', () => {
            this.updateCanvasSize();
            this.render();
        });
    }

    sendMessage(type, payload) {
        if (this.ws && this.ws.readyState === WebSocket.OPEN) {
            this.ws.send(JSON.stringify({
                type: type,
                payload: payload
            }));
        } else {
            this.updateStatus('WebSocket not connected', 'error');
        }
    }
}

// Initialize the visualizer when the page loads
window.addEventListener('DOMContentLoaded', () => {
    new GreenThreadsVisualizer();
});
