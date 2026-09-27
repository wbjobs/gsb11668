'use strict';

/* WorkerPool：负载均衡 Worker 池
 * - 按优先级队列分发，选择近期利用率最低的空闲 Worker（负载均衡/纠偏）
 * - 超时：terminate + 重建 Worker（真正的中断），并按重试策略重投
 * - 崩溃：onerror 捕获，任务重投，Worker 自动重建
 * - 创建失败：降级为主线程分块执行器，任务不丢
 * - 乱序：结果统一按 taskId 归集，与完成顺序无关
 * - 取消：队列中直接移除；运行中先协作取消，超时未响应则强杀重建
 */
class WorkerPool {
  constructor(options = {}) {
    this.workerUrl = options.workerUrl || 'js/worker.js';
    this.defaultTimeout = options.defaultTimeout ?? 8000;
    this.defaultMaxRetries = options.defaultMaxRetries ?? 2;
    this.cancelGraceMs = options.cancelGraceMs ?? 500;
    this.onEvent = options.onEvent || (() => {});

    this.workers = new Map();   // workerId -> workerSlot
    this.tasks = new Map();     // taskId -> task（乱序结果按 id 归集）
    this.queue = [];            // 待分发任务（按优先级排序）
    this.nextWorkerId = 1;
    this.nextTaskId = 1;
    this.destroyed = false;
  }

  // ---------- Worker 生命周期 ----------

  addWorker() {
    const id = this.nextWorkerId++;
    const slot = {
      id,
      worker: null,
      degraded: false,   // 创建失败 -> 主线程降级执行
      crashed: false,
      busy: false,
      currentTaskId: null,
      createdAt: performance.now(),
      busyMs: 0,
      busySince: 0,
      progress: 0,
    };
    this.workers.set(id, slot);
    this._spawn(slot);
    this._emit('worker', { workerId: id, action: 'added' });
    this._dispatch();
    return id;
  }

  _spawn(slot) {
    slot.crashed = false;
    try {
      const w = new Worker(this.workerUrl);
      w.onmessage = (e) => this._onMessage(slot, e.data);
      w.onerror = (e) => this._onCrash(slot, e);
      slot.worker = w;
      slot.degraded = false;
    } catch (err) {
      // 创建失败降级：主线程执行器
      slot.worker = null;
      slot.degraded = true;
      this._emit('worker', { workerId: slot.id, action: 'degraded', reason: String(err) });
    }
  }

  removeWorker(workerId) {
    const slot = this.workers.get(workerId);
    if (!slot) return false;
    // 运行中的任务迁移回队列，不丢任务
    if (slot.currentTaskId != null) {
      const task = this.tasks.get(slot.currentTaskId);
      this._clearTaskTimer(task);
      if (task && task.status === 'running') {
        task.status = 'queued';
        task.workerId = null;
        this.queue.push(task);
        this._sortQueue();
        this._emit('task', { taskId: task.taskId, status: 'queued', note: 'worker 移除，任务迁移' });
      }
    }
    this._teardown(slot);
    this.workers.delete(workerId);
    this._emit('worker', { workerId, action: 'removed' });
    this._dispatch();
    return true;
  }

  _teardown(slot) {
    if (slot.worker) {
      try { slot.worker.terminate(); } catch (_) { /* 忽略 */ }
      slot.worker = null;
    }
    if (slot._fallbackCancel) slot._fallbackCancel();
    slot.busy = false;
    slot.currentTaskId = null;
    slot.progress = 0;
  }

  _onCrash(slot, event) {
    if (this.destroyed) return;
    slot.crashed = true;
    const taskId = slot.currentTaskId;
    this._emit('worker', { workerId: slot.id, action: 'crashed', reason: event.message || 'error' });

    const task = taskId != null ? this.tasks.get(taskId) : null;
    this._clearTaskTimer(task);
    this._teardown(slot);

    // 崩溃恢复：重建 Worker
    this._spawn(slot);
    this._emit('worker', { workerId: slot.id, action: 'recovered' });

    // 在途任务重投
    if (task && task.status === 'running') {
      this._retryOrFail(task, 'Worker 崩溃');
    }
    this._dispatch();
  }

  // ---------- 任务提交 / 取消 ----------

  submit({ payload = {}, priority = 0, timeout, maxRetries } = {}) {
    const taskId = this.nextTaskId++;
    const task = {
      taskId,
      payload,
      priority,
      timeout: timeout ?? this.defaultTimeout,
      retriesLeft: maxRetries ?? this.defaultMaxRetries,
      maxRetries: maxRetries ?? this.defaultMaxRetries,
      attempts: 0,
      status: 'queued',       // queued | running | done | failed | cancelled | timeout
      workerId: null,
      enqueuedAt: Date.now(),
      startedAt: null,
      finishedAt: null,
      result: null,
      error: null,
      timer: null,
    };
    this.tasks.set(taskId, task);
    this.queue.push(task);
    this._sortQueue();
    this._emit('task', { taskId, status: 'queued' });
    this._dispatch();
    return taskId;
  }

  cancel(taskId) {
    const task = this.tasks.get(taskId);
    if (!task) return false;

    if (task.status === 'queued') {
      this._dequeue(task);
      this._settle(task, 'cancelled', { error: '已取消（未执行）' });
      return true;
    }
    if (task.status !== 'running') return false;

    const slot = this.workers.get(task.workerId);
    this._clearTaskTimer(task);

    if (slot && slot.degraded && slot._fallbackCancel) {
      slot._fallbackCancel();           // 主线程降级：协作取消
      return true;
    }
    if (slot && slot.worker) {
      slot.worker.postMessage({ type: 'cancel', taskId });
      // 宽限期后仍未响应则强杀重建，保证取消生效
      task.timer = setTimeout(() => {
        if (task.status === 'running') {
          this._teardown(slot);
          this._spawn(slot);
          this._settle(task, 'cancelled', { error: '已取消（强制中断）' });
          this._dispatch();
        }
      }, this.cancelGraceMs);
      return true;
    }
    return false;
  }

  // ---------- 分发与负载均衡 ----------

  _sortQueue() {
    // 优先级高者优先；同级按入队时间 FIFO
    this.queue.sort((a, b) => b.priority - a.priority || a.enqueuedAt - b.enqueuedAt);
  }

  _dequeue(task) {
    const i = this.queue.indexOf(task);
    if (i >= 0) this.queue.splice(i, 1);
  }

  _utilization(slot) {
    const now = performance.now();
    const busy = slot.busyMs + (slot.busy ? now - slot.busySince : 0);
    return busy / Math.max(1, now - slot.createdAt);
  }

  _pickWorker() {
    // 在空闲 Worker 中选近期利用率最低者 -> 负载不均时自动纠偏
    let best = null;
    let bestUtil = Infinity;
    for (const slot of this.workers.values()) {
      if (slot.busy || slot.crashed) continue;
      const u = this._utilization(slot);
      if (u < bestUtil) { bestUtil = u; best = slot; }
    }
    return best;
  }

  _dispatch() {
    if (this.destroyed) return;
    while (this.queue.length > 0) {
      const slot = this._pickWorker();
      if (!slot) break;
      const task = this.queue.shift();
      this._run(slot, task);
    }
  }

  _run(slot, task) {
    task.status = 'running';
    task.workerId = slot.id;
    task.attempts++;
    task.startedAt = Date.now();
    slot.busy = true;
    slot.busySince = performance.now();
    slot.currentTaskId = task.taskId;
    slot.progress = 0;

    // 超时定时器：到期强制中断
    task.timer = setTimeout(() => this._onTimeout(slot, task), task.timeout);

    this._emit('task', { taskId: task.taskId, status: 'running', workerId: slot.id, attempt: task.attempts });

    if (slot.degraded) {
      this._runOnMainThread(slot, task);
    } else {
      slot.worker.postMessage({ type: 'run', taskId: task.taskId, payload: task.payload, attempt: task.attempts });
    }
  }

  // 主线程降级执行器（Worker 创建失败时使用）
  _runOnMainThread(slot, task) {
    const work = Math.max(1, (task.payload.work | 0) || 50);
    const totalChunks = 20;
    const perChunk = Math.max(1, Math.floor((work * 10000) / totalChunks));
    let chunk = 0;
    let count = 0;
    let cancelled = false;
    let timer = null;

    const isPrime = (n) => {
      if (n < 2) return false;
      if (n % 2 === 0) return n === 2;
      for (let i = 3; i * i <= n; i += 2) if (n % i === 0) return false;
      return true;
    };

    const step = () => {
      if (cancelled || task.status !== 'running') return;
      const base = chunk * perChunk;
      for (let i = 0; i < perChunk; i++) if (isPrime(base + i)) count++;
      chunk++;
      slot.progress = Math.round((chunk / totalChunks) * 100);
      this._emit('task', { taskId: task.taskId, status: 'progress', pct: slot.progress });
      if (chunk >= totalChunks) {
        this._finishTask(slot, task, 'done', { primes: count, work });
      } else {
        timer = setTimeout(step, 0);
      }
    };

    slot._fallbackCancel = () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
      this._finishTask(slot, task, 'cancelled', null, '已取消');
    };
    timer = setTimeout(step, 0);
  }

  // ---------- 结果回收（乱序安全） ----------

  _onMessage(slot, msg) {
    if (this.destroyed) return;
    const task = this.tasks.get(msg.taskId);
    if (!task || task.status !== 'running' || slot.currentTaskId !== msg.taskId) {
      return; // 迟到/乱序消息：任务已结算或已迁移，直接丢弃，不影响结果归集
    }
    if (msg.type === 'progress') {
      slot.progress = msg.pct;
      this._emit('task', { taskId: task.taskId, status: 'progress', pct: msg.pct });
    } else if (msg.type === 'done') {
      this._finishTask(slot, task, 'done', msg.result);
    } else if (msg.type === 'cancelled') {
      this._finishTask(slot, task, 'cancelled', null, '已取消');
    }
  }

  _onTimeout(slot, task) {
    if (task.status !== 'running') return;
    this._emit('task', { taskId: task.taskId, status: 'timeout', attempt: task.attempts });
    // 中断：强杀并重建 Worker（降级模式取消主线程执行）
    if (slot.degraded) task.status = 'timeout'; // 阻止降级取消回调把任务结算为 cancelled
    this._teardown(slot);
    if (!slot.degraded) this._spawn(slot);
    this._retryOrFail(task, '任务超时（' + task.timeout + 'ms）');
    this._dispatch();
  }

  _retryOrFail(task, reason) {
    if (task.retriesLeft > 0) {
      task.retriesLeft--;
      task.status = 'queued';
      task.workerId = null;
      this.queue.push(task);
      this._sortQueue();
      this._emit('task', { taskId: task.taskId, status: 'retry', left: task.retriesLeft, reason });
    } else {
      this._settle(task, 'failed', { error: reason + '，重试耗尽' });
    }
  }

  _finishTask(slot, task, status, result, error) {
    if (task.status !== 'running') return;
    this._teardownSlotOnly(slot);
    this._settle(task, status, { result, error });
    this._dispatch();
  }

  _teardownSlotOnly(slot) {
    const now = performance.now();
    slot.busyMs += now - slot.busySince;
    slot.busy = false;
    slot.currentTaskId = null;
    slot.progress = 0;
    slot._fallbackCancel = null;
  }

  _settle(task, status, { result = null, error = null } = {}) {
    this._clearTaskTimer(task);
    task.status = status;
    task.finishedAt = Date.now();
    task.result = result;
    task.error = error;
    if (status === 'done') {
      const slot = this.workers.get(task.workerId);
      if (slot && slot.busy) this._teardownSlotOnly(slot);
    }
    this._emit('task', {
      taskId: task.taskId, status, result, error,
      duration: task.finishedAt - (task.startedAt || task.enqueuedAt),
      workerId: task.workerId, attempts: task.attempts,
    });
  }

  _clearTaskTimer(task) {
    if (task && task.timer) {
      clearTimeout(task.timer);
      task.timer = null;
    }
  }

  // ---------- 统计与清理 ----------

  getStats() {
    const workers = [];
    for (const slot of this.workers.values()) {
      workers.push({
        id: slot.id,
        busy: slot.busy,
        crashed: slot.crashed,
        degraded: slot.degraded,
        progress: slot.progress,
        currentTaskId: slot.currentTaskId,
        utilization: this._utilization(slot),
      });
    }
    let queued = 0, running = 0;
    for (const t of this.tasks.values()) {
      if (t.status === 'queued') queued++;
      else if (t.status === 'running') running++;
    }
    return { workers, queued, running };
  }

  getTask(taskId) { return this.tasks.get(taskId); }

  destroy() {
    this.destroyed = true;
    for (const task of this.tasks.values()) {
      this._clearTaskTimer(task);
    }
    for (const slot of this.workers.values()) {
      this._teardown(slot);
    }
    this.workers.clear();
    this.queue.length = 0;
  }

  _emit(type, data) {
    try { this.onEvent(type, data); } catch (err) { console.error(err); }
  }
}
