// Worker 池：优先级队列 + 负载均衡调度 + 超时/重试/取消 + 崩溃恢复 + 创建失败降级。
const TaskStatus = {
  PENDING: 'pending',
  RUNNING: 'running',
  SUCCESS: 'success',
  FAILED: 'failed',
  CANCELLED: 'cancelled',
};

let taskSeq = 0;
let workerSeq = 0;

class PoolWorker {
  constructor(pool) {
    this.pool = pool;
    this.id = ++workerSeq;
    this.label = 'W' + this.id;
    this.task = null;
    this.retiring = false;
    this.degraded = false;
    this.busyMs = 0;          // 累计忙碌时长，作为负载均衡依据
    this.completed = 0;
    this._busySince = 0;
    this._crashCount = 0;
    this._lastCrashAt = 0;
    this._spawn();
  }

  get idle() { return this.task === null && !this.retiring; }

  get utilization() {
    const now = performance.now();
    const busy = this.busyMs + (this.task ? now - this._busySince : 0);
    return busy / (now - this.pool.createdAt);
  }

  _spawn() {
    let backend = null;
    if (!this.pool.forceFallback) {
      try {
        backend = new Worker(this.pool.workerUrl);
      } catch (err) {
        backend = null; // 创建失败，走降级
      }
    }
    if (backend) {
      this.degraded = false;
    } else {
      backend = new InlineWorker(); // 降级：主线程执行
      this.degraded = true;
    }
    this.backend = backend;
    this.backend.onmessage = (e) => this.pool._onMessage(this, e.data);
    this.backend.onerror = (e) => this.pool._onWorkerError(this, e);
  }

  // 崩溃/超时后重建
  recycle() {
    try { this.backend.terminate(); } catch (err) { /* ignore */ }
    const now = Date.now();
    // 短时间内反复崩溃且从未成功过 -> 判定环境不支持 Worker，永久降级
    if (now - this._lastCrashAt < 5000) this._crashCount++; else this._crashCount = 1;
    this._lastCrashAt = now;
    if (this._crashCount >= 3 && this.completed === 0) this.pool.forceFallback = true;
    this._spawn();
  }

  startTask(task) {
    this.task = task;
    this._busySince = performance.now();
    this.backend.postMessage({ type: 'run', taskId: task.id, payload: task.payload });
  }

  finishTask() {
    if (this.task) this.busyMs += performance.now() - this._busySince;
    this.task = null;
  }

  requestCancel(taskId) {
    try { this.backend.postMessage({ type: 'cancel', taskId }); } catch (err) { /* ignore */ }
  }

  destroy() {
    try { this.backend.terminate(); } catch (err) { /* ignore */ }
  }
}

class WorkerPool {
  constructor({ workerUrl = 'js/worker.js', size = 4 } = {}) {
    this.workerUrl = workerUrl;
    this.forceFallback = false;
    this.workers = [];
    this.queue = [];            // 等待中的任务（优先级队列）
    this.tasks = new Map();     // 所有任务，按 id 索引（乱序完成不丢结果）
    this.createdAt = performance.now();
    this.onChange = null;       // UI 回调
    this.onTaskFinalized = null;// 任务进入终态回调（写历史）
    this._destroyed = false;
    this.resize(size);
  }

  _emit() { if (this.onChange) this.onChange(); }

  submit({ name, payload, priority = 0, timeout = 5000, maxRetries = 0 }) {
    if (this._destroyed) return null;
    const task = {
      id: ++taskSeq,
      name: name || ('task-' + taskSeq),
      payload,
      priority,
      timeout,
      maxRetries,
      attempts: 0,
      status: TaskStatus.PENDING,
      progress: 0,
      workerLabel: '-',
      submittedAt: Date.now(),
      startedAt: 0,
      endedAt: 0,
      duration: 0,
      result: null,
      error: null,
      _timer: null,
      _cancelGrace: null,
      _worker: null,
    };
    this.tasks.set(task.id, task);
    this.queue.push(task);
    this._schedule();
    this._emit();
    return task.id;
  }

  cancel(taskId) {
    const task = this.tasks.get(taskId);
    if (!task) return;
    if (task.status === TaskStatus.PENDING) {
      this.queue = this.queue.filter((t) => t !== task);
      this._finalize(task, TaskStatus.CANCELLED, 'Cancelled before dispatch');
    } else if (task.status === TaskStatus.RUNNING) {
      // 先协作式取消，500ms 未响应则硬终止 Worker（保证取消生效）
      const worker = task._worker;
      if (worker) worker.requestCancel(task.id);
      task._cancelGrace = setTimeout(() => {
        if (task.status === TaskStatus.RUNNING) this._interrupt(task, TaskStatus.CANCELLED, 'Cancel forced (terminate)');
      }, 500);
    }
    this._emit();
  }

  cancelAll() {
    for (const task of [...this.tasks.values()]) {
      if (task.status === TaskStatus.PENDING || task.status === TaskStatus.RUNNING) this.cancel(task.id);
    }
  }

  // 动态增减 Worker：减少时优先摘空闲的，忙碌的标记退休、干完再退
  resize(n) {
    n = Math.max(0, Math.min(16, n | 0));
    while (this.workers.length < n) this.workers.push(new PoolWorker(this));
    if (this.workers.length > n) {
      let excess = this.workers.length - n;
      for (const w of this.workers) {
        if (excess === 0) break;
        if (w.idle) { w.retiring = true; excess--; }
      }
      for (const w of this.workers) {
        if (excess === 0) break;
        if (!w.retiring) { w.retiring = true; excess--; }
      }
      this._reap();
    }
    this._schedule();
    this._emit();
  }

  _reap() {
    this.workers = this.workers.filter((w) => {
      if (w.retiring && w.task === null) { w.destroy(); return false; }
      return true;
    });
  }

  // 负载均衡：把队首任务派给"累计忙碌时间最短"的空闲 Worker
  _schedule() {
    if (this._destroyed) return;
    this.queue.sort((a, b) => b.priority - a.priority || a.id - b.id);
    while (this.queue.length > 0) {
      const idle = this.workers.filter((w) => w.idle);
      if (idle.length === 0) break;
      idle.sort((a, b) => a.busyMs - b.busyMs);
      const worker = idle[0];
      const task = this.queue.shift();
      task.status = TaskStatus.RUNNING;
      task.attempts++;
      task.startedAt = Date.now();
      task.workerLabel = worker.label + (worker.degraded ? ' (降级)' : '');
      task._worker = worker;
      worker.startTask(task);
      task._timer = setTimeout(() => this._onTimeout(task), task.timeout);
    }
  }

  _onMessage(worker, msg) {
    const task = worker.task;
    if (!task || task.id !== msg.taskId) return; // 迟到/乱序消息直接忽略，不影响其他任务
    if (msg.type === 'progress') {
      task.progress = msg.progress;
      this._emit();
    } else if (msg.type === 'done') {
      task.progress = 1;
      this._settle(worker, task, TaskStatus.SUCCESS, null, msg.result);
    } else if (msg.type === 'error') {
      this._retryOrFail(worker, task, msg.message || 'Task error');
    } else if (msg.type === 'cancelled') {
      this._settle(worker, task, TaskStatus.CANCELLED, 'Cancelled', null);
    }
  }

  _onWorkerError(worker, event) {
    // Worker 崩溃：重建 Worker，并把在途任务重新入队（可重试）
    const task = worker.task;
    worker.recycle();
    if (task) {
      worker.finishTask();
      this._clearTimers(task);
      task._worker = null;
      this._requeue(task, 'Worker crashed: ' + (event.message || 'unknown'));
    }
    this._reap();
    this._schedule();
    this._emit();
  }

  _onTimeout(task) {
    if (task.status !== TaskStatus.RUNNING) return;
    // 超时中断：终止并重建 Worker（真实 Worker 为硬中断，降级模式为协作式）
    this._interrupt(task, null, 'Timeout after ' + task.timeout + 'ms', true);
  }

  // 硬中断当前任务；asRetry=true 时走重试逻辑，否则按给定终态结束
  _interrupt(task, finalStatus, reason, asRetry = false) {
    const worker = task._worker;
    if (worker) {
      worker.finishTask();
      worker.recycle();
      this._reap();
    }
    this._clearTimers(task);
    task._worker = null;
    if (asRetry) this._requeue(task, reason);
    else this._finalize(task, finalStatus, reason);
    this._schedule();
    this._emit();
  }

  _retryOrFail(worker, task, errorMsg) {
    this._settleCommon(worker, task);
    this._requeue(task, errorMsg);
    this._schedule();
    this._emit();
  }

  _requeue(task, errorMsg) {
    task.error = errorMsg;
    if (task.attempts <= task.maxRetries) {
      task.status = TaskStatus.PENDING;
      task.progress = 0;
      this.queue.push(task); // 重新排队，保持原优先级
    } else {
      this._finalize(task, TaskStatus.FAILED, errorMsg);
    }
  }

  _settle(worker, task, status, error, result) {
    if (status === TaskStatus.SUCCESS) worker.completed++;
    this._settleCommon(worker, task);
    this._finalize(task, status, error, result);
    this._schedule();
    this._emit();
  }

  _settleCommon(worker, task) {
    worker.finishTask();
    this._clearTimers(task);
    task._worker = null;
    this._reap();
  }

  _clearTimers(task) {
    if (task._timer) { clearTimeout(task._timer); task._timer = null; }
    if (task._cancelGrace) { clearTimeout(task._cancelGrace); task._cancelGrace = null; }
  }

  _finalize(task, status, error, result = null) {
    this._clearTimers(task);
    task.status = status;
    task.error = error || null;
    task.result = result;
    task.endedAt = Date.now();
    task.duration = task.startedAt ? task.endedAt - task.startedAt : 0;
    if (this.onTaskFinalized) this.onTaskFinalized(task);
  }

  // 页面卸载时清理：终止所有 Worker、清空计时器
  destroy() {
    this._destroyed = true;
    for (const task of this.tasks.values()) {
      if (task.status === TaskStatus.PENDING || task.status === TaskStatus.RUNNING) {
        this._finalize(task, TaskStatus.CANCELLED, 'Page unloaded');
      }
    }
    this.queue = [];
    for (const w of this.workers) w.destroy();
    this.workers = [];
  }
}
