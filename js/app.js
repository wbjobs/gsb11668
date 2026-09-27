'use strict';

(function () {
  // ---------- DOM ----------
  const $ = (sel) => document.querySelector(sel);
  const workerListEl = $('#worker-list');
  const taskListEl = $('#task-list');
  const historyListEl = $('#history-list');
  const canvas = $('#load-canvas');
  const ctx = canvas.getContext('2d');
  const statEl = $('#pool-stats');
  const logEl = $('#event-log');

  // ---------- 池 ----------
  const pool = new WorkerPool({
    workerUrl: 'js/worker.js',
    defaultTimeout: 8000,
    defaultMaxRetries: 2,
    onEvent: handlePoolEvent,
  });

  const taskRows = new Map(); // taskId -> DOM 行
  const loadHistory = [];     // 负载采样（Canvas 折线）
  const MAX_SAMPLES = 120;

  function log(msg) {
    const line = document.createElement('div');
    line.textContent = `[${new Date().toLocaleTimeString()}] ${msg}`;
    logEl.prepend(line);
    while (logEl.children.length > 60) logEl.lastChild.remove();
  }

  // ---------- 事件处理 ----------
  function handlePoolEvent(type, data) {
    if (type === 'task') handleTaskEvent(data);
    else if (type === 'worker') handleWorkerEvent(data);
  }

  function handleTaskEvent(e) {
    const task = pool.getTask(e.taskId);
    if (!task) return;
    ensureTaskRow(task);
    renderTaskRow(task);

    switch (e.status) {
      case 'queued': log(`任务 #${e.taskId} 入队（优先级 ${task.priority}）`); break;
      case 'running': log(`任务 #${e.taskId} 在 Worker ${e.workerId} 上运行（第 ${e.attempt} 次尝试）`); break;
      case 'retry': log(`任务 #${e.taskId} 重试，剩余 ${e.left} 次：${e.reason}`); break;
      case 'timeout': log(`任务 #${e.taskId} 超时，已中断`); break;
      case 'done':
        log(`任务 #${e.taskId} 完成，耗时 ${e.duration}ms`);
        persistHistory(task);
        break;
      case 'failed':
        log(`任务 #${e.taskId} 失败：${e.error}`);
        persistHistory(task);
        break;
      case 'cancelled':
        log(`任务 #${e.taskId} 已取消`);
        persistHistory(task);
        break;
    }
  }

  function handleWorkerEvent(e) {
    switch (e.action) {
      case 'added': log(`Worker ${e.workerId} 已创建`); break;
      case 'removed': log(`Worker ${e.workerId} 已移除`); break;
      case 'degraded': log(`Worker ${e.workerId} 创建失败，降级为主线程执行`); break;
      case 'crashed': log(`Worker ${e.workerId} 崩溃：${e.reason}`); break;
      case 'recovered': log(`Worker ${e.workerId} 已重建恢复`); break;
    }
  }

  function persistHistory(task) {
    TaskHistoryDB.put({
      taskId: task.taskId,
      priority: task.priority,
      status: task.status,
      attempts: task.attempts,
      workerId: task.workerId,
      enqueuedAt: task.enqueuedAt,
      startedAt: task.startedAt,
      finishedAt: task.finishedAt,
      duration: task.finishedAt - (task.startedAt || task.enqueuedAt),
      result: task.result,
      error: task.error,
    }).then(renderHistory);
  }

  // ---------- 任务列表渲染 ----------
  const STATUS_LABEL = {
    queued: '排队中', running: '运行中', done: '完成',
    failed: '失败', cancelled: '已取消', timeout: '超时',
  };

  function ensureTaskRow(task) {
    if (taskRows.has(task.taskId)) return;
    const row = document.createElement('div');
    row.className = 'task-row';
    row.innerHTML = `
      <span class="t-id"></span>
      <span class="t-status"></span>
      <span class="t-prog"><span class="t-prog-bar"></span></span>
      <span class="t-meta"></span>
      <button class="t-cancel" type="button">取消</button>`;
    row.querySelector('.t-cancel').addEventListener('click', () => pool.cancel(task.taskId));
    taskListEl.prepend(row);
    taskRows.set(task.taskId, row);
    while (taskListEl.children.length > 30) {
      const last = taskListEl.lastChild;
      for (const [id, el] of taskRows) if (el === last) taskRows.delete(id);
      last.remove();
    }
  }

  function renderTaskRow(task) {
    const row = taskRows.get(task.taskId);
    if (!row) return;
    const stats = pool.getStats();
    const slot = stats.workers.find((w) => w.id === task.workerId);
    const pct = task.status === 'done' ? 100 : (slot && slot.currentTaskId === task.taskId ? slot.progress : 0);
    row.querySelector('.t-id').textContent = `#${task.taskId} P${task.priority}`;
    const statusEl = row.querySelector('.t-status');
    statusEl.textContent = STATUS_LABEL[task.status] || task.status;
    statusEl.className = 't-status st-' + task.status;
    row.querySelector('.t-prog-bar').style.width = pct + '%';
    row.querySelector('.t-meta').textContent =
      task.status === 'running' ? `W${task.workerId} ${pct}% 试${task.attempts}` :
      task.status === 'done' ? `W${task.workerId} ${task.finishedAt - task.startedAt}ms` :
      (task.error || '');
    row.querySelector('.t-cancel').disabled = !(task.status === 'queued' || task.status === 'running');
  }

  // ---------- Worker 列表渲染 ----------
  function renderWorkers(stats) {
    workerListEl.innerHTML = '';
    for (const w of stats.workers) {
      const card = document.createElement('div');
      card.className = 'worker-card' + (w.crashed ? ' w-crashed' : w.degraded ? ' w-degraded' : w.busy ? ' w-busy' : '');
      const state = w.crashed ? '崩溃' : w.degraded ? '降级' : w.busy ? '忙碌' : '空闲';
      card.innerHTML = `
        <div class="w-head">Worker ${w.id} <span class="w-state">${state}</span></div>
        <div class="w-info">利用率 ${(w.utilization * 100).toFixed(0)}%${w.busy ? ` · 任务 #${w.currentTaskId} ${w.progress}%` : ''}</div>
        <button type="button" class="w-remove">移除</button>`;
      card.querySelector('.w-remove').addEventListener('click', () => pool.removeWorker(w.id));
      workerListEl.appendChild(card);
    }
  }

  // ---------- Canvas 负载可视化 ----------
  function drawLoad(stats) {
    loadHistory.push({
      t: performance.now(),
      utils: stats.workers.map((w) => (w.busy ? 1 : 0)),
    });
    if (loadHistory.length > MAX_SAMPLES) loadHistory.shift();

    const W = canvas.width;
    const H = canvas.height;
    ctx.clearRect(0, 0, W, H);

    const workers = stats.workers;
    if (workers.length === 0) {
      ctx.fillStyle = '#888';
      ctx.font = '13px sans-serif';
      ctx.fillText('无 Worker', 10, 20);
      return;
    }

    // 上半：各 Worker 当前负载柱状图
    const barAreaH = H * 0.55;
    const barW = Math.min(48, (W - 20) / workers.length - 8);
    workers.forEach((w, i) => {
      const x = 10 + i * ((W - 20) / workers.length) + 4;
      const load = w.busy ? Math.max(0.05, w.progress / 100) : 0;
      const h = load * (barAreaH - 24);
      ctx.fillStyle = w.crashed ? '#d64545' : w.degraded ? '#d9a13b' : w.busy ? '#3b82f6' : '#3a3f4a';
      ctx.fillRect(x, barAreaH - 20 - h, barW, h);
      ctx.fillStyle = '#aab';
      ctx.font = '11px sans-serif';
      ctx.fillText('W' + w.id, x, barAreaH - 6);
    });

    // 下半：负载时间线（每个 Worker 一条占用轨迹）
    const tlY = barAreaH + 8;
    const tlH = H - tlY - 6;
    const laneH = tlH / workers.length;
    workers.forEach((w, i) => {
      const y = tlY + i * laneH;
      ctx.strokeStyle = '#2a2f3a';
      ctx.strokeRect(10, y, W - 20, laneH - 2);
      loadHistory.forEach((s, j) => {
        if (s.utils[i]) {
          const x = 10 + (j / MAX_SAMPLES) * (W - 20);
          ctx.fillStyle = w.degraded ? '#d9a13b' : '#3b82f6';
          ctx.fillRect(x, y + 1, Math.max(1, (W - 20) / MAX_SAMPLES), laneH - 4);
        }
      });
    });
  }

  // ---------- 历史 ----------
  async function renderHistory() {
    const rows = await TaskHistoryDB.getAll(50);
    historyListEl.innerHTML = '';
    for (const r of rows) {
      const div = document.createElement('div');
      div.className = 'history-row';
      const time = r.finishedAt ? new Date(r.finishedAt).toLocaleTimeString() : '-';
      div.textContent = `#${r.taskId} [${STATUS_LABEL[r.status] || r.status}] P${r.priority} W${r.workerId ?? '-'} 试${r.attempts} ${r.duration ?? '-'}ms @${time}${r.error ? ' · ' + r.error : ''}`;
      historyListEl.appendChild(div);
    }
    if (rows.length === 0) historyListEl.innerHTML = '<div class="empty">暂无历史</div>';
  }

  // ---------- 控制 ----------
  function submitBatch(count, opts) {
    for (let i = 0; i < count; i++) pool.submit(opts());
  }

  $('#btn-add-worker').addEventListener('click', () => pool.addWorker());
  $('#btn-remove-worker').addEventListener('click', () => {
    const stats = pool.getStats();
    if (stats.workers.length === 0) return;
    // 优先移除空闲 Worker
    const idle = stats.workers.find((w) => !w.busy);
    pool.removeWorker(idle ? idle.id : stats.workers[stats.workers.length - 1].id);
  });

  $('#btn-submit').addEventListener('click', () => {
    const priority = parseInt($('#opt-priority').value, 10);
    const timeout = parseInt($('#opt-timeout').value, 10);
    const work = parseInt($('#opt-work').value, 10);
    const count = parseInt($('#opt-count').value, 10);
    submitBatch(count, () => ({ payload: { work }, priority, timeout }));
  });

  $('#btn-submit-crash').addEventListener('click', () => {
    pool.submit({ payload: { work: 30, crash: true }, priority: 0 });
  });

  $('#btn-submit-long').addEventListener('click', () => {
    // 超长任务 + 短超时 -> 演示超时中断与重试
    pool.submit({ payload: { work: 400 }, priority: 0, timeout: 1500, maxRetries: 1 });
  });

  $('#btn-burst').addEventListener('click', () => {
    // 混合优先级突发 -> 演示乱序完成与优先级调度
    submitBatch(12, () => ({
      payload: { work: 20 + Math.floor(Math.random() * 60) },
      priority: Math.floor(Math.random() * 3),
    }));
  });

  $('#btn-clear-history').addEventListener('click', async () => {
    await TaskHistoryDB.clear();
    renderHistory();
  });

  // ---------- 主循环 ----------
  function tick() {
    const stats = pool.getStats();
    statEl.textContent = `Worker: ${stats.workers.length} · 运行中: ${stats.running} · 排队: ${stats.queued}`;
    renderWorkers(stats);
    drawLoad(stats);
    for (const [id] of taskRows) {
      const task = pool.getTask(id);
      if (task) renderTaskRow(task);
    }
    requestAnimationFrame(tick);
  }

  // ---------- 页面卸载清理 ----------
  window.addEventListener('pagehide', () => pool.destroy());
  window.addEventListener('beforeunload', () => pool.destroy());

  // ---------- 启动 ----------
  const initialSize = Math.max(2, Math.min(8, (navigator.hardwareConcurrency || 4) - 1));
  for (let i = 0; i < initialSize; i++) pool.addWorker();
  renderHistory();
  tick();
})();
