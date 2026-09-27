// UI  wiring：控制条、实时任务表、负载可视化、历史。
const $ = (id) => document.getElementById(id);

const pool = new WorkerPool({ workerUrl: 'js/worker.js', size: 4 });
const viz = new LoadViz($('loadCanvas'));

const STATUS_TEXT = {
  pending: '排队中',
  running: '运行中',
  success: '成功',
  failed: '失败',
  cancelled: '已取消',
};

// ---------- 渲染 ----------
function render() {
  renderStats();
  renderWorkers();
  renderTasks();
}

function renderStats() {
  const counts = { pending: 0, running: 0, success: 0, failed: 0, cancelled: 0 };
  for (const t of pool.tasks.values()) counts[t.status]++;
  $('stats').innerHTML =
    `Worker: <b>${pool.workers.length}</b>` +
    ` （真实 ${pool.workers.filter((w) => !w.degraded).length} / 降级 ${pool.workers.filter((w) => w.degraded).length}）` +
    ` ｜ 排队 <b>${counts.pending}</b> ｜ 运行 <b>${counts.running}</b>` +
    ` ｜ 成功 <b class="ok">${counts.success}</b> ｜ 失败 <b class="bad">${counts.failed}</b> ｜ 取消 ${counts.cancelled}`;
}

function renderWorkers() {
  const tbody = $('workerTable').querySelector('tbody');
  tbody.innerHTML = pool.workers.map((w) => {
    const state = w.retiring ? '退出中' : w.task ? '忙碌' : '空闲';
    return `<tr>
      <td>${w.label}${w.degraded ? ' <span class="tag warn">降级</span>' : ''}</td>
      <td>${state}</td>
      <td>${w.task ? '#' + w.task.id + ' ' + w.task.name : '-'}</td>
      <td>${(Math.min(1, w.utilization) * 100).toFixed(1)}%</td>
      <td>${w.completed}</td>
    </tr>`;
  }).join('');
}

function renderTasks() {
  const tbody = $('taskTable').querySelector('tbody');
  const rows = [...pool.tasks.values()]
    .filter((t) => t.status === 'pending' || t.status === 'running')
    .sort((a, b) => a.id - b.id);
  const recent = [...pool.tasks.values()]
    .filter((t) => t.status !== 'pending' && t.status !== 'running')
    .sort((a, b) => b.endedAt - a.endedAt)
    .slice(0, 8);
  tbody.innerHTML = rows.concat(recent).map((t) => {
    const active = t.status === 'pending' || t.status === 'running';
    const pct = Math.round(t.progress * 100);
    return `<tr class="st-${t.status}">
      <td>#${t.id}</td>
      <td>${t.name}</td>
      <td>${t.priority}</td>
      <td>${STATUS_TEXT[t.status]}${t.error ? ` <span class="tag bad" title="${t.error}">!</span>` : ''}</td>
      <td><div class="bar"><div style="width:${pct}%"></div></div>${pct}%</td>
      <td>${t.workerLabel}</td>
      <td>${t.attempts}/${t.maxRetries + 1}</td>
      <td>${active ? `<button data-cancel="${t.id}">取消</button>` : ''}</td>
    </tr>`;
  }).join('');
}

async function renderHistory() {
  const rows = await TaskHistory.loadAll();
  const tbody = $('historyTable').querySelector('tbody');
  tbody.innerHTML = rows.map((r) => `<tr class="st-${r.status}">
    <td>#${r.id}</td>
    <td>${r.name}</td>
    <td>${STATUS_TEXT[r.status] || r.status}</td>
    <td>${r.workerLabel}</td>
    <td>${r.attempts}</td>
    <td>${r.duration}</td>
    <td>${new Date(r.endedAt).toLocaleTimeString()}</td>
    <td>${r.error ? r.error : (r.result != null ? r.result : '-')}</td>
  </tr>`).join('');
}

pool.onChange = render;
pool.onTaskFinalized = (task) => {
  TaskHistory.save(task).then(renderHistory);
};

// ---------- 事件 ----------
$('applyResize').addEventListener('click', () => {
  pool.forceFallback = $('forceFallback').checked;
  pool.resize(Number($('workerCount').value));
});

$('forceFallback').addEventListener('change', (e) => {
  pool.forceFallback = e.target.checked;
  // 重建全部 Worker 以应用/解除降级
  const n = pool.workers.length;
  pool.resize(0);
  pool.resize(n);
});

function submitOne(overrides = {}) {
  const work = Number($('taskWork').value);
  pool.submit({
    name: overrides.name || $('taskName').value || undefined,
    payload: {
      iterations: work * 1e6 / 20, // 1 单位工作量 ≈ 5ms
      crashAt: overrides.crash != null ? overrides.crash : ($('taskCrash').checked ? 0.5 : null),
      failAt: overrides.fail != null ? overrides.fail : ($('taskFail').checked ? 0.5 : null),
    },
    priority: overrides.priority != null ? overrides.priority : Number($('taskPriority').value),
    timeout: Number($('taskTimeout').value),
    maxRetries: Number($('taskRetries').value),
  });
}

$('submitTask').addEventListener('click', () => submitOne());

$('burstTask').addEventListener('click', () => {
  const modes = [null, null, null, 'crash', 'fail'];
  for (let i = 0; i < 12; i++) {
    const mode = modes[Math.floor(Math.random() * modes.length)];
    submitOne({
      name: 'burst-' + i + (mode ? '-' + mode : ''),
      priority: [10, 0, 0, -10][Math.floor(Math.random() * 4)],
      crash: mode === 'crash' ? 0.5 : null,
      fail: mode === 'fail' ? 0.5 : null,
    });
  }
});

$('cancelAll').addEventListener('click', () => pool.cancelAll());

$('taskTable').addEventListener('click', (e) => {
  const id = e.target.dataset && e.target.dataset.cancel;
  if (id) pool.cancel(Number(id));
});

$('reloadHistory').addEventListener('click', renderHistory);
$('clearHistory').addEventListener('click', async () => {
  await TaskHistory.clear();
  renderHistory();
});

// ---------- 可视化循环 ----------
setInterval(() => {
  viz.sample(pool);
  viz.draw(pool);
  renderWorkers(); // 利用率随时间变化，需周期刷新
}, 300);

// ---------- 页面卸载清理 ----------
window.addEventListener('pagehide', () => {
  pool.destroy(); // 终止所有 Worker、清理计时器，并把未完成任务落库为已取消
});
window.addEventListener('beforeunload', () => {
  pool.destroy();
});

render();
renderHistory();
