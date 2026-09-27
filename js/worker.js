'use strict';

// 任务执行体：分块计算，块间检查取消标记并上报进度。
// 协议（主线程 -> Worker）：
//   { type: 'run',    taskId, payload: { work, crash } }
//   { type: 'cancel', taskId }
// 协议（Worker -> 主线程）：
//   { type: 'progress', taskId, pct }
//   { type: 'done',     taskId, result }
//   { type: 'cancelled',taskId }

let currentTaskId = null;
let cancelRequested = false;

function isPrime(n) {
  if (n < 2) return false;
  if (n % 2 === 0) return n === 2;
  for (let i = 3; i * i <= n; i += 2) {
    if (n % i === 0) return false;
  }
  return true;
}

function runTask(taskId, payload) {
  const work = Math.max(1, payload.work | 0); // 迭代规模（万次）
  const totalChunks = 20;
  const perChunk = Math.max(1, Math.floor((work * 10000) / totalChunks));
  let count = 0;

  for (let chunk = 0; chunk < totalChunks; chunk++) {
    if (cancelRequested) {
      postMessage({ type: 'cancelled', taskId });
      return;
    }
    const base = chunk * perChunk;
    for (let i = 0; i < perChunk; i++) {
      if (isPrime(base + i)) count++;
    }
    postMessage({ type: 'progress', taskId, pct: Math.round(((chunk + 1) / totalChunks) * 100) });
  }
  postMessage({ type: 'done', taskId, result: { primes: count, work } });
}

self.onmessage = (e) => {
  const msg = e.data;
  if (msg.type === 'run') {
    currentTaskId = msg.taskId;
    cancelRequested = false;
    if (msg.payload && msg.payload.crash && msg.attempt === 1) {
      // 模拟崩溃（仅首次尝试）：抛出未捕获异常，触发主线程的 error 事件
      throw new Error('Simulated worker crash (task ' + msg.taskId + ')');
    }
    try {
      runTask(msg.taskId, msg.payload || {});
    } finally {
      currentTaskId = null;
    }
  } else if (msg.type === 'cancel') {
    if (msg.taskId === currentTaskId) cancelRequested = true;
  }
};

postMessage({ type: 'ready' });
