// 真实 Web Worker：分块执行计算任务，支持协作式取消与模拟崩溃/失败。
const CHUNK = 1e6;

let currentTaskId = null;
let cancelRequested = false;

self.onmessage = (event) => {
  const msg = event.data;
  if (msg.type === 'run') {
    currentTaskId = msg.taskId;
    cancelRequested = false;
    run(msg.taskId, msg.payload);
  } else if (msg.type === 'cancel' && msg.taskId === currentTaskId) {
    cancelRequested = true;
  }
};

function run(taskId, payload) {
  const iterations = payload.iterations;
  let index = 0;
  let sum = 0;

  function step() {
    if (cancelRequested) {
      currentTaskId = null;
      self.postMessage({ type: 'cancelled', taskId });
      return;
    }
    const end = Math.min(index + CHUNK, iterations);
    for (; index < end; index++) sum += Math.sqrt(index);
    const progress = index / iterations;

    if (payload.crashAt != null && progress >= payload.crashAt) {
      // 模拟硬崩溃：未捕获异常会触发主线程 Worker 的 error 事件
      throw new Error('Simulated worker crash');
    }
    if (payload.failAt != null && progress >= payload.failAt) {
      currentTaskId = null;
      self.postMessage({ type: 'error', taskId, message: 'Simulated task failure' });
      return;
    }

    self.postMessage({ type: 'progress', taskId, progress });
    if (index < iterations) {
      setTimeout(step, 0);
    } else {
      currentTaskId = null;
      self.postMessage({ type: 'done', taskId, result: Math.round(sum) });
    }
  }
  step();
}
