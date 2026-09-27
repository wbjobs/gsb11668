// 降级执行器：当 Worker 创建失败时，在主线程分块执行，接口与 Worker 对齐。
// 取消/超时为协作式中断（每个 chunk 边界检查标志）。
class InlineWorker {
  constructor() {
    this.onmessage = null;
    this.onerror = null;
    this._taskId = null;
    this._cancelled = false;
    this._timer = null;
  }

  postMessage(msg) {
    if (msg.type === 'run') {
      this._taskId = msg.taskId;
      this._cancelled = false;
      this._run(msg.taskId, msg.payload);
    } else if (msg.type === 'cancel' && msg.taskId === this._taskId) {
      this._cancelled = true;
    }
  }

  terminate() {
    this._cancelled = true;
    if (this._timer != null) {
      clearTimeout(this._timer);
      this._timer = null;
    }
  }

  _emit(data) {
    if (this.onmessage) this.onmessage({ data });
  }

  _run(taskId, payload) {
    const CHUNK = 1e6;
    const iterations = payload.iterations;
    let index = 0;
    let sum = 0;

    const step = () => {
      this._timer = null;
      if (this._cancelled) {
        this._taskId = null;
        this._emit({ type: 'cancelled', taskId });
        return;
      }
      const end = Math.min(index + CHUNK, iterations);
      for (; index < end; index++) sum += Math.sqrt(index);
      const progress = index / iterations;

      if (payload.failAt != null && progress >= payload.failAt) {
        this._taskId = null;
        this._emit({ type: 'error', taskId, message: 'Simulated task failure' });
        return;
      }
      // 降级模式不支持"崩溃"（主线程无法被终止），按失败处理
      if (payload.crashAt != null && progress >= payload.crashAt) {
        this._taskId = null;
        this._emit({ type: 'error', taskId, message: 'Simulated crash (degraded to failure on main thread)' });
        return;
      }

      this._emit({ type: 'progress', taskId, progress });
      if (index < iterations) {
        this._timer = setTimeout(step, 0);
      } else {
        this._taskId = null;
        this._emit({ type: 'done', taskId, result: Math.round(sum) });
      }
    };
    this._timer = setTimeout(step, 0);
  }
}
