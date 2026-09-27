// Canvas 负载可视化：每个 Worker 一根利用率条 + 全局负载曲线。
class LoadViz {
  constructor(canvas) {
    this.canvas = canvas;
    this.ctx = canvas.getContext('2d');
    this.samples = [];       // 全局平均利用率采样
    this.maxSamples = 240;
  }

  sample(pool) {
    const ws = pool.workers;
    const avg = ws.length ? ws.reduce((s, w) => s + Math.min(1, w.utilization), 0) / ws.length : 0;
    this.samples.push(avg);
    if (this.samples.length > this.maxSamples) this.samples.shift();
  }

  draw(pool) {
    const { ctx, canvas } = this;
    const dpr = window.devicePixelRatio || 1;
    const width = canvas.clientWidth;
    const height = canvas.clientHeight;
    if (canvas.width !== width * dpr) { canvas.width = width * dpr; canvas.height = height * dpr; }
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, height);

    const workers = pool.workers;
    const chartH = 60;             // 顶部全局曲线高度
    const barH = 22;
    const gap = 8;
    const labelW = 90;

    // 全局负载曲线
    ctx.strokeStyle = '#3b82f6';
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    this.samples.forEach((v, i) => {
      const x = (i / (this.maxSamples - 1)) * width;
      const y = chartH - v * (chartH - 6);
      i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
    });
    ctx.stroke();
    ctx.fillStyle = '#64748b';
    ctx.font = '11px sans-serif';
    ctx.fillText('全局平均利用率', 6, 12);

    // 每个 Worker 的利用率条
    workers.forEach((w, i) => {
      const y = chartH + 10 + i * (barH + gap);
      if (y + barH > height) return;
      const util = Math.min(1, w.utilization);
      const color = w.retiring ? '#94a3b8'
        : w.degraded ? '#f59e0b'
        : w.task ? '#22c55e' : '#cbd5e1';

      ctx.fillStyle = '#1e293b';
      ctx.font = '12px monospace';
      const label = w.label + (w.degraded ? '⛨' : '') + (w.retiring ? ' (退出中)' : '');
      ctx.fillText(label, 4, y + barH / 2 + 4);

      ctx.fillStyle = '#e2e8f0';
      ctx.fillRect(labelW, y, width - labelW - 60, barH);
      ctx.fillStyle = color;
      ctx.fillRect(labelW, y, (width - labelW - 60) * util, barH);

      ctx.fillStyle = '#1e293b';
      ctx.fillText((util * 100).toFixed(0) + '%', width - 50, y + barH / 2 + 4);
    });
  }
}
