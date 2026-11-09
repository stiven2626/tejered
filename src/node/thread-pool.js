'use strict';
/**
 * Patrón THREAD POOL (patrón de concurrencia).
 * Un número fijo de hilos (worker_threads) se crea una sola vez y se reutiliza.
 * Las tareas esperan en una cola FIFO acotada; si la cola se llena se rechaza la
 * tarea (contrapresión) en lugar de agotar la memoria del nodo.
 * Si un hilo muere, se reemplaza automáticamente (auto-recuperación).
 */
const { Worker } = require('node:worker_threads');
const path = require('node:path');

class QueueFullError extends Error {
  constructor() { super('cola del nodo llena'); this.status = 503; }
}

class ThreadPool {
  constructor({ size, maxQueue, iterations, ioWaitMs = 0 }) {
    this.size = size;
    this.maxQueue = maxQueue;
    this.iterations = iterations;
    this.ioWaitMs = ioWaitMs;
    this.queue = [];
    this.idle = [];
    this.pending = new Map(); // jobId -> {resolve, reject, worker}
    this.seq = 0;
    this.stats = { completed: 0, failed: 0, rejected: 0, restarts: 0, maxQueueSeen: 0 };
    for (let i = 0; i < size; i++) this._spawn();
  }

  _spawn() {
    const w = new Worker(path.join(__dirname, 'worker-thread.js'));
    w.on('message', ({ jobId, ok, result, error }) => {
      const job = this.pending.get(jobId);
      this.pending.delete(jobId);
      if (ok) { this.stats.completed++; job.resolve(result); } else { this.stats.failed++; job.reject(new Error(error)); }
      this._release(w);
    });
    w.on('error', (err) => {
      for (const [id, job] of this.pending) if (job.worker === w) { this.pending.delete(id); job.reject(err); }
    });
    w.on('exit', (code) => {
      if (this.closing) return;
      this.idle = this.idle.filter((x) => x !== w);
      this.stats.restarts++;
      if (code !== 0) this._spawn();
    });
    this._release(w);
  }

  _release(w) {
    const next = this.queue.shift();
    if (next) this._run(w, next); else this.idle.push(w);
  }

  _run(w, job) {
    this.pending.set(job.jobId, { ...job, worker: w });
    w.postMessage({ jobId: job.jobId, message: job.message, iterations: this.iterations, ioWaitMs: this.ioWaitMs });
  }

  submit(message) {
    return new Promise((resolve, reject) => {
      const job = { jobId: ++this.seq, message, resolve, reject };
      const w = this.idle.pop();
      if (w) return this._run(w, job);
      if (this.queue.length >= this.maxQueue) { this.stats.rejected++; return reject(new QueueFullError()); }
      this.queue.push(job);
      this.stats.maxQueueSeen = Math.max(this.stats.maxQueueSeen, this.queue.length);
    });
  }

  load() {
    return { busy: this.size - this.idle.length, queued: this.queue.length, size: this.size };
  }

  async close() {
    this.closing = true;
    await Promise.all([...this.idle, ...[...this.pending.values()].map((j) => j.worker)].map((w) => w.terminate()));
  }
}

module.exports = { ThreadPool, QueueFullError };
