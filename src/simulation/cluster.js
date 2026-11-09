'use strict';
/**
 * Simulador de red local: lanza el broker, el gateway y N nodos workers como
 * PROCESOS SEPARADOS (cada uno con su propio event loop, memoria y puerto),
 * comunicados solo por HTTP — igual que si estuvieran en máquinas distintas.
 */
const { fork } = require('node:child_process');
const path = require('node:path');
const { requestJson } = require('../common/http');

const ROOT = path.join(__dirname, '..');
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

class Cluster {
  constructor({
    nodes = 3, poolSize = 2, strategy = 'least-loaded', keepAlive = true, workIterations = 20000, ioWaitMs = 20,
    basePort = 3000, logLevel = 'warn', silent = false, extraEnv = {}, nodeEnv = {},
  } = {}) {
    Object.assign(this, { nodeCount: nodes, poolSize, strategy, keepAlive, workIterations, ioWaitMs, logLevel, silent, extraEnv, nodeEnv });
    this.gatewayPort = basePort;
    this.brokerPort = basePort + 1000;
    this.nodePortBase = basePort + 2001;
    this.procs = new Map();
  }

  get brokerUrl() { return `http://127.0.0.1:${this.brokerPort}`; }
  get gatewayUrl() { return `http://127.0.0.1:${this.gatewayPort}`; }

  _env(extra) {
    return {
      ...process.env,
      LOG_LEVEL: this.logLevel,
      KEEP_ALIVE: String(this.keepAlive),
      BROKER_URL: this.brokerUrl,
      BROKER_PORT: String(this.brokerPort),
      GATEWAY_PORT: String(this.gatewayPort),
      LB_STRATEGY: this.strategy,
      ...this.extraEnv,
      ...extra,
    };
  }

  _fork(name, file, extra) {
    const child = fork(path.join(ROOT, file), [], { env: this._env(extra), stdio: this.silent ? 'ignore' : 'inherit' });
    this.procs.set(name, child);
    child.on('exit', () => { if (this.procs.get(name) === child) this.procs.delete(name); });
    return child;
  }

  startBroker() { return this._fork('broker', 'broker/broker.js', {}); }
  startGateway() { return this._fork('gateway', 'gateway/gateway.js', {}); }

  startNode(i) {
    const id = `nodo-${i}`;
    return this._fork(id, 'node/worker-node.js', {
      NODE_ID: id,
      NODE_PORT: String(this.nodePortBase + i - 1),
      POOL_SIZE: String(this.poolSize),
      WORK_ITERATIONS: String(this.workIterations),
      IO_WAIT_MS: String(this.ioWaitMs),
      ...(this.nodeEnv[i] || {}),
    });
  }

  async start() {
    this.startBroker();
    await this.waitFor(`${this.brokerUrl}/health`);
    this.startGateway();
    for (let i = 1; i <= this.nodeCount; i++) this.startNode(i);
    await this.waitFor(`${this.gatewayUrl}/health`);
    await this.waitForNodes(this.nodeCount);
    return this;
  }

  async waitFor(url, timeoutMs = 10000) {
    const end = Date.now() + timeoutMs;
    while (Date.now() < end) {
      try { const r = await requestJson(url, { method: 'GET', timeout: 500 }); if (r.status === 200) return; } catch { /* aún no */ }
      await sleep(100);
    }
    throw new Error(`timeout esperando ${url}`);
  }

  async waitForNodes(n, timeoutMs = 10000) {
    const end = Date.now() + timeoutMs;
    while (Date.now() < end) {
      try {
        const r = await requestJson(`${this.brokerUrl}/health`, { method: 'GET', timeout: 500 });
        if (r.data?.nodes >= n) return;
      } catch { /* */ }
      await sleep(100);
    }
    throw new Error(`timeout esperando ${n} nodos`);
  }

  /** Simula la caída abrupta de un componente (sin aviso al broker). */
  crash(name) {
    const p = this.procs.get(name);
    if (p) { p.kill('SIGKILL'); this.procs.delete(name); }
  }

  stats() { return requestJson(`${this.brokerUrl}/stats`, { method: 'GET', timeout: 2000 }).then((r) => r.data); }
  gatewayStats() { return requestJson(`${this.gatewayUrl}/api/stats`, { method: 'GET', timeout: 2000 }).then((r) => r.data); }

  async stop() {
    const procs = [...this.procs.values()];
    this.procs.clear();
    await Promise.all(procs.map((p) => new Promise((r) => {
      if (p.exitCode !== null) return r();
      p.once('exit', r);
      p.kill('SIGKILL');
    })));
  }
}

module.exports = { Cluster, sleep };
