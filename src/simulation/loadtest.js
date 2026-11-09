'use strict';
/**
 * Generador de carga: N clientes concurrentes publican mensajes a través del gateway.
 * Mide throughput, latencias (p50/p95/p99), errores, reparto por nodo y failovers.
 *   node src/simulation/loadtest.js --total 2000 --concurrency 32
 */
const { requestJson, createAgent } = require('../common/http');

function percentile(sorted, p) {
  if (!sorted.length) return 0;
  return sorted[Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1)];
}

async function runLoad({
  url = 'http://127.0.0.1:3000', apiKey = 'tejered-demo-key', topic = 'tareas',
  total = 1000, concurrency = 16, keepAlive = true, onProgress, timeline = false,
} = {}) {
  const agent = createAgent(keepAlive);
  const latencies = [];
  const perNode = {};
  const errors = {};
  const events = [];
  let sent = 0; let ok = 0; let failovers = 0;
  const t0 = Date.now();

  async function client() {
    while (sent < total) {
      const n = ++sent;
      const start = process.hrtime.bigint();
      let status = 0; let data = null;
      try {
        ({ status, data } = await requestJson(`${url}/api/messages`, {
          body: { topic, payload: `mensaje ${n} de prueba de carga para TejeRed` },
          headers: { 'x-api-key': apiKey }, agent, timeout: 15000,
        }));
      } catch (e) { status = e.code || 'ERR'; }
      const ms = Number(process.hrtime.bigint() - start) / 1e6;
      if (status === 200) {
        ok++;
        latencies.push(ms);
        for (const d of data.deliveries || []) {
          if (d.node) perNode[d.node] = (perNode[d.node] || 0) + 1;
          if (d.attempts > 1) failovers++;
        }
      } else {
        errors[status] = (errors[status] || 0) + 1;
      }
      if (timeline) events.push({ t: Date.now() - t0, ok: status === 200, ms });
      if (onProgress && n % 100 === 0) onProgress(n);
    }
  }

  await Promise.all(Array.from({ length: concurrency }, client));
  const elapsed = (Date.now() - t0) / 1000;
  latencies.sort((a, b) => a - b);
  const r2 = (x) => +x.toFixed(2);
  return {
    total, concurrency, ok, failed: total - ok, errors, failovers,
    seconds: r2(elapsed),
    throughput: r2(ok / elapsed),
    latency: {
      avg: r2(latencies.reduce((a, b) => a + b, 0) / (latencies.length || 1)),
      p50: r2(percentile(latencies, 50)), p95: r2(percentile(latencies, 95)),
      p99: r2(percentile(latencies, 99)), max: r2(latencies[latencies.length - 1] || 0),
    },
    perNode,
    ...(timeline ? { events } : {}),
  };
}

if (require.main === module) {
  const args = Object.fromEntries(process.argv.slice(2).join(' ').split('--').filter(Boolean)
    .map((s) => s.trim().split(/\s+/)).map(([k, v]) => [k, isNaN(v) ? v : Number(v)]));
  runLoad(args).then((r) => console.log(JSON.stringify(r, null, 2)));
}

module.exports = { runLoad, percentile };
