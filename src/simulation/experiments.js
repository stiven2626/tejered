'use strict';
/**
 * Batería de pruebas de escalabilidad y robustez. Guarda todo en results/resultados.json
 *   npm run experiments
 */
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { Cluster, sleep } = require('./cluster');
const { runLoad } = require('./loadtest');
const { requestJson } = require('../common/http');

const OUT = path.join(__dirname, '..', '..', 'results', 'resultados.json');
const TOTAL = Number(process.env.TOTAL || 1500);
const log = (...a) => console.log('\x1b[33m[exp]\x1b[0m', ...a);

async function withCluster(opts, fn) {
  const c = new Cluster({ silent: true, logLevel: 'silent', ...opts });
  await c.start();
  try { await runLoad({ url: c.gatewayUrl, total: 100, concurrency: 8 }); return await fn(c); } // calentamiento
  finally { await c.stop(); await sleep(300); }
}

const summary = (r) => `ok=${r.ok}/${r.total} thr=${r.throughput}/s p50=${r.latency.p50}ms p95=${r.latency.p95}ms`;

async function e1Escalabilidad() {
  log('E1 escalabilidad horizontal (v2, 32 clientes)');
  const rows = [];
  for (const nodes of [1, 2, 3, 4]) {
    const r = await withCluster({ nodes }, (c) => runLoad({ url: c.gatewayUrl, total: TOTAL, concurrency: 32 }));
    log(`  ${nodes} nodo(s): ${summary(r)}`);
    rows.push({ nodes, ...r });
  }
  return rows;
}

async function e2Ajustes() {
  log('E2 ajustes incrementales v1 -> v2 (2 nodos, 32 clientes)');
  const variants = [
    { name: 'v1: hilo principal, round-robin, sin keep-alive', poolSize: 0, strategy: 'round-robin', keepAlive: false },
    { name: 'v1.1: + pool de 2 hilos', poolSize: 2, strategy: 'round-robin', keepAlive: false },
    { name: 'v1.2: + keep-alive', poolSize: 2, strategy: 'round-robin', keepAlive: true },
    { name: 'v2: + least-loaded', poolSize: 2, strategy: 'least-loaded', keepAlive: true },
  ];
  const rows = [];
  for (const v of variants) {
    const r = await withCluster({ nodes: 2, ...v }, (c) => runLoad({ url: c.gatewayUrl, total: TOTAL, concurrency: 32 }));
    log(`  ${v.name}: ${summary(r)}`);
    rows.push({ variant: v.name, ...v, ...r });
  }
  return rows;
}

async function e3Heterogeneo() {
  log('E3 nodos heterogéneos: nodo-3 lento (E/S 80 ms) — round-robin vs least-loaded');
  const rows = [];
  for (const strategy of ['round-robin', 'least-loaded']) {
    const r = await withCluster({ nodes: 3, strategy, nodeEnv: { 3: { IO_WAIT_MS: '80' } } },
      (c) => runLoad({ url: c.gatewayUrl, total: TOTAL, concurrency: 32 }));
    log(`  ${strategy}: ${summary(r)} reparto=${JSON.stringify(r.perNode)}`);
    rows.push({ strategy, ...r });
  }
  return rows;
}

async function e4Concurrencia() {
  log('E4 barrido de concurrencia (2 nodos v2)');
  return withCluster({ nodes: 2 }, async (c) => {
    const rows = [];
    for (const concurrency of [4, 8, 16, 32, 64, 128]) {
      const r = await runLoad({ url: c.gatewayUrl, total: Math.max(600, concurrency * 10), concurrency });
      log(`  ${concurrency} clientes: ${summary(r)}`);
      rows.push({ concurrency, ...r });
    }
    return rows;
  });
}

async function e5CaidaNodo() {
  log('E5 robustez: caída abrupta de nodo-2 y reincorporación (3 nodos)');
  return withCluster({ nodes: 3 }, async (c) => {
    const marks = {};
    const t0 = Date.now();
    const load = runLoad({ url: c.gatewayUrl, total: 3000, concurrency: 24, timeline: true });
    await sleep(2000); c.crash('nodo-2'); marks.crashAt = Date.now() - t0;
    await sleep(4000); c.startNode(2); marks.restartAt = Date.now() - t0;
    const r = await load;
    const st = await c.stats();
    marks.evictions = st.stats.evictions;
    marks.groupFailovers = st.topics.flatMap((t) => t.groups).reduce((a, g) => a + g.failovers, 0);
    marks.nodesAtEnd = st.nodes.map((n) => n.id);
    // serie temporal agregada en ventanas de 250 ms
    const win = 250; const series = {};
    for (const e of r.events) {
      const k = Math.floor(e.t / win);
      series[k] ??= { t: k * win / 1000, ok: 0, fail: 0, lat: [] };
      if (e.ok) { series[k].ok++; series[k].lat.push(e.ms); } else series[k].fail++;
    }
    const timeline = Object.values(series).map((s) => ({
      t: s.t, throughput: s.ok * (1000 / win), failed: s.fail,
      p95: s.lat.length ? s.lat.sort((a, b) => a - b)[Math.ceil(0.95 * s.lat.length) - 1] : null,
    }));
    delete r.events;
    log(`  ${summary(r)} errores=${JSON.stringify(r.errors)} failovers=${r.failovers} ${JSON.stringify(marks)}`);
    return { ...r, ...marks, timeline };
  });
}

async function e6CircuitBreaker() {
  log('E6 robustez: caída del broker y Circuit Breaker del gateway');
  return withCluster({ nodes: 2, extraEnv: { BREAKER_THRESHOLD: '5', BREAKER_RESET_MS: '2000' } }, async (c) => {
    const send = async () => {
      const s = process.hrtime.bigint();
      let status;
      try { status = (await requestJson(`${c.gatewayUrl}/api/messages`, { body: { topic: 'tareas', payload: 'x' }, headers: { 'x-api-key': 'tejered-demo-key' }, timeout: 5000 })).status; }
      catch { status = 'ERR'; }
      return { status, ms: +(Number(process.hrtime.bigint() - s) / 1e6).toFixed(2) };
    };
    const t0 = Date.now();
    const calls = [];
    c.crash('broker');
    const crashAt = Date.now() - t0;
    for (let i = 0; i < 15; i++) {
      const r = await send(); const g = await c.gatewayStats();
      calls.push({ i: i + 1, t: Date.now() - t0, ...r, circuit: g.gateway.circuit });
      await sleep(50);
    }
    c.startBroker(); const brokerRestartAt = Date.now() - t0;
    await c.waitFor(`${c.brokerUrl}/health`);
    // los nodos detectan 404 en el heartbeat y se registran de nuevo
    await c.waitForNodes(2); const nodesBackAt = Date.now() - t0;
    let recoveredAt = null;
    for (let i = 0; i < 60; i++) {
      const r = await send(); const g = await c.gatewayStats();
      calls.push({ i: calls.length + 1, t: Date.now() - t0, ...r, circuit: g.gateway.circuit });
      if (r.status === 200) { recoveredAt = Date.now() - t0; break; }
      await sleep(100);
    }
    const g = await c.gatewayStats();
    log(`  rechazos rápidos=${g.gateway.breaker.rejected} recuperado en t=${recoveredAt} ms (broker reiniciado en ${brokerRestartAt} ms)`);
    return { crashAt, brokerRestartAt, nodesBackAt, recoveredAt, breaker: g.gateway.breaker, calls };
  });
}

(async () => {
  const which = process.argv[2];
  const all = { e1: e1Escalabilidad, e2: e2Ajustes, e3: e3Heterogeneo, e4: e4Concurrencia, e5: e5CaidaNodo, e6: e6CircuitBreaker };
  const prev = fs.existsSync(OUT) ? JSON.parse(fs.readFileSync(OUT, 'utf8')) : {};
  const results = { ...prev, entorno: { node: process.version, cpus: os.cpus().length, cpu: os.cpus()[0].model, ramGB: +(os.totalmem() / 1e9).toFixed(1), fecha: new Date().toISOString(), totalPorPrueba: TOTAL } };
  for (const [k, fn] of Object.entries(all)) {
    if (which && which !== k) continue;
    results[k] = await fn();
    fs.mkdirSync(path.dirname(OUT), { recursive: true });
    fs.writeFileSync(OUT, JSON.stringify(results, null, 2));
  }
  log(`resultados guardados en ${path.relative(process.cwd(), OUT)}`);
  process.exit(0);
})().catch((e) => { console.error(e); process.exit(1); });
