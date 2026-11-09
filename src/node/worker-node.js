'use strict';
/**
 * Nodo worker: proceso independiente (otra máquina/contenedor en un despliegue real).
 *  - Se registra en el broker con sus suscripciones (tema:grupo).
 *  - Envía heartbeats periódicos; si el broker no lo reconoce, se vuelve a registrar.
 *  - Procesa cada mensaje en un pool de hilos (POOL_SIZE>0) o en el hilo principal (POOL_SIZE=0, v1).
 */
const { createServer, readJson, sendJson, requestJson, createAgent } = require('../common/http');
const { ThreadPool } = require('./thread-pool');
const { processMessage } = require('./task');
const { createLogger } = require('../common/logger');
const config = require('../common/config');

function parseSubscriptions(text) {
  return text.split(',').map((s) => s.trim()).filter(Boolean).map((s) => {
    const [topic, group] = s.split(':');
    return { topic, group: group || 'default' };
  });
}

async function startNode(overrides = {}) {
  const cfg = { ...config.node, ...overrides };
  const log = createLogger(cfg.id.startsWith('nodo') ? cfg.id : `nodo-${cfg.id}`);
  const agent = createAgent(config.keepAlive);
  const pool = cfg.poolSize > 0
    ? new ThreadPool({ size: cfg.poolSize, maxQueue: cfg.maxQueue, iterations: cfg.workIterations, ioWaitMs: cfg.ioWaitMs })
    : null;
  const stats = { received: 0, processed: 0, errors: 0, inline: 0 };
  let inflightInline = 0;

  const execute = async (message) => {
    if (pool) return pool.submit(message);
    // v1: el trabajo de CPU bloquea el event loop del nodo mientras dura
    inflightInline++;
    try {
      if (message?.payload?.__crash) throw new Error('fallo simulado');
      stats.inline++;
      return { ...processMessage(message, cfg.workIterations, cfg.ioWaitMs), threadId: 0 };
    } finally { inflightInline--; }
  };

  const load = () => (pool ? pool.load() : { busy: inflightInline, queued: 0, size: 1 });

  const server = createServer({
    'POST /deliver': async (req, res) => {
      const message = await readJson(req);
      stats.received++;
      try {
        const result = await execute(message);
        stats.processed++;
        sendJson(res, 200, { nodeId: cfg.id, ...result });
      } catch (err) {
        stats.errors++;
        sendJson(res, err.status || 500, { nodeId: cfg.id, error: err.message });
      }
    },
    'GET /health': (_q, res) => sendJson(res, 200, { ok: true, id: cfg.id, load: load(), stats, pool: pool?.stats }),
  }, (e) => log.error(e.message));
  server.keepAliveTimeout = 5000;

  await new Promise((r) => server.listen(cfg.port, r));
  const url = `http://${cfg.host}:${cfg.port}`;
  const subscriptions = parseSubscriptions(cfg.subscriptions);

  const register = async () => {
    for (let i = 0; ; i++) {
      try {
        const r = await requestJson(`${cfg.brokerUrl}/register`, {
          body: { id: cfg.id, url, subscriptions, poolSize: cfg.poolSize }, agent, timeout: 2000,
        });
        if (r.status === 201) { log.info(`registrado en el broker (${cfg.poolSize || 'sin'} hilos de trabajo)`); return; }
      } catch { /* broker aún no disponible */ }
      await new Promise((r) => setTimeout(r, Math.min(2000, 200 * (i + 1))));
    }
  };
  await register();

  let reRegistering = false;
  const hb = setInterval(async () => {
    try {
      const r = await requestJson(`${cfg.brokerUrl}/heartbeat`, { body: { id: cfg.id, load: load() }, agent, timeout: 1000 });
      if (r.status === 404 && !reRegistering) {
        reRegistering = true;
        log.warn('el broker no me reconoce: me registro de nuevo');
        await register();
        reRegistering = false;
      }
    } catch { /* broker caído: se reintenta en el siguiente latido */ }
  }, cfg.heartbeatMs);

  const stop = async () => {
    clearInterval(hb);
    try { await requestJson(`${cfg.brokerUrl}/deregister`, { body: { id: cfg.id }, timeout: 500 }); } catch { /* ignorar */ }
    server.close();
    if (pool) await pool.close();
  };
  process.on('SIGTERM', async () => { await stop(); process.exit(0); });
  return { server, stop, stats };
}

if (require.main === module) startNode();
module.exports = { startNode, parseSubscriptions };
