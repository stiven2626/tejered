'use strict';
/** Servidor HTTP del broker: expone el Mediator a la red. */
const { createServer, readJson, sendJson, createAgent } = require('../common/http');
const { MessageMediator } = require('./mediator');
const { createStrategy } = require('./strategies');
const { createLogger } = require('../common/logger');
const config = require('../common/config');

function startBroker(overrides = {}) {
  const cfg = { ...config.broker, ...overrides };
  const log = createLogger('broker');
  const mediator = new MessageMediator({
    strategy: createStrategy(cfg.strategy),
    maxAttempts: cfg.maxAttempts,
    heartbeatTimeoutMs: cfg.heartbeatTimeoutMs,
    deliveryTimeoutMs: cfg.deliveryTimeoutMs,
    agent: createAgent(config.keepAlive),
    log,
  });

  const server = createServer({
    'POST /register': async (req, res) => sendJson(res, 201, { ok: true, id: mediator.register(await readJson(req)).id }),
    'POST /heartbeat': async (req, res) => {
      const known = mediator.heartbeat(await readJson(req));
      sendJson(res, known ? 200 : 404, { ok: known });
    },
    'POST /deregister': async (req, res) => sendJson(res, 200, { ok: mediator.deregister((await readJson(req)).id) }),
    'POST /publish': async (req, res) => {
      const r = await mediator.publish(await readJson(req));
      sendJson(res, r.status, r.body);
    },
    'GET /stats': (_req, res) => sendJson(res, 200, mediator.snapshot()),
    'GET /health': (_req, res) => sendJson(res, 200, { ok: true, nodes: mediator.nodes.size }),
  }, (e) => log.error(e.message));
  server.keepAliveTimeout = 5000;

  const timer = setInterval(() => mediator.sweep(), cfg.sweepIntervalMs);
  server.on('close', () => clearInterval(timer));

  return new Promise((resolve) => server.listen(cfg.port, () => {
    log.info(`Broker (Mediator) escuchando en :${cfg.port} — estrategia ${cfg.strategy}`);
    resolve({ server, mediator });
  }));
}

if (require.main === module) startBroker();
module.exports = { startBroker };
