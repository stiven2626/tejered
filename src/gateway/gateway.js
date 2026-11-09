'use strict';
/**
 * API Gateway — patrón PROXY.
 * El cliente nunca habla directamente con el broker: habla con este sustituto, que
 * expone la misma operación (publicar un mensaje) y añade responsabilidades transversales:
 *   - Proxy de protección: valida la API key y el formato del mensaje.
 *   - Proxy remoto: oculta la ubicación real del broker en la red.
 *   - Circuit Breaker: corta el tráfico si el broker falla repetidamente.
 *   - Métricas: cuenta peticiones y mide la latencia extremo a extremo.
 */
const { createServer, readJson, sendJson, requestJson, createAgent } = require('../common/http');
const { CircuitBreaker } = require('./circuit-breaker');
const { createLogger } = require('../common/logger');
const config = require('../common/config');

class BrokerClient {
  /** Sujeto real (RealSubject) remoto: el broker. */
  constructor(url, agent, timeout) { this.url = url; this.agent = agent; this.timeout = timeout; }
  async publish(message) {
    const r = await requestJson(`${this.url}/publish`, { body: message, agent: this.agent, timeout: this.timeout });
    if (r.status >= 500) throw Object.assign(new Error(r.data?.error || `broker ${r.status}`), { status: r.status, data: r.data });
    return r;
  }
  stats() { return requestJson(`${this.url}/stats`, { method: 'GET', agent: this.agent, timeout: 2000 }); }
}

class ApiGatewayProxy {
  constructor({ brokerClient, apiKey, breaker, log }) {
    this.broker = brokerClient;
    this.apiKey = apiKey;
    this.breaker = breaker;
    this.log = log;
    this.metrics = { received: 0, unauthorized: 0, invalid: 0, forwarded: 0, failed: 0 };
  }

  authorize(req) {
    return req.headers['x-api-key'] === this.apiKey;
  }

  validate(msg) {
    if (!msg || typeof msg.topic !== 'string' || !msg.topic) return 'el campo "topic" es obligatorio';
    if (msg.payload === undefined) return 'el campo "payload" es obligatorio';
    return null;
  }

  async publish(req, res) {
    this.metrics.received++;
    if (!this.authorize(req)) { this.metrics.unauthorized++; return sendJson(res, 401, { error: 'API key inválida' }); }
    const msg = await readJson(req);
    const problem = this.validate(msg);
    if (problem) { this.metrics.invalid++; return sendJson(res, 400, { error: problem }); }

    const t0 = process.hrtime.bigint();
    try {
      const r = await this.breaker.exec(() => this.broker.publish({ topic: msg.topic, payload: msg.payload }));
      this.metrics.forwarded++;
      const gatewayMs = Number(process.hrtime.bigint() - t0) / 1e6;
      return sendJson(res, r.status, { ...r.data, gatewayMs: +gatewayMs.toFixed(2) });
    } catch (err) {
      this.metrics.failed++;
      this.log.warn(`no se pudo publicar: ${err.message} (circuito ${this.breaker.state})`);
      return sendJson(res, err.status && err.status >= 500 ? err.status : 503, {
        error: err.message, circuit: this.breaker.state, ...(err.data ? { detail: err.data } : {}),
      });
    }
  }

  async stats(_req, res) {
    let broker = null;
    try { broker = (await this.broker.stats()).data; } catch (e) { broker = { error: e.message }; }
    sendJson(res, 200, { gateway: { ...this.metrics, circuit: this.breaker.state, breaker: this.breaker.stats }, broker });
  }
}

function startGateway(overrides = {}) {
  const cfg = { ...config.gateway, ...overrides };
  const log = createLogger('gateway');
  const agent = createAgent(config.keepAlive);
  const breaker = new CircuitBreaker({
    threshold: cfg.breakerThreshold,
    resetMs: cfg.breakerResetMs,
    onStateChange: (s) => log.warn(`circuit breaker -> ${s}`),
  });
  const proxy = new ApiGatewayProxy({
    brokerClient: new BrokerClient(cfg.brokerUrl, agent, cfg.requestTimeoutMs),
    apiKey: cfg.apiKey, breaker, log,
  });
  const server = createServer({
    'POST /api/messages': (q, s) => proxy.publish(q, s),
    'GET /api/stats': (q, s) => proxy.stats(q, s),
    'GET /health': (_q, s) => sendJson(s, 200, { ok: true, circuit: breaker.state }),
  }, (e) => log.error(e.message));
  server.keepAliveTimeout = 5000;
  return new Promise((resolve) => server.listen(cfg.port, () => {
    log.info(`API Gateway escuchando en :${cfg.port} -> broker ${cfg.brokerUrl}`);
    resolve({ server, proxy });
  }));
}

if (require.main === module) startGateway();
module.exports = { startGateway, ApiGatewayProxy };
