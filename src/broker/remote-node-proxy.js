'use strict';
/**
 * Patrón PROXY (proxy remoto) del lado del broker.
 * Representa localmente a un nodo worker que vive en otra máquina/proceso.
 * El resto del broker llama a deliver(message) como si fuera un objeto local;
 * el proxy se encarga de la red, el timeout y la contabilidad de carga (inflight).
 */
const { requestJson } = require('../common/http');

class NodeUnavailableError extends Error {
  constructor(nodeId, cause, { network = false, status } = {}) {
    super(`nodo ${nodeId} no disponible: ${cause}`);
    this.nodeId = nodeId; this.network = network; this.status = status;
  }
}

class RemoteNodeProxy {
  constructor(record, { agent, timeout }) {
    this.record = record; // NodeRecord del registro (id, url, inflight, ...)
    this.agent = agent;
    this.timeout = timeout;
  }

  async deliver(message) {
    const rec = this.record;
    rec.inflight++;
    try {
      const r = await requestJson(`${rec.url}/deliver`, { body: message, agent: this.agent, timeout: this.timeout });
      if (r.status !== 200) throw new NodeUnavailableError(rec.id, r.data?.error || `HTTP ${r.status}`, { status: r.status });
      rec.delivered++;
      return r.data;
    } catch (err) {
      rec.failures++;
      if (err instanceof NodeUnavailableError) throw err;
      throw new NodeUnavailableError(rec.id, err.code || err.message, { network: true });
    } finally {
      rec.inflight--;
    }
  }
}

module.exports = { RemoteNodeProxy, NodeUnavailableError };
