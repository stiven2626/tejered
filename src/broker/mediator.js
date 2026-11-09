'use strict';
/**
 * Patrón MEDIATOR.
 * Productores (vía gateway) y nodos consumidores nunca se conocen entre sí: toda la
 * coordinación pasa por este objeto, que encapsula cómo interactúan:
 *   - registro y expulsión de nodos (registro de servicios + heartbeat),
 *   - suscripción de nodos a temas/grupos,
 *   - enrutamiento de cada mensaje publicado hacia los grupos observadores.
 * Así, agregar un nodo o un tema nuevo no exige modificar a los demás componentes
 * (bajo acoplamiento => mantenibilidad).
 */
const { Topic, ConsumerGroup } = require('./topics');
const { RemoteNodeProxy } = require('./remote-node-proxy');

class MessageMediator {
  constructor({ strategy, maxAttempts, heartbeatTimeoutMs, agent, deliveryTimeoutMs, log }) {
    this.strategy = strategy;
    this.maxAttempts = maxAttempts;
    this.heartbeatTimeoutMs = heartbeatTimeoutMs;
    this.agent = agent;
    this.deliveryTimeoutMs = deliveryTimeoutMs;
    this.log = log;
    this.nodes = new Map();   // nodeId -> NodeRecord
    this.topics = new Map();  // topic -> Topic
    this.seq = 0;
    this.stats = { published: 0, delivered: 0, failed: 0, partial: 0, evictions: 0, registrations: 0 };
  }

  // ---------- registro de nodos ----------
  register({ id, url, subscriptions = [], poolSize = 0 }) {
    if (!id || !url) throw Object.assign(new Error('id y url son obligatorios'), { status: 400 });
    if (this.nodes.has(id)) this._removeNode(id, 're-registro');
    const record = {
      id, url, poolSize, subscriptions, lastSeen: Date.now(), registeredAt: Date.now(),
      inflight: 0, delivered: 0, failures: 0, load: null,
    };
    this.nodes.set(id, record);
    const proxy = new RemoteNodeProxy(record, { agent: this.agent, timeout: this.deliveryTimeoutMs });
    for (const { topic, group } of subscriptions) this._group(topic, group).add(record, proxy);
    this.stats.registrations++;
    this.log.info(`nodo ${id} registrado (${url}) en ${subscriptions.map((s) => `${s.topic}/${s.group}`).join(', ')}`);
    return record;
  }

  heartbeat({ id, load }) {
    const rec = this.nodes.get(id);
    if (!rec) return false; // el nodo debe volver a registrarse
    rec.lastSeen = Date.now();
    rec.load = load || null;
    return true;
  }

  deregister(id) { return this._removeNode(id, 'baja voluntaria'); }

  /** Barrido periódico: expulsa nodos sin latido reciente (detección de fallos). */
  sweep() {
    const now = Date.now();
    for (const rec of this.nodes.values()) {
      if (now - rec.lastSeen > this.heartbeatTimeoutMs) this._removeNode(rec.id, `sin heartbeat ${now - rec.lastSeen} ms`);
    }
  }

  /** Llamado por los grupos cuando una entrega falla. */
  onNodeFailure(nodeId, err) {
    if (err.network && this.nodes.has(nodeId)) this._removeNode(nodeId, `error de red (${err.message})`);
  }

  // ---------- publicación ----------
  async publish({ topic, payload }) {
    const t = this.topics.get(topic);
    const message = { id: `m-${++this.seq}`, topic, payload, publishedAt: Date.now() };
    this.stats.published++;
    if (!t || t.observers.size === 0) {
      this.stats.failed++;
      return { status: 503, body: { messageId: message.id, error: `no hay suscriptores para el tema "${topic}"` } };
    }
    const deliveries = await t.notify(message);
    const ok = deliveries.filter((d) => d.ok).length;
    let status = 200;
    if (ok === 0) { status = 503; this.stats.failed++; }
    else if (ok < deliveries.length) { status = 207; this.stats.partial++; }
    else this.stats.delivered++;
    return { status, body: { messageId: message.id, topic, deliveries } };
  }

  // ---------- utilidades ----------
  _group(topicName, groupName) {
    let topic = this.topics.get(topicName);
    if (!topic) { topic = new Topic(topicName); this.topics.set(topicName, topic); }
    let group = topic.getGroup(groupName);
    if (!group) {
      group = new ConsumerGroup(topicName, groupName, {
        strategy: this.strategy,
        maxAttempts: this.maxAttempts,
        onNodeFailure: (id, err) => this.onNodeFailure(id, err),
      });
      topic.attach(group);
    }
    return group;
  }

  _removeNode(id, reason) {
    const rec = this.nodes.get(id);
    if (!rec) return false;
    this.nodes.delete(id);
    for (const topic of this.topics.values()) {
      for (const group of [...topic.observers.values()]) {
        group.remove(id);
        if (group.size === 0) topic.detach(group.name);
      }
    }
    if (reason !== 're-registro') { this.stats.evictions++; this.log.warn(`nodo ${id} fuera del clúster: ${reason}`); }
    return true;
  }

  snapshot() {
    return {
      strategy: this.strategy.name,
      stats: this.stats,
      nodes: [...this.nodes.values()].map((n) => ({
        id: n.id, url: n.url, poolSize: n.poolSize, inflight: n.inflight, delivered: n.delivered,
        failures: n.failures, lastSeenMsAgo: Date.now() - n.lastSeen, load: n.load,
      })),
      topics: [...this.topics.values()].map((t) => ({
        topic: t.name, published: t.published,
        groups: [...t.observers.values()].map((g) => ({ group: g.name, members: [...g.members.keys()], failovers: g.failovers })),
      })),
    };
  }
}

module.exports = { MessageMediator };
