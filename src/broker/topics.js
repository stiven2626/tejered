'use strict';
/**
 * Patrón OBSERVER distribuido.
 *   Subject  -> Topic: mantiene la lista de observadores y los notifica al llegar un mensaje.
 *   Observer -> ConsumerGroup: grupo de nodos remotos interesados en un tema.
 * Cada grupo recibe TODOS los mensajes del tema (difusión entre grupos), pero dentro del
 * grupo solo UN nodo procesa cada mensaje (reparto de carga entre réplicas), igual que
 * los "consumer groups" de Kafka. Dentro del grupo hay failover: si un nodo falla,
 * el mensaje se reintenta en otro.
 */

class Topic {
  constructor(name) { this.name = name; this.observers = new Map(); this.published = 0; }
  attach(group) { this.observers.set(group.name, group); }
  detach(groupName) { this.observers.delete(groupName); }
  getGroup(name) { return this.observers.get(name); }
  /** notify: entrega el mensaje a cada grupo observador en paralelo. */
  async notify(message) {
    this.published++;
    const groups = [...this.observers.values()];
    const results = await Promise.allSettled(groups.map((g) => g.update(message)));
    return results.map((r, i) => (r.status === 'fulfilled'
      ? { group: groups[i].name, ok: true, ...r.value }
      : { group: groups[i].name, ok: false, error: r.reason.message, attempts: r.reason.attempts }));
  }
}

class ConsumerGroup {
  constructor(topicName, name, { strategy, maxAttempts, onNodeFailure }) {
    this.topicName = topicName;
    this.name = name;
    this.members = new Map(); // nodeId -> { record, proxy }
    this.strategy = strategy;
    this.maxAttempts = maxAttempts;
    this.onNodeFailure = onNodeFailure;
    this.failovers = 0;
  }
  get key() { return `${this.topicName}/${this.name}`; }
  add(record, proxy) { this.members.set(record.id, { record, proxy }); }
  remove(nodeId) { this.members.delete(nodeId); }
  get size() { return this.members.size; }

  /** update(): método del observador invocado por Topic.notify() */
  async update(message) {
    const tried = new Set();
    const errors = [];
    for (let attempt = 1; attempt <= this.maxAttempts; attempt++) {
      const records = [...this.members.values()].map((m) => m.record);
      const target = this.strategy.pick(this.key, records, tried);
      if (!target) break;
      tried.add(target.id);
      try {
        const result = await this.members.get(target.id)?.proxy.deliver(message);
        if (!result) throw new Error(`nodo ${target.id} salió del grupo`);
        if (attempt > 1) this.failovers++;
        return { node: target.id, attempts: attempt, result };
      } catch (err) {
        errors.push(err.message);
        this.onNodeFailure(target.id, err);
      }
    }
    const e = new Error(this.members.size === 0
      ? `sin consumidores activos en ${this.key}`
      : `entrega fallida en ${this.key}: ${errors.join(' | ')}`);
    e.attempts = tried.size;
    throw e;
  }
}

module.exports = { Topic, ConsumerGroup };
