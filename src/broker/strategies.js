'use strict';
/**
 * Patrón STRATEGY: algoritmos intercambiables para elegir qué nodo de un grupo
 * de consumidores recibe el mensaje. Se selecciona con LB_STRATEGY.
 */
class RoundRobinStrategy {
  constructor() { this.name = 'round-robin'; this.cursor = new Map(); }
  pick(groupKey, members, exclude) {
    const candidates = members.filter((m) => !exclude.has(m.id));
    if (!candidates.length) return null;
    const i = (this.cursor.get(groupKey) || 0) % candidates.length;
    this.cursor.set(groupKey, i + 1);
    return candidates[i];
  }
}

class LeastLoadedStrategy {
  constructor() { this.name = 'least-loaded'; }
  pick(_groupKey, members, exclude) {
    let best = null;
    for (const m of members) {
      if (exclude.has(m.id)) continue;
      if (!best || m.inflight < best.inflight || (m.inflight === best.inflight && m.delivered < best.delivered)) best = m;
    }
    return best;
  }
}

function createStrategy(name) {
  if (name === 'round-robin') return new RoundRobinStrategy();
  return new LeastLoadedStrategy();
}

module.exports = { createStrategy, RoundRobinStrategy, LeastLoadedStrategy };
