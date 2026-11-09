'use strict';
/**
 * Patrón Circuit Breaker (patrón de nube / estabilidad).
 * Evita que el gateway siga enviando peticiones a un broker caído:
 *   CLOSED    -> las llamadas pasan; se cuentan fallos consecutivos.
 *   OPEN      -> las llamadas fallan de inmediato (fail-fast) durante resetMs.
 *   HALF_OPEN -> se deja pasar una llamada de prueba; si funciona se cierra, si no se reabre.
 */
class CircuitOpenError extends Error {
  constructor() { super('circuito abierto: broker no disponible'); this.status = 503; }
}

class CircuitBreaker {
  constructor({ threshold = 5, resetMs = 2000, onStateChange = () => {} } = {}) {
    this.threshold = threshold;
    this.resetMs = resetMs;
    this.onStateChange = onStateChange;
    this.state = 'CLOSED';
    this.failures = 0;
    this.openedAt = 0;
    this.trialInFlight = false;
    this.stats = { calls: 0, rejected: 0, failures: 0, opened: 0 };
  }

  _set(state) {
    if (this.state !== state) { this.state = state; this.onStateChange(state); }
  }

  async exec(fn) {
    this.stats.calls++;
    if (this.state === 'OPEN') {
      if (Date.now() - this.openedAt < this.resetMs) { this.stats.rejected++; throw new CircuitOpenError(); }
      this._set('HALF_OPEN');
    }
    if (this.state === 'HALF_OPEN') {
      if (this.trialInFlight) { this.stats.rejected++; throw new CircuitOpenError(); }
      this.trialInFlight = true;
    }
    try {
      const result = await fn();
      this.failures = 0;
      this.trialInFlight = false;
      this._set('CLOSED');
      return result;
    } catch (err) {
      this.trialInFlight = false;
      this.failures++;
      this.stats.failures++;
      if (this.state === 'HALF_OPEN' || this.failures >= this.threshold) {
        this.openedAt = Date.now();
        this.stats.opened++;
        this._set('OPEN');
      }
      throw err;
    }
  }
}

module.exports = { CircuitBreaker, CircuitOpenError };
