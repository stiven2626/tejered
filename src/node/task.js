'use strict';
/**
 * Trabajo de negocio simulado que realiza un nodo al recibir un mensaje:
 *   1) CPU: PBKDF2 representa validar/firmar el documento (WORK_ITERATIONS).
 *   2) E/S: una espera bloqueante representa la consulta síncrona a una base de
 *      datos o servicio externo (IO_WAIT_MS). Al ser bloqueante, ocupa el hilo que
 *      la ejecuta: por eso importa en qué hilo se procesa el mensaje.
 */
const crypto = require('node:crypto');

const sleepBuffer = new Int32Array(new SharedArrayBuffer(4));

function processMessage(message, iterations, ioWaitMs = 0) {
  const t0 = process.hrtime.bigint();
  const text = typeof message.payload === 'string' ? message.payload : JSON.stringify(message.payload);
  const digest = crypto.pbkdf2Sync(text, message.id || 'sal', iterations, 32, 'sha256').toString('hex');
  if (ioWaitMs > 0) Atomics.wait(sleepBuffer, 0, 0, ioWaitMs);
  return {
    messageId: message.id,
    words: text.split(/\s+/).filter(Boolean).length,
    signature: digest.slice(0, 16),
    processingMs: +(Number(process.hrtime.bigint() - t0) / 1e6).toFixed(2),
  };
}

module.exports = { processMessage };
