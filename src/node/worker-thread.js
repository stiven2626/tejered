'use strict';
/** Código que corre dentro de cada hilo (worker_threads) del pool. */
const { parentPort, threadId } = require('node:worker_threads');
const { processMessage } = require('./task');

parentPort.on('message', ({ jobId, message, iterations, ioWaitMs }) => {
  try {
    if (message?.payload?.__crash) throw new Error('fallo simulado dentro del hilo');
    parentPort.postMessage({ jobId, ok: true, result: { ...processMessage(message, iterations, ioWaitMs), threadId } });
  } catch (err) {
    parentPort.postMessage({ jobId, ok: false, error: err.message });
  }
});
