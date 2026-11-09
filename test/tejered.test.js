'use strict';
// Pruebas automatizadas: node --test
const test = require('node:test');
const assert = require('node:assert/strict');
const { CircuitBreaker } = require('../src/gateway/circuit-breaker');
const { RoundRobinStrategy, LeastLoadedStrategy } = require('../src/broker/strategies');
const { ThreadPool } = require('../src/node/thread-pool');
const { parseSubscriptions } = require('../src/node/worker-node');
const { Cluster, sleep } = require('../src/simulation/cluster');
const { requestJson } = require('../src/common/http');

test('Circuit Breaker: CLOSED -> OPEN -> HALF_OPEN -> CLOSED', async () => {
  const cb = new CircuitBreaker({ threshold: 2, resetMs: 100 });
  const fail = () => Promise.reject(new Error('x'));
  await assert.rejects(cb.exec(fail));
  assert.equal(cb.state, 'CLOSED');
  await assert.rejects(cb.exec(fail));
  assert.equal(cb.state, 'OPEN');
  await assert.rejects(cb.exec(() => Promise.resolve(1)), /circuito abierto/);
  await sleep(120);
  assert.equal(await cb.exec(() => Promise.resolve(42)), 42);
  assert.equal(cb.state, 'CLOSED');
});

test('Strategy round-robin reparte en orden y respeta exclusiones', () => {
  const s = new RoundRobinStrategy();
  const m = [{ id: 'a' }, { id: 'b' }, { id: 'c' }];
  assert.deepEqual([1, 2, 3, 4].map(() => s.pick('g', m, new Set()).id), ['a', 'b', 'c', 'a']);
  assert.equal(s.pick('h', m, new Set(['a', 'b'])).id, 'c');
  assert.equal(s.pick('h', m, new Set(['a', 'b', 'c'])), null);
});

test('Strategy least-loaded elige el nodo con menos mensajes en curso', () => {
  const s = new LeastLoadedStrategy();
  const m = [{ id: 'a', inflight: 3, delivered: 0 }, { id: 'b', inflight: 1, delivered: 9 }, { id: 'c', inflight: 1, delivered: 2 }];
  assert.equal(s.pick('g', m, new Set()).id, 'c');
});

test('parseSubscriptions interpreta tema:grupo', () => {
  assert.deepEqual(parseSubscriptions('pedidos:facturacion, alertas'), [
    { topic: 'pedidos', group: 'facturacion' }, { topic: 'alertas', group: 'default' }]);
});

test('ThreadPool procesa en paralelo y aplica contrapresión', async () => {
  const pool = new ThreadPool({ size: 2, maxQueue: 1, iterations: 1000, ioWaitMs: 50 });
  const jobs = [1, 2, 3].map((i) => pool.submit({ id: `m-${i}`, payload: 'hola' }));
  await assert.rejects(pool.submit({ id: 'm-4', payload: 'x' }), /cola del nodo llena/);
  const results = await Promise.all(jobs);
  assert.equal(results.length, 3);
  assert.ok(new Set(results.map((r) => r.threadId)).size === 2, 'usa los dos hilos');
  await assert.rejects(pool.submit({ id: 'm-5', payload: { __crash: true } }), /fallo simulado/);
  await pool.close();
});

test('Integración: gateway -> broker -> nodos, seguridad y failover', { timeout: 30000 }, async () => {
  const c = await new Cluster({ nodes: 2, basePort: 3100, silent: true, logLevel: 'silent' }).start();
  try {
    const url = `${c.gatewayUrl}/api/messages`;
    const headers = { 'x-api-key': 'tejered-demo-key' };
    assert.equal((await requestJson(url, { body: { topic: 'tareas', payload: 1 } })).status, 401);
    assert.equal((await requestJson(url, { body: { payload: 1 }, headers })).status, 400);
    assert.equal((await requestJson(url, { body: { topic: 'nadie', payload: 1 }, headers })).status, 503);

    const r = await requestJson(url, { body: { topic: 'pedidos', payload: { id: 7 } }, headers });
    assert.equal(r.status, 200);
    assert.deepEqual(r.data.deliveries.map((d) => d.group).sort(), ['facturacion', 'notificaciones']);

    c.crash('nodo-1');
    for (let i = 0; i < 5; i++) {
      const x = await requestJson(url, { body: { topic: 'tareas', payload: i }, headers });
      assert.equal(x.status, 200);
      assert.equal(x.data.deliveries[0].node, 'nodo-2');
    }
  } finally { await c.stop(); }
});
