'use strict';
/**
 * Demostración guiada para el video (npm run demo):
 *   1. Levanta broker, gateway y 3 nodos.
 *   2. Publica un pedido (Observer: dos grupos lo reciben).
 *   3. Prueba de seguridad del Proxy (sin API key).
 *   4. Ráfaga de carga y reparto entre nodos.
 *   5. Tumba nodo-2 en plena carga -> failover.
 *   6. Tumba el broker -> Circuit Breaker abre; lo reinicia -> recuperación.
 */
const { Cluster, sleep } = require('./cluster');
const { runLoad } = require('./loadtest');
const { requestJson } = require('../common/http');

const step = async (title) => { console.log(`\n\x1b[1;33m==== ${title} ====\x1b[0m`); await sleep(1500); };
const KEY = { 'x-api-key': 'tejered-demo-key' };

(async () => {
  const c = new Cluster({ nodes: 3, logLevel: 'info' });
  await step('1. Arranque del sistema distribuido (procesos independientes)');
  await c.start();
  const url = `${c.gatewayUrl}/api/messages`;

  await step('2. Publicar un pedido: el tema "pedidos" tiene 2 grupos observadores');
  const r = await requestJson(url, { body: { topic: 'pedidos', payload: { pedido: 1024, cliente: 'ACME', total: 250000 } }, headers: KEY });
  for (const d of r.data.deliveries) console.log(`   grupo ${d.group} -> ${d.node} (hilo ${d.result.threadId}, ${d.result.processingMs} ms)`);

  await step('3. Proxy de protección: petición sin API key');
  console.log('   HTTP', (await requestJson(url, { body: { topic: 'pedidos', payload: 1 } })).status, '(rechazada en el gateway)');

  await step('4. Carga: 600 mensajes con 24 clientes concurrentes');
  const a = await runLoad({ url: c.gatewayUrl, total: 600, concurrency: 24 });
  console.log(`   ${a.ok}/${a.total} ok · ${a.throughput} msg/s · p95 ${a.latency.p95} ms · reparto ${JSON.stringify(a.perNode)}`);

  await step('5. Caída abrupta de nodo-2 en plena carga');
  const load = runLoad({ url: c.gatewayUrl, total: 1200, concurrency: 24 });
  await sleep(1500);
  console.log('   >>> kill -9 nodo-2');
  c.crash('nodo-2');
  const b = await load;
  console.log(`   ${b.ok}/${b.total} ok · failovers ${b.failovers} · errores ${JSON.stringify(b.errors)} · reparto ${JSON.stringify(b.perNode)}`);

  await step('6. Caída del broker: el Circuit Breaker del gateway');
  c.crash('broker');
  for (let i = 1; i <= 8; i++) {
    const t = Date.now();
    let s; try { s = (await requestJson(url, { body: { topic: 'tareas', payload: i }, headers: KEY })).status; } catch { s = 'ERR'; }
    const g = await c.gatewayStats();
    console.log(`   petición ${i}: HTTP ${s} en ${Date.now() - t} ms · circuito ${g.gateway.circuit}`);
  }
  console.log('   >>> reinicio del broker');
  c.startBroker();
  await c.waitForNodes(2);
  await sleep(2200);
  const ok = await requestJson(url, { body: { topic: 'tareas', payload: 'de vuelta' }, headers: KEY });
  console.log(`   HTTP ${ok.status} · atendido por ${ok.data.deliveries?.[0]?.node} · circuito ${(await c.gatewayStats()).gateway.circuit}`);

  await step('Fin de la demostración');
  await c.stop();
  process.exit(0);
})().catch(async (e) => { console.error(e); process.exit(1); });
