'use strict';
/** Levanta el clúster local y lo deja corriendo:  npm start -- 4   (4 nodos) */
const { Cluster } = require('./cluster');

(async () => {
  const nodes = Number(process.argv[2] || 3);
  const c = await new Cluster({ nodes, logLevel: 'info' }).start();
  console.log(`\nTejeRed listo: gateway ${c.gatewayUrl} · broker ${c.brokerUrl} · ${nodes} nodos`);
  console.log(`Prueba:  curl -X POST ${c.gatewayUrl}/api/messages -H "x-api-key: tejered-demo-key" -H "content-type: application/json" -d '{"topic":"pedidos","payload":{"pedido":1}}'`);
  console.log(`Estado:  curl ${c.gatewayUrl}/api/stats\nCtrl+C para detener.`);
  const bye = async () => { await c.stop(); process.exit(0); };
  process.on('SIGINT', bye);
  process.on('SIGTERM', bye);
})();
