'use strict';
/**
 * Configuración centralizada. Todo se puede sobreescribir con variables de entorno,
 * lo que permite desplegar cada componente en un host/contenedor distinto.
 */
const env = (name, def) => (process.env[name] !== undefined ? process.env[name] : def);
const num = (name, def) => Number(env(name, def));

module.exports = {
  gateway: {
    port: num('GATEWAY_PORT', 3000),
    brokerUrl: env('BROKER_URL', 'http://127.0.0.1:4000'),
    apiKey: env('API_KEY', 'tejered-demo-key'),
    breakerThreshold: num('BREAKER_THRESHOLD', 5),   // fallos consecutivos para abrir el circuito
    breakerResetMs: num('BREAKER_RESET_MS', 2000),   // tiempo en OPEN antes de pasar a HALF_OPEN
    requestTimeoutMs: num('GATEWAY_TIMEOUT_MS', 8000),
  },
  broker: {
    port: num('BROKER_PORT', 4000),
    heartbeatTimeoutMs: num('HEARTBEAT_TIMEOUT_MS', 3000), // sin latido en este tiempo => nodo expulsado
    sweepIntervalMs: num('SWEEP_INTERVAL_MS', 500),
    maxAttempts: num('MAX_ATTEMPTS', 3),               // reintentos de entrega en otros nodos (failover)
    deliveryTimeoutMs: num('DELIVERY_TIMEOUT_MS', 5000),
    strategy: env('LB_STRATEGY', 'least-loaded'),      // 'least-loaded' | 'round-robin'
  },
  node: {
    id: env('NODE_ID', 'nodo-1'),
    host: env('NODE_HOST', '127.0.0.1'),
    port: num('NODE_PORT', 5001),
    brokerUrl: env('BROKER_URL', 'http://127.0.0.1:4000'),
    // formato tema:grupo separados por coma
    subscriptions: env('SUBSCRIPTIONS', 'tareas:procesadores,pedidos:facturacion,pedidos:notificaciones'),
    poolSize: num('POOL_SIZE', 2),          // 0 = procesamiento en el hilo principal (versión v1)
    maxQueue: num('MAX_QUEUE', 500),        // contrapresión: más tareas en cola => 503
    heartbeatMs: num('HEARTBEAT_MS', 1000),
    workIterations: num('WORK_ITERATIONS', 20000), // costo de CPU simulado por mensaje (~5 ms)
    ioWaitMs: num('IO_WAIT_MS', 20),               // espera de E/S simulada (consulta a BD/servicio externo)
  },
  keepAlive: env('KEEP_ALIVE', 'true') === 'true', // v1 = false (conexión nueva por petición)
};
