# TejeRed — arquitectura de software entre hilos y nodos

Prototipo del mini proyecto de la **Unidad 2 de Arquitectura de Software** (Tecnológica del Oriente).
Sistema distribuido de mensajería *publicación/suscripción* compuesto por un **API Gateway**, un **Broker**
y **N nodos workers** que procesan mensajes en un **pool de hilos**.

**Equipo:** Yeison Stiven Gutiérrez Rozo (líder y arquitecto) · Michael Yesid Torres Bello (desarrollador y evaluador de calidad)

```
Cliente ──► API Gateway (Proxy + Circuit Breaker) ──► Broker (Mediator + Observer + Strategy) ──► Nodo 1..N (Thread Pool)
                                                          ▲                                           │
                                                          └──────────── register / heartbeat ─────────┘
```

## Requisitos
- Node.js 18 o superior. **No hay dependencias que instalar.**
- (Opcional) Docker y Docker Compose.

## Uso rápido

```bash
npm start -- 3          # levanta gateway, broker y 3 nodos (procesos independientes)
npm run demo            # demostración guiada (la que se usa en el video)
npm test                # 6 pruebas automatizadas
npm run experiments     # pruebas de escalabilidad y robustez -> results/resultados.json
```

Publicar un mensaje:

```bash
curl -X POST http://localhost:3000/api/messages \
  -H "x-api-key: tejered-demo-key" -H "content-type: application/json" \
  -d '{"topic":"pedidos","payload":{"pedido":1,"total":50000}}'

curl http://localhost:3000/api/stats     # estado del gateway, broker, nodos y temas
```

Despliegue en contenedores:

```bash
docker compose up --build --scale worker=3
```

## Patrones de diseño aplicados

| Patrón | Archivo | Para qué |
|---|---|---|
| Proxy (protección y remoto) | `src/gateway/gateway.js`, `src/broker/remote-node-proxy.js` | Autenticar/validar y ocultar la red |
| Circuit Breaker | `src/gateway/circuit-breaker.js` | Fail-fast si el broker cae y recuperación automática |
| Mediator | `src/broker/mediator.js` | Coordinar registro, suscripciones y enrutamiento |
| Observer distribuido | `src/broker/topics.js` | Temas (Subject) y grupos de consumidores (Observer) con failover |
| Strategy | `src/broker/strategies.js` | Balanceo round-robin o least-loaded (`LB_STRATEGY`) |
| Thread Pool | `src/node/thread-pool.js` | Hilos `worker_threads` con cola acotada (contrapresión) |
| Heartbeat + registro | `src/broker/mediator.js`, `src/node/worker-node.js` | Detección de caídas y auto-registro |

## Configuración (variables de entorno)

| Variable | Defecto | Descripción |
|---|---|---|
| `POOL_SIZE` | 2 | Hilos por nodo (0 = procesar en el hilo principal, versión v1) |
| `LB_STRATEGY` | least-loaded | `least-loaded` o `round-robin` |
| `KEEP_ALIVE` | true | Reutilizar conexiones HTTP internas |
| `WORK_ITERATIONS` / `IO_WAIT_MS` | 20000 / 20 | Costo simulado de cada mensaje (~5 ms CPU + 20 ms E/S) |
| `HEARTBEAT_TIMEOUT_MS` | 3000 | Tiempo sin latido para expulsar un nodo |
| `MAX_ATTEMPTS` | 3 | Intentos de entrega (failover) |
| `BREAKER_THRESHOLD` / `BREAKER_RESET_MS` | 5 / 2000 | Parámetros del Circuit Breaker |

## Resultados principales (2 vCPU, ver `results/resultados.json`)

| Prueba | Resultado |
|---|---|
| E1 Escalabilidad | 79 → 253 msg/s de 1 a 4 nodos (eficiencia 81 %) |
| E2 Ajustes | Pool de hilos: +89 % de throughput con los mismos nodos |
| E3 Balanceo | Least-loaded: +137 % de throughput con un nodo lento |
| E5 Caída de nodo | 3000/3000 mensajes entregados, 8 failovers, 0 errores |
| E6 Caída del broker | Respuesta de 1 ms con circuito abierto; recuperación automática |

## Estructura

```
src/common      configuración, HTTP/JSON y logs
src/gateway     API Gateway y Circuit Breaker
src/broker      Mediator, temas/grupos, estrategias y proxy remoto
src/node        nodo worker, pool de hilos y tarea simulada
src/simulation  simulador de clúster, generador de carga, experimentos y demo
test            pruebas automatizadas (node --test)
docs/uml        diagramas PlantUML
docs/img        gráficas de resultados
results         métricas de las pruebas
```
