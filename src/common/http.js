'use strict';
/**
 * Utilidades HTTP/JSON compartidas por todos los nodos.
 * Se usa solo el módulo nativo `http` para que el prototipo no tenga dependencias.
 */
const http = require('node:http');

function createAgent(keepAlive) {
  return new http.Agent({ keepAlive, maxSockets: 256, keepAliveMsecs: 1000 });
}

/** Petición HTTP que envía y recibe JSON. Rechaza ante error de red o timeout. */
function requestJson(url, { method = 'POST', body, headers = {}, timeout = 5000, agent } = {}) {
  return new Promise((resolve, reject) => {
    const payload = body === undefined ? null : Buffer.from(JSON.stringify(body));
    const req = http.request(url, {
      method,
      agent,
      headers: {
        'content-type': 'application/json',
        ...(payload ? { 'content-length': payload.length } : {}),
        ...headers,
      },
    }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf8');
        let data = null;
        try { data = text ? JSON.parse(text) : null; } catch { data = { raw: text }; }
        resolve({ status: res.statusCode, data });
      });
    });
    req.setTimeout(timeout, () => req.destroy(new Error(`timeout ${timeout} ms`)));
    req.on('error', reject);
    if (payload) req.write(payload);
    req.end();
  });
}

/** Lee el cuerpo JSON de una petición entrante (con límite de tamaño). */
function readJson(req, limit = 1_000_000) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on('data', (c) => {
      size += c.length;
      if (size > limit) { reject(Object.assign(new Error('payload demasiado grande'), { status: 413 })); req.destroy(); return; }
      chunks.push(c);
    });
    req.on('end', () => {
      if (!chunks.length) return resolve({});
      try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8'))); }
      catch { reject(Object.assign(new Error('JSON inválido'), { status: 400 })); }
    });
    req.on('error', reject);
  });
}

function sendJson(res, status, data) {
  const body = JSON.stringify(data);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(body) });
  res.end(body);
}

/** Enrutador mínimo: { 'POST /ruta': handler } */
function createServer(routes, onError) {
  return http.createServer(async (req, res) => {
    const path = req.url.split('?')[0];
    const handler = routes[`${req.method} ${path}`];
    if (!handler) return sendJson(res, 404, { error: 'ruta no encontrada' });
    try {
      await handler(req, res);
    } catch (err) {
      if (onError) onError(err);
      if (!res.headersSent) sendJson(res, err.status || 500, { error: err.message });
    }
  });
}

module.exports = { createAgent, requestJson, readJson, sendJson, createServer };
