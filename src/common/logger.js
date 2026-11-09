'use strict';
const COLORS = { gateway: 36, broker: 35, nodo: 32, sim: 33 };
const LEVELS = { debug: 10, info: 20, warn: 30, error: 40, silent: 99 };
const current = LEVELS[process.env.LOG_LEVEL || 'info'] ?? 20;

function createLogger(name) {
  const kind = Object.keys(COLORS).find((k) => name.startsWith(k));
  const color = COLORS[kind] || 37;
  const tag = `\x1b[${color}m[${name}]\x1b[0m`;
  const log = (lvl, ...args) => {
    if (LEVELS[lvl] < current) return;
    const ts = new Date().toISOString().slice(11, 23);
    const fn = lvl === 'error' ? console.error : console.log;
    fn(`${ts} ${tag} ${lvl === 'info' ? '' : lvl.toUpperCase() + ' '}${args.join(' ')}`);
  };
  return {
    debug: (...a) => log('debug', ...a),
    info: (...a) => log('info', ...a),
    warn: (...a) => log('warn', ...a),
    error: (...a) => log('error', ...a),
  };
}
module.exports = { createLogger };
