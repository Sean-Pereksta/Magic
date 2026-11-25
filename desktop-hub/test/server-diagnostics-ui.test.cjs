'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const filename = path.join(__dirname, '../ui/hub.js');
const source = fs.readFileSync(filename, 'utf8');

// Run the shipped UI script. The harness only supplies the dialog DOM and IPC
// lifecycle so these tests need neither Electron nor a running public tunnel.
function createUI() {
  let panel, bodyHTML = '', nextTimer = 0;
  const timers = new Map(), listeners = new Map(), requests = [];
  const dialog = {
    open: false,
    showModal() { this.open = true; },
    close() {
      this.open = false;
      for (const listener of [...(listeners.get('close') || [])]) listener();
    },
    addEventListener(type, listener) {
      if (!listeners.has(type)) listeners.set(type, new Set());
      listeners.get(type).add(listener);
    },
    removeEventListener(type, listener) { listeners.get(type)?.delete(listener); },
    querySelector() { return panel; },
  };
  const body = {
    get innerHTML() { return bodyHTML; },
    set innerHTML(value) {
      bodyHTML = value;
      if (panel) panel.isConnected = false;
      panel = value.includes('data-server-diagnostics') ? {isConnected: true, innerHTML: ''} : null;
    },
  };
  const nodes = {'#dialog': dialog, '#dialog-body': body, '#view': {}, '#search': {addEventListener() {}}};
  const context = vm.createContext({
    document: {querySelector: selector => nodes[selector], querySelectorAll: () => [], addEventListener() {}},
    window: {
      addEventListener() {},
      hub: {
        invoke(action, payload) {
          if (action === 'state') return new Promise(() => {});
          assert.equal(action, 'serverDiagnostics');
          return new Promise((resolve, reject) => { requests.push({payload, resolve, reject}); });
        },
      },
    },
    icon: () => '',
    setTimeout(callback, delay) {
      const id = ++nextTimer;
      timers.set(id, {callback, delay});
      return id;
    },
    clearTimeout: id => timers.delete(id),
  });
  const api = vm.runInContext(source + '\n;({serverDiagnosticContent, showServerDiagnostics, showDialog});', context, {filename});
  return {
    ...api, dialog, requests, timers,
    get panel() { return panel; },
    async flush() { await Promise.resolve(); await Promise.resolve(); },
    async tick() {
      const first = timers.entries().next().value;
      assert.ok(first, 'A refresh should have been scheduled');
      timers.delete(first[0]);
      await first[1].callback();
    },
  };
}

function metric(html, label, value) {
  assert.ok(html.includes(`<dt>${label}</dt><dd>${value}</dd>`), `Expected ${label}: ${value}`);
}

test('diagnostics show measured latency, occupied seats, connected players, queue, and uptime', () => {
  const ui = createUI();
  const html = ui.serverDiagnosticContent({status: 'online', diagnosticsState: 'live', sampleAgeMs: 500, metrics: {
    playerCount: 3, maxPlayers: 8, connectedPlayers: 2, queuedPlayers: 1, uptimeSeconds: 3661,
    tunnelRttMs: 112.3, readyPlayers: 2, expectedPlayers: 3,
    players: [
      {name: '<Player one>', connected: true, transport: 'websocket', rttMs: 40, latencyAgeMs: 1250},
      {name: 'Player two', connected: true, transport: 'poll', rttMs: 80},
      {name: 'Player three', connected: false, transport: 'websocket', rttMs: 900},
    ],
  }});
  metric(html, 'Players on server', '3 / 8');
  metric(html, 'Connected now', '2');
  metric(html, 'Players queued', '1');
  metric(html, 'Host uptime', '1h 1m 1s');
  metric(html, 'Average player ping', '60 ms');
  metric(html, 'Public tunnel latency', '112.3 ms');
  assert.match(html, /Live statistics/);
  assert.match(html, /Reconnecting/);
  assert.match(html, /&lt;Player one&gt;/);
  assert.doesNotMatch(html, /<Player one>/);
  assert.match(html, /Latency measured 1\.3 s ago/);
});

test('zero players are explicit and absent measurements never become zero latency', () => {
  const ui = createUI();
  const html = ui.serverDiagnosticContent({status: 'lan-only', diagnosticsState: 'live', sampleAgeMs: 0, metrics: {
    playerCount: 0, maxPlayers: 8, connectedPlayers: 0, queuedPlayers: 0, uptimeSeconds: 0, players: [],
  }});
  metric(html, 'Players on server', '0 / 8');
  metric(html, 'Average player ping', '—');
  metric(html, 'Public tunnel latency', '—');
  metric(html, 'Host uptime', '0s');
  assert.match(html, /No players connected/);
  assert.doesNotMatch(html, /<dd>0 ms<\/dd>/);
});

test('old or missing host packets show actionable states without fabricating occupancy', () => {
  const ui = createUI();
  const unavailable = ui.serverDiagnosticContent({status: 'online', diagnosticsState: 'unavailable', uptimeSeconds: 23});
  assert.match(unavailable, /Statistics unavailable/);
  assert.match(unavailable, /Update the Acorn Rally hosting pack in Downloads/);
  metric(unavailable, 'Players on server', '—');
  metric(unavailable, 'Host uptime', '23s');
  assert.doesNotMatch(unavailable, /Waiting for live diagnostics/);
  const delayed = ui.serverDiagnosticContent({status: 'online', diagnosticsState: 'delayed', sampleAgeMs: 26_000, metrics: {
    playerCount: 1, maxPlayers: 8, players: [],
  }});
  assert.match(delayed, /Updates delayed/);
  assert.match(delayed, /Last sample 26s ago/);
  assert.match(delayed, /last reported values/);
  assert.doesNotMatch(delayed, /Live statistics/);
});

test('stopped hosts do not display cached values or continue a waiting message', () => {
  const ui = createUI();
  const html = ui.serverDiagnosticContent({status: 'offline', diagnosticsState: 'offline', metrics: {playerCount: 8}});
  assert.match(html, /Start this server/);
  assert.doesNotMatch(html, /Players on server|Waiting|Live statistics/);
});

test('polling waits for each request and stops when the dialog closes', async () => {
  const ui = createUI();
  ui.showServerDiagnostics('acornrally');
  assert.equal(ui.requests.length, 1);
  assert.equal(ui.timers.size, 0, 'Do not schedule overlapping requests');
  ui.requests[0].resolve({status: 'online', diagnosticsState: 'starting'});
  await ui.flush();
  assert.equal(ui.timers.size, 1);
  assert.equal([...ui.timers.values()][0].delay, 3000);
  ui.dialog.close();
  assert.equal(ui.timers.size, 0);
  assert.equal(ui.requests.length, 1);
});

test('a late IPC result cannot overwrite a replacement dialog or schedule more polling', async () => {
  const ui = createUI();
  ui.showServerDiagnostics('acornrally');
  const oldPanel = ui.panel;
  ui.showDialog('Player profiles', '<p>Choose a profile</p>');
  ui.requests[0].resolve({status: 'online', diagnosticsState: 'live', metrics: {playerCount: 8}});
  await ui.flush();
  assert.equal(oldPanel.innerHTML, '');
  assert.equal(ui.panel, null);
  assert.equal(ui.timers.size, 0);
});

test('IPC failures are escaped and the open panel retries', async () => {
  const ui = createUI();
  ui.showServerDiagnostics('acornrally');
  ui.requests[0].reject(new Error('<failed>'));
  await ui.flush();
  assert.match(ui.panel.innerHTML, /&lt;failed&gt;/);
  assert.match(ui.panel.innerHTML, /retry automatically/);
  assert.equal(ui.timers.size, 1);
  ui.dialog.close();
});
