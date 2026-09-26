import { makeCouncilContext, scriptedCouncil, validateCouncilResponse } from './alliance-council.mjs';
import { isAiHouse } from './house-control.mjs';
import { relationshipResponse } from './diplomacy.mjs';
import { makeContext, scriptedReply, validateResponse } from './diplomacy.mjs';
import { diagnosticDetails, makeDiagnostic, readDiagnostic } from './diagnostics.mjs';

async function readJSON(response, limit) {
  if (!response.body) throw new Error('empty');
  const reader = response.body.getReader(), chunks = []; let size = 0;
  try {
    while (true) {
      const { value, done } = await reader.read(); if (done) break;
      size += value.byteLength;
      if (size > limit) { await reader.cancel(); throw new Error('oversize'); }
      chunks.push(value);
    }
  } finally { reader.releaseLock(); }
  const bytes = new Uint8Array(size); let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  return JSON.parse(new TextDecoder().decode(bytes));
}

export function validEndpoint(value) {
  if (!value) return '';
  try {
    const u = new URL(value);
    if (u.protocol !== 'https:' || u.username || u.password || u.search || u.hash || !['/', '/diplomacy'].includes(u.pathname)) return '';
    u.pathname = '/diplomacy'; return u.href;
  } catch { return ''; }
}
export class DiplomacyClient {
  constructor({ endpoint = '', fetcher = fetch, now = Date.now } = {}) {
    this.endpoint = validEndpoint(endpoint);
    this.fetcher = (...args) => fetcher(...args); this.now = now;
    this.cooldownUntil = 0; this.busy = false; this.cache = new Map(); this.controller = null;
    // Never serialize authentication into a campaign, export, or localStorage.
    this.session = null; this.sessionRequest = null; this.sessionController = null;
    this.lastDiagnostic = null;
  }
  hasSession() { return !!this.session && this.session.expires > this.now() + 5000; }
  cancel() { this.controller?.abort(); }
  recordFailure(code, details = {}, path = '') {
    this.lastDiagnostic = { ...makeDiagnostic(code, details), at: this.now(), path,
      ...(Number.isInteger(details.httpStatus) ? { httpStatus: details.httpStatus } : {}),
      ...(details.retryAt > this.now() ? { retryAt: details.retryAt } : {}) };
    return this.lastDiagnostic;
  }
  async readFailure(response, path, retryAt = 0) {
    let diagnostic;
    try { diagnostic = readDiagnostic((await readJSON(response, 8000))?.diagnostics); } catch { /* Older Worker or unreadable error body. */ }
    const code = response.status === 401 ? 'SESSION_EXPIRED' : response.status === 403 ? 'WORKER_ACCESS_DENIED'
      : response.status === 404 ? 'WORKER_ROUTE_MISSING' : response.status === 429 ? (path === '/session' ? 'SESSION_RATE_LIMIT' : 'RATE_LIMIT_UNKNOWN')
      : [400, 405, 413, 415].includes(response.status) ? 'REQUEST_INVALID' : 'WORKER_UNAVAILABLE';
    return this.recordFailure(diagnostic?.code || code, { ...diagnostic, httpStatus: response.status, retryAt }, path);
  }
  async openSession(turnstileToken = '') {
    if (this.sessionRequest) return this.sessionRequest;
    if (this.hasSession() && this.session.expires > this.now() + 5 * 60000) return true;
    const current = this.hasSession() ? this.session.token : '';
    if (!this.endpoint || (!current && !turnstileToken)) { this.session = null; this.recordFailure(this.endpoint ? 'SESSION_NOT_READY' : 'CLIENT_CONFIG'); return false; }
    const controller = new AbortController(); this.sessionController = controller;
    const timer = setTimeout(() => controller.abort(), 10000);
    this.sessionRequest = Promise.resolve().then(async () => {
      try {
        const response = await this.fetcher(new URL('/session', this.endpoint).href, { method: 'POST', headers: { 'Content-Type': 'application/json', ...(current ? { Authorization: `Bearer ${current}` } : {}) }, credentials: 'omit', signal: controller.signal, body: JSON.stringify(current ? {} : { turnstileToken }) });
        if (!response.ok) {
          if ([401, 403].includes(response.status)) this.session = null;
          const seconds = Math.min(3600, Math.max(0, Number(response.headers.get('Retry-After')) || 0));
          await this.readFailure(response, '/session', seconds ? this.now() + seconds * 1000 : 0); return false;
        }
        let value;
        try { value = await readJSON(response, 3000); } catch (error) { if (controller.signal.aborted) throw error; /* Report a malformed session below. */ }
        if (typeof value?.token !== 'string' || !value.token || value.token.length > 1600 || !Number.isSafeInteger(value.expires) || value.expires <= this.now() + 5000) {
          this.recordFailure('SESSION_RESPONSE_INVALID', { httpStatus: response.status }, '/session'); return false;
        }
        this.session = { token: value.token, expires: value.expires }; return true;
      } catch { this.recordFailure(controller.signal.aborted ? 'SESSION_TIMEOUT' : 'NETWORK_UNREADABLE', {}, '/session'); return false; }
      finally { clearTimeout(timer); this.sessionRequest = null; this.sessionController = null; }
    });
    return this.sessionRequest;
  }
  async send(state, rulerId, message, token = '', useGemini = true, options = {}) {
    const council = options.councilId && state.allianceCouncils?.find(c=>c.id===options.councilId);
    const aiIds = council?.participants.filter(id=>id!==options.actorHouseId&&isAiHouse(state,id));
    const fallback = (detail = '', includeDiagnostic = true) => {
      const diagnostic = includeDiagnostic ? this.lastDiagnostic : null;
      return { ...(council ? scriptedCouncil(state,council,options.actorHouseId,message) : scriptedReply(state, rulerId, message, options)), source: 'scripted', diagnostic,
        notice: `Council response delivered through local diplomacy.${detail ? ` ${detail}` : diagnostic ? ` ${diagnosticDetails(diagnostic).reason}` : ''}${diagnostic ? ' Open Diagnostics for details.' : ''}` };
    };
    if (!useGemini || council && !aiIds.length) return fallback('', false);
    if (!this.endpoint) { this.recordFailure('CLIENT_CONFIG'); return fallback(); }
    if (this.busy) return fallback('An envoy is already travelling.', false);
    if (this.now() < this.cooldownUntil) return fallback(`Gemini can be tried again in ${Math.ceil((this.cooldownUntil-this.now())/1000)} seconds. ${this.lastDiagnostic ? diagnosticDetails(this.lastDiagnostic).reason : ''}`);
    if (!this.hasSession() && !token && !this.sessionRequest) {
      if (!this.lastDiagnostic) this.recordFailure(this.session ? 'SESSION_EXPIRED' : 'SESSION_NOT_READY');
      return fallback();
    }
    const context = council ? makeCouncilContext(state,council,options.actorHouseId,message) : makeContext(state, rulerId, message, options);
    if(!context)return fallback('Council context is unavailable.',false);
    const key = JSON.stringify(context);
    this.busy = true; this.controller = new AbortController();
    let timedOut = false;
    const controller = this.controller, timeout = setTimeout(() => { timedOut = true; controller.abort(); }, 18000);
    try {
      if (this.sessionRequest) await this.sessionRequest;
      else if (token || (this.hasSession() && this.session.expires < this.now() + 5 * 60000)) await this.openSession(token);
      if (!this.hasSession()) { if (!this.lastDiagnostic) this.recordFailure('SESSION_NOT_READY'); return fallback(); }
      if (controller.signal.aborted) { this.recordFailure(timedOut ? 'REQUEST_TIMEOUT' : 'REQUEST_CANCELLED'); return fallback(); }
      if (this.cache.has(key)) { this.lastDiagnostic = null; return { ...this.cache.get(key), source: 'gemini', notice: 'Gemini council · proposals await your word' }; }
      const response = await this.fetcher(this.endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${this.session.token}` }, credentials: 'omit', signal: controller.signal, body: JSON.stringify(context) });
      const fresh = response.headers.get('X-Diplomacy-Session'), expires = Number(response.headers.get('X-Diplomacy-Expires'));
      if (fresh && fresh.length <= 1600 && Number.isSafeInteger(expires) && expires > this.now()) this.session = { token: fresh, expires };
      if (!response.ok) {
        const seconds = Math.min(3600, Math.max(1, Number(response.headers.get('Retry-After')) || (response.status === 429 ? 300 : 60)));
        if (response.status !== 401) this.cooldownUntil = this.now() + seconds * 1000;
        await this.readFailure(response, '/diplomacy', this.cooldownUntil);
        if (response.status === 401) { this.session = null; return fallback('Your diplomacy session needs verification.'); }
        return fallback(response.status === 429 ? 'Conversation quota reached.' : 'Gemini is temporarily unavailable.');
      }
      let parsed;
      try { const raw=await readJSON(response, 10000); parsed = council ? validateCouncilResponse(raw,council.participants,aiIds) : validateResponse(raw); } catch (error) { if (controller.signal.aborted) throw error; /* Invalid reply is handled below. */ }
      if (!parsed) { this.cooldownUntil = this.now() + 5000; this.recordFailure('GEMINI_RESPONSE_INVALID', { httpStatus: response.status, retryAt: this.cooldownUntil }, '/diplomacy'); return fallback(); }
      if(!council)parsed=relationshipResponse(state,rulerId,message,parsed,options);
      if (this.cache.size >= 30) this.cache.delete(this.cache.keys().next().value);
      this.cache.set(key, parsed);
      this.lastDiagnostic = null;
      return { ...parsed, source: 'gemini', notice: 'Gemini council · proposals await your word' };
    } catch {
      this.cooldownUntil = this.now() + 60000;
      this.recordFailure(controller.signal.aborted ? (timedOut ? 'REQUEST_TIMEOUT' : 'REQUEST_CANCELLED') : 'NETWORK_UNREADABLE', { retryAt: this.cooldownUntil }, '/diplomacy'); return fallback();
    }
    finally { clearTimeout(timeout); this.busy = false; this.controller = null; }
  }
}
