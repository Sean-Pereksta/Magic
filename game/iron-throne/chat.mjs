import { DiplomacyQueue } from './diplomacy-queue.mjs';
import { diplomacyTiming } from './diplomacy-timing.mjs';
import { makeCouncilDispatchContext } from './council-dispatch.mjs';
import { generalContext, localGeneralReply, validateGeneralResponse } from './generals.mjs';
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
    this.cooldownUntil = 0; this.cooldownDiagnostic = null; this.busy = false; this.cache = new Map(); this.controllers = new Set();
    // Never serialize authentication into a campaign, export, or localStorage.
    this.session = null; this.sessionRequest = null; this.sessionController = null;
    this.lastDiagnostic = null; this.queue = new DiplomacyQueue();
  }
  hasSession() { return !!this.session && this.session.expires > this.now() + 5000; }
  cancel() { this.queue.cancel(); for (const controller of this.controllers) controller.abort(); }
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
      finally { clearTimeout(timer); this.sessionRequest = null; this.sessionController = null; this.queue.wake(); }
    });
    return this.sessionRequest;
  }
  send(state, rulerId, message, token = '', useGemini = true, options = {}) {
    const council = state.allianceCouncils?.find(c => c.id === options.councilId);
    const queuedAI = useGemini && this.endpoint && council?.participants.some(id => id !== options.actorHouseId && isAiHouse(state, id));
    // Only Alliance Council uses a queue. A slow/background council request
    // must not serialize private ruler conversations or general conversations.
    if (!queuedAI) return this.sendDirect(state, rulerId, message, token, useGemini, options);
    if (queuedAI && token && !this.hasSession() && !this.sessionRequest) void this.openSession(token);
    return this.sendCouncil(state, council, rulerId, message, useGemini, options);
  }
  async sendDirect(state, rulerId, message, token, useGemini, options) {
    const generation = this.queue.generation;
    const current = () => generation === this.queue.generation && (!options.isCurrent || options.isCurrent());
    if (!current()) return this.queue.cancelled();
    const response = await this.sendNow(state, rulerId, message, token, useGemini, options);
    return current() ? response : this.queue.cancelled();
  }
  async sendCouncil(state, council, rulerId, message, useGemini, options) {
    const context = options.councilDispatch ? makeCouncilDispatchContext(state,council,options.actorHouseId,options.councilDispatch)
      : makeCouncilContext(state,council,options.actorHouseId,message,options.location,options.formalDecision);
    if (!context) return this.queue.cancelled();
    const eligible = context.world.participants.filter(p => p.ai && p.id !== options.actorHouseId).map(p => p.id);
    const speakers = options.councilDispatch ? options.councilDispatch.entries.map(e => e.speakerHouseId) : eligible;
    const generation = this.queue.generation, responses = [];
    let failure = null;
    for (const speaker of speakers) {
      // This is the existing Worker contract: only the current leader is
      // eligible to speak. Earlier leaders' replies become shared history.
      const leaderContext = structuredClone(context);
      for (const p of leaderContext.world.participants) p.ai = p.id === speaker && eligible.includes(p.id);
      leaderContext.history.push(...responses.map(r => ({speakerHouseId:r.speakerHouseId,message:r.message.slice(0,450),turn:context.turn})));
      leaderContext.history = leaderContext.history.slice(-10);
      if (leaderContext.world.dispatch) leaderContext.world.dispatch.entries = leaderContext.world.dispatch.entries.filter(e => e.speakerHouseId === speaker);
      while (new TextEncoder().encode(JSON.stringify(leaderContext)).length > 22000 && leaderContext.history.length) leaderContext.history.shift();
      const response = await this.queue.enqueue(async () => {
        const result = await this.sendNow(state,rulerId,message,'',useGemini,{...options,leaderContext,speaker});
        if (result.diagnostic) options.onDiagnostic?.(result.diagnostic);
        // Worker allowance refusals did not reach Google. Wait for their real
        // reset; do not retry paid generation or format/validation failures.
        if (['CLIENT_RATE_LIMIT','GLOBAL_RATE_LIMIT','PROVIDER_COOLDOWN'].includes(result.diagnostic?.code)) return {source:'deferred'};
        return result;
      }, {...options,isCurrent:()=>generation===this.queue.generation&&(!options.isCurrent||options.isCurrent()),
        ready:()=>this.now()<this.cooldownUntil?'cooldown':!this.hasSession()?'verification':true});
      if (response.source === 'cancelled') return response;
      const row = response.responses?.find(r => r.speakerHouseId === speaker);
      if (row) responses.push({...row,source:response.source});
      if (response.diagnostic) failure = response;
    }
    const source = responses.every(r=>r.source==='gemini') ? 'gemini' : responses.some(r=>r.source==='gemini') ? 'mixed' : 'scripted';
    return {responses,source,diagnostic:failure?.diagnostic || null,
      notice:failure?.notice || 'Gemini council · proposals await your word'};
  }
  async sendNow(state, rulerId, message, token = '', useGemini = true, options = {}) {
    const general=options.generalId;
    const council = options.councilId && state.allianceCouncils?.find(c=>c.id===options.councilId);
    const aiIds = options.speaker ? [options.speaker] : council?.participants.filter(id=>id!==options.actorHouseId&&isAiHouse(state,id));
    let requestDiagnostic = null;
    const recordFailure = (...args) => (requestDiagnostic = this.recordFailure(...args));
    const fallback = (detail = '', includeDiagnostic = true) => {
      const diagnostic = includeDiagnostic ? requestDiagnostic : null;
      const local = options.formalDecision ? {responses:[{speakerHouseId:options.formalDecision.house,message:state.cooperation?.formalProposals?.find(p=>p.id===options.formalDecision.proposalId)?.responses[options.formalDecision.house]?.message||'The recorded decision stands.'}]} : general ? localGeneralReply(state,options.actorHouseId,general,message) : council ? scriptedCouncil(state,council,options.actorHouseId,message,options.location) : scriptedReply(state, rulerId, message, options);
      if (options.speaker) local.responses = [local.responses?.find(r=>r.speakerHouseId===options.speaker) || {speakerHouseId:options.speaker,message:'A Gemini reply could not be delivered. Please try this council again.'}];
      return { ...local, source: 'scripted', diagnostic,
        notice: `Council response delivered through local diplomacy.${detail ? ` ${detail}` : ''}${diagnostic ? ` ${diagnosticDetails(diagnostic).reason} [${diagnostic.code}]` : ''}${diagnostic ? ' Open Diagnostics for details.' : ''}` };
    };
    if (!useGemini || council && !aiIds.length) return fallback('', false);
    if (!this.endpoint) { recordFailure('CLIENT_CONFIG'); return fallback(); }
    if (this.now() < this.cooldownUntil) { requestDiagnostic = this.cooldownDiagnostic; return fallback(`Gemini can be tried again in ${Math.ceil((this.cooldownUntil-this.now())/1000)} seconds.`); }
    if (!this.hasSession() && !token && !this.sessionRequest) {
      requestDiagnostic = this.lastDiagnostic?.path === '/session' ? this.lastDiagnostic : recordFailure(this.session ? 'SESSION_EXPIRED' : 'SESSION_NOT_READY');
      return fallback();
    }
    const context = options.leaderContext || (general ? generalContext(state,options.actorHouseId,general,message) : council ? makeCouncilContext(state,council,options.actorHouseId,message,options.location,options.formalDecision) : makeContext(state, rulerId, message, options));
    if(!context)return fallback('Council context is unavailable.',false);
    const key = JSON.stringify(context);
    this.busy = true;
    let timedOut = false;
    const controller = new AbortController();
    let timeout;
    this.controllers.add(controller);
    try {
      if (this.sessionRequest) await this.sessionRequest;
      else if (token || (this.hasSession() && this.session.expires < this.now() + 5 * 60000)) await this.openSession(token);
      if (!this.hasSession()) { requestDiagnostic = this.lastDiagnostic || recordFailure('SESSION_NOT_READY'); return fallback(); }
      if (controller.signal.aborted) { recordFailure(timedOut ? 'REQUEST_TIMEOUT' : 'REQUEST_CANCELLED'); return fallback(); }
      if (this.cache.has(key)) { this.lastDiagnostic = null; return { ...this.cache.get(key), source: 'gemini', notice: 'Gemini council · proposals await your word' }; }
      // Verification and queue waiting must not consume the reply's deadline.
      timeout = setTimeout(() => { timedOut = true; controller.abort(); }, diplomacyTiming(context.mode).clientMs);
      const response = await this.fetcher(this.endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${this.session.token}` }, credentials: 'omit', signal: controller.signal, body: JSON.stringify(context) });
      const fresh = response.headers.get('X-Diplomacy-Session'), expires = Number(response.headers.get('X-Diplomacy-Expires'));
      if (fresh && fresh.length <= 1600 && Number.isSafeInteger(expires) && expires > this.now()) this.session = { token: fresh, expires };
      if (!response.ok) {
        requestDiagnostic = await this.readFailure(response, '/diplomacy');
        const limited = response.status === 429 || requestDiagnostic.providerStatus === 429 || ['DAILY_LIMIT','CLIENT_RATE_LIMIT','GLOBAL_RATE_LIMIT','PROVIDER_COOLDOWN','GEMINI_QUOTA'].includes(requestDiagnostic.code);
        if (limited) {
          const seconds = Math.min(3600, Math.max(1, Number(response.headers.get('Retry-After')) || 300));
          this.cooldownUntil = Math.max(this.cooldownUntil, this.now() + seconds * 1000);
          requestDiagnostic = recordFailure(requestDiagnostic.code, {...requestDiagnostic,retryAt:this.cooldownUntil}, '/diplomacy');
          this.cooldownDiagnostic = requestDiagnostic;
        }
        if (response.status === 401) { this.session = null; return fallback('Your diplomacy session needs verification.'); }
        return fallback();
      }
      let parsed;
      try { const raw=await readJSON(response, 10000); parsed = general ? validateGeneralResponse(raw) : council ? validateCouncilResponse(raw,council.participants,aiIds) : validateResponse(raw); } catch (error) { if (controller.signal.aborted) throw error; /* Invalid reply is handled below. */ }
      if (!parsed || options.speaker && parsed.responses.length !== 1) { recordFailure('GEMINI_RESPONSE_INVALID', { httpStatus: response.status }, '/diplomacy'); return fallback(); }
      if(!council&&!general&&!options.formalDecision)parsed=relationshipResponse(state,rulerId,message,parsed,options);
      if (this.cache.size >= 30) this.cache.delete(this.cache.keys().next().value);
      this.cache.set(key, parsed);
      this.lastDiagnostic = null;
      return { ...parsed, source: 'gemini', notice: 'Gemini council · proposals await your word' };
    } catch {
      if(controller.signal.aborted && !timedOut)return this.queue.cancelled();
      recordFailure(controller.signal.aborted ? (timedOut ? 'REQUEST_TIMEOUT' : 'REQUEST_CANCELLED') : 'NETWORK_UNREADABLE', {}, '/diplomacy'); return fallback();
    }
    finally { clearTimeout(timeout); this.controllers.delete(controller); this.busy = this.controllers.size > 0; }
  }
}
