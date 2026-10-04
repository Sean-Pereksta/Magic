import { DiplomacyQueue } from './diplomacy-queue.mjs';
import { diplomacyTiming } from './diplomacy-timing.mjs';
import { makeCouncilDispatchContext } from './council-dispatch.mjs';
import { generalContext, validateGeneralConversationResponse, validateGeneralResponse } from './generals.mjs';
import { CAMPAIGN_HOUSES } from './data.mjs';
import { makeCouncilContext, validateCouncilResponse } from './alliance-council.mjs';
import { isAiHouse } from './house-control.mjs';
import { geminiRelationshipResponse } from './diplomacy.mjs';
import { makeContext, validateResponse } from './diplomacy.mjs';
import { diagnosticDetails, makeDiagnostic, readDiagnostic, requestMetrics } from './diagnostics.mjs';

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
    this.verificationFailure = null; this.lastVerificationToken = '';
    this.lastDiagnostic = null; this.queue = new DiplomacyQueue();this.retryFailures=new Map();
  }
  hasSession() { return !!this.session && this.session.expires > this.now() + 5000; }
  cancel() { this.queue.cancel();this.retryFailures.clear(); for (const controller of this.controllers) controller.abort(); }
  recordFailure(code, details = {}, path = '') {
    this.lastDiagnostic = { ...makeDiagnostic(code, details), at: this.now(), path,
      ...(Number.isInteger(details.httpStatus) ? { httpStatus: details.httpStatus } : {}),
      ...(details.retryAt > this.now() ? { retryAt: details.retryAt } : {}) };
    if (['/session', '/verification'].includes(path) || code === 'SESSION_EXPIRED' && path === '/diplomacy') {
      this.verificationFailure = this.lastDiagnostic;
      // An explicit authentication failure completes queued speakers without speech.
      // Waiting for the player's first verification remains supported.
      queueMicrotask(() => this.queue.wake());
    }
    return this.lastDiagnostic;
  }
  async readFailure(response, path, retryAt = 0, clientDetails = {}) {
    let diagnostic,bodyRetryAt=0;
    try {
      const value=await readJSON(response,8000);diagnostic=readDiagnostic(value?.diagnostics);
      if(typeof value?.retryAfter==='number'&&Number.isFinite(value.retryAfter)&&value.retryAfter>0)bodyRetryAt=this.now()+Math.ceil(Math.min(3600,value.retryAfter)*1000);
    } catch { /* Older Worker or unreadable error body. */ }
    const code = response.status === 401 ? 'SESSION_EXPIRED' : response.status === 403 ? 'WORKER_ACCESS_DENIED'
      : response.status === 404 ? 'WORKER_ROUTE_MISSING' : response.status === 429 ? (path === '/session' ? 'SESSION_RATE_LIMIT' : 'RATE_LIMIT_UNKNOWN')
      : [400, 405, 413, 415].includes(response.status) ? 'REQUEST_INVALID' : 'WORKER_UNAVAILABLE';
    return this.recordFailure(diagnostic?.code || code, { ...diagnostic,...clientDetails, httpStatus: response.status, retryAt:retryAt||bodyRetryAt }, path);
  }
  async openSession(turnstileToken = '') {
    if (this.sessionRequest) return this.sessionRequest;
    if (this.hasSession() && this.session.expires > this.now() + 5 * 60000) return true;
    const current = this.hasSession() ? this.session.token : '';
    if (!this.endpoint || (!current && !turnstileToken)) { this.session = null; this.recordFailure(this.endpoint ? 'SESSION_NOT_READY' : 'CLIENT_CONFIG'); return false; }
    this.verificationFailure = null;
    if (turnstileToken) this.lastVerificationToken = turnstileToken;
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
        this.session = { token: value.token, expires: value.expires }; this.verificationFailure = null; return true;
      } catch { this.recordFailure(controller.signal.aborted ? 'SESSION_TIMEOUT' : 'NETWORK_UNREADABLE', {}, '/session'); return false; }
      finally { clearTimeout(timer); this.sessionRequest = null; this.sessionController = null; this.queue.wake(); }
    });
    return this.sessionRequest;
  }
  async testConnection({mode='private',actorHouseId='ashen',rulerId='wintermere'}={}) {
    if(this.probeBusy)return {ok:false,notice:'A connection test is already running.'};
    const ids=CAMPAIGN_HOUSES.map(h=>h.id);
    if(!ids.includes(actorHouseId))actorHouseId='ashen';
    if(!ids.includes(rulerId)||rulerId===actorHouseId)rulerId=ids.find(h=>h!==actorHouseId);
    // A unique, non-secret test ID prevents a previous successful Worker cache
    // entry from looking like proof of current provider availability.
    const testId=`${this.now()}-${this.probeSequence=(this.probeSequence||0)+1}`;
    const message=`Connection test. Reply with a brief greeting only; propose no actions or orders. Test ID: ${testId}`,participants=[actorHouseId,rulerId];
    const context=mode==='council'?{mode:'allianceCouncil',turn:1,actorHouseId,councilId:'council-1',participants,message,history:[],world:{mood:'Cordial',participants:participants.map(id=>({id,ai:id===rulerId}))}}
      :mode==='general'?{mode:'general',turn:1,actorHouseId,generalId:'general-1',message,history:[],world:{}}
      :{turn:1,actorHouseId,rulerId,targetHouseId:rulerId,message,history:[],memories:[],summary:'',world:{}};
    // A separate client keeps this explicit, one-request probe out of game
    // conversations, caching, queues and failed-reply diagnostics.
    if(!this.probeClient||this.probeClient.endpoint!==this.endpoint)this.probeClient=new DiplomacyClient({endpoint:this.endpoint,fetcher:this.fetcher,now:this.now});
    const probe=this.probeClient;probe.session=this.session;this.probeBusy=true;
    try{
      const state={seed:0,allianceCouncils:mode==='council'?[{id:'council-1',participants}]:[]};
      const response=await probe.sendNow(state,rulerId,message,'',true,{actorHouseId,leaderContext:context,diagnosticOnly:true,bypassCache:true,...(mode==='council'?{councilId:'council-1',speaker:rulerId}:{}),...(mode==='general'?{generalId:'general-1'}:{})});
      if(probe.hasSession()&&(!this.session||probe.session.expires>this.session.expires))this.session=probe.session;
      return {ok:response.source==='gemini',retryBlocked:!!response.retryBlocked,diagnostic:response.diagnostic||null,clientRequest:response.clientRequest||response.diagnostic?.clientRequest,notice:response.notice};
    }finally{this.probeBusy=false;}
  }
  send(state, rulerId, message, token = '', useGemini = true, options = {}) {
    // A council call represents one ruler. The game owns the public sequence
    // and commits each reply before requesting its next speaker.
    if (options.councilId) return this.sendCouncilSpeaker(state, rulerId, message, token, useGemini, options);
    return this.sendDirect(state, rulerId, message, token, useGemini, options);
  }
  async sendDirect(state, rulerId, message, token, useGemini, options) {
    const generation = this.queue.generation;
    const current = () => generation === this.queue.generation && (!options.isCurrent || options.isCurrent());
    if (!current()) return this.queue.cancelled();
    const response = await this.sendNow(state, rulerId, message, token, useGemini, options);
    return current() ? response : this.queue.cancelled();
  }
  async sendCouncilSpeaker(state, rulerId, message, token = '', useGemini = true, options = {}) {
    const speaker = options.speakerHouseId || options.formalDecision?.house || rulerId;
    const generation = this.queue.generation;
    const current = () => generation === this.queue.generation && (!options.isCurrent || options.isCurrent());
    const eligible = latest => {
      const council = latest.allianceCouncils?.find(c => c.id === options.councilId);
      return council && council.participants.includes(options.actorHouseId) && speaker !== options.actorHouseId &&
        council.participants.includes(speaker) && isAiHouse(latest, speaker);
    };
    const run = async () => {
      if (!current()) return this.queue.cancelled();
      const latest = options.getState?.() || state;
      const council = latest.allianceCouncils?.find(c => c.id === options.councilId);
      if (!eligible(latest)) return this.queue.cancelled();
      // Build after queue readiness so this ruler sees committed public
      // history, never an uncommitted private response accumulator.
      const leaderContext = options.councilDispatch ? makeCouncilDispatchContext(latest, council, options.actorHouseId, options.councilDispatch)
        : makeCouncilContext(latest, council, options.actorHouseId, message, options.location, options.formalDecision);
      if (!leaderContext || !leaderContext.world.participants.some(p => p.id === speaker && p.ai)) return this.queue.cancelled();
      for (const p of leaderContext.world.participants) p.ai = p.id === speaker;
      if (leaderContext.world.dispatch) {
        leaderContext.world.dispatch.entries = leaderContext.world.dispatch.entries.filter(e => e.speakerHouseId === speaker);
        if (leaderContext.world.dispatch.entries.length !== 1) return this.queue.cancelled();
      }
      const response = await this.sendNow(latest, rulerId, message, '', useGemini, {...options, leaderContext, speaker});
      if (!current()) return this.queue.cancelled();
      if (response.diagnostic) options.onDiagnostic?.(response.diagnostic);
      return response;
    };
    if (!current() || !eligible(options.getState?.() || state)) return this.queue.cancelled();
    if (!useGemini || !this.endpoint) return run();
    if (token && !this.hasSession() && !this.sessionRequest &&
        (!this.verificationFailure || token !== this.lastVerificationToken)) void this.openSession(token);
    // Only council requests enter this transport queue. Private rulers and
    // generals still use sendDirect, even while a council ruler is generating.
    return this.queue.enqueue(async () => {
      const response = await run();
      // These are pre-provider allowance refusals. Respect their existing reset
      // without automatically retrying any paid Gemini generation or timeout.
      if (['CLIENT_RATE_LIMIT', 'GLOBAL_RATE_LIMIT', 'PROVIDER_COOLDOWN'].includes(response.diagnostic?.code)) return {source:'deferred'};
      return response;
    }, {...options, isCurrent: current,
      ready: () => this.now() < this.cooldownUntil ? 'cooldown'
        : !this.hasSession() && (this.sessionRequest || !this.verificationFailure) ? 'verification' : true});
  }
  async sendNow(state, rulerId, message, token = '', useGemini = true, options = {}) {
    const general=options.generalId;
    const council = options.councilId && state.allianceCouncils?.find(c=>c.id===options.councilId);
    const aiIds = options.speaker ? [options.speaker] : council?.participants.filter(id=>id!==options.actorHouseId&&isAiHouse(state,id));
    let requestDiagnostic = null,metrics=null,requestStarted=null;
    const measured=()=>metrics&&requestStarted!==null?{clientRequest:{...metrics,durationMs:Math.max(0,Math.round(this.now()-requestStarted))}}:{};
    const recordFailure = (code,details={},path='') => (requestDiagnostic = this.recordFailure(code,{...measured(),...details},path));
    const failed = (detail = '', includeDiagnostic = true) => {
      const diagnostic = includeDiagnostic ? requestDiagnostic : null;
      return { source:'failed', responses:[], reply:'', intents:[], diagnostic,
        notice: `No Gemini response was delivered.${detail ? ` ${detail}` : ''}${diagnostic ? ` ${diagnosticDetails(diagnostic).reason} [${diagnostic.code}] Open Diagnostics for details.` : ''}` };
    };
    if (!useGemini || council && !aiIds.length) return failed('Enable Gemini to receive ruler responses.', false);
    if (!this.endpoint) { recordFailure('CLIENT_CONFIG'); return failed(); }
    if (this.now() < this.cooldownUntil) { requestDiagnostic = this.cooldownDiagnostic; return failed(`Gemini can be tried again in ${Math.ceil((this.cooldownUntil-this.now())/1000)} seconds.`); }
    if (!this.hasSession() && !token && !this.sessionRequest) {
      requestDiagnostic = this.verificationFailure || (this.lastDiagnostic?.path === '/session' ? this.lastDiagnostic : recordFailure(this.session ? 'SESSION_EXPIRED' : 'SESSION_NOT_READY'));
      return failed();
    }
    const context = options.leaderContext || (general ? generalContext(state,options.actorHouseId,general,message) : council ? makeCouncilContext(state,council,options.actorHouseId,message,options.location,options.formalDecision) : makeContext(state, rulerId, message, options));
    if(!context)return failed('Council context is unavailable.',false);
    const key = JSON.stringify(context);
    // Respect Retry-After for this message only. A failed ruler must not hold
    // other council speakers or unrelated private/general conversations.
    const retryKey=JSON.stringify([state.seed,context.turn,options.actorHouseId||context.actorHouseId||'ashen',options.councilId||null,general||rulerId,options.speaker||null,options.formalDecision?.proposalId||null,options.diagnosticOnly?'connection-test':message]);
    const previous=this.retryFailures.get(retryKey);
    if(previous?.retryAt>this.now()){
      requestDiagnostic=previous;this.lastDiagnostic=previous;
      return {...failed(`Wait ${Math.ceil((previous.retryAt-this.now())/1000)} seconds before retrying this reply.`),retryBlocked:true};
    }
    this.retryFailures.delete(retryKey);
    this.busy = true;
    let timedOut = false;
    const controller = new AbortController();
    let timeout;
    this.controllers.add(controller);
    try {
      if (this.sessionRequest) await this.sessionRequest;
      else if (token || (this.hasSession() && this.session.expires < this.now() + 5 * 60000)) await this.openSession(token);
      if (!this.hasSession()) { requestDiagnostic = this.lastDiagnostic || recordFailure('SESSION_NOT_READY'); return failed(); }
      if (controller.signal.aborted) { recordFailure(timedOut ? 'REQUEST_TIMEOUT' : 'REQUEST_CANCELLED'); return failed(); }
      if (!options.bypassCache && this.cache.has(key)) { this.lastDiagnostic = null; return { ...this.cache.get(key), source: 'gemini', notice: 'Gemini council · proposals await your word' }; }
      // Verification and queue waiting must not consume the reply's deadline.
      timeout = setTimeout(() => { timedOut = true; controller.abort(); }, diplomacyTiming(context.mode).clientMs);
      metrics=requestMetrics(context);requestStarted=this.now();
      const response = await this.fetcher(this.endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${this.session.token}` }, credentials: 'omit', signal: controller.signal, body: key });
      const fresh = response.headers.get('X-Diplomacy-Session'), expires = Number(response.headers.get('X-Diplomacy-Expires'));
      if (fresh && fresh.length <= 1600 && Number.isSafeInteger(expires) && expires > this.now()) this.session = { token: fresh, expires };
      if (!response.ok) {
        const retryHeader=response.headers.get('Retry-After'),numeric=Number(retryHeader);
        const seconds=Math.min(3600,Math.max(0,Number.isFinite(numeric)?numeric:(Date.parse(retryHeader)-this.now())/1000)||0);
        requestDiagnostic = await this.readFailure(response, '/diplomacy',seconds?this.now()+Math.ceil(seconds*1000):0,measured());
        const limited = response.status === 429 || requestDiagnostic.providerStatus === 429 || ['DAILY_LIMIT','CLIENT_RATE_LIMIT','GLOBAL_RATE_LIMIT','PROVIDER_COOLDOWN','GEMINI_QUOTA'].includes(requestDiagnostic.code);
        if (limited) {
          const seconds = Math.min(3600, Math.max(1, Number(response.headers.get('Retry-After')) || 300));
          this.cooldownUntil = Math.max(this.cooldownUntil,requestDiagnostic.retryAt||this.now() + seconds * 1000);
          requestDiagnostic = recordFailure(requestDiagnostic.code, {...requestDiagnostic,retryAt:this.cooldownUntil}, '/diplomacy');
          this.cooldownDiagnostic = requestDiagnostic;
        }
        else if(requestDiagnostic.retryAt>this.now()){
          for(const [id,d] of this.retryFailures)if(d.retryAt<=this.now())this.retryFailures.delete(id);
          if(this.retryFailures.size>=32)this.retryFailures.delete(this.retryFailures.keys().next().value);
          this.retryFailures.set(retryKey,requestDiagnostic);
        }
        if (response.status === 401) { this.session = null; return failed('Your diplomacy session needs verification.'); }
        return failed();
      }
      let parsed;
      try { const raw=await readJSON(response, 10000); parsed = general ? options.diagnosticOnly?validateGeneralResponse(raw):validateGeneralConversationResponse(state,options.actorHouseId,general,raw) : council ? validateCouncilResponse(raw,council.participants,aiIds) : validateResponse(raw); } catch (error) { if (controller.signal.aborted) throw error; /* Invalid reply is handled below. */ }
      if (!parsed || options.speaker && parsed.responses.length !== 1 || options.councilDispatch && parsed.responses.some(r => r.requestedIntent)) { recordFailure('GEMINI_RESPONSE_INVALID', { httpStatus: response.status }, '/diplomacy'); return failed(); }
      if (!council && !general && !options.formalDecision && !options.diagnosticOnly) {
        const guarded = geminiRelationshipResponse(state,rulerId,message,parsed,options);
        if (!guarded) { recordFailure('GEMINI_RESPONSE_INVALID', {httpStatus:response.status}, '/diplomacy'); return failed('The reply contradicted the recorded game state.'); }
        parsed = guarded;
      }
      if (this.cache.size >= 30) this.cache.delete(this.cache.keys().next().value);
      this.cache.set(key, parsed);
      this.lastDiagnostic = null;
      return { ...parsed, source: 'gemini', notice: 'Gemini council · proposals await your word',...(options.diagnosticOnly?measured():{}) };
    } catch {
      if(controller.signal.aborted && !timedOut)return this.queue.cancelled();
      recordFailure(controller.signal.aborted ? (timedOut ? 'REQUEST_TIMEOUT' : 'REQUEST_CANCELLED') : 'NETWORK_UNREADABLE', {}, '/diplomacy'); return failed();
    }
    finally { clearTimeout(timeout); this.controllers.delete(controller); this.busy = this.controllers.size > 0; }
  }
}
