import { makeContext, scriptedReply, validateResponse } from './diplomacy.mjs';

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
  }
  hasSession() { return !!this.session && this.session.expires > this.now() + 5000; }
  cancel() { this.controller?.abort(); }
  async openSession(turnstileToken = '') {
    if (this.sessionRequest) return this.sessionRequest;
    if (this.hasSession() && this.session.expires > this.now() + 5 * 60000) return true;
    const current = this.hasSession() ? this.session.token : '';
    if (!this.endpoint || (!current && !turnstileToken)) { this.session = null; return false; }
    this.sessionController = new AbortController();
    const timer = setTimeout(() => this.sessionController?.abort(), 10000);
    this.sessionRequest = (async () => {
      try {
        const response = await this.fetcher(new URL('/session', this.endpoint).href, { method: 'POST', headers: { 'Content-Type': 'application/json', ...(current ? { Authorization: `Bearer ${current}` } : {}) }, credentials: 'omit', signal: this.sessionController.signal, body: JSON.stringify(current ? {} : { turnstileToken }) });
        if (!response.ok) { if ([401, 403].includes(response.status)) this.session = null; return false; }
        const text = await response.text(); if (text.length > 3000) return false;
        const value = JSON.parse(text);
        if (typeof value.token !== 'string' || value.token.length > 1600 || !Number.isSafeInteger(value.expires) || value.expires <= this.now()) return false;
        this.session = { token: value.token, expires: value.expires }; return true;
      } catch { return false; }
      finally { clearTimeout(timer); this.sessionRequest = null; this.sessionController = null; }
    })();
    return this.sessionRequest;
  }
  async send(state, rulerId, message, token = '', useGemini = true, options = {}) {
    const fallback = detail => ({ ...scriptedReply(state, rulerId, message, options), source: 'scripted', notice: `Council response delivered through local diplomacy.${detail ? ` ${detail}` : ''}` });
    if (!useGemini || !this.endpoint) return fallback('');
    if (this.busy) return fallback('An envoy is already travelling.');
    if (this.now() < this.cooldownUntil) return fallback('Gemini is resting; your conversation continues.');
    if (!this.hasSession() && !token && !this.sessionRequest) return fallback('Verification is not ready.');
    const context = makeContext(state, rulerId, message, options), key = JSON.stringify(context);
    this.busy = true; this.controller = new AbortController();
    const controller = this.controller, timeout = setTimeout(() => controller.abort(), 18000);
    try {
      if (this.sessionRequest) await this.sessionRequest;
      else if (token || (this.hasSession() && this.session.expires < this.now() + 5 * 60000)) await this.openSession(token);
      if (!this.hasSession()) return fallback('Verification is not ready.');
      if (controller.signal.aborted) return fallback('The envoy was recalled.');
      if (this.cache.has(key)) return { ...this.cache.get(key), source: 'gemini', notice: 'Gemini council · proposals await your word' };
      const response = await this.fetcher(this.endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${this.session.token}` }, credentials: 'omit', signal: controller.signal, body: JSON.stringify(context) });
      const fresh = response.headers.get('X-Diplomacy-Session'), expires = Number(response.headers.get('X-Diplomacy-Expires'));
      if (fresh && fresh.length <= 1600 && Number.isSafeInteger(expires) && expires > this.now()) this.session = { token: fresh, expires };
      if (!response.ok) {
        if (response.status === 401) { this.session = null; return fallback('Your diplomacy session needs verification.'); }
        const seconds = Math.min(3600, Math.max(30, Number(response.headers.get('Retry-After')) || (response.status === 429 ? 300 : 60)));
        this.cooldownUntil = this.now() + seconds * 1000;
        return fallback(response.status === 429 ? 'Gemini quota reached.' : 'Gemini is temporarily unavailable.');
      }
      const text = await response.text();
      const parsed = text.length <= 10000 ? validateResponse(text) : null;
      if (!parsed) { this.cooldownUntil = this.now() + 60000; return fallback(''); }
      if (this.cache.size >= 30) this.cache.delete(this.cache.keys().next().value);
      this.cache.set(key, parsed);
      return { ...parsed, source: 'gemini', notice: 'Gemini council · proposals await your word' };
    } catch { this.cooldownUntil = this.now() + 60000; return fallback(''); }
    finally { clearTimeout(timeout); this.busy = false; this.controller = null; }
  }
}
