import { makeContext, scriptedReply, validateResponse } from './diplomacy.mjs';

export function validEndpoint(value) {
  if (!value) return '';
  try { const u = new URL(value); return u.protocol === 'https:' && !u.username && !u.password && !u.search && !u.hash && u.pathname === '/diplomacy' ? u.href : ''; } catch { return ''; }
}
export class DiplomacyClient {
  constructor({ endpoint = '', fetcher = fetch, now = Date.now } = {}) {
    this.endpoint = validEndpoint(endpoint);
    // Native browser fetch must not receive this DiplomacyClient as its receiver.
    this.fetcher = (...args) => fetcher(...args); this.now = now;
    this.cooldownUntil = 0; this.busy = false; this.cache = new Map(); this.controller = null;
  }
  cancel() { this.controller?.abort(); }
  async send(state, rulerId, message, token = '', useGemini = false) {
    const fallback = reason => ({ ...scriptedReply(state, rulerId, message), source: 'scripted', notice: reason });
    if (!useGemini) return fallback('Scripted council');
    if (!this.endpoint) return fallback('Gemini is not connected. The treaty desk and scripted council work normally.');
    if (this.busy) return fallback('A conversation is already in progress.');
    if (this.now() < this.cooldownUntil) return fallback('Gemini is resting after a rate limit. Scripted diplomacy is available.');
    if (!token) return fallback('Complete verification to use Gemini, or continue with the scripted council.');
    const context = makeContext(state, rulerId, message), key = JSON.stringify(context);
    if (this.cache.has(key)) return { ...this.cache.get(key), source: 'gemini', notice: 'Cached Gemini reply' };
    this.busy = true; this.controller = new AbortController();
    const timeout = setTimeout(() => this.controller?.abort(), 18000);
    try {
      const response = await this.fetcher(this.endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json' }, credentials: 'omit', signal: this.controller.signal, body: JSON.stringify({ ...context, turnstileToken: token }) });
      if (!response.ok) {
        const seconds = Math.min(3600, Math.max(30, Number(response.headers.get('Retry-After')) || (response.status === 429 ? 300 : 60)));
        this.cooldownUntil = this.now() + seconds * 1000;
        return fallback(response.status === 429 ? 'Gemini quota reached. Switched to the scripted council.' : 'Gemini is unavailable. Switched to the scripted council.');
      }
      const responseText = await response.text();
      if (responseText.length > 10000) return fallback('Unusable ruler reply. The scripted council is ready.');
      const parsed = validateResponse(responseText);
      if (!parsed) return fallback('Unusable ruler reply. The scripted council is ready.');
      if (this.cache.size >= 30) this.cache.delete(this.cache.keys().next().value);
      this.cache.set(key, parsed);
      return { ...parsed, source: 'gemini', notice: 'Gemini reply · proposals require ratification' };
    } catch { this.cooldownUntil = this.now() + 60000; return fallback('Conversation timed out or disconnected. The scripted council is ready.'); }
    finally { clearTimeout(timeout); this.busy = false; this.controller = null; }
  }
}
