// Cross-site Workers cookies can be blocked by browser policy. Use a signed,
// origin/client-bound bearer credential held only in page memory instead.
const encoder = new TextEncoder();
const TTL = 30 * 60 * 1000, MAX_AGE = 8 * 60 * 60 * 1000;
const encode = bytes => btoa(String.fromCharCode(...bytes)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
function decode(value) {
  if (!/^[A-Za-z0-9_-]+$/.test(value)) throw new Error('encoding');
  const base = value.replace(/-/g, '+').replace(/_/g, '/');
  return Uint8Array.from(atob(base + '='.repeat((4 - base.length % 4) % 4)), ch => ch.charCodeAt(0));
}
async function key(env) {
  // Domain separation allows existing deployments to upgrade without moving a
  // secret into the client. Operators may rotate a separate SESSION_SECRET.
  return crypto.subtle.importKey('raw', encoder.encode(`iron-throne-session-v1:${env.SESSION_SECRET || env.TURNSTILE_SECRET}`), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign', 'verify']);
}
export async function issueSession(env, origin, clientId, now = Date.now(), previous = null) {
  const started = previous?.started ?? now;
  const claims = { v: 1, origin, clientId, started, expires: Math.min(now + TTL, started + MAX_AGE), id: previous?.id || crypto.randomUUID() };
  const payload = encode(encoder.encode(JSON.stringify(claims)));
  const signature = encode(new Uint8Array(await crypto.subtle.sign('HMAC', await key(env), encoder.encode(payload))));
  return { token: `${payload}.${signature}`, expires: claims.expires };
}
export async function verifySession(token, env, origin, clientId, now = Date.now()) {
  try {
    if (typeof token !== 'string' || token.length > 1600) return null;
    const [payload, signature, extra] = token.split('.');
    if (!payload || !signature || extra || !await crypto.subtle.verify('HMAC', await key(env), decode(signature), encoder.encode(payload))) return null;
    const c = JSON.parse(new TextDecoder().decode(decode(payload)));
    if (c.v !== 1 || c.origin !== origin || c.clientId !== clientId || typeof c.id !== 'string' || c.id.length > 80 || !Number.isSafeInteger(c.started) || !Number.isSafeInteger(c.expires) || c.started > now || c.expires <= now || c.expires > c.started + MAX_AGE || c.expires > now + TTL) return null;
    return c;
  } catch { return null; }
}
export async function reserveSessionBudget(storage, clientId, now = Date.now()) {
  return storage.transaction(async txn => {
    const minute = Math.floor(now / 60000);
    let value = await txn.get('sessions');
    if (!value || value.minute !== minute) value = { minute, calls: 0, clients: {} };
    if (value.calls >= 100 || (value.clients[clientId] || 0) >= 6) return false;
    value.calls++; value.clients[clientId] = (value.clients[clientId] || 0) + 1;
    await txn.put('sessions', value); return true;
  });
}
