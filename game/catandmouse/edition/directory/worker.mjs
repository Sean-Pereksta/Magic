import { DurableObject } from 'cloudflare:workers';
import { PROTOCOL_VERSION } from '../directory-config.mjs';

const ttl = 65_000;
const headers = {
  'Access-Control-Allow-Origin': '*',
  'Cache-Control': 'no-store'
};
const response = (data, status = 200) => Response.json(data, { status, headers });
const text = (value, max) => typeof value === 'string' && value.length > 0
  && value.length <= max && !/[\u0000-\u001f\u007f]/.test(value);

async function digest(value) {
  const bytes = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(value));
  return Array.from(new Uint8Array(bytes), byte => byte.toString(16).padStart(2, '0')).join('');
}

function validatedListing(data, id, productionSuffix) {
  if (!data || data.public !== true || data.version !== PROTOCOL_VERSION
    || !text(data.roomName, 48) || !text(data.hostName, 28) || !text(data.map, 48)
    || !text(data.difficulty, 24) || !Number.isInteger(data.capacity)
    || data.capacity < 1 || data.capacity > 4 || !Number.isInteger(data.openSeats)
    || data.openSeats < 0 || data.openSeats >= data.capacity) return null;
  let serverUrl;
  try {
    const url = new URL(data.serverUrl);
    const quick = /^[a-z0-9-]+\.trycloudflare\.com$/.test(url.hostname);
    const managed = productionSuffix && url.hostname.endsWith('.' + productionSuffix);
    if (url.protocol !== 'https:' || url.username || url.password || url.port
      || url.search || url.hash || url.pathname !== '/' || (!quick && !managed)) return null;
    serverUrl = url.origin;
  } catch {
    return null;
  }
  const { roomName, hostName, map, difficulty, capacity, openSeats, version } = data;
  return { game: 'catandmouse', id, roomName, hostName, map, difficulty, capacity, openSeats, version, serverUrl };
}

export class CatMouseDirectory extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.db = ctx.storage.sql;
    this.db.exec(`CREATE TABLE IF NOT EXISTS rooms (
      id TEXT PRIMARY KEY, origin TEXT UNIQUE NOT NULL, owner TEXT NOT NULL,
      record TEXT NOT NULL, expires_at INTEGER NOT NULL
    )`);
  }

  purge() {
    this.db.exec('DELETE FROM rooms WHERE expires_at <= ?', Date.now());
  }

  async alarm() {
    this.purge();
    const next = this.db.exec('SELECT MIN(expires_at) AS next FROM rooms').one().next;
    if (next !== null) await this.ctx.storage.setAlarm(next);
  }

  async fetch(request) {
    const { pathname } = new URL(request.url);
    this.purge();
    if (request.method === 'GET' && pathname === '/rooms') {
      const records = this.db.exec('SELECT record, expires_at FROM rooms ORDER BY expires_at DESC LIMIT 100');
      return response({ rooms: Array.from(records, row => ({
        ...JSON.parse(row.record), expiresAt: new Date(row.expires_at).toISOString()
      })) });
    }

    const id = /^\/rooms\/([a-f0-9-]{36})$/.exec(pathname)?.[1];
    if (!id || !['PUT', 'DELETE'].includes(request.method)) return response({ error: 'Unknown route' }, 404);
    const credential = /^Bearer ([A-Za-z0-9_-]{32,80})$/.exec(request.headers.get('Authorization') ?? '')?.[1];
    if (!credential) return response({ error: 'Room credential required' }, 401);
    const owner = await digest(credential);
    const owned = () => this.db.exec('SELECT owner FROM rooms WHERE id = ?', id).toArray()[0];
    if (request.method === 'DELETE') {
      this.db.exec('DELETE FROM rooms WHERE id = ? AND owner = ?', id, owner);
      return response({ ok: true });
    }
    const existing = owned();
    if (existing && existing.owner !== owner) return response({ error: 'Room belongs to a different host' }, 403);

    let listing;
    try {
      const reader = request.body?.getReader();
      if (!reader) return response({ error: 'Missing listing' }, 400);
      const decoder = new TextDecoder();
      let body = '';
      let bytes = 0;
      for (;;) {
        const part = await reader.read();
        if (part.done) break;
        bytes += part.value.length;
        if (bytes > 2048) {
          await reader.cancel();
          return response({ error: 'Listing is too large' }, 413);
        }
        body += decoder.decode(part.value, { stream: true });
      }
      listing = validatedListing(JSON.parse(body + decoder.decode()), id, this.env.MANAGED_HOST_SUFFIX);
    } catch {
      return response({ error: 'Invalid listing' }, 400);
    }
    if (!listing) return response({ error: 'Invalid public room metadata' }, 400);

    try {
      const upstream = await fetch(listing.serverUrl + '/health', {
        redirect: 'manual', signal: AbortSignal.timeout(5_000)
      });
      if (!upstream.ok) throw new Error('Origin returned ' + upstream.status);
      const proof = await upstream.json();
      if (proof.game !== 'catandmouse' || proof.roomId !== id || proof.version !== listing.version
        || proof.listingProof !== owner) throw new Error('Room identity differed');
    } catch (error) {
      console.warn('Room origin verification failed:', error.message);
      return response({ error: 'The room origin could not be verified' }, 422);
    }

    if (owned()?.owner && owned().owner !== owner) return response({ error: 'Room belongs to a different host' }, 403);
    const originOwner = this.db.exec('SELECT id FROM rooms WHERE origin = ?', listing.serverUrl).toArray()[0];
    if (originOwner && originOwner.id !== id) return response({ error: 'Origin already lists a room' }, 409);
    const expiresAt = Date.now() + ttl;
    this.db.exec(`INSERT INTO rooms VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET origin=excluded.origin, record=excluded.record, expires_at=excluded.expires_at`,
    id, listing.serverUrl, owner, JSON.stringify(listing), expiresAt);
    const alarm = await this.ctx.storage.getAlarm();
    if (alarm === null || alarm > expiresAt) await this.ctx.storage.setAlarm(expiresAt);
    return response({ expiresAt: new Date(expiresAt).toISOString() });
  }
}

export default {
  fetch(request, env) {
    const path = new URL(request.url).pathname;
    if (path !== '/rooms' && !path.startsWith('/rooms/')) return response({ error: 'Unknown route' }, 404);
    return env.CAT_MOUSE_ROOMS.getByName('public-rooms-v1').fetch(request);
  }
};
