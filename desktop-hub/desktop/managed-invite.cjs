'use strict';

const managedGames = new Set(['bannerlands', 'trashtactics', 'cat', 'lastsignal', 'mobamania', 'orbitbloom']);
const tunnel = /^[a-z0-9]+(?:-[a-z0-9]+)*\.trycloudflare\.com$/;
const nameOK = name => typeof name === 'string' && name.length <= 22 && !/[\x00-\x1f\x7f]/.test(name);
const codeOK = code => /^\d{4}$/.test(code);
const exactFields = (params, fields) => [...params].every(([key, val]) =>
  Object.hasOwn(fields, key) && params.getAll(key).length === 1 && fields[key](val));

function acornHost(url) {
  return url.protocol === 'https:' && !url.port && !url.username && !url.password &&
    (tunnel.test(url.hostname) || url.hostname === 'rally.catnmice.com');
}

function managedInvite(id, raw) {
  try {
    const url = new URL(raw);
    if (url.protocol !== 'https:' || url.port || url.username || url.password) return null;
    const fragment = new URLSearchParams(url.hash.slice(1));
    if (id === 'acornrally') {
      if (!acornHost(url) || url.pathname !== '/' || url.search !== '?room=join' ||
          !exactFields(fragment, {join: v => v === '1', name: nameOK, code: codeOK}) || fragment.get('join') !== '1') return null;
    } else if (!managedGames.has(id) || !tunnel.test(url.hostname) || url.pathname !== '/__catnmice/join' ||
        !exactFields(url.searchParams, {name: nameOK}) || !exactFields(fragment, {code: codeOK})) return null;
    return url.href;
  } catch { return null; }
}

// The local player's save session, host credentials and application permissions
// must never follow an invitation into somebody else's server.
function restrictGuestSession(session, origin) {
  const host = new URL(origin).host;
  session.setPermissionRequestHandler((_wc, permission, reply) => reply(permission === 'fullscreen'));
  session.setPermissionCheckHandler(() => false);
  session.webRequest.onBeforeRequest((details, reply) => {
    let allow = false;
    try {
      const target = new URL(details.url);
      const frame = ['mainFrame', 'subFrame'].includes(details.resourceType);
      allow = !target.username && !target.password && (
        (target.protocol === 'https:' && target.origin === origin) ||
        (target.protocol === 'wss:' && target.host === host && !frame) ||
        (!frame && (target.protocol === 'data:' || (target.protocol === 'blob:' && target.origin === origin)))
      );
    } catch {}
    reply({cancel: !allow});
  });
}

module.exports = { managedInvite, restrictGuestSession, acornHost };
