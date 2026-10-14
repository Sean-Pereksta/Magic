'use strict';
const { createTurncraftService } = require('./service.cjs');
const { TurncraftError } = require('./domain.cjs');
function createRouter({ db, auth, allowedOrigins = [], service }) {
  const express = require('express'),
    router = express.Router(),
    game = service || createTurncraftService({ db, auth }),
    windows = new Map();
  router.use((req, res, next) => {
    const origin = req.get('Origin');
    if (origin) {
      if (!allowedOrigins.includes(origin))
        return res
          .status(403)
          .json({
            error: {
              code: 'ORIGIN_DENIED',
              message: 'This game origin is not configured.'
            }
          });
      res.set('Access-Control-Allow-Origin', origin);
      res.set('Vary', 'Origin');
    }
    res.set('Cache-Control', 'no-store');
    res.set('X-Content-Type-Options', 'nosniff');
    if (req.method === 'OPTIONS') {
      res.set('Access-Control-Allow-Headers', 'Authorization, Content-Type');
      res.set('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
      return res.sendStatus(204);
    }
    next();
  });
  router.use(express.json({ limit: '16kb' }));
  router.get('/health', (_req, res) =>
    res.json({ service: 'turncraft', version: 1, ...game.capabilities() })
  );
  router.use(async (req, res, next) => {
    try {
      req.turncraftUser = await game.identity(
        /^Bearer (\S+)$/.exec(req.get('Authorization') || '')?.[1]
      );
      const stamp = Date.now(),
        kind = /\/command$/.test(req.path)
          ? 'command'
          : /^\/matches\//.test(req.path) && req.method === 'GET'
            ? 'state'
            : 'other',
        uid = `${req.turncraftUser.uid}:${kind}`,
        cap = kind === 'command' ? 1200 : kind === 'state' ? 120 : 180;
      let window = windows.get(uid);
      if (!window || window.until < stamp) {
        window = { until: stamp + 60000, count: 0 };
        windows.set(uid, window);
      }
      if (++window.count > cap)
        return res
          .status(429)
          .json({
            error: {
              code: 'RATE_LIMITED',
              message: 'Wait a moment before retrying.'
            }
          });
      if (windows.size > 5000)
        for (const [key, value] of windows)
          if (value.until < stamp) windows.delete(key);
      next();
    } catch (error) {
      next(error);
    }
  });
  const route = (fn) => async (req, res, next) => {
    try {
      res.json(await fn(req));
    } catch (error) {
      next(error);
    }
  };
  router.get(
    '/profile',
    route((req) => game.profile(req.turncraftUser))
  );
  router.post(
    '/progress/campaign',
    route((req) => game.saveCampaign(req.turncraftUser, req.body))
  );
  router.post(
    '/progress/survival',
    route((req) => game.saveSurvival(req.turncraftUser, req.body))
  );
  router.get(
    '/lobbies',
    route((req) => game.browse(req.turncraftUser))
  );
  router.post(
    '/lobbies',
    route((req) => game.createLobby(req.turncraftUser, req.body))
  );
  router.post(
    '/lobbies/join',
    route((req) => game.join(req.turncraftUser, req.body))
  );
  router.get(
    '/lobbies/:id',
    route((req) => game.getLobby(req.turncraftUser, req.params.id))
  );
  for (const action of [
    'configure',
    'player',
    'start',
    'leave',
    'complete',
    'rematch'
  ])
    router.post(
      `/lobbies/:id/${action}`,
      route((req) => game[action](req.turncraftUser, req.params.id, req.body))
    );
  router.get(
    '/leaderboard',
    route(() => game.leaderboard())
  );
  // These endpoints accept player inputs only. The server advances the actual
  // RTS simulation and calculates terminal results; no winner-claim route exists.
  router.post(
    '/ranked/queue',
    route((req) => game.authority.queue(req.turncraftUser, req.body))
  );
  router.post(
    '/ranked/cancel',
    route((req) => game.authority.cancel(req.turncraftUser))
  );
  router.get(
    '/matches/:id',
    route((req) => game.authority.state(req.turncraftUser, req.params.id))
  );
  router.post(
    '/matches/:id/command',
    route((req) =>
      game.authority.command(req.turncraftUser, req.params.id, req.body)
    )
  );
  router.post(
    '/matches/:id/forfeit',
    route((req) => game.authority.forfeit(req.turncraftUser, req.params.id))
  );
  router.use((error, _req, res, _next) => {
    if (error instanceof TurncraftError)
      return res
        .status(error.status)
        .json({ error: { code: error.code, message: error.message } });
    if (error.code?.startsWith('auth/'))
      return res
        .status(401)
        .json({
          error: {
            code: 'UNAUTHENTICATED',
            message: 'Your sign-in has expired. Sign in again.'
          }
        });
    if (error.type === 'entity.too.large')
      return res
        .status(413)
        .json({
          error: { code: 'INVALID_ARGUMENT', message: 'Request is too large.' }
        });
    if (error instanceof SyntaxError)
      return res
        .status(400)
        .json({
          error: {
            code: 'INVALID_ARGUMENT',
            message: 'Request JSON is invalid.'
          }
        });
    console.error('TurnCraft request failed:', error.code || error.name);
    res
      .status(503)
      .json({
        error: {
          code: 'SERVICE_UNAVAILABLE',
          message:
            'The TurnCraft service is unavailable. Your local campaign progress is safe.'
        }
      });
  });
  return router;
}
module.exports = { createRouter };
