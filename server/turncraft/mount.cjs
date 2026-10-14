'use strict';
const { createRouter } = require('./http.cjs');
const PROJECT_ID = 'bible-game-246c0';
const DEFAULT_ORIGINS = Object.freeze([
  'https://catnmice.com',
  'https://www.catnmice.com'
]);
function mountTurncraft(
  app,
  {
    db,
    auth,
    projectId,
    allowedOrigins,
    routerFactory = createRouter,
    logger = console
  } = {}
) {
  try {
    if (projectId !== PROJECT_ID)
      throw new Error(
        'TurnCraft must use the existing CatnMice Firebase project.'
      );
    const configured = process.env.TURNCRAFT_ALLOWED_ORIGINS,
      origins =
        allowedOrigins ||
        (configured === undefined
          ? DEFAULT_ORIGINS
          : configured
              .split(',')
              .map((v) => v.trim())
              .filter(Boolean));
    if (!db || !auth || !origins.length)
      throw new Error(
        'TurnCraft requires the existing Firebase Admin connection and exact game origins.'
      );
    app.use(
      '/api/turncraft',
      routerFactory({ db, auth, allowedOrigins: origins })
    );
    return true;
  } catch (error) {
    logger.error(
      'TurnCraft could not be mounted; existing games remain available.',
      error.code || error.name
    );
    return false;
  }
}
module.exports = { mountTurncraft, PROJECT_ID, DEFAULT_ORIGINS };
