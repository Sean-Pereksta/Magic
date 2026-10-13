'use strict';
const {createRouter}=require('./http.cjs');
const PROJECT_ID='bible-game-246c0';
const DEFAULT_ORIGINS=Object.freeze(['https://catnmice.com','https://www.catnmice.com']);
/** Reuse the existing host's Firebase Admin connection. A failed optional game
 * mount must not disable the Grid Overlord HTTP/Socket.IO host. */
function mountFactionForge(app,{db,auth,projectId,allowedOrigins,routerFactory=createRouter,logger=console}={}) {
  try {
    if(projectId!==PROJECT_ID){const error=new Error('Faction Forge must use the existing CatnMice Firebase project.');error.code='PROJECT_MISMATCH';throw error;}
    const configured=process.env.FACTION_FORGE_ALLOWED_ORIGINS;
    const origins=allowedOrigins||(configured===undefined?DEFAULT_ORIGINS:configured.split(',').map(value=>value.trim()).filter(Boolean));
    if(!db||!auth||!Array.isArray(origins)||!origins.length)throw new Error('Faction Forge requires the existing Admin connection and exact allowed origins.');
    app.use('/api/faction-forge',routerFactory({db,auth,allowedOrigins:origins}));
    return true;
  } catch(error) {
    // Do not print credentials or request bodies from an initialization failure.
    logger.error('Faction Forge progression could not be mounted; existing host remains available.',error.code||error.name);
    return false;
  }
}
module.exports={mountFactionForge,DEFAULT_ORIGINS,PROJECT_ID};
