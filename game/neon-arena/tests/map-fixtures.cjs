const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const maps=require('../neon-survival-maps.js'),base=path.resolve(__dirname,'../assets/survival');
const tactical=require('../neon-tactical-ai.js');
const read=p=>JSON.parse(fs.readFileSync(p,'utf8'));
function survivalFixture(id){
  const b=read(path.join(base,'maps',id+'.json')),t=read(path.join(base,'tilesets',id,'tileset.json')),definitions={};
  for(const p of read(path.join(base,'tilesets',id,'prefabs.json')).items){const file=path.join(base,'tilesets',id,p.file),d=read(file);definitions[p.id]={...d,mesh:read(path.resolve(path.dirname(file),d.geometry))};}
  maps.hydrateForTest(b,t,definitions);const pieces=[];
  maps.install({addPlatform:(x,z,w,d,y,h)=>{const p={type:'platform',x:x*1.45,z:z*1.45,w:w*1.45,d:d*1.45,y,h,active:true};pieces.push(p);return p;},addWall:(x,z,w,d,h,c,y)=>{const p={type:'wall',x:x*1.45,z:z*1.45,w:w*1.45,d:d*1.45,y:y+h/2,h,active:true,destructible:true};pieces.push(p);return p;}});
  return {b,t,definitions,pieces};
}
const source=fs.readFileSync(path.resolve(__dirname,'../neon-arena.js'),'utf8');
function standardFixture(map){
  survivalFixture({grid:'breach_ring',pastel:'black_ice_cryolab',reactor:'drowned_powerworks'}[map]);
  const layout=source.slice(source.indexOf('// Authored standard arenas'),source.indexOf('function applyFragArena(){'));
  const spawn=source.slice(source.indexOf('function safeSpawnCandidate('),source.indexOf('function spawn(ent){'));
  const build=source.slice(source.indexOf('function buildMap(){'),source.indexOf('function buildSurvivorBreachMap(){'));
  const physical=source.slice(source.indexOf('function actorOverSurface('),source.indexOf('// Keep the collider'));
  const traversal=source.slice(source.indexOf('function tacticalPointInfo('),source.indexOf('function humanBotController(){'));
  const context=vm.createContext({Math,console,maps});
  vm.runInContext(`const WORLD_SCALE=1.45,PLAYER_H=1.86,ARENA_SAFE=64.3;let walls=[],platforms=[],nav=[],edgeSpawns=[],mapTheme={},spawnIndex=0,bots=[],player={dead:true},player2=null,activeBlocks=[],blocksDirty=true,pickups=[],buildPieces=[],buildOccupancy=new Map(),structureId=0,activeBuildCount=0;const game={map:${JSON.stringify(map)},mode:'dm'};const clamp=(x,a,b)=>Math.max(a,Math.min(b,x)),rand=(a,b)=>(a+b)/2;const survivalMapRuntime=maps;function lineClear(){return true}function actorStandingHeight(){return 2.39}function registerStaticGridOccupancy(){}function rebuildActiveBlocks(){cachedBlocks=null;}function topAtPoint(){return 0}function addPlat(x,z,w,d,y,h,c,safety){platforms.push({type:'platform',x:x*1.45,z:z*1.45,w:w*1.45,d:d*1.45,y,h,c,safety,active:true});nav.push({x:x*1.45,z:z*1.45,y:y+h+1.86})}function addTopPlat(x,z,w,d,top,h,c,safety){addPlat(x,z,w,d,top-h,h,c,safety)}function addWall(x,z,w,d,h,c,y=0,safety){walls.push({type:'wall',x:x*1.45,z:z*1.45,w:w*1.45,d:d*1.45,y:y+h/2,h,c,safety,active:true})}function addArenaBounds(){addWall(0,-46,96,2.2,22,'#fff',0,true);addWall(0,46,96,2.2,22,'#fff',0,true);addWall(-46,0,2.2,96,22,'#fff',0,true);addWall(46,0,2.2,96,22,'#fff',0,true)}function addPickup(){}function addArmorPickup(){}let cachedBlocks=null;function blocksNear(x,z,r=0){cachedBlocks ||= [...walls.map(w=>({...w,kind:'wall',structure:w})),...platforms.map(p=>({...p,y:p.y+p.h/2,kind:'platform',structure:p}))];return cachedBlocks.filter(b=>Math.abs(x-b.x)<=b.w/2+r&&Math.abs(z-b.z)<=b.d/2+r)}
  ${layout}
  ${spawn}
  ${build}
  ${physical}
  ${traversal}
  buildMap();globalThis.result={walls,platforms,nav,spawns:edgeSpawns,ramps:arenaRamps,props:arenaProps,arenaRampHeight,safeSpawnCandidate,chooseSafeSpawn,tacticalPointInfo,tacticalCanTraverse,setEnemies:enemies=>{bots=enemies}};`,context);
  context.result.evaluate=code=>vm.runInContext(code,context);
  return context.result;
}

module.exports={standardFixture,survivalFixture,source,read,base};
