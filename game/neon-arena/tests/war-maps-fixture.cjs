const fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const maps=require('../neon-survival-maps.js'),war=require('../neon-war-maps.js');
const base=path.resolve(__dirname,'../assets/survival');
const read=p=>JSON.parse(fs.readFileSync(p,'utf8'));
const source=fs.readFileSync(path.resolve(__dirname,'../neon-arena.js'),'utf8');
function warFixture(id){
  const meta=war.get(id),root=path.join(base,'tilesets',meta.tileset),definitions={};
  for(const p of read(path.join(root,'prefabs.json')).items){const file=path.join(root,p.file),definition=read(file);definitions[p.id]={...definition,mesh:read(path.resolve(path.dirname(file),definition.geometry))};}
  maps.hydrateForTest(read(path.join(base,'maps',meta.tileset+'.json')),read(path.join(root,'tileset.json')),definitions);
  const helpers=source.slice(source.indexOf('let arenaRamps=[],arenaProps=[],arenaAmbient=[];'),source.indexOf('function buildStarterGridArenaMap(){'));
  const physical=source.slice(source.indexOf('function actorOverSurface('),source.indexOf('// Keep the collider'));
  const traversal=source.slice(source.indexOf('function tacticalPointInfo('),source.indexOf('function humanBotController(){'));
  const context=vm.createContext({Math,console,maps,war});
  vm.runInContext(`const WORLD_SCALE=1.45,PLAYER_H=1.86,ARENA_SAFE=64.3;let walls=[],platforms=[],nav=[],edgeSpawns=[],mapTheme={},pickups=[];const game={map:${JSON.stringify(id)},mode:'tdm'},clamp=(n,a,b)=>Math.max(a,Math.min(b,n));
  function actorStandingHeight(){return 2.39}function addPlat(x,z,w,d,y,h,c,safety){const p={type:'platform',x:x*1.45,z:z*1.45,w:w*1.45,d:d*1.45,y,h,c,safety,active:true};platforms.push(p);return p}function addTopPlat(x,z,w,d,top,h,c,safety){return addPlat(x,z,w,d,top-h,h,c,safety)}function addWall(x,z,w,d,h,c,y=0,safety){const p={type:'wall',x:x*1.45,z:z*1.45,w:w*1.45,d:d*1.45,y:y+h/2,h,c,safety,active:true};walls.push(p);return p}function addArenaBounds(){addWall(0,-46,96,2.2,22,'#fff',0,true);addWall(0,46,96,2.2,22,'#fff',0,true);addWall(-46,0,2.2,96,22,'#fff',0,true);addWall(46,0,2.2,96,22,'#fff',0,true)}function addPickup(){}function addArmorPickup(){}let cachedBlocks=null;function blocksNear(x,z,r=0){cachedBlocks ||= [...walls.map(w=>({...w,kind:'wall',structure:w})),...platforms.map(p=>({...p,y:p.y+p.h/2,kind:'platform',structure:p}))];return cachedBlocks.filter(b=>Math.abs(x-b.x)<=b.w/2+r&&Math.abs(z-b.z)<=b.d/2+r)}
  ${helpers}
  ${physical}
  ${traversal}
  const layout=war.build(game.map,{addPlat,addArenaBounds,arenaWall,arenaSurface,arenaDeck,arenaRamp,arenaProp,arenaNavAndSpawns,arenaLoadoutPickups,setTheme:t=>mapTheme=t,addAmbient:p=>arenaAmbient.push(p),addNavPoint:p=>nav.push({...p,x:p.x*1.45,z:p.z*1.45})});
  maps.installScenery(arenaProps,{addWall},arenaAmbient);
  // The fixture is immutable. Memoize repeated physical edge checks so the
  // tests measure real routes without retracing the same static corridor for
  // every campaign anchor. No production collision rule is replaced.
  const infoCache=new Map(),edgeCache=new Map(),rawInfo=tacticalPointInfo,rawTraverse=tacticalCanTraverse;
  tacticalPointInfo=(x,z,a={})=>{const key=[x,z,a.y,a.r].join(':');if(!infoCache.has(key))infoCache.set(key,rawInfo(x,z,a));return infoCache.get(key)};
  tacticalCanTraverse=(from,to,a={})=>{const key=[from.x,from.y,from.z,to.x,to.y,to.z,a.r].join(':');if(!edgeCache.has(key))edgeCache.set(key,rawTraverse(from,to,a));return edgeCache.get(key)};
  globalThis.result={layout,walls,platforms,nav,spawns:edgeSpawns,ramps:arenaRamps,props:arenaProps,tacticalPointInfo,tacticalCanTraverse,arenaRampHeight};`,context);
  return context.result;
}
module.exports={warFixture};
