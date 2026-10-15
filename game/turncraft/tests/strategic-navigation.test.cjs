const {test}=require('node:test');
const assert=require('node:assert/strict');
const {simulation}=require('./helpers/expansion-harness.cjs');

test('48 mixed troops including Medics cross a long narrow pass using shared routes',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    setBoardDimensions('rift-dominion');terrain=Array.from({length:BOARD_W*BOARD_H},(_,i)=>({x:i%BOARD_W,y:Math.floor(i/BOARD_W),type:'ground',blocked:false}));
    for(let y=0;y<BOARD_H;y++)if(y<60||y>62)tileAt(64,y).blocked=true;
    const units=Array.from({length:48},(_,i)=>addEntity('unit',['rifleman','medic','siegeTank','goliath','ultralisk'][i%5],0,10+i%8,52+Math.floor(i/8),{moveReadyAt:testClock+500}));
    orderFormation(units.map(u=>u.id),110,64,0);let invalid=false;
    for(let i=0;i<1400&&units.some(u=>u.moveDestination);i++){testClock+=250;updatePersistentOrders();invalid||=units.some(u=>groundBlocked(u.x,u.y,u))||new Set(units.map(u=>u.x+','+u.y)).size!==units.length}
    return {arrived:units.filter(u=>!u.moveDestination&&u.x>100).length,invalid,builds:navigationCache.builds,hits:navigationCache.hits,fields:navigationCache.fields.size};
  })()`);
  assert.equal(result.arrived,48);assert.equal(result.invalid,false);
  assert.ok(result.hits>result.builds*4,JSON.stringify(result));assert.ok(result.builds<200,JSON.stringify(result));assert.ok(result.fields<=64);
});

test('a new blocked choke invalidates shared terrain routes and uses the alternate gate',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    for(let y=0;y<BOARD_H;y++)if(![8,9,10,23,24,25].includes(y))tileAt(20,y).blocked=true;
    const u=addEntity('unit','medic',0,10,9);issueMoveOrder(u,32,9);const first=navigationCache.builds;
    for(let y=8;y<=10;y++)addEntity('structure','supplyDepot',1,20,y);
    let usedAlternate=false;for(let i=0;i<700&&u.moveDestination;i++){testClock+=250;updatePersistentOrders();if(u.y>=23)usedAlternate=true}
    return {x:u.x,y:u.y,moving:!!u.moveDestination,usedAlternate,rebuilt:navigationCache.builds>first};
  })()`);
  assert.deepEqual(result,{x:32,y:9,moving:false,usedAlternate:true,rebuilt:true});
});

test('idle friendly traffic yields into a side pocket while held troops keep their position',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    terrain.forEach(t=>{t.blocked=true});for(let x=2;x<=35;x++)tileAt(x,15).blocked=false;tileAt(12,14).blocked=false;
    const medic=addEntity('unit','medic',0,10,15),idle=addEntity('unit','rifleman',0,12,15),held=addEntity('unit','rifleman',0,30,15);held.order='hold';
    issueMoveOrder(medic,25,15);for(let i=0;i<250;i++){testClock+=250;updatePersistentOrders()}
    return {arrived:medic.x===25,side:idle.x!==12||idle.y!==15,held:{x:held.x,y:held.y,order:held.order}};
  })()`);
  assert.equal(result.arrived,true);assert.equal(result.side,true);assert.deepEqual(result.held,{x:30,y:15,order:'hold'});
});

test('Hard and Pro split approaches to fortified bases while Easy keeps a direct route',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    chosenMapId='fourfold-divide';setBoardDimensions();terrain=mapLayout().terrain;const layout=mapLayout(),home=layout.starts[0],foe=layout.starts[2];addEntity('structure','commandHub',0,home.x,home.y);addEntity('structure','nexus',1,foe.x,foe.y);
    addEntity('structure','photonCannon',1,foe.x+2,foe.y);addEntity('structure','photonCannon',1,foe.x,foe.y+2);
    const army=Array.from({length:12},(_,i)=>addEntity('unit',i%4?'rifleman':'medic',0,home.x+i%4,home.y+Math.floor(i/4)));
    players[0].aiDifficulty='easy';const easy=aiAttackApproaches(0,army,foe);players[0].aiDifficulty='hard';const hard=aiAttackApproaches(0,army,foe);players[0].aiDifficulty='pro';const pro=aiAttackApproaches(0,army,foe);
    return {easy,hard,pro};
  })()`);
  assert.equal(result.easy.length,1);assert.equal(result.easy[0].points.length,1);
  for(const approaches of [result.hard,result.pro]){assert.equal(approaches.length,2);assert.equal(new Set(approaches.flatMap(p=>p.ids)).size,12);assert.ok(approaches.every(p=>p.points.length>=2));assert.notDeepEqual(approaches[0].points,approaches[1].points)}
});

test('every faction plans defensive structures beside a choke without obstructing its mouth',()=>{
  for(const [faction,type]of [['frontier','bunker'],['ascendant','photonCannon'],['brood','sunkenColony']]){
    const run=simulation(faction),result=run.plain(`(()=>{chosenMapId='triad-basin';setBoardDimensions();terrain=mapLayout().terrain;const b=mapLayout().starts[0];addEntity('structure',FACTIONS[players[0].factionId].base,0,b.x,b.y);const site=aiDefensiveSite(0,${JSON.stringify(type)});return {site,open:site&&!groundBlocked(site.x,site.y),clear:site&&mapLayout().chokes.every(c=>dist(c,site)>=Math.max(2,c.width/2)),near:site&&mapLayout().bases[0].entrances.some(e=>dist(e.staging,site)<6)}})()`);
    assert.ok(result.site,faction);assert.equal(result.open,true);assert.equal(result.clear,true);assert.equal(result.near,true);
  }
});
