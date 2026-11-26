import test from 'node:test';
import assert from 'node:assert/strict';
import { Game } from '../game/moba-mania/engine.mjs';
import { HEROES } from '../game/moba-mania/data.mjs';
import { expansionBotAim, nextBuildItem, chooseBotTalent } from '../game/moba-mania/strategy.mjs';

const close=(a,b)=>assert(Math.abs(a-b)<1e-6,`${a} != ${b}`);
function arena(id,level=18){
  const game=new Game({hero:id,difficulty:'hard',seed:719}),hero=game.player;
  game.units=[hero];game.campStates=[];game.effects=[];game.waveIn=game.beastIn=game.planIn=1e8;
  Object.assign(hero,{x:1400,y:1100,level,attackTimer:0,lastDamageAt:0});game.refreshStats(hero);hero.hp=hero.maxHp;hero.mana=hero.maxMana;
  const add=(dx,dy=0,team=1,kind='footman')=>game.addUnit(kind,team,hero.x+dx,hero.y+dy,{hp:10000,r:17,speed:0,damage:0,range:20,attackTimer:1e8,name:'Practice target'});
  const addHero=(id='dragon',dx=120,dy=0,team=1)=>{const h=game.makeHero(id,team,1);Object.assign(h,{x:hero.x+dx,y:hero.y+dy,bot:false,hp:10000,maxHp:10000,speed:0,attackTimer:1e8,lastDamageAt:game.time});return h;};
  return {game,hero,add,addHero};
}
function advance(game,seconds){for(let left=seconds;left>1e-8;left-=.025)game.update(Math.min(.025,left));}
function cast(game,hero,slot,at=hero){hero.cooldowns[slot]=0;hero.mana=hero.maxMana;game.rebuildGrid();assert(game.cast(hero,slot,{x:at.x,y:at.y}));}
function talent(game,hero,key){const row=HEROES[hero.heroId].talents.find(t=>t.choices.some(c=>key in c.bonuses));assert(row,key);hero.talents[row.level]=row.choices.find(c=>key in c.bonuses).id;game.refreshStats(hero);}
const power=(game,h)=>(1+(h.level-1)*.075)*(1+game.bonus(h,'damage'))*(1+game.bonus(h,'spellDamage'));
const shieldPower=(game,h,n)=>n*(1+game.bonus(h,'shield'))*(1+(h.level-1)*.08);

// These tests use actual Game entry points, not duplicate versions of the mechanics.
test('Ferrite and Cadence have complete independent kits, talent trees and safe build paths',()=>{
  for(const id of ['ferrite','cadence']){
    const h=HEROES[id],game=new Game({hero:id,difficulty:'impossible',seed:8});
    assert.equal(h.abilities.length,4);assert.deepEqual(h.talents.map(t=>t.level),[3,5,8,10,12,15,18]);
    assert(h.talents.every(t=>t.choices.length===(t.level>10?4:3)));assert.equal(new Set(h.talents.flatMap(t=>t.choices.map(c=>c.id))).size,24);
    for(const row of h.talents)assert(row.choices.includes(chooseBotTalent(game.player,row)));
    assert(nextBuildItem(game.player));assert.equal(game.units.filter(u=>u.kind==='hero'&&u.team===1).length,4);
    assert.equal(Game.restore(game.serialize()).player.heroId,id);
  }
  assert.notEqual(HEROES.ferrite.passive.name,HEROES.cadence.passive.name);
});

test('Ferrite salvages only successful basic or rail hits, with a shared gate and three-scrap cap',()=>{
  const {game,hero,add}=arena('ferrite'),enemy=add(50),far=add(220);
  game.rebuildGrid();assert(game.basicAttack(hero,enemy));assert.equal(hero.resonance.scrap,1);
  hero.attackTimer=0;game.basicAttack(hero,enemy);assert.equal(hero.resonance.scrap,1);
  cast(game,hero,0,far);advance(game,.4);assert.equal(hero.resonance.scrap,1);
  game.time+=.7;hero.attackTimer=0;game.basicAttack(hero,enemy);assert.equal(hero.resonance.scrap,2);
  game.time+=.7;hero.attackTimer=0;game.basicAttack(hero,enemy);assert.equal(hero.resonance.scrap,3);
  game.time+=.7;hero.attackTimer=0;game.basicAttack(hero,enemy);assert.equal(hero.resonance.scrap,3);
  hero.resonance.scrap=0;game.damage(enemy,20,hero,'magic',false,{proc:true});assert.equal(hero.resonance.scrap,0);
  const tower=add(55,0,1,'tower');hero.attackTimer=0;game.basicAttack(hero,tower);assert.equal(hero.resonance.scrap,0);
});

test('a piercing Rail Spike awards at most one scrap and observes terrain and hidden targets',()=>{
  const {game,hero,add}=arena('ferrite');talent(game,hero,'ferritePierce');talent(game,hero,'ferriteRailRange');
  const a=add(70),b=add(260),c=add(450),hidden=add(180),realSee=game.canSee.bind(game);game.canSee=(team,u)=>u.id!==hidden.id&&realSee(team,u);
  cast(game,hero,0,c);advance(game,1);assert.equal(hero.resonance.scrap,1);assert(a.hp<a.maxHp&&b.hp<b.maxHp&&c.hp<c.maxHp);close(hidden.hp,hidden.maxHp);
  const g=arena('ferrite');g.game.clearLine=()=>false;const blocked=g.add(200);cast(g.game,g.hero,0,blocked);advance(g.game,.7);close(blocked.hp,blocked.maxHp);assert.equal(g.hero.resonance.scrap,0);
});

test('Scrap Barrier spends its real resource and shares only to nearby allied heroes',()=>{
  const {game,hero,add,addHero}=arena('ferrite'),ally=addHero('cadence',90,0,0),minion=add(70,30,0),far=addHero('arc',300,0,0);
  talent(game,hero,'ferriteSharedAlloy');talent(game,hero,'ferriteScrapShield');hero.resonance.scrap=3;cast(game,hero,1);
  close(hero.shield,shieldPower(game,hero,100+3*65));close(ally.shield,hero.shield*.4);close(minion.shield,0);close(far.shield,0);assert.equal(hero.resonance.scrap,0);
  const first=hero.shield;cast(game,hero,1);close(hero.shield,first);assert.equal(hero.resonance.scrap,0);
});

test('Insulated Core never weakens a stronger existing guard',()=>{
  const {game,hero}=arena('ferrite');talent(game,hero,'ferriteInsulated');game.applyStatus(hero,'guard',3,{factor:.5,sourceId:hero.id});cast(game,hero,1);close(hero.status.guard.factor,.5);
});

test('Polarity Pulse visibly waits, alternates pull and push, and respects displacement immunity',()=>{
  const {game,hero,add,addHero}=arena('ferrite'),enemy=addHero('vela',130),boss=add(110,50,1,'beast'),tower=add(110,-50,1,'tower');
  assert.equal(hero.resonance.polarity,'push');cast(game,hero,2);assert.equal(hero.resonance.polarity,'pull');const original=enemy.x;
  advance(game,.3);close(enemy.x,original);advance(game,.08);assert(enemy.x<original);close(boss.x,1510);close(tower.x,1510);
  advance(game,.55);const pulled=enemy.x;cast(game,hero,2);advance(game,.4);assert.equal(hero.resonance.polarity,'push');assert(enemy.x>pulled);
});

test('magnetic displacement cannot cross obstacles or repeatedly move a recovering target',()=>{
  const {game,hero,addHero}=arena('ferrite'),enemy=addHero('vela',100);
  const x=enemy.x;game.clearLine=()=>false;cast(game,hero,2);advance(game,.4);close(enemy.x,x);
  game.clearLine=()=>true;enemy.displacementRecoveryUntil=game.time+2;cast(game,hero,2);advance(game,.4);close(enemy.x,x);
});

test('Lodestone Storm consumes scraps at release and its warning can be interrupted',()=>{
  const {game,hero,add}=arena('ferrite'),enemy=add(190);hero.resonance.scrap=3;
  cast(game,hero,3,enemy);assert.equal(hero.resonance.scrap,0);assert.equal(game.scheduledImpacts[0].scrap,3);
  advance(game,.7);close(enemy.hp,enemy.maxHp);game.applyStatus(hero,'stun',1,{sourceId:enemy.id});advance(game,.2);close(enemy.hp,enemy.maxHp);assert.equal(game.scheduledImpacts.length,0);
});

test('Lodestone Storm talents add a separate warned blast or a resource-scaled shield',()=>{
  for(const key of ['ferriteAftershock','ferriteMonolith','ferriteMassDriver']){
    const {game,hero,add}=arena('ferrite'),enemy=add(190);talent(game,hero,key);hero.resonance.scrap=3;cast(game,hero,3,enemy);advance(game,.85);
    assert(enemy.hp<enemy.maxHp);assert(enemy.status.slow);
    if(key==='ferriteAftershock'){assert(game.scheduledImpacts.some(i=>i.kind==='lodestone-echo'));const before=enemy.hp;enemy.x=1590;enemy.y=1100;advance(game,.65);assert(enemy.hp<before);}
    if(key==='ferriteMonolith')close(hero.shield,shieldPower(game,hero,195));
    if(key==='ferriteMassDriver')assert(enemy.status.slow.remaining>2.8);
  }
});

test('Ferrite slows snapshot equipment and talents at cast without double scaling',()=>{
  const {game,hero,add}=arena('ferrite'),enemy=add(250);talent(game,hero,'ferriteStaticPoint');
  hero.items=['rime-shard','frost-compass','winters-dominion'];cast(game,hero,0,enemy);const shot=game.projectiles[0],factor=shot.slow,duration=shot.slowDuration;assert(factor>.2);
  hero.items=[];advance(game,.4);close(enemy.status.slow.factor,factor);assert(enemy.status.slow.remaining<=duration&&enemy.status.slow.remaining>duration-.15);
  const {game:g,hero:h,add:add2}=arena('ferrite'),e=add2(250);cast(g,h,0,e);talent(g,h,'ferriteStaticPoint');advance(g,.4);assert(!e.status.slow,'buying/learning after launch cannot retrofit the shot');
});

function compose(game,hero,slots){for(const slot of slots){cast(game,hero,slot,{x:hero.x+60,y:hero.y});}game.projectiles=[];}
test('Cadence scores ordered notes, charges only three different notes, and expires the score',()=>{
  const {game,hero}=arena('cadence');compose(game,hero,[0,0,1]);assert.equal(hero.resonance.chordUntil,0);assert.deepEqual(hero.resonance.notes.map(n=>n.slot),[0,0,1]);
  compose(game,hero,[2]);assert(hero.resonance.chordUntil>game.time);assert.deepEqual(hero.resonance.notes.map(n=>n.slot),[0,1,2]);
  advance(game,8.1);assert.deepEqual(hero.resonance.notes,[]);assert.equal(hero.resonance.chordUntil,0);
  talent(game,hero,'cadenceNoteWindow');compose(game,hero,[1]);advance(game,8.5);assert.equal(hero.resonance.notes.length,1);advance(game,1.6);assert.equal(hero.resonance.notes.length,0);
});

test('charged musical basic hit bursts once, keeps personal credit and cannot trigger itself',()=>{
  const {game,hero,add}=arena('cadence');compose(game,hero,[0,1,2]);const a=add(100),b=add(125,25);game.rebuildGrid();hero.attackTimer=0;
  const beforeA=a.hp,beforeB=b.hp;assert(game.basicAttack(hero,a));advance(game,.25);close(beforeA-a.hp,hero.damage+45*power(game,hero));close(beforeB-b.hp,45*power(game,hero));assert.equal(hero.resonance.chordUntil,0);
  hero.attackTimer=0;game.basicAttack(hero,a);advance(game,.25);close(beforeB-b.hp,45*power(game,hero));
  compose(game,hero,[0,1,2]);a.x=hero.x+100;b.x=hero.x+125;a.hp=b.hp=1;hero.attackTimer=0;game.rebuildGrid();const earned=hero.gold;game.basicAttack(hero,a);advance(game,.25);assert.equal(hero.gold-earned,36);assert(!a.alive&&!b.alive);
});

test('Skipbeat traces terrain and does not deal contact damage',()=>{
  const {game,hero,add}=arena('cadence'),enemy=add(60),start=hero.x;game.clearLine=(a,b)=>b.x<start+12;
  cast(game,hero,2,{x:hero.x+140,y:hero.y});assert(hero.x-start<140);close(enemy.hp,enemy.maxHp);assert(hero.status.haste);assert.equal(hero.resonance.finaleNote,2);
});

test('Grand Finale ending Q gives a second warned burst and clears the sequence',()=>{
  const {game,hero,add}=arena('cadence');compose(game,hero,[1,2,0]);const enemy=add(170);const radius=game.getAbility(hero,3).radius;close(radius,180);
  cast(game,hero,3,enemy);assert.equal(hero.resonance.notes.length,0);assert.equal(hero.resonance.chordUntil,0);advance(game,.7);close(enemy.maxHp-enemy.hp,120*power(game,hero));assert(game.scheduledImpacts.some(i=>i.kind==='finale-echo'));
  const before=enemy.hp;advance(game,.6);close(before-enemy.hp,80*power(game,hero));
});

test('Grand Finale ending E gives a group shield and optional slow cleanse instead of an echo',()=>{
  const {game,hero,add,addHero}=arena('cadence');talent(game,hero,'cadenceGrandShelter');compose(game,hero,[0,2,1]);
  const ally=addHero('ferrite',80,0,0),enemy=add(100);game.applyStatus(ally,'slow',4,{factor:.5,sourceId:enemy.id});cast(game,hero,3,ally);advance(game,.7);
  close(ally.shield,shieldPower(game,hero,200));assert(!ally.status.slow);assert(!game.scheduledImpacts.some(i=>i.kind==='finale-echo'));assert(!game.zones.some(z=>z.type==='resonance-stage'));
});

test('Grand Finale ending R creates a bounded stage whose buffs end after leaving it',()=>{
  const {game,hero,add,addHero}=arena('cadence');talent(game,hero,'cadenceLastDance');compose(game,hero,[0,1,2]);
  const ally=addHero('vela',80,0,0),enemy=add(120);cast(game,hero,3,enemy);advance(game,.75);
  const stage=game.zones.find(z=>z.type==='resonance-stage');assert(stage&&stage.maxLife===6);assert(ally.status.haste);assert(enemy.status.slow);close(enemy.status.slow.factor,.35);
  ally.x=2600;enemy.x=2700;advance(game,.3);assert(!ally.status.haste);assert(!enemy.status.slow);
  compose(game,hero,[2]);cast(game,hero,3,hero);advance(game,.7);assert.equal(game.zones.filter(z=>z.type==='resonance-stage').length,1);
});

test('new kits never damage or displace concealed units through their area effects',()=>{
  for(const id of ['ferrite','cadence']){
    const {game,hero,add}=arena(id),hidden=add(100),visible=add(140),realSee=game.canSee.bind(game);game.canSee=(team,u)=>u.id!==hidden.id&&realSee(team,u);
    const x=hidden.x;cast(game,hero,3,hidden);advance(game,1.3);close(hidden.hp,hidden.maxHp);close(hidden.x,x);assert(visible.hp<visible.maxHp);
  }
});

test('note projectile echo and slow talents are captured at launch and occur at most once',()=>{
  const {game,hero,addHero}=arena('cadence'),a=addHero('vela',120),b=addHero('ranger',230);talent(game,hero,'cadenceReverberation');talent(game,hero,'cadenceHoldingNote');cast(game,hero,0,b);advance(game,.42);
  assert(a.status.slow&&b.status.slow);assert.equal(game.scheduledImpacts.filter(i=>i.kind==='note-echo').length,1);
  const fresh=arena('cadence'),enemy=fresh.addHero('vela',120);cast(fresh.game,fresh.hero,0,enemy);talent(fresh.game,fresh.hero,'cadenceReverberation');talent(fresh.game,fresh.hero,'cadenceHoldingNote');advance(fresh.game,.42);assert(!enemy.status.slow);assert(!fresh.game.scheduledImpacts.some(i=>i.kind==='note-echo'));
});

test('death clears resource loops and pending interruptible ultimates',()=>{
  for(const id of ['ferrite','cadence']){
    const {game,hero,add}=arena(id),enemy=add(150);if(id==='ferrite')hero.resonance.scrap=3;else compose(game,hero,[0,1,2]);cast(game,hero,3,enemy);game.kill(hero,enemy);advance(game,1);
    assert.equal(game.scheduledImpacts.length,0);if(id==='ferrite')assert.equal(hero.resonance.scrap,0);else {assert.equal(hero.resonance.chordUntil,0);assert.equal(hero.resonance.notes.length,0);}
  }
});

test('mid-warning and mid-stage save/resume preserve deterministic new mechanics',()=>{
  for(const id of ['ferrite','cadence']){
    const {game,hero,add}=arena(id),enemy=add(160);if(id==='ferrite'){hero.resonance.scrap=3;talent(game,hero,'ferriteAftershock');}else compose(game,hero,[0,1,2]);cast(game,hero,3,enemy);advance(game,.2);
    const resume=Game.restore(game.serialize());for(let i=0;i<100;i++){game.update(.025);resume.update(.025);}assert.deepEqual(resume.serialize(),game.serialize());
    const again=Game.restore(game.serialize());advance(game,.4);advance(again,.4);assert.deepEqual(again.serialize(),game.serialize());
  }
});

test('malformed magnetic and musical saves are rejected and old missing state initializes safely',()=>{
  for(const id of ['ferrite','cadence']){
    const game=new Game({hero:id,seed:26}),snapshot=game.serialize(),player=snapshot.state.units.find(u=>u.isPlayer);
    if(id==='ferrite')player.resonance.scrap=4;else player.resonance.notes=[{slot:8,at:0}];assert.throws(()=>Game.restore(snapshot),/Invalid saved match/);
    const old=game.serialize();delete old.state.units.find(u=>u.isPlayer).resonance;assert(Game.restore(old).player.resonance);
  }
});

test('new bots make visible-target, defensive polarity and meaningful support decisions',()=>{
  const cfg={coordination:1,lead:1},f=arena('ferrite'),foe=f.addHero('vela',100);
  assert(expansionBotAim(f.game,f.hero,f.game.getAbility(f.hero,2),foe,false,cfg));f.hero.resonance.polarity='pull';assert.equal(expansionBotAim(f.game,f.hero,f.game.getAbility(f.hero,2),foe,false,cfg),null);
  assert(expansionBotAim(f.game,f.hero,f.game.getAbility(f.hero,2),foe,true,cfg));
  const c=arena('cadence'),enemy=c.addHero('dragon',120),ally=c.addHero('vela',90,0,0);ally.hp=200;enemy.targetId=ally.id;
  assert(expansionBotAim(c.game,c.hero,c.game.getAbility(c.hero,1),enemy,false,cfg));Object.assign(c.hero,{x:1200,y:990});Object.assign(enemy,{x:1420,y:990});Object.assign(ally,{x:900,y:990});assert.equal(c.game.canSee(0,enemy),false);assert.equal(expansionBotAim(c.game,c.hero,c.game.getAbility(c.hero,0),enemy,false,cfg),null);
});


test('Ferrite uses Scrap Barrier under visible tower fire even without nearby enemy troops',()=>{
  const {game,hero,add}=arena('ferrite'),tower=add(200,0,1,'tower');
  tower.range=270;tower.targetId=hero.id;hero.resonance.scrap=2;
  assert(expansionBotAim(game,hero,game.getAbility(hero,1),null,true,{coordination:1,lead:1}));
});
