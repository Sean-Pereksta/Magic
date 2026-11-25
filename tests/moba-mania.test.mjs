import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {execFileSync} from 'node:child_process';
import {Game} from '../game/moba-mania/engine.mjs';
import {HEROES,DIFFICULTIES,WORLD,RULES} from '../game/moba-mania/data.mjs';

const gameFor=(hero='arc',difficulty='normal')=>new Game({hero,difficulty,seed:42});
function advance(game,seconds,input={}){for(let remaining=seconds;remaining>1e-8;){const dt=Math.min(.05,remaining);game.update(dt,input);remaining-=dt;}}
function arena(hero='arc'){
  const game=gameFor(hero),player=game.player;
  const enemy=game.units.find(u=>u.kind==='hero'&&u.team===1);
  const ally=game.units.find(u=>u.kind==='hero'&&u.team===0&&u.id!==player.id);
  game.units=[player,enemy,ally];
  Object.assign(player,{x:1450,y:1100,level:12,mana:1000,maxMana:1000});
  Object.assign(enemy,{x:1600,y:1100,bot:false,speed:0,hp:3000,maxHp:3000});
  Object.assign(ally,{x:1460,y:1125,bot:false,speed:0,hp:300,maxHp:1000});
  return {game,player,enemy,ally};
}
const aim=u=>({x:u.x,y:u.y});
function summons(game,subtype){return game.units.filter(u=>u.kind==='summon'&&(!subtype||u.subtype===subtype));}

test('five playable hero types fill exactly six heroes, one human and five bots',()=>{
  assert.deepEqual(Object.keys(HEROES).sort(),['angel','arc','dragon','druid','ranger']);
  for(const hero of Object.keys(HEROES)){
    const game=gameFor(hero),units=game.units.filter(u=>u.kind==='hero');
    assert.equal(units.length,6);assert.equal(units.filter(u=>u.bot).length,5);
    assert.equal(units.filter(u=>u.team===0).length,3);assert.equal(units.filter(u=>u.team===1).length,3);
    assert.equal(game.player.heroId,hero);assert.equal(game.player.bot,false);
    assert(units.every(u=>Object.hasOwn(HEROES,u.heroId)));
  }
});

test('three separated lanes, jungle camps, river pits, two tower layers and healing wells',()=>{
  const game=gameFor();
  assert.equal(WORLD.lanes.length,3);assert(WORLD.width>=3000&&WORLD.height>=2000);
  assert(WORLD.obstacles.length>40);assert(WORLD.camps.length>=6);assert(WORLD.beastPits.length>=2);
  assert.equal(game.units.filter(u=>u.kind==='core').length,2);
  assert.equal(game.units.filter(u=>u.kind==='tower').length,12);
  for(let team=0;team<2;team++)for(let lane=0;lane<3;lane++){
    const towers=game.units.filter(u=>u.kind==='tower'&&u.team===team&&u.lane===lane);
    assert.deepEqual(towers.map(u=>u.tier).sort(),[0,1]);
  }
  assert(WORLD.bases[0].well.x<WORLD.bases[0].x);assert(WORLD.bases[1].well.x>WORLD.bases[1].x);
});

test('each timed wave sends three footmen and two archers per lane on both teams',()=>{
  const game=gameFor();advance(game,RULES.firstWave+.05);
  for(let team=0;team<2;team++)for(let lane=0;lane<3;lane++){
    const wave=game.units.filter(u=>u.team===team&&u.lane===lane);
    assert.equal(wave.filter(u=>u.kind==='footman').length,3);assert.equal(wave.filter(u=>u.kind==='archer').length,2);
  }
});

test('outer towers protect their own inner towers; breaching a lane opens the crystal',()=>{
  const game=gameFor(),player=game.player;
  const outer=game.units.find(u=>u.kind==='tower'&&u.team===1&&u.lane===1&&u.tier===0);
  const inner=game.units.find(u=>u.kind==='tower'&&u.team===1&&u.lane===1&&u.tier===1);
  const otherInner=game.units.find(u=>u.kind==='tower'&&u.team===1&&u.lane===0&&u.tier===1);
  const core=game.units.find(u=>u.kind==='core'&&u.team===1);
  const innerHP=inner.hp,coreHP=core.hp;
  game.damage(inner,1000,player,'test');game.damage(core,1000,player,'test');
  assert.equal(inner.hp,innerHP);assert.equal(core.hp,coreHP);
  game.damage(outer,1e7,player,'test');assert.equal(outer.alive,false);
  assert.equal(game.isProtected(inner),false);assert.equal(game.isProtected(otherInner),true);
  game.damage(inner,1e7,player,'test');assert.equal(inner.alive,false);
  assert.equal(game.isProtected(core),false);
  game.damage(core,1e7,player,'test');assert.equal(game.winner,0);
  assert.equal(game.cast(player,0,aim(core)),false,'finished matches cannot cast');
});

test('abilities respect levels, mana, cooldowns, and dead heroes',()=>{
  const game=gameFor(),player=game.player;
  assert.equal(game.cast(player,1,{x:500,y:1100}),false,'level 2 slot locked');
  const mana=player.mana;player.mana=0;
  assert.equal(game.cast(player,0,{x:500,y:1100}),false);assert.equal(player.cooldowns[0],0);
  player.mana=mana;assert.equal(game.cast(player,0,{x:500,y:1100}),true);
  assert(player.mana<mana);assert(player.cooldowns[0]>0);
  const spent=player.mana;assert.equal(game.cast(player,0,{x:500,y:1100}),false);assert.equal(player.mana,spent);
  player.alive=false;player.cooldowns[0]=0;assert.equal(game.cast(player,0,{x:500,y:1100}),false);
});

test('XP levels unlock abilities and exclusive talent branches; stat choices have consequences',()=>{
  const game=gameFor('dragon'),hero=game.player,tier=HEROES.dragon.talents[0];
  assert.equal(game.chooseTalent(hero,3,tier.choices[1].id),false);
  game.gainXP(hero,hero.xpNext);assert.equal(hero.level,2);
  assert.equal(game.getAbility(hero,1).unlock,2);
  game.gainXP(hero,hero.xpNext);assert.equal(hero.level,3);
  const hp=hero.maxHp;
  assert.equal(game.chooseTalent(hero,3,'dragon-vigor'),true);assert(hero.maxHp>hp*1.15);
  assert.equal(game.chooseTalent(hero,3,'dragon-embers'),false,'tier cannot be changed after choosing');
  assert.equal(game.chooseTalent(hero,5,'dragon-haste'),false,'future choices locked');
  game.gainXP(hero,1e7);assert.equal(hero.level,RULES.maxLevel);
  assert.equal(game.chooseTalent(hero,5,'not-a-talent'),false);
  assert.equal(hero.talents[3],'dragon-vigor');
});

test('Arc Rift storm has a readable delay, forcefields protect, spirits fight, and blink moves',()=>{
  const {game,player,enemy,ally}=arena('arc'),hp=enemy.hp;
  assert(game.cast(player,0,aim(enemy)));advance(game,.3);assert.equal(enemy.hp,hp,'storm telegraph gives time to evade');
  advance(game,.5);assert(enemy.hp<hp);
  assert(game.cast(player,1,aim(ally)));advance(game,.2);assert(ally.shield>0,'forcefield protects allies');
  assert(game.cast(player,2,aim(enemy)));assert.equal(summons(game,'spirit').length,3);
  const x=player.x;assert(game.cast(player,3,{x:x+220,y:player.y}));assert(player.x>x+100);
});

test('Arc Rift talent choices alter storm damage and spirit count',()=>{
  function stormDamage(talent){const {game,player,enemy}=arena('arc');if(talent)assert(game.chooseTalent(player,3,'arc-thunder'));const hp=enemy.hp;game.cast(player,0,aim(enemy));advance(game,1);return hp-enemy.hp;}
  const baseline=stormDamage(false),boosted=stormDamage(true);assert(boosted>baseline*1.2);
  const {game,player,enemy}=arena('arc');assert(game.chooseTalent(player,8,'arc-legion'));game.cast(player,2,aim(enemy));assert.equal(summons(game,'spirit').length,5);
});

test('dragon straight-line skill shot can hit or miss and applies burning',()=>{
  for(const hit of [false,true]){
    const {game,player,enemy}=arena('dragon');enemy.y=hit?player.y:player.y+180;const hp=enemy.hp;
    assert(game.cast(player,0,{x:player.x+450,y:player.y}));advance(game,1);
    if(hit){assert(enemy.hp<hp);assert(enemy.status.burn,'dragon causes burn');}else assert.equal(enemy.hp,hp,'aimed line does not home onto an off-axis hero');
  }
});

test('dragon area fire, damage reduction and charge each change combat',()=>{
  const {game,player,enemy}=arena('dragon');enemy.x=player.x+80;
  const hp=enemy.hp;assert(game.cast(player,1,aim(player)));advance(game,.2);assert(enemy.hp<hp);
  assert(game.cast(player,2,aim(player)));const before=player.hp;game.damage(player,100,enemy,'test');assert(before-player.hp<=50,'scales reduce incoming damage');
  const x=player.x;assert(game.cast(player,3,{x:x+230,y:player.y}));advance(game,.5);assert(player.x>x+100,'charge advances toward aim');
});

test('druid heals an area and separately summons treants, bears and wolves',()=>{
  const {game,player,ally}=arena('druid');player.hp=player.maxHp-200;
  const ownHP=player.hp,allyHP=ally.hp;
  assert(game.cast(player,0,aim(player)));advance(game,1.5);assert(player.hp>ownHP);assert(ally.hp>allyHP);
  assert(game.cast(player,1,aim(player)));assert.equal(summons(game,'treant').length,2);
  assert(game.cast(player,2,aim(player)));assert.equal(summons(game,'bear').length,1);
  assert(game.cast(player,3,aim(player)));assert.equal(summons(game,'wolf').length,3);
  const upgraded=arena('druid');assert(upgraded.game.chooseTalent(upgraded.player,8,'druid-twins'));upgraded.game.cast(upgraded.player,2,aim(upgraded.player));assert.equal(summons(upgraded.game,'bear').length,2);
});

test('angel healing, inspiration, group shields and line damage work for allies and enemies',()=>{
  const {game,player,enemy,ally}=arena('angel');player.hp-=150;
  const hp=ally.hp;game.cast(player,0,aim(ally));advance(game,1);assert(ally.hp>hp);
  assert(game.cast(player,1,aim(ally)));assert(ally.status.haste,'inspiration speeds allies');
  assert(game.cast(player,2,aim(ally)));assert(ally.shield>0&&player.shield>0,'sanctuary shields the group');
  const enemyHP=enemy.hp;game.cast(player,3,{x:player.x+500,y:player.y});advance(game,.8);assert(enemy.hp<enemyHP,'lance damages in its aimed path');
});

test('ranger poison, spread arrows, delayed stun and piercing long shot remain distinct',()=>{
  const {game,player,enemy}=arena('ranger');game.cast(player,0,aim(enemy));advance(game,.5);assert(enemy.status.poison,'venom applies poison');
  const before=game.projectiles.length;assert(game.cast(player,1,aim(enemy)));assert.equal(game.projectiles.length-before,5);
  assert(game.cast(player,2,aim(enemy)));advance(game,.3);assert(!enemy.status.stun,'bomb has a visible fuse');advance(game,.4);assert(enemy.status.stun,'bomb stuns after its fuse');
  const second={...enemy,id:99999,x:enemy.x+160,y:player.y,status:{},hp:3000,maxHp:3000,cooldowns:[0,0,0,0],talents:{}};game.units.push(second);
  const hp=enemy.hp,hp2=second.hp;assert(game.cast(player,3,{x:player.x+700,y:player.y}));advance(game,1);assert(enemy.hp<hp);assert(second.hp<hp2,'long shot pierces its first target');
});

test('cleared jungle camps award experience and respawn new creatures after the full timer',()=>{
  const game=gameFor(),hero=game.player,creatures=game.units.filter(u=>u.kind==='camp');assert(creatures.length>=6);
  const campId=creatures[0].campId,group=creatures.filter(u=>u.campId===campId),oldIds=new Set(group.map(u=>u.id));
  assert.equal(group.length,3);hero.x=group[0].x;hero.y=group[0].y;const before=hero.xp;
  for(const target of group)game.damage(target,1e7,hero,'test');
  assert(!game.units.some(u=>u.kind==='camp'&&u.campId===campId&&u.alive));assert(hero.xp>before||hero.level>1);
  for(const u of game.units)if(u.kind==='hero'){u.bot=false;u.x=WORLD.bases[u.team].well.x;u.y=1100;}
  advance(game,RULES.campRespawn-.1);
  assert(!game.units.some(u=>u.kind==='camp'&&u.campId===campId&&u.alive),'camp does not return early');
  advance(game,.2);
  const returned=game.units.filter(u=>u.kind==='camp'&&u.campId===campId&&u.alive);
  assert.equal(returned.length,3,'the whole cleared camp returns');
  assert(returned.every(u=>!oldIds.has(u.id)&&u.hp===u.maxHp),'respawned creatures have new identities and full health');
});

test('river beasts spawn periodically, each team can claim them, and later beasts grow stronger',()=>{
  for(const team of [0,1]){
    const game=gameFor(),killer=game.units.find(u=>u.kind==='hero'&&u.team===team);
    for(const u of game.units)if(u.kind==='hero')u.bot=false;
    advance(game,RULES.firstBeast+.05);
    let neutral=game.units.find(u=>u.kind==='beast'&&u.team===-1&&u.alive);assert(neutral,'periodic beast spawned');
    const first={hp:neutral.maxHp,damage:neutral.damage};
    game.damage(neutral,1e7,killer,'test');assert.equal(neutral.alive,false);
    const captured=game.units.find(u=>u.kind==='beast'&&u.team===team&&u.alive);assert(captured,'killer team receives siege beast');assert(Number.isInteger(captured.lane));
    const firstCapture={hp:captured.maxHp,damage:captured.damage};
    advance(game,RULES.beastInterval+1);
    neutral=game.units.find(u=>u.kind==='beast'&&u.team===-1&&u.alive&&u.maxHp>first.hp);assert(neutral,'later beast is stronger');assert(neutral.damage>first.damage);
    game.damage(neutral,1e7,killer,'test');
    const later=game.units.filter(u=>u.kind==='beast'&&u.team===team&&u.alive).find(u=>u.maxHp>firstCapture.hp);assert(later,'later capture becomes a stronger lane beast');assert(later.damage>firstCapture.damage);
  }
});

test('death respawns at the healing well and recall can complete or be interrupted',()=>{
  const game=gameFor(),hero=game.player,enemy=game.units.find(u=>u.kind==='hero'&&u.team===1);
  game.damage(hero,1e7,enemy,'test');assert.equal(hero.alive,false);assert(hero.respawnIn>0);
  advance(game,hero.respawnIn+.1);assert.equal(hero.alive,true);assert(Math.hypot(hero.x-WORLD.bases[0].well.x,hero.y-WORLD.bases[0].well.y)<100);
  hero.x=900;hero.y=1100;assert(game.recall(hero));advance(game,RULES.recallTime+.1);assert(Math.abs(hero.x-WORLD.bases[0].well.x)<100);
  hero.x=900;hero.y=1100;assert(game.recall(hero));advance(game,.2,{move:{x:1,y:0}});advance(game,RULES.recallTime+.1);assert(hero.x>700,'moving cancels recall');
  assert(game.recall(hero));game.damage(hero,1,enemy,'test');advance(game,RULES.recallTime+.1);assert(hero.x>700,'damage cancels recall');
});

test('obstacles block heroes and wading through the river creates splashes',()=>{
  const game=gameFor(),hero=game.player,obstacle=WORLD.obstacles[0];
  hero.x=obstacle.x-obstacle.r-hero.r-2;hero.y=obstacle.y;advance(game,.3,{move:{x:1,y:0}});
  assert(Math.hypot(hero.x-obstacle.x,hero.y-obstacle.y)>=obstacle.r+hero.r-.5,'hero cannot walk through tree trunks');
  hero.x=1590;hero.y=1100;advance(game,.4,{move:{x:0,y:1}});assert(game.effects.some(e=>e.type==='splash'));
});

test('difficulty improves decision speed and accuracy without inflated hero stats',()=>{
  const easy=gameFor('arc','easy'),hard=gameFor('arc','ultimate');
  assert.equal(easy.player.maxHp,hard.player.maxHp);
  assert(DIFFICULTIES.ultimate.think<DIFFICULTIES.easy.think);assert(DIFFICULTIES.ultimate.aimError<DIFFICULTIES.easy.aimError);
  assert.equal(DIFFICULTIES.ultimate.dodge,true);
  const bot=hard.units.find(u=>u.kind==='hero'&&u.team===1);bot.x=2300;bot.y=1100;bot.hp=bot.maxHp*.08;
  const distance=Math.hypot(bot.x-WORLD.bases[1].well.x,bot.y-WORLD.bases[1].well.y);advance(hard,RULES.recallTime+2);
  assert(Math.hypot(bot.x-WORLD.bases[1].well.x,bot.y-WORLD.bases[1].well.y)<distance,'wounded bot retreats to safety');
});

test('healthy allied bots coordinate a requested lane push and automatically defend a threatened tower',()=>{
  const pushing=gameFor('arc','hard');pushing.time=120;
  assert.equal(pushing.command('push',0),true);advance(pushing,.6);
  assert.equal(pushing.teamPlans[0].type,'push');assert.equal(pushing.teamPlans[0].lane,0);
  const allies=pushing.units.filter(u=>u.kind==='hero'&&u.team===0&&u.bot);
  assert.equal(allies.length,2);assert(allies.every(u=>u.lane===0&&u.brain.action==='push'),'both healthy bots join the called lane');
  const defense=gameFor('arc','hard'),tower=defense.units.find(u=>u.kind==='tower'&&u.team===0&&u.lane===2&&u.tier===0);
  const raider=defense.units.find(u=>u.kind==='hero'&&u.team===1);Object.assign(raider,{x:tower.x+65,y:tower.y,bot:false});
  defense.rebuildGrid();defense.planTeams();advance(defense,.6);
  assert.equal(defense.teamPlans[0].type,'defend');assert.equal(defense.teamPlans[0].lane,2);
  assert(defense.units.filter(u=>u.kind==='hero'&&u.team===0&&u.bot).every(u=>u.lane===2&&u.brain.action==='defend'),'allies rotate to the threatened lane');
});

test('paused games do not advance; seeded matches resolve naturally with bounded state',()=>{
  const game=gameFor('arc','normal');game.paused=true;advance(game,1);assert.equal(game.time,0);game.paused=false;
  let maxUnits=game.units.length;
  for(let ticks=0;ticks<18*60*20&&game.time<18*60&&game.winner==null;ticks++){game.update(.05);maxUnits=Math.max(maxUnits,game.units.length);}
  assert.notEqual(game.winner,null,'bots and minions must finish a match without human action');
  assert(maxUnits<650,'state does not accumulate dead units indefinitely');
  assert(game.units.every(u=>Number.isFinite(u.x)&&Number.isFinite(u.y)&&Number.isFinite(u.hp)),'no invalid coordinates or health');
});

test('published catalogs keep the same entry route and describe the new game',()=>{
  const source=fs.readFileSync(new URL('../lobby/lobby-core.html',import.meta.url),'utf8');
  const row=source.split('\n').find(s=>s.includes('key:"mobamania"'));assert(row);assert(row.includes('/game/CatLanes.html'));assert(row.includes('3v3'));assert(!row.includes('Incomplete'));
  const art=JSON.parse(fs.readFileSync(new URL('../lobby/assets/game-artwork.json',import.meta.url),'utf8')).games.mobamania;
  assert.equal(art.title,'MOBA Mania');assert(art.desc.includes('five heroes'));assert.equal(art.route,'/game/CatLanes.html');
});

test('offline pack builder copies the complete runtime and resolves every local dependency',()=>{
  const repo=fileURLToPath(new URL('../',import.meta.url)),temp=fs.mkdtempSync(path.join(os.tmpdir(),'moba-pack-'));
  try{
    for(const name of ['build-packs.py','catalog.json','pack-index.json'])fs.copyFileSync(path.join(repo,'desktop-hub/content',name),path.join(temp,name));
    execFileSync(process.env.PYTHON||'python3',[path.join(temp,'build-packs.py'),repo,repo,'--only','mobamania'],{stdio:'pipe'});
    const pack=path.join(temp,'packs/mobamania'),entry=path.join(pack,'index.html'),html=fs.readFileSync(entry,'utf8');
    assert(!html.includes('/hub/play-signal.mjs'),'web-only tracking stripped');assert(!html.includes('\r'),'pack text uses normalized newlines');
    const queue=[entry],seen=new Set();
    while(queue.length){
      const file=queue.shift();if(seen.has(file))continue;seen.add(file);
      const source=fs.readFileSync(file,'utf8');
      const refs=file.endsWith('.html')?[...source.matchAll(/(?:src|href)=["']([^"']+)["']/g)].map(m=>m[1]):[...source.matchAll(/(?:from\s+|import\s*)["'](\.[^"']+)["']/g)].map(m=>m[1]);
      for(const ref of refs){
        if(ref.startsWith('#')||ref.startsWith('data:'))continue;
        assert(!/^https?:/.test(ref),'runtime has no CDN dependency');
        const dependency=path.resolve(path.dirname(file),ref.split('?')[0]);
        assert(dependency.startsWith(pack+path.sep),'dependency remains inside the offline pack');assert(fs.existsSync(dependency),ref+' is packaged');
        if(/\.(?:mjs|js|html)$/.test(dependency))queue.push(dependency);
      }
    }
    for(const name of ['data.mjs','engine.mjs','renderer.mjs','ui.mjs'])assert(seen.has(path.join(pack,'moba-mania',name)),name+' reachable from entry');
    assert(!fs.existsSync(path.join(pack,'moba-mania/CONTRACT.md')),'coordination notes not shipped');
  }finally{fs.rmSync(temp,{recursive:true,force:true});}
});
