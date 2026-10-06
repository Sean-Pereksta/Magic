import test from 'node:test';
import assert from 'node:assert/strict';
import {runtime} from './runtime-test-helper.mjs';

function combat(touch=false,difficulty=4){
  const h=runtime(touch);
  h.run('startNewGame();');
  h.run("game.enemies=[];game.effects=[];game.projectiles=[];game.hazards=[];game.telegraphs=[];game.summons=[];game.roomData={key:'ability-test',biome:'ruins',difficulty:"+difficulty+",town:false,cleared:false};intensityState().encounter=null;intensityState().hazards=[];game.player.x=9;game.player.y=7;game.player.hp=game.player.maxHp=10000;game.player.armor=0;game.player.invuln=0;game.player.attackTimer=10000;playerMovement(0);updateEnemyEffects(1.1);");
  return h;
}
const advance=(h,seconds)=>{for(let t=0;t<seconds;t+=.016)h.run('update(.016)');};
const enemy=(h,type='charger',x=4,y=7)=>h.run("var subject=spawnEnemy('"+type+"',{x:"+x+",y:"+y+"});subject.x="+x+";subject.y="+y+";subject.hp=subject.maxHp=10000;subject.damage=10;subject.attack=100;subject.trait='';subject.intensityTrait='';subject.combatModifier='';");
const cast=(h,id)=>h.run("var attack=AWEnemyCombat.start(subject,'"+id+"');");
const value=(h,source)=>JSON.parse(h.run('JSON.stringify('+source+')'));

for(const touch of [false,true])test((touch?'touch':'desktop')+' all signatures warn before damage and render finite geometry',()=>{
  const h=combat(touch);try{
    const ids=value(h,'Object.keys(AWEnemyCombat.definitions).filter(k=>k!=="pool")');
    for(const id of ids){
      h.run('game.enemies=[];game.effects=[];game.projectiles=[];game.summons=[];updateEnemyEffects(1);');
      enemy(h,'charger',7.8,7);cast(h,id);
      assert.ok(h.run('!!attack'),id);
      assert.ok(h.run('attack.wind>=.55'),id);
      const hp=h.run('game.player.hp');
      h.run('attack.age=attack.wind-.08;drawTelegraphs();drawEnemyTelegraph(subject);drawEffect(attack,true);updateEnemyEffects(.016);');
      assert.equal(h.run('game.player.hp'),hp,id+' has no windup damage');
      h.run('attack.age=attack.wind+.01;drawEffect(attack,true);');
    }
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});

test('windups freeze targets and prediction follows observed world movement',()=>{
  const h=combat();try{
    enemy(h,'archer');
    h.run('AWInput.move.x=1;AWInput.move.y=0;');
    for(let i=0;i<65;i++)h.run('playerMovement(.016)');
    assert.ok(h.run('AWEnemyCombat.movement.steady>.6'));
    cast(h,'predictive');
    const dir=value(h,'attack.dir'),target=value(h,'attack.target'),hero=value(h,'({x:game.player.x,y:game.player.y})');
    assert.ok(Math.hypot(target.x-hero.x,target.y-hero.y)>.2);
    assert.ok(Math.hypot(target.x-hero.x,target.y-hero.y)<=1.36);
    h.run('game.player.x=3;game.player.y=2;updateEnemyEffects(.016)');
    assert.deepEqual(value(h,'attack.dir'),dir);
    assert.deepEqual(value(h,'attack.target'),target);
    h.run('AWInput.move.x=-1;for(let i=0;i<5;i++)playerMovement(.016);');
    assert.ok(h.run('AWEnemyCombat.movement.steady<.2'));
  }finally{h.close();}
});

test('line warnings end on the correct ray at walls; cone and lane safe sides stay safe',()=>{
  const h=combat();try{
    enemy(h,'charger',16,11);cast(h,'charge');
    assert.ok(h.run('Math.abs((attack.marks[0].to.x-attack.x)*attack.dir.y-(attack.marks[0].to.y-attack.y)*attack.dir.x)<1e-8'));
    assert.equal(h.run('AWEnemyCombat.inShape(attack.marks[0],{x:16,y:4,r:.3})'),false);
    h.run('game.effects=[];subject.combatAbility=null;updateEnemyEffects(1);subject.x=7;subject.y=7;');cast(h,'cleave');
    assert.equal(h.run('AWEnemyCombat.inShape(attack.marks[0],{x:8,y:7,r:.3})'),true);
    assert.equal(h.run('AWEnemyCombat.inShape(attack.marks[0],{x:6,y:7,r:.3})'),false);
  }finally{h.close();}
});

test('sidestepping a frozen meteor mark avoids it and barrage marks land sequentially',()=>{
  const h=combat();try{
    enemy(h,'bomber');cast(h,'meteor');
    assert.deepEqual(value(h,'attack.marks.map(s=>s.delay)'),[0,.3,.6]);
    h.run('game.player.x=2;game.player.y=2;');
    const hp=h.run('game.player.hp');advance(h,2.8);
    assert.equal(h.run('game.player.hp'),hp);
    assert.equal(h.run('subject.combatAbility'),null);
    assert.ok(h.run('subject.attack>0'));
  }finally{h.close();}
});

for(const id of ['leapSlam','vaultSlash'])test(id+' travels visibly, damages only on landing, and exposes a recovery window',()=>{
  const h=combat();try{
    enemy(h,'wolf');cast(h,id);
    const landing=value(h,'attack.jumpEnd');
    h.run('attack.age=attack.wind;updateEnemyEffects(.22);drawEnemy(subject);drawTelegraphs();');
    assert.ok(h.run('subject.combatJumpHeight>40'));
    assert.equal(h.run('game.player.hp'),10000,'no airborne contact damage');
    h.run('updateEnemyEffects(.23);');
    assert.equal(h.run('subject.combatJumpHeight'),0);
    assert.ok(h.run('game.player.hp<10000'));
    assert.ok(Math.abs(h.run('subject.x')-landing.x)<1e-8);
    assert.ok(Math.abs(h.run('subject.y')-landing.y)<1e-8);
    const hp=h.run('game.player.hp');
    h.run('attack.age=attack.wind+attack.active+.01;updateEnemyEffects(.016);');
    assert.equal(h.run('subject.state'),'abilityRecovery');
    assert.equal(h.run('game.player.hp'),hp,'one landing hit');
    h.run('updateEnemyEffects(1);');
    assert.equal(h.run('subject.combatAbility'),null);
    assert.ok(h.run('subject.attack>0'));
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});

test('leap landing stays locked when the hero changes direction, and cancelling drops its height',()=>{
  for(const stop of ['miss','stun','room']){
    const h=combat();try{
      enemy(h,'wolf');cast(h,'leapSlam');
      const landing=value(h,'attack.jumpEnd');
      h.run('game.player.x=2;game.player.y=2;attack.age=attack.wind;updateEnemyEffects(.22);');
      if(stop==='stun')h.run('subject.stun=1;');
      if(stop==='room')h.run("game.roomData={key:'next',town:true,difficulty:1};");
      h.run('updateEnemyEffects(.23);');
      assert.equal(h.run('game.player.hp'),10000,stop);
      assert.equal(h.run('subject.combatJumpHeight'),0,stop);
      if(stop==='miss')assert.deepEqual(value(h,'({x:subject.x,y:subject.y})'),landing);
      else assert.equal(h.run('subject.combatAbility'),null);
    }finally{h.close();}
  }
});

test('a dodge through a leap landing earns the major reward once',()=>{
  const h=combat();try{
    enemy(h,'wolf');cast(h,'leapSlam');
    h.run('attack.age=attack.wind+.40;game.player.dodgeCd=0;intensityState().momentum=0;dodge();updateEnemyEffects(.05);');
    assert.equal(h.run('game.player.hp'),10000);
    assert.equal(h.run('intensityState().momentum'),18);
    h.run('updateEnemyEffects(.04);');
    assert.equal(h.run('intensityState().momentum'),18);
  }finally{h.close();}
});

test('crosscut dash advertises two locked lanes and pauses before the second strike',()=>{
  const h=combat();try{
    enemy(h,'bladeDancer');cast(h,'doubleDash');
    const lanes=value(h,'attack.marks');
    assert.equal(lanes.length,2);assert.equal(lanes[1].delay,.8);
    h.run('game.player.x=2;game.player.y=2;attack.age=attack.wind;updateEnemyEffects(.29);');
    assert.deepEqual(value(h,'({x:subject.x,y:subject.y})'),lanes[0].to);
    h.run('updateEnemyEffects(.40);drawTelegraphs();');
    assert.deepEqual(value(h,'({x:subject.x,y:subject.y})'),lanes[0].to,'dash pause');
    assert.deepEqual(value(h,'attack.marks'),lanes,'no second-leg tracking');
    h.run('updateEnemyEffects(.13);');
    assert.ok(h.run('Math.hypot(subject.x-attack.dashes[0].to.x,subject.y-attack.dashes[0].to.y)>.1'));
    h.run('updateEnemyEffects(.28);');
    assert.deepEqual(value(h,'({x:subject.x,y:subject.y})'),lanes[1].to);
    assert.equal(h.run('game.player.hp'),10000);
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});

test('vault volley fires only after landing and each projectile follows its advertised lane',()=>{
  const h=combat();try{
    enemy(h,'archer',8,7);cast(h,'leapVolley');
    const lanes=value(h,'AWEnemyCombat.shapes(attack)');
    h.run('attack.age=attack.wind;updateEnemyEffects(.22);');
    assert.equal(h.run('game.projectiles.length'),0);
    assert.ok(h.run('subject.combatJumpHeight>40'));
    h.run('game.player.x=2;game.player.y=2;updateEnemyEffects(.23);');
    assert.equal(h.run('game.projectiles.length'),3);
    const shots=value(h,'game.projectiles.map(q=>({x:q.x,y:q.y,vx:q.vx,vy:q.vy}))');
    for(let i=0;i<shots.length;i++){
      assert.equal(shots[i].x,lanes[i].x);assert.equal(shots[i].y,lanes[i].y);
      assert.ok(Math.abs((lanes[i].to.x-shots[i].x)*shots[i].vy-(lanes[i].to.y-shots[i].y)*shots[i].vx)<1e-8);
    }
    assert.equal(h.run('subject.combatJumpHeight'),0);
  }finally{h.close();}
});

test('crowd separation cannot push a charging enemy off its warned lane',()=>{
  const h=combat();try{
    enemy(h);cast(h,'charge');
    h.run("var crowd=spawnEnemy('bandit',{x:4.2,y:7});crowd.x=4.2;crowd.y=7;separateEnemies();");
    assert.equal(h.run('subject.x'),4);assert.equal(h.run('subject.y'),7);
  }finally{h.close();}
});

test('a late dodge through a major cleave rewards once; an early dodge does not',()=>{
  for(const late of [false,true]){
    const h=combat();try{
      enemy(h,'skeleton',7.8,7);cast(h,'cleave');
      h.run('game.player.dodgeCd=0;intensityState().momentum=0;game.player.spellState.firebolt={cd:4};');
      if(late)h.run('attack.age=attack.wind-.02;');
      h.run('dodge();');
      assert.equal(h.run('intensityState().momentum'),0,'starting a dodge cannot farm a warning');
      if(late){
        h.run('updateEnemyEffects(.04);');
        assert.equal(h.run('intensityState().momentum'),18);
        assert.ok(h.run('game.player.spellState.firebolt.cd<4'));
        assert.ok(h.run('game.player.tailwind>0'));
        h.run('updateEnemyEffects(.02);');
        assert.equal(h.run('intensityState().momentum'),18);
      }else{
        h.run('game.player.dodgeTime=0;game.player.invuln=0;attack.age=attack.wind-.02;updateEnemyEffects(.04);');
        assert.equal(h.run('intensityState().momentum'),0);
        assert.ok(h.run('game.player.hp<10000'));
      }
    }finally{h.close();}
  }
});

test('charge misses and collides with a wall, opening a stagger window',()=>{
  const h=combat();try{
    enemy(h,'charger',16,7);h.run('game.player.x=17;');cast(h,'charge');
    h.run('game.player.x=3;game.player.y=3;');advance(h,1.05);
    assert.ok(h.run('subject.stun>0'));
    assert.equal(h.run('game.player.hp'),10000);
    assert.equal(h.run('subject.combatAbility'),null);
  }finally{h.close();}
});

test('thorn cage always leaves a walkable opening, including room corners',()=>{
  const h=combat();try{
    enemy(h,'briarWitch');
    for(const [x,y] of [[9,7],[.7,.7],[17.3,.7],[.7,13.3],[17.3,13.3]]){
      h.run('game.effects=[];subject.combatAbility=null;game.player.x='+x+';game.player.y='+y+';updateEnemyEffects(1);');cast(h,'cage');
      assert.ok(h.run('!!attack'));
      assert.equal(h.run('Array.from({length:20},(_,i)=>{const p={x:attack.target.x+attack.gap.x*(i/19*2.9),y:attack.target.y+attack.gap.y*(i/19*2.9),r:.34};return p.x>=.28&&p.x<=ROOM_W-.28&&p.y>=.28&&p.y<=ROOM_H-.28&&!attack.marks.some(s=>AWEnemyCombat.inShape(s,p));}).every(Boolean)'),true);
    }
  }finally{h.close();}
});

test('pincer synchronizes two enemies on opposing sides toward a locked target',()=>{
  const h=combat();try{
    enemy(h,'packAlpha',5,7);
    h.run("var partner=spawnEnemy('thornrunner',{x:13,y:7});partner.attack=0;partner.stun=0;");cast(h,'pincer');
    assert.equal(h.run('AWEnemyCombat.actions().length'),2);
    assert.equal(h.run('attack.ability'),'pincer');
    assert.equal(h.run('partner.combatAbility.partner'),h.run('attack.id'));
    assert.deepEqual(value(h,'attack.target'),value(h,'partner.combatAbility.target'));
    assert.ok(h.run('attack.dir.x*partner.combatAbility.dir.x<0'));
  }finally{h.close();}
});

test('beam rotates gradually and its live damage lane follows the drawn geometry',()=>{
  const h=combat();try{
    enemy(h,'cultist');cast(h,'beam');
    const initial=value(h,'AWEnemyCombat.shapes(attack)[0]');
    h.run('attack.age=attack.wind+1;');
    const rotated=value(h,'AWEnemyCombat.shapes(attack)[0]');
    assert.notDeepEqual(initial.to,rotated.to);
    assert.equal(h.run('AWEnemyCombat.inShape(AWEnemyCombat.shapes(attack)[0],{x:attack.x,y:attack.y-3,r:.3})'),false);
    h.run('drawEffect(attack,true);');assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});

test('gravity movement remains escapable and frost trails slow without disabling dodge',()=>{
  const h=combat();try{
    enemy(h,'voideye');cast(h,'gravity');
    h.run('attack.age=attack.wind+.1;game.player.x=attack.target.x+2;game.player.y=attack.target.y;AWInput.move.x=1;AWInput.move.y=1;');
    const before=h.run('Math.hypot(game.player.x-attack.target.x,game.player.y-attack.target.y)');
    for(let i=0;i<10;i++)h.run('playerMovement(.016);updateEnemyEffects(.016);');
    assert.ok(h.run('Math.hypot(game.player.x-attack.target.x,game.player.y-attack.target.y)')>before);
    h.run('game.player.combatChill=1;game.player.dodgeCd=0;dodge();');
    assert.ok(h.run('game.player.dodgeTime>0'));
  }finally{h.close();}
});

test('kill/stun cancels windup and room changes remove attack records',()=>{
  for(const stop of ['kill','stun','room']){
    const h=combat();try{
      enemy(h);cast(h,'burrow');
      if(stop==='kill')h.run('subject.dead=true;');
      if(stop==='stun')h.run('subject.stun=1;');
      if(stop==='room')h.run("game.roomData={key:'next-room',town:true,difficulty:1};");
      h.run('updateEnemyEffects(.1);');
      assert.equal(h.run('AWEnemyCombat.actions().length'),0,stop);
      assert.equal(h.run('subject.hidden'),false,stop);
      assert.equal(h.run('game.player.hp'),10000);
    }finally{h.close();}
  }
});

test('cosmetic budgets never hide warnings, and attack admission bounds gameplay effects',()=>{
  const h=combat(true,9);try{
    h.run("AWPresentation.settings.particles='off';for(let i=0;i<100;i++)game.effects.push({kind:'castRing',x:4,y:4,life:10,maxLife:10,color:'#fff'});");
    enemy(h);cast(h,'meteor');
    assert.ok(h.run('!!attack'));const before=h.draws();h.run('drawTelegraphs();');assert.ok(h.draws()>before);
    for(let i=0;i<20;i++){
      h.run('updateEnemyEffects(.31);');
      enemy(h);cast(h,'beam');
      assert.ok(h.run('AWEnemyCombat.actions().length<=40'));
      assert.ok(h.run('AWEnemyCombat.actions().filter(a=>a.ability!=="pool"&&a.age<a.wind+a.active).length<=4'));
    }
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});

for(const trait of ['Echoing','Volatile','Blinking','Twin Cast','Stormbound','Frozen','Bulwark','Vengeful'])test(trait+' modifies abilities without multiplying health',()=>{
  const h=combat();try{
    enemy(h,'mage');h.run("subject.elite=true;subject.combatModifier='"+trait+"';");
    const hp=h.run('subject.maxHp');cast(h,trait==='Twin Cast'?'fan':'meteor');if(trait==='Twin Cast')h.run('game.player.x=2;game.player.y=2;');advance(h,trait==='Twin Cast'?1.3:2.1);
    assert.equal(h.run('subject.maxHp'),hp);
    assert.ok(h.run('AWEnemyCombat.actions().length<=40'));
    if(trait==='Twin Cast')assert.ok(h.run('game.projectiles.filter(p=>p.combatAbilityId).length>=10'));
    if(trait==='Volatile'||trait==='Frozen')assert.ok(h.run('AWEnemyCombat.actions().some(a=>a.ability==="pool")'));
    if(trait==='Echoing')assert.ok(h.run('AWEnemyCombat.actions().some(a=>a.detached&&a.ability==="meteor")'));
    if(trait==='Blinking')assert.ok(h.run('Math.abs(subject.x-4)+Math.abs(subject.y-7)>1'));
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});

test('echoing jump attacks repeat ground marks without moving the enemy a second time',()=>{
  const h=combat();try{
    enemy(h,'wolf');h.run("subject.combatModifier='Echoing';");cast(h,'leapSlam');
    h.run('game.player.x=2;game.player.y=2;');advance(h,1.7);
    const landing=value(h,'({x:subject.x,y:subject.y})');
    assert.ok(h.run('AWEnemyCombat.actions().some(a=>a.detached&&a.ability==="rupture")'));
    h.run('updateEnemyEffects(.8);');
    assert.deepEqual(value(h,'({x:subject.x,y:subject.y})'),landing);
    assert.equal(h.run('subject.combatJumpHeight'),0);
  }finally{h.close();}
});

test('secondary elite attacks share the room admission limit',()=>{
  const h=combat();try{
    for(const trait of ['Stormbound','Echoing','Echoing']){
      h.run('updateEnemyEffects(.31);');enemy(h,'mage');h.run("subject.combatModifier='"+trait+"';");cast(h,'meteor');
    }
    h.run('game.player.x=2;game.player.y=2;');
    for(let i=0;i<200;i++){
      h.run('update(.016);');
      assert.ok(h.run('AWEnemyCombat.actions().filter(a=>a.ability!=="pool"&&a.age<a.wind+a.active).length<=3'));
    }
  }finally{h.close();}
});

test('Shadow traits preserve regeneration, vampiric healing, and a warned summoning ritual',()=>{
  const h=combat();try{
    enemy(h,'skeleton',7.8,7);
    h.run("subject.shadow=true;subject.shadowTraits=['Regenerating','Vampiric','Summoner'];subject.hp=9000;updateEnemyAI(subject,enemyDir(subject),dist(subject,game.player),subject.speed,.1);");
    assert.ok(h.run('subject.hp>9000'));
    cast(h,'cleave');h.run('attack.age=attack.wind-.01;updateEnemyEffects(.02);');
    assert.ok(h.run('subject.hp>9007'));
    h.run('game.effects=[];subject.combatAbility=null;subject.state="idle";subject.attack=0;subject.combatSequence=2;updateEnemyEffects(1);updateEnemyAI(subject,enemyDir(subject),dist(subject,game.player),0,.016);');
    assert.equal(h.run('subject.combatAbility.ability'),'summon');
    h.run('subject.combatAbility.age=subject.combatAbility.wind-.01;updateEnemyEffects(.02);');
    assert.equal(h.run('game.enemies.filter(e=>e.shadowMinion).length'),2);
    assert.deepEqual(h.errors,[]);
  }finally{h.close();}
});

test('boss phases introduce distinct attack sequences without extra legacy hazard stacking',()=>{
  const h=combat();try{
    h.run("var subject=spawnBoss(BOSSES.find(b=>b.ai==='bossDrake'),game.roomData);subject.combatManaged=true;");
    const one=value(h,'AWEnemyCombat.profile(subject)');
    h.run('subject.intensityPhase=2;');const two=value(h,'AWEnemyCombat.profile(subject)');
    h.run('subject.intensityPhase=3;subject.intensityBossCd=0;intensityTickBosses(.1);');
    const three=value(h,'AWEnemyCombat.profile(subject)');
    assert.notDeepEqual(one,two);assert.notDeepEqual(two,three);
    assert.ok(two.includes('beam'));assert.ok(three.includes('meteor'));
    assert.equal(h.run('intensityState().hazards.length'),0);
  }finally{h.close();}
});

test('all regional families and 107 catalog enemies retain valid signatures/support behavior',()=>{
  const h=combat();try{
    assert.ok(h.run('Object.keys(ENEMY_TYPES).length>=107'));
    assert.equal(h.run("Object.keys(ENEMY_TYPES).every(type=>{const e={type,ai:ENEMY_TYPES[type].ai,id:1,damage:ENEMY_TYPES[type].damage};const p=AWEnemyCombat.profile(e);return !p||p.every(id=>AWEnemyCombat.definitions[id]);})"),true);
    assert.deepEqual(value(h,"['frostwitch','bomber','stormcaller','briarWitch','voidShepherd','sunwarden','marrowboar'].map(type=>AWEnemyCombat.family({type,ai:ENEMY_TYPES[type].ai}))"),['frost','fire','storm','nature','void','celestial','bloodroot']);
  }finally{h.close();}
});
