'use strict';
/* Affix behavior, pursuit and coordinated squads share the existing attack
 * admission limits, warnings, damage resolver and simulation clock. */
(() => {
  const C=AWDangerContent,B=AWEnemyCombat,F=AWCombatAffinity;
  let room=null,spawnIndex=0,corpses=[],lastSchool='fire',bossObjects=[];
  const live=()=>running&&!paused&&!modalPause&&!roomTransition&&!!game.player;
  const has=(e,key)=>e.dangerAffixes?.includes(key);
  function reset(){room=game.roomData;spawnIndex=0;corpses=[];bossObjects=[];lastSchool='fire';}
  function ensure(){if(room!==game.roomData)reset();}
  function affix(e,requested){
    const names=Object.keys(C.affixes),seed=C.hash(game.seed,game.roomData?.key+'/'+spawnIndex++);
    e.dangerAffixes=(requested||C.shuffled(names,seed).slice(0,game.roomData?.difficulty>=10&&seed%4===0?2:1)).filter(k=>C.affixes[k]);
    e.intensityTrait='';e.trait='';e.combatModifier='';e.dangerProfile=null;e._combatProfileKey=null;delete e._combatTags;
    e.name=e.dangerAffixes.join(' / ')+' '+(ENEMY_TYPES[e.type]?.name||e.name);
    e.intensityColor=C.affixes[e.dangerAffixes[0]]?.color||e.color;
    e.dangerCd=2.4;e.dangerTrailCd=.8;
    if(has(e,'Blazing'))F.tags(e).add('FIRE');
    if(has(e,'Frostborn'))F.tags(e).add('ICE');
    if(has(e,'Stormbound'))F.tags(e).add('LIGHTNING');
    if(has(e,'Juggernaut'))F.tags(e).add('ARMORED');
    const first=B.profile(e)||['cleave'];
    if(has(e,'Predator')||has(e,'Teleporter')||has(e,'Mirror')||has(e,'Stormbound')){
      const abilities=has(e,'Predator')?['hunterDash','pounceWave','chainDash']:[...first];
      if(has(e,'Teleporter'))abilities.unshift('shadowAmbush');
      if(has(e,'Mirror'))abilities.push('fan');
      if(has(e,'Stormbound'))abilities.push('hunterDash','chain');
      e.dangerProfile=[...new Set(abilities)];
    }
    e._combatProfileKey=null;return e;
  }
  const oldSpawn=spawnEnemy;
  spawnEnemy=function(id,pos,elite=false,scale=1){
    ensure();const e=oldSpawn(id,pos,elite,scale);
    if(elite&&scale===1&&!game.roomData?.town&&!e.shadow)affix(e);
    return e;
  };
  function pool(e,at,r,element,active=2.4){
    return B.hazard(e,[{shape:'circle',x:at.x,y:at.y,r,delay:0}],{element,color:F.colors[element]||e.color,active,damage:e.damage*.22});
  }
  const oldAI=updateEnemyAI;
  updateEnemyAI=function(e,d,range,speed,dt){
    ensure();
    if(!e.boss&&(game.roomData?.difficulty||1)>=4&&!e.combatAbility&&e.state==='idle'){
      e.dangerEvadeCd=Math.max(0,(e.dangerEvadeCd||0)-dt);
      if(e.dangerEvade>0){e.dangerEvade-=dt;moveEnemy(e,e.dangerEvadeDir,speed*1.35,dt);return;}
      if(!e.dangerEvadeCd){
        const shot=game.projectiles.find(q=>q.owner==='player'&&q.life>0&&dist(q,e)<2.2&&(e.x-q.x)*q.vx+(e.y-q.y)*q.vy>0);
        if(shot){const side=e.id%2?1:-1;e.dangerEvadeDir=norm(-shot.vy*side,shot.vx*side);e.dangerEvade=.2;e.dangerEvadeCd=3.6;fx('dashEcho',e.x,e.y,.25,e.color,{dir:e.dangerEvadeDir});}
      }
    }
    if(e.dangerProfile&&!e.dangerAffixes?.length)e._combatProfileKey=null;
    const commander=game.enemies.find(o=>o!==e&&!o.dead&&has(o,'Commander')&&dist(o,e)<5);
    if(commander&&!e.boss){
      e.dangerCommander=commander;
      if(!e.combatAbility&&e.state==='idle'){
        const ranged=/ranged|archer|caster|shaman|mage|oracle|sniper|Artillery/i.test(e.ai);
        const screen=game.enemies.find(o=>o!==e&&!o.dead&&/ranged|archer|mage|shaman|Artillery/i.test(o.ai));
        let goal=null;
        if(e.hp<e.maxHp*.3&&range<4)goal={x:e.x-d.x*2,y:e.y-d.y*2};
        else if(!ranged&&screen&&range>2)goal={x:screen.x+(game.player.x-screen.x)*.4,y:screen.y+(game.player.y-screen.y)*.4};
        else if(!ranged&&range>3)goal={x:game.player.x-d.y*(e.id%2?2:-2),y:game.player.y+d.x*(e.id%2?2:-2)};
        else if(ranged&&range<4)goal={x:e.x-d.x*2,y:e.y-d.y*2};
        if(goal){moveEnemy(e,norm(goal.x-e.x,goal.y-e.y),speed,dt);speed=0;}
      }
    }else e.dangerCommander=null;
    if(has(e,'Berserker'))speed*=e.hp<=e.maxHp*.25?1.55:e.hp<e.maxHp*.5?1.2:1;
    if(has(e,'Predator')||C.pursuits[e.type])speed*=1.08;
    if(e.dangerRitual){e.facing=enemyDir(e);return;}
    return oldAI(e,d,range,speed,dt);
  };
  const oldKill=killEnemy;
  killEnemy=function(e,tag){
    if(e&&!e.dead&&!e.boss&&!e.dangerResurrected&&!/regional_(plant|wall|echo)/.test(e.ai)&&e.damage>0){
      corpses.push({type:e.type,x:e.x,y:e.y,life:10,used:false});if(corpses.length>12)corpses.shift();
    }return oldKill(e,tag);
  };
  const oldDamage=damageEnemy;
  damageEnemy=function(e,amount,tag='',dot=false,source=null){
    if(!e||e.dead)return;
    const element=source?.combatElement||F.element(tag),origin=source?.damageOrigin||source||game.player;
    if(e.dangerPuzzleGuard&&!window.AWChallengeRooms?.guardExposed())return;
    if(has(e,'Juggernaut')&&!(e.dangerArmorBroken>0)&&!dot&&origin){
      const toward=norm(origin.x-e.x,origin.y-e.y),f=e.facing||enemyDir(e);
      if(toward.x*f.x+toward.y*f.y>.25&&!['earth','lightning','shadow'].includes(element)){
        amount*=.22;fx('shieldHit',e.x,e.y,.2,'#d9d0b9',{r:e.r+ .2});
      }
    }
    if(e.dangerArenaArmor&&!(e.dangerArmorBroken>0))amount*=.55;
    if(has(e,'Blazing')&&element==='frost'){e.dangerTrailCd=3;B.extinguish(e,2);}
    if(has(e,'Frostborn')&&element==='fire')e.dangerTrailCd=3;
    if(e.dangerRitual&&!dot&&['wind','earth','lightning'].includes(element)){e.stun=Math.max(e.stun,.5);e.dangerRitual.cancelled=true;}
    return oldDamage(e,amount,tag,dot,source);
  };
  const oldHurt=damagePlayer;
  damagePlayer=function(amount,...args){
    const p=game.player,source=window.AWRegionalContent?.incoming||game.projectiles.find(q=>q.owner==='enemy'&&q.life>0&&dist(q,p)<q.r+p.r+.2)?.regionalSource;
    const before=p?.hp;oldHurt(amount,...args);
    if(p&&game.roomData===room&&before>p.hp&&source&&!source.dead&&has(source,'Vampiric')){
      const healed=Math.min(source.maxHp-source.hp,(before-p.hp)*.65);
      source.hp+=healed;if(healed>0)fx('soulLink',p.x,p.y,.35,'#f18eac',{toX:source.x,toY:source.y});
    }
  };
  const oldCast=castSpell;
  castSpell=function(slot){
    const p=game.player,id=p?.activeSpells[slot],before=p?.spellState[id]?.cd||0,result=oldCast(slot);
    if(id&&(p.spellState[id]?.cd||0)>before&&SPELLS[id].damage>0){
      lastSchool=F.spellElement(id);
      if(lastSchool==='wind')for(const q of game.projectiles)if(q.owner==='enemy'&&dist(q,p)<5){q.vx*=.5;q.vy*=.5;q.damage*=.7;}
    }return result;
  };
  function ritual(e,dt){
    const job=e.dangerRitual;if(!job)return;
    job.age+=dt;
    if(e.dead||e.stun>0||job.cancelled||job.corpse.used||job.corpse.life<=0){e.dangerRitual=null;e.attack=2;e.dangerCd=4;return;}
    if(job.age>=1.4){
      job.corpse.used=true;const minion=spawnEnemy(job.corpse.type,job.corpse,false,.65);
      minion.dangerResurrected=true;minion.xp=0;minion.name='Risen '+minion.name;
      e.dangerRitual=null;e.attack=2;e.dangerCd=5;fx('summon',minion.x,minion.y,.5,'#cdb5f4',{r:1});
    }
  }
  function eliteTick(e,dt){
    e.dangerArmorBroken=Math.max(0,(e.dangerArmorBroken||0)-dt);e.wet=Math.max(0,(e.wet||0)-dt);
    if(!e.dangerAffixes?.length)return;
    e.dangerCd-=dt;e.dangerTrailCd-=dt;ritual(e,dt);
    if(has(e,'Berserker')&&e.hp<e.maxHp*.25&&!e.combatAbility)e.attack-=dt*.35;
    if(e.dangerTrailCd<=0&&(has(e,'Blazing')||has(e,'Frostborn'))){
      const element=has(e,'Frostborn')?'frost':'fire';
      if(pool(e,e,.5,element)){e.dangerTrailCd=1.5;
        if(element==='frost'&&bossObjects.filter(o=>o.kind==='iceWall').length<3){
          bossObjects.push({kind:'iceWall',x:clamp(e.x+1.4,.8,17.2),y:clamp(e.y,.8,13.2),r:.35,life:4,color:'#b2edff',owner:e});
        }
      }else e.dangerTrailCd=.6;
    }
    if(e.dangerCd>0||e.combatAbility||e.stun>0||e.dangerRitual)return;
    if(has(e,'Necromancer')&&game.enemies.length<14){
      const corpse=corpses.find(c=>!c.used&&c.life>2&&dist(e,c)<8);
      if(corpse){e.dangerRitual={corpse,age:0};e.attack=2;e.dangerCd=5;return;}
    }
    if(has(e,'Stormbound')&&pool(e,game.player,.7,'lightning',.2))e.dangerCd=3;
    if(has(e,'Mirror')){
      const id=({fire:'flameWave',frost:'frostTrail',lightning:'chain',earth:'rupture',wind:'fan',poison:'cage',nature:'cage',shadow:'gravity',arcane:'fan',solar:'beam'})[lastSchool]||'fan';
      const a=B.start(e,id);if(a){a.element=lastSchool;a.color=F.colors[lastSchool]||e.color;a.modifier='';e.dangerCd=4;}
    }
    if(e.dangerCd<=0)e.dangerCd=1;
  }
  const oldEffects=updateEnemyEffects;
  updateEnemyEffects=function(dt){
    ensure();const before=room;const positions=new Map(game.enemies.map(e=>[e,{x:e.x,y:e.y}]));
    oldEffects(dt);if(!live()||game.roomData!==before)return;
    for(const c of corpses)c.life-=dt;corpses=corpses.filter(c=>c.life>0&&!c.used);
    for(const e of game.enemies)if(!e.dead)eliteTick(e,dt);
    for(const o of bossObjects)o.life-=dt;bossObjects=bossObjects.filter(o=>o.life>0);
    for(const e of game.enemies){
      const from=positions.get(e),a=e.combatAbility;if(!from||!a||a.age<a.wind||a.age>=a.wind+a.active||dist(from,e)<.01||!['charge','hunterDash','chainDash','doubleDash'].includes(a.ability))continue;
      const hit=bossObjects.find(o=>['pillar','barrel'].includes(o.kind)&&o.life>0&&B.distanceToSegment(o,from,e)<o.r+e.r);
      if(hit){hit.life=0;e.dangerArmorBroken=4;e.stun=Math.max(e.stun,e.boss?1:1.5);
        B.cancel(e);e.attack=1.8;
        burst(hit.x,hit.y,hit.color,16,1);floatText(e.x,e.y,'ARMOR EXPOSED', '#fff0c2');
        if(hit.kind==='barrel')damageEnemy(e,e.maxHp*.08,'earth');
        if(!bossObjects.some(o=>o.owner===e&&['pillar','barrel','shieldCrystal'].includes(o.kind)&&o.life>0))e.dangerArenaArmor=false;
      }
    }
  };
  const oldMove=playerMovement;
  playerMovement=function(dt){
    const p=game.player,from=p&&{x:p.x,y:p.y},result=oldMove(dt);
    if(from&&room===game.roomData&&!p.dodgeTime&&bossObjects.some(o=>o.life>0&&o.kind==='iceWall'&&dist(o,p)<o.r+p.r)){p.x=from.x;p.y=from.y;}return result;
  };
  function arena(e,family){
    if(bossObjects.some(o=>o.type==='dangerArena'&&o.owner===e))return;
    e.dangerFamily=family;e.dangerArenaArmor=['stone','sand'].includes(family);
    const kind=family==='sand'?'barrel':family==='frost'?'iceCrystal':family==='void'?'shieldCrystal':'pillar';
    const added=[[4,4],[14,4],[4,10],[14,10]].map(([x,y])=>({kind,x,y,r:.5,life:99999,color:family==='frost'?'#bef2ff':'#d7b794',owner:e,type:'dangerArena',label:kind==='pillar'?'Lure a charge into the pillar':kind==='barrel'?'Lure the worm into explosives':'Disrupt crystal'}));
    bossObjects.push(...added);game.interactables.push(...added);
  }
  function breakObject(o,element){
    if(!o||o.life<=0)return false;
    if(o.kind==='iceWall'&&element!=='fire'&&element!=='earth')return false;
    if(o.kind==='pillar'||o.kind==='barrel')return false;
    o.life=0;burst(o.x,o.y,o.color,12,.8);const e=o.owner;
    if(e&&!e.dead){e.dangerArmorBroken=4;e.stun=Math.max(e.stun,.6);B.extinguish(o,2);}
    if(o.kind==='iceCrystal')for(const a of B.actions())if(a.element==='frost'&&a.marks.some(s=>dist(s,o)<4))a.life=0;
    if(o.kind==='iceCrystal')window.AWWorldDanger?.clearFrostAt(o);
    if(e&&!bossObjects.some(o=>o.owner===e&&['pillar','barrel','shieldCrystal'].includes(o.kind)&&o.life>0))e.dangerArenaArmor=false;
    return true;
  }
  const oldInteract=interact;interact=function(){const o=currentInteraction();if(o?.type==='dangerArena')return breakObject(o,'earth');return oldInteract();};
  const oldDraw=drawEnemy;
  drawEnemy=function(e){
    oldDraw(e);if(!e.dangerAffixes?.length&&!e.dangerRitual)return;
    const p=worldToScreen(e.x,e.y,56);ctx.save();ctx.textAlign='center';ctx.font='bold 10px system-ui';ctx.fillStyle=e.intensityColor||'#d4b4ff';
    if(e.dangerAffixes?.length)ctx.fillText(e.dangerAffixes.join(' / '),p.x,p.y-12);
    if(e.dangerRitual){const c=e.dangerRitual.corpse;B.drawShape({shape:'circle',...c,r:1},'#d8bbff',.1+e.dangerRitual.age*.08);ctx.fillText('RESURRECT • INTERRUPT',p.x,p.y-28);}
    if(e.dangerCommander){ctx.strokeStyle='#e4d79a';ctx.globalAlpha=.45;ctx.beginPath();const q=worldToScreen(e.dangerCommander.x,e.dangerCommander.y,8);ctx.moveTo(q.x,q.y);ctx.lineTo(p.x,p.y+48);ctx.stroke();}
    ctx.restore();
  };
  const oldMarks=drawTelegraphs;
  drawTelegraphs=function(){oldMarks();for(const o of bossObjects)if(o.life>0){B.drawShape({shape:'circle',...o,r:o.r},o.color,.25);const p=worldToScreen(o.x,o.y,28);ctx.save();ctx.fillStyle=o.color;ctx.font='22px system-ui';ctx.textAlign='center';ctx.fillText(o.kind==='pillar'?'▥':o.kind==='barrel'?'💥':'◆',p.x,p.y);ctx.restore();}};
  window.AWDangerCombat={affix,has,pool,arena,breakObject,objects:()=>bossObjects,corpses:()=>corpses,reset};
})();
