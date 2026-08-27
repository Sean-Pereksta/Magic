'use strict';

function nearestEnemy(origin,range=999,filter=()=>true){let best=null,bd=range;for(const e of game.enemies){if(e.dead||!filter(e))continue;const d=Math.hypot(e.x-origin.x,e.y-origin.y);if(d<bd){bd=d;best=e}}return best}
function damageEnemy(e,amount,tag='',dot=false){
 if(!e||e.dead)return;amount=Math.max(1,amount);if(e.shield>0){const used=Math.min(e.shield,amount);e.shield-=used;amount-=used;fx('shieldHit',e.x,e.y,.2,'#b9ddff',{r:e.r*1.4});if(amount<=0)return}
 if(tag==='iceExecute'&&(e.slow>0||e.stun>0))amount*=1.55;e.hp-=amount;e.flash=1;if(!dot){floatText(e.x,e.y,Math.round(amount),tag==='fire'?'#ffad74':tag==='frost'?'#c8f3ff':tag==='lightning'?'#a8f4ff':tag==='soul'?'#db8dff':'#f2f6fb');burst(e.x,e.y,tag==='fire'?'#ff8253':tag==='frost'?'#b7edff':tag==='lightning'?'#9ceeff':'#d4e2ef',4,.35,8)}if(e.hp<=0)killEnemy(e,tag)
}
function killEnemy(e,tag=''){
 if(e.dead)return;e.dead=true;game.kills++;game.gold+=e.boss?55:e.elite?14:2+Math.floor(Math.random()*4);gainXP(e.xp);burst(e.x,e.y,e.color,e.boss?42:e.elite?22:13,e.boss?2.8:1.2,14);fx('deathBurst',e.x,e.y,e.boss?.8:.45,e.color,{r:e.r*(e.boss?4:2.2)});shake=Math.max(shake,e.boss?12:e.elite?5:2);
 if(e.curse){if(e.curse.heal)healPlayer(e.boss?12:5);if(e.curse.ghost){const t=nearestEnemy(e,6,o=>o!==e&&!o.dead);if(t){const d=norm(t.x-e.x,t.y-e.y);magicProjectile({x:e.x,y:e.y,z:12,vx:d.x*5,vy:d.y*5,r:.14,damage:e.curse.damage*3,color:'#d86fff',kind:'ghost',life:2,seek:2.2})}}if(e.curse.spread){const t=nearestEnemy(e,4,o=>o!==e&&!o.dead);if(t)t.curse={...e.curse,life:4}}}
 updateHUD();
}
function damagePlayer(amount){
 const p=game.player;if(!p||p.invuln>0||p.dodgeTime>0)return;let dmg=amount*(1-clamp(p.armor,0,.72));if(p.shield>0){const used=Math.min(p.shield,dmg);p.shield-=used;dmg-=used;fx('shieldHit',p.x,p.y,.22,'#96dfff',{r:.8});if(p.wardReflect&&used>0){const t=nearestEnemy(p,7);if(t)damageEnemy(t,used*p.wardReflect,'arcane')}if(dmg<=0){updateHUD();return}}
 p.hp-=dmg;p.invuln=.18;shake=Math.max(shake,6);$('damageFlash').style.background='rgba(255,30,45,.24)';setTimeout(()=>$('damageFlash').style.background='rgba(255,30,45,0)',80);floatText(p.x,p.y,`-${Math.round(dmg)}`,'#ff8b97');if(p.hp<=0)playerDeath();updateHUD()
}
function healPlayer(amount){const p=game.player;if(!p)return;const before=p.hp;p.hp=Math.min(p.maxHp,p.hp+amount);if(p.hp>before)floatText(p.x,p.y,`+${Math.round(p.hp-before)}`,'#76efa6');updateHUD()}
function gainXP(amount){game.xp+=amount;while(game.xp>=game.xpNeed){game.xp-=game.xpNeed;game.level++;game.xpNeed=Math.round(80*Math.pow(game.level,1.18));game.pendingLevelUps++;game.player.baseMaxHp+=4;recomputePlayerStats(true)}if(game.pendingLevelUps>0&&!modalPause)setTimeout(openLevelChoice,120)}
function floatText(x,y,text,color){const s=worldToScreen(x,y,26),el=document.createElement('div');el.className='float-text';el.textContent=text;el.style.left=s.x+'px';el.style.top=s.y+'px';el.style.color=color;$('floatLayer').appendChild(el);setTimeout(()=>el.remove(),950)}
function toastMsg(text){$('toast').textContent=text;$('toast').classList.add('show');clearTimeout(toastTimer);toastTimer=setTimeout(()=>$('toast').classList.remove('show'),1800)}

function recomputePlayerStats(preserveRatio=false){
 const p=game.player;if(!p)return;const ratio=p.maxHp?p.hp/p.maxHp:1,armor=p.armorGear||makeArmor(ARMOR_SETS[0],1,'Common');p.maxHp=(p.baseMaxHp+armor.hpBonus)*armor.hp;p.speed=p.baseSpeed*armor.move;p.armor=clamp(armor.armorBonus,0,.6);if(preserveRatio)p.hp=Math.max(1,p.maxHp*ratio);else p.hp=Math.min(p.hp,p.maxHp);updateHUD()
}
function weaponDamage(){const w=game.player.weapon;return w.power*w.damage*(1+(w.forge||0)*.13)}
function autoAttack(dt){
 const p=game.player,w=p.weapon;p.attackTimer-=dt;const rate=(1/w.attack)*(p.haste>0?1.28:1);if(p.attackTimer>0)return;let target=null;
 if(aimStick.active||mouse.active){const dir=spellAim();let best=1.2;for(const e of game.enemies){const v=norm(e.x-p.x,e.y-p.y),dot=v.x*dir.x+v.y*dir.y,d=dist(e,p);if(d<=w.range&&dot>.78){const score=(1-dot)*3+d*.03;if(score<best){best=score;target=e}}}}
 if(!target)target=nearestEnemy(p,w.range);if(!target)return;const d=norm(target.x-p.x,target.y-p.y);p.facing=d;p.attackTimer=Math.max(.16,rate);
 const kind=w.type==='bow'?'arrow':w.type==='blade'?'bladeWave':w.type==='spear'?'sunLance':w.type==='chakram'?'miniChakram':w.type==='scepter'?'sparkBolt':'staffBolt';
 magicProjectile({x:p.x,y:p.y,z:17,vx:d.x*w.speed,vy:d.y*w.speed,r:w.type==='blade'?.18:.1,damage:weaponDamage(),color:w.color,color2:'#fff',kind,life:w.range/w.speed+0.3,pierce:w.type==='spear'?2:w.type==='blade'?1:0,seek:w.type==='scepter'?.7:0,trail:'weapon'});fx('muzzle',p.x,p.y,.18,w.color,{dir:d});
}
function playerMovement(dt){
 const p=game.player;let sx=moveStick.active?moveStick.x:((keys.has('d')||keys.has('arrowright')?1:0)-(keys.has('a')||keys.has('arrowleft')?1:0)),sy=moveStick.active?moveStick.y:((keys.has('s')||keys.has('arrowdown')?1:0)-(keys.has('w')||keys.has('arrowup')?1:0));let dir=screenToWorldDir(sx,sy);if(Math.hypot(sx,sy)<.08)dir={x:0,y:0};let speed=p.speed*(p.tailwind>0?1.36:1);
 if(p.dodgeTime>0){speed*=2.75;dir=p.dodgeDir}else if(Math.hypot(dir.x,dir.y)>.1)p.facing=dir;p.x+=dir.x*speed*dt;p.y+=dir.y*speed*dt;
 const door=1.75,open=doorOpen();if(p.x<.28&&!(open&&Math.abs(p.y-ROOM_H/2)<door))p.x=.28;if(p.x>ROOM_W-.28&&!(open&&Math.abs(p.y-ROOM_H/2)<door))p.x=ROOM_W-.28;if(p.y<.28&&!(open&&Math.abs(p.x-ROOM_W/2)<door))p.y=.28;if(p.y>ROOM_H-.28&&!(open&&Math.abs(p.x-ROOM_W/2)<door))p.y=ROOM_H-.28;tryRoomExit()
}
function dodge(){const p=game.player;if(!p||p.dodgeCd>0||paused||modalPause)return;let sx=moveStick.active?moveStick.x:((keys.has('d')||keys.has('arrowright')?1:0)-(keys.has('a')||keys.has('arrowleft')?1:0)),sy=moveStick.active?moveStick.y:((keys.has('s')||keys.has('arrowdown')?1:0)-(keys.has('w')||keys.has('arrowup')?1:0));p.dodgeDir=Math.hypot(sx,sy)>.1?screenToWorldDir(sx,sy):p.facing;p.dodgeTime=.24;p.invuln=.34;p.dodgeCd=1.25;burst(p.x,p.y,p.armorGear.trim||'#dcefff',12,.85,7);fx('dashEcho',p.x,p.y,.32,p.armorGear.color,{dir:p.dodgeDir})}

function updateProjectiles(dt){
 for(const p of game.projectiles){p.life-=dt;if(p.seek&&p.owner==='player'){const t=nearestEnemy(p,5);if(t){const desired=norm(t.x-p.x,t.y-p.y),spd=Math.hypot(p.vx,p.vy);p.vx=lerp(p.vx,desired.x*spd,clamp(dt*p.seek,0,1));p.vy=lerp(p.vy,desired.y*spd,clamp(dt*p.seek,0,1))}}
  if(p.kind==='orbitalMissile'&&p.maxLife-p.life<.65){const age=p.maxLife-p.life;p.x=game.player.x+Math.cos(p.phase+age*8)*(.8+age);p.y=game.player.y+Math.sin(p.phase+age*8)*(.8+age);continue}
  p.x+=p.vx*dt;p.y+=p.vy*dt;p.z+=p.vz*dt;if(p.trail&&Math.random()<dt*28)game.particles.push({x:p.x,y:p.y,z:p.z,vx:-p.vx*.04+rnd(.1,-.1),vy:-p.vy*.04+rnd(.1,-.1),vz:0,life:.3,maxLife:.3,size:p.r*16+1,color:p.color,soft:true});
  if(p.owner==='player'){for(const e of [...game.enemies]){if(e.dead||p.hit.has(e))continue;if(Math.hypot(p.x-e.x,p.y-e.y)<p.r+e.r){p.hit.add(e);damageEnemy(e,p.damage,p.tag);if(p.splash){radialDamage(p.x,p.y,p.splash,p.damage*.55,p.tag,p.knock);fx('explosion',p.x,p.y,.24,p.color,{r:p.splash})}if(p.tag.includes('chain')){const t=nearestEnemy(e,3,o=>o!==e&&!p.hit.has(o));if(t){fx('lightning',e.x,e.y,.16,p.color,{toX:t.x,toY:t.y,width:2});damageEnemy(t,p.damage*.45,'arcane')}}if(p.onHit)p.onHit(e,p);if(p.pierce>0)p.pierce--;else{p.life=0;break}}}}
  else if(Math.hypot(p.x-game.player.x,p.y-game.player.y)<p.r+game.player.r){damagePlayer(p.damage);if(p.splash)radialPlayerThreat(p.x,p.y,p.splash,p.damage*.5);p.life=0;burst(p.x,p.y,p.color,7,.5)}
  if(p.x<-.8||p.x>ROOM_W+.8||p.y<-.8||p.y>ROOM_H+.8)p.life=0;
 }
 game.projectiles=game.projectiles.filter(p=>p.life>0)
}
function updateEnemyEffects(dt){
 for(const e of game.effects){if(e.kind==='enemyBomb'&&!e.landed&&e.life<.12){e.landed=true;radialPlayerThreat(e.x,e.y,e.r,e.damage);fx('explosion',e.x,e.y,.32,e.color,{r:e.r});burst(e.x,e.y,e.color,15,1.3);shake=Math.max(shake,5)}
  if(e.kind==='enemyBeam'){if(!e.hit){const p=game.player,rx=p.x-e.x,ry=p.y-e.y,along=rx*e.dx+ry*e.dy,side=Math.abs(rx*(-e.dy)+ry*e.dx);if(along>0&&along<14&&side<e.width+p.r){damagePlayer(e.damage);e.hit=true}}}
  if(e.kind==='enemyIcePatch'){e.tick-=dt;if(e.tick<=0){e.tick=.38;if(dist(game.player,e)<e.r){damagePlayer(e.damage);game.player.tailwind=-.2}}}
  if(e.kind==='enemyVoidWell'){e.tick-=dt;const d=dist(game.player,e);if(d<e.r*1.7&&d>.05){const pull=(1-d/(e.r*1.8))*dt*.7;game.player.x+=(e.x-game.player.x)/d*pull;game.player.y+=(e.y-game.player.y)/d*pull}if(e.tick<=0&&d<e.r){e.tick=.42;damagePlayer(e.damage)}}
 }
}
function updateParticles(dt){for(const p of game.particles){p.life-=dt;p.x+=p.vx*dt;p.y+=p.vy*dt;p.z+=p.vz*dt;if(!p.ambient)p.vz-=3.4*dt;else if(p.z<2)p.z=12}game.particles=game.particles.filter(p=>p.life>0)}
function updateTelegraphs(dt){for(const t of game.telegraphs)t.time-=dt;game.telegraphs=game.telegraphs.filter(t=>t.time>0)}
function updatePlayerTimers(dt){const p=game.player;p.invuln=Math.max(0,p.invuln-dt);p.dodgeCd=Math.max(0,p.dodgeCd-dt);p.dodgeTime=Math.max(0,p.dodgeTime-dt);p.shieldTime=Math.max(0,p.shieldTime-dt);p.tailwind=Math.max(0,p.tailwind-dt);p.haste=Math.max(0,p.haste-dt);if(p.shieldTime<=0)p.shield=0;if(p.wardHeal>0){p.wardHeal-=dt;healPlayer(dt*1.3)}for(const id of Object.keys(p.spellState))p.spellState[id].cd=Math.max(0,p.spellState[id].cd-dt)}

function rollLoot(chance=1){if(Math.random()>chance)return;const roll=Math.random(),lv=game.level;let rarity='Common';if(lv>=18&&roll>.965)rarity='Legendary';else if(lv>=12&&roll>.9)rarity='Epic';else if(lv>=7&&roll>.77)rarity='Rare';else if(lv>=3&&roll>.55)rarity='Uncommon';const isWeapon=Math.random()<.52,base=isWeapon?WEAPON_BASES[irnd(WEAPON_BASES.length)]:ARMOR_SETS[irnd(ARMOR_SETS.length)];game.loot=isWeapon?makeWeapon(base,lv,rarity):makeArmor(base,lv,rarity);setTimeout(openLootOverlay,220)}
function equipLoot(){const item=game.loot;if(!item)return;if(item.slot==='weapon')game.player.weapon=item;else game.player.armorGear=item;recomputePlayerStats(true);game.loot=null;closeOverlay('lootOverlay');toastMsg('Equipped new gear.');burst(game.player.x,game.player.y,item.color||'#fff',24,1.2);saveGame();updateHUD()}

function saveGame(){if(!game.player)return;try{const rooms={};for(const [k,r] of Object.entries(game.rooms))rooms[k]={x:r.x,y:r.y,biome:r.biome,town:r.town,boss:r.boss,elite:r.elite,difficulty:r.difficulty,name:r.name,seen:r.seen,cleared:r.cleared,scenery:r.scenery,deco:r.deco,chests:r.chests,seed:r.seed};const data={seed:game.seed,room:game.room,rooms,level:game.level,xp:game.xp,xpNeed:game.xpNeed,gold:game.gold,kills:game.kills,player:{hp:game.player.hp,baseMaxHp:game.player.baseMaxHp,activeSpells:game.player.activeSpells,unlocked:game.player.unlocked,spellState:game.player.spellState,upgrades:game.player.upgrades,weapon:game.player.weapon,armorGear:game.player.armorGear}};localStorage.setItem(SAVE_KEY,JSON.stringify(data));game.lastSave=performance.now()}catch(err){console.warn('Arcane Wilds save failed',err)}}
function loadGame(){try{const raw=localStorage.getItem(SAVE_KEY);if(!raw)return false;const d=JSON.parse(raw);game.seed=d.seed;game.room=d.room||{x:0,y:0};game.rooms=d.rooms||{};game.level=d.level||1;game.xp=d.xp||0;game.xpNeed=d.xpNeed||80;game.gold=d.gold||0;game.kills=d.kills||0;game.player=newPlayer();Object.assign(game.player,d.player||{});recomputePlayerStats(false);game.player.hp=clamp(d.player?.hp??game.player.maxHp,1,game.player.maxHp);return true}catch(e){console.warn(e);return false}}
function startNewGame(){localStorage.removeItem(SAVE_KEY);game.seed=Math.floor(Math.random()*1e9);game.room={x:0,y:0};game.rooms={};game.level=1;game.xp=0;game.xpNeed=80;game.gold=0;game.kills=0;game.pendingLevelUps=0;game.player=newPlayer();recomputePlayerStats(false);game.player.hp=game.player.maxHp;beginWorld()}
function beginWorld(){game.player.x=ROOM_W/2;game.player.y=ROOM_H/2;$('startOverlay').classList.add('hidden');$('hud').classList.remove('hidden');if(isTouch)$('mobileControls').classList.remove('hidden');running=true;paused=false;modalPause=false;loadRoom();updateHUD();last=performance.now();requestAnimationFrame(loop)}
function playerDeath(){paused=true;modalPause=true;saveGame();setTimeout(()=>{alert(`Your journey ended at level ${game.level}. Sunmere's magic restores you.`);game.player.hp=game.player.maxHp;game.room={x:0,y:0};game.player.x=ROOM_W/2;game.player.y=ROOM_H/2;modalPause=false;paused=false;loadRoom();saveGame()},120)}

function update(dt){if(!running||paused||modalPause||roomTransition)return;elapsed+=dt;playerMovement(dt);updatePlayerTimers(dt);autoAttack(dt);updateEnemies(dt);updateSpellEntities(dt);updateProjectiles(dt);updateEnemyEffects(dt);updateTelegraphs(dt);updateParticles(dt);if(performance.now()-game.lastSave>10000)saveGame();shake=Math.max(0,shake-dt*18);updateHUD()}
function loop(now){if(!running)return;const dt=Math.min(.033,(now-last)/1000||0);last=now;update(dt);render();requestAnimationFrame(loop)}

addEventListener('keydown',ev=>{const k=ev.key.toLowerCase();keys.add(k);if(['1','2','3'].includes(k))castSpell(Number(k)-1);if(k===' '){ev.preventDefault();dodge()}if(k==='e')interact();if(k==='escape')togglePause()});addEventListener('keyup',ev=>keys.delete(ev.key.toLowerCase()));
canvas.addEventListener('mousemove',e=>{mouse.x=e.clientX;mouse.y=e.clientY;mouse.active=true});canvas.addEventListener('mouseleave',()=>mouse.active=false);canvas.addEventListener('contextmenu',e=>{e.preventDefault();dodge()});

function bindStick(zoneId,stick,knobId){const zone=$(zoneId),knob=$(knobId);function set(e){const r=zone.getBoundingClientRect(),cx=r.left+r.width/2,cy=r.top+r.height/2,dx=e.clientX-cx,dy=e.clientY-cy,len=Math.hypot(dx,dy),max=43,k=Math.min(1,len/max);stick.x=len?dx/len*k:0;stick.y=len?dy/len*k:0;stick.active=true;knob.style.transform=`translate(calc(-50% + ${stick.x*34}px),calc(-50% + ${stick.y*34}px))`}zone.addEventListener('pointerdown',e=>{stick.pointer=e.pointerId;zone.setPointerCapture(e.pointerId);set(e)});zone.addEventListener('pointermove',e=>{if(stick.pointer===e.pointerId)set(e)});const end=e=>{if(stick.pointer!==e.pointerId)return;stick.pointer=null;stick.x=stick.y=0;stick.active=false;knob.style.transform='translate(-50%,-50%)'};zone.addEventListener('pointerup',end);zone.addEventListener('pointercancel',end)}
