'use strict';

/* High-level second render pass: parallax silhouettes, local lights, boss arenas, weather and corrected simulation order. */
const _baseAtmosphere=drawAtmosphere;
drawAtmosphere=function(room,pal){
 _baseAtmosphere(room,pal);ctx.save();
 const horizon=H*.24;ctx.globalAlpha=.18;ctx.fillStyle=colorAlpha(pal.accent,.14);
 for(let layer=0;layer<3;layer++){ctx.beginPath();ctx.moveTo(0,horizon+70+layer*28);for(let x=0;x<=W+80;x+=80){const y=horizon+layer*36+Math.sin(x*.008+room.x*.7+layer)*22+Math.sin(x*.021+room.y)*10;ctx.lineTo(x,y)}ctx.lineTo(W,H*.48);ctx.lineTo(0,H*.48);ctx.closePath();ctx.fill();ctx.globalAlpha*=.6}
 if(room.biome==='forest'||room.biome==='swamp'){ctx.globalAlpha=.11;ctx.fillStyle='#020805';for(let i=0;i<15;i++){const x=(i*137+room.x*43)%W,h=50+(i%5)*20;ctx.fillRect(x-2,horizon-h,4,h);ctx.beginPath();ctx.arc(x,horizon-h,14+(i%3)*6,0,TAU);ctx.fill()}}
 if(room.biome==='volcanic'){ctx.globalAlpha=.16;for(let i=0;i<7;i++){const x=(i*211+room.x*77)%W,g=ctx.createRadialGradient(x,horizon+30,0,x,horizon+30,70);g.addColorStop(0,'rgba(255,80,42,.2)');g.addColorStop(1,'rgba(255,80,42,0)');ctx.fillStyle=g;ctx.fillRect(x-80,horizon-50,160,160)}}ctx.restore();
};
const _baseRoomLight=drawRoomLight;
drawRoomLight=function(room,pal){
 _baseRoomLight(room,pal);ctx.save();ctx.globalCompositeOperation='screen';const lights=[];
 if(game.player){const a=game.player.armorGear?.aura;if(a)lights.push({x:game.player.x,y:game.player.y,c:a==='ember'?'#ff7448':a==='frost'?'#9eeaff':a==='leaf'?'#6ddd7e':a==='void'?'#b86cff':a==='sun'?'#ffd365':'#76e5ff',r:95})}
 for(const p of game.projectiles)lights.push({x:p.x,y:p.y,c:p.color,r:45+p.r*90});for(const h of game.hazards)if(['fire','void','singularity','tempest','lightningField'].includes(h.kind))lights.push({x:h.x,y:h.y,c:h.color,r:80+h.r*35});for(const o of game.interactables)if(o.type!=='chest')lights.push({x:o.x,y:o.y,c:o.type==='well'?'#78e4ff':o.type==='forge'?'#ff864f':'#c780ff',r:72});
 for(const l of lights.slice(0,30)){const s=worldToScreen(l.x,l.y),g=ctx.createRadialGradient(s.x,s.y,0,s.x,s.y,l.r);g.addColorStop(0,colorAlpha(l.c,.1));g.addColorStop(1,colorAlpha(l.c,0));ctx.fillStyle=g;ctx.fillRect(s.x-l.r,s.y-l.r,l.r*2,l.r*2)}ctx.restore();if(game.enemies.some(e=>e.boss))drawBossArena();drawForegroundWeather(room,pal)
};
function drawBossArena(){const s=worldToScreen(ROOM_W/2,ROOM_H/2);ctx.save();ctx.strokeStyle='rgba(216,120,255,.18)';ctx.lineWidth=1.5;ctx.setLineDash([7,9]);for(let i=0;i<3;i++){const r=95+i*34;ctx.beginPath();ctx.ellipse(s.x,s.y,r,r*.5,elapsed*(i%2?-.08:.08),0,TAU);ctx.stroke()}ctx.setLineDash([]);for(let i=0;i<8;i++){const a=i/8*TAU+elapsed*.05,rx=Math.cos(a)*150,ry=Math.sin(a)*74;ctx.fillStyle='rgba(232,177,255,.2)';ctx.font='16px serif';ctx.textAlign='center';ctx.fillText(i%2?'◇':'✦',s.x+rx,s.y+ry)}ctx.restore()}
function drawForegroundWeather(room,pal){ctx.save();ctx.globalCompositeOperation='screen';if(room.biome==='frost'){ctx.strokeStyle='rgba(220,249,255,.15)';ctx.lineWidth=1;for(let i=0;i<20;i++){const x=(i*89+elapsed*28)%W,y=(i*67+elapsed*44)%H;ctx.beginPath();ctx.moveTo(x,y);ctx.lineTo(x-5,y+12);ctx.stroke()}}else if(room.biome==='volcanic'){ctx.fillStyle='rgba(255,135,81,.16)';for(let i=0;i<18;i++){const x=(i*113+elapsed*14)%W,y=H-((i*71+elapsed*34)%H);ctx.beginPath();ctx.arc(x,y,1+(i%3),0,TAU);ctx.fill()}}else if(room.biome==='swamp'){ctx.fillStyle='rgba(188,233,134,.08)';for(let i=0;i<10;i++){const x=(i*163+Math.sin(elapsed+i)*24)%W,y=H*.25+(i*73)%Math.max(1,H*.55);ctx.beginPath();ctx.arc(x,y,4+Math.sin(elapsed*2+i)*1.5,0,TAU);ctx.fill()}}ctx.restore()}

update=function(dt){if(!running||paused||modalPause||roomTransition)return;elapsed+=dt;playerMovement(dt);updatePlayerTimers(dt);autoAttack(dt);updateSpellEntities(dt);updateEnemies(dt);updateProjectiles(dt);updateEnemyEffects(dt);updateTelegraphs(dt);updateParticles(dt);if(performance.now()-game.lastSave>10000)saveGame();shake=Math.max(0,shake-dt*18);updateHUD()};

const _baseCastSpell=castSpell;
castSpell=function(slot){const id=game.player?.activeSpells?.[slot],before=id?game.player.spellState[id]?.cd||0:0;_baseCastSpell(slot);if(id&&before<=0&&SPELLS[id].rarity==='Legendary'){const color=rarityColors.Legendary;$('roomFade').style.background=`radial-gradient(circle at center,${colorAlpha(color,.15)},#02040a 70%)`;$('roomFade').style.opacity='.16';setTimeout(()=>{$('roomFade').style.opacity='0';setTimeout(()=>$('roomFade').style.background='',220)},90);shake=Math.max(shake,5)}};
