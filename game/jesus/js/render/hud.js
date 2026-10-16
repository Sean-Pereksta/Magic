const byId=id=>document.getElementById(id);
export const formatTime=seconds=>{const n=Math.max(0,Math.floor(Number(seconds)||0));return`${Math.floor(n/60)}:${String(n%60).padStart(2,"0")}`;};
export class HUD{
  constructor(assets){this.assets=assets;this.bannerTime=0;this.lastHearts="";this.updateTimer=0;}
  showBanner(title,detail="",duration=2.5){const element=byId("banner");element.querySelector("strong").textContent=title;element.querySelector("span").textContent=detail;this.bannerTime=duration;element.classList.add("visible");}
  update(game,dt){if(this.bannerTime>0){this.bannerTime-=dt;if(this.bannerTime<=0)byId("banner").classList.remove("visible");}this.updateTimer-=dt;if(this.updateTimer>0)return;this.updateTimer=.08;
    const p=game.player,key=`${p.hearts}/${p.maxHearts}`;if(key!==this.lastHearts){this.lastHearts=key;const hearts=byId("hearts");hearts.replaceChildren();for(let i=0;i<p.maxHearts;i++){const img=new Image();img.src=this.assets.getURL(i<p.hearts?"ui-heart-full":"ui-heart-empty");img.alt="";hearts.append(img);}hearts.setAttribute("aria-label",`${p.hearts} of ${p.maxHearts} hearts`);}
    byId("faithCount").textContent=`✦ ${game.runStats?.collectibles||0}`;byId("regionText").textContent=game.world.regionName||game.world.name||"";
    const progress=game.world.bossArena?100:Math.min(100,Math.floor((game.world.furthestX||p.x)/game.world.width*100));byId("progressFill").style.width=`${progress}%`;byId("progressTrack").setAttribute("aria-valuenow",progress);
    const light=Number(p.powers.holyLight)||0,rain=Number(p.powers.fireRain)||0,ability=byId("abilityBtn");ability.hidden=light+rain<=0;byId("abilityCount").textContent=rain||light;
    ability.dataset.power=rain>0?'fire':'light';ability.setAttribute('aria-label',rain>0?`Cast Fire Rain, ${rain} charges`:`Cast Holy Light, ${light} charges`);
    const status=byId('pickupStatus'),boosts=p.boosts||{},parts=[];
    if(boosts.water>0)parts.push(`Water ${Math.ceil(boosts.water)}s`);
    if(boosts.wind>0)parts.push(`Wind ${Math.ceil(boosts.wind)}s`);
    if(rain>0)parts.push(`Fire ×${rain}`);
    status.textContent=parts.join(' · ');status.hidden=game.screen!=='playing'||!parts.length;
    if(parts.length)byId('rotateHint').hidden=true;
    const boss=game.bossController.boss;byId("bossHud").classList.toggle("hidden",!boss||game.screen!=="playing");if(boss){byId("bossName").textContent=boss.definition.name;byId("bossFill").style.width=`${boss.health/boss.maxHealth*100}%`;byId("bossHint").textContent=boss.state==="VULNERABLE"?"Now — stomp or use light":boss.state==="TELL"?"Watch the warning":boss.phase?.name||"";}
  }
}
