(() => {
  'use strict';
  const {items, enemies, regions, elements} = window.Catalog;
  const E = window.GameEngine;
  const relics = Object.values(items);
  const recipes = relics.filter(item => item.recipe);
  const tabs = {items:'Relics', recipes:'Fusion recipes', enemies:'Bestiary', upgrades:'Upgrades'};
  const escapes = {'&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;'};
  const esc = value => String(value).replace(/[&<>"']/g, c => escapes[c]);
  const cap = s => s[0].toUpperCase() + s.slice(1);
  const nfmt = n => n.toLocaleString();
  const styleLabel = style => style==='mend'?'Healing':style==='drain'?'Lifesteal':cap(style);
  const elementText={leaf:'Every hit adds poison. Every third hit roots the foe, slowing attack charge by 75% for 0.8s.',fire:'Every hit leaves a burning ember. Fire ignites poisoned foes into a wildfire that splashes nearby enemies.',frost:'Every hit chills attack charge. Every third hit freezes the foe for 0.65s.',storm:'Hits disrupt windups. Every third hit chains lightning to two other foes; chilled foes conduct an extra arc.',stone:'Hits stagger windups and stack up to five Fractures. Each fracture increases direct relic damage taken by 3.5%.',spirit:'Hits leave a four-second haunt and grant a small shield. Every third spirit hit releases a delayed soul echo.'};
  const afflictionText={corrosive:'Corrosion: incoming damage rises 15% and shields erode for six seconds. Counter: Purify.',entangling:'Entangle: relics recharge 30% more slowly for 2.5 seconds. Counter: Unbinding.',hexed:'Hex: relics deal 20% less damage for four seconds. Counter: Purify.',scorching:'Scorch: strikes leave lingering fire damage. Counter: Blastguard cuts it 75%; Thaw reduces it; Cleanse removes it.',freezing:'Frostbite: relics recharge 35% more slowly for 2.5 seconds. Counter: Thaw.',shocking:'Static: abilities recharge 40% more slowly for 0.8 seconds. Counter: Insulated also reduces these strikes 15%.',bulwark:'Bulwark: begins warded and renews its ward every second strike. Counter: Wardbreaker, pierce attacks, or a Wardbreak signature.'};
  function relicEffects(item){return '<div class="subhead">Element · '+esc(elements[item.element].name)+'</div><p class="body-copy">'+esc(elementText[item.element])+'</p>'+(item.special?'<div class="subhead">Signature · '+esc(item.special)+'</div><p class="body-copy">'+esc(item.specialDescription)+'</p>':'')+(item.counter?'<div class="subhead">Counter · '+esc(item.counter)+'</div><p class="body-copy">'+esc(item.counterDescription)+'</p>':'');}
  function elementHandbook(){return '<details class="element-handbook frame"><summary>Element effects & counter gear</summary><div class="element-notes">'+Object.entries(elementText).map(([id,text])=>'<article style="--el:'+elements[id].color+'"><strong>'+esc(elements[id].name)+'</strong><p>'+esc(text)+'</p></article>').join('')+'</div><p>Counter gear works while equipped. Cleansers remove ailments when that relic attacks. Search for Cleanse, Antivenom, Insulated, Thaw, Unbinding, Purify, Blastguard, or Wardbreaker. Inspect enemies to plan your loadout.</p></details>';}
  function relicSearchText(item){
    const effect=item.style==='mend'?'healing healer restore health':item.style==='drain'?'lifesteal life steal life-steal leech siphon':'';
    const home=regions[item.dropRegion];
    return [item.name,item.element,item.style,effect,item.rarity,item.special||'',item.specialDescription||'',item.counter||'',item.counterDescription||'',home?.name||''].join(' ').toLowerCase();
  }
  const glyph = name => '<i aria-hidden="true" data-glyph="'+name+'"></i>';
  const traitText = {
    armored:'Its shell absorbs 26% of direct damage. Pierce attacks, Wardbreaker gear and Wardbreak signatures cut through the shell.',
    swift:'Its attack takes about one-third less time to charge.',
    venom:'Each strike builds up venom that deals damage over time. Antivenom reduces the buildup and accelerates decay. Cleanse removes existing venom when its relic activates.',
    brute:'It hits 32% harder, with a longer windup between blows.',
    regenerating:'Each second it regrows 1.8% of its maximum health.',
    warded:'Its ward absorbs 72% of the next direct hit and returns after every third attack. Pierce attacks bypass the ward.',
    leech:'It heals for 45% of the damage it deals through Pip’s shield.',
    veteran:'Its third strike hits 38% harder. Below 45% health, it shrugs off 17% of direct damage.',
    swarm:'It attacks more frequently, with 18% less damage per strike.',
    volatile:'Its final burst hits for 48% of its usual attack damage. Equipped Blastguard gear reduces the burst by 75%.'
  };
  function lead(kicker, title, description) {
    return `<header class="page-lead"><div><div class="eyebrow"><span class="mini-line"></span>${kicker}</div><h1>${title}<span class="gold">.</span></h1><p class="subtitle">${description}</p></div></header>`;
  }
  function tierBadge(item,includeCommon=true){
    const tier=item.rarity;
    if(tier==='common'&&!includeCommon)return '';
    return '<span class="tier-badge" data-rarity="'+tier+'">'+esc(window.Catalog.rarities[tier].name)+'</span>';
  }
  function relicSource(item){
    const firstOrder=item.recipe?.every(id=>!items[id].recipe),forgeOnly=item.recipe&&!firstOrder;
    let line=forgeOnly?'Forge the two ingredients listed above. This relic is available only through fusion.':
      (firstOrder?'Guardians can drop this fusion starting at wave '+item.unlockRound+'. You can also forge it whenever you have both ingredients.':
        'Enemy drops can contain this relic from wave '+item.unlockRound+'. Guardians have better odds of dropping high-rarity gear.');
    if(!forgeOnly&&Number.isInteger(item.dropRegion))line+=' It is more likely to drop in '+regions[item.dropRegion].name+'.';
    return '<div class="subhead">How to find it</div><p class="body-copy">'+esc(line)+'</p>';
  }
  function routeAtlas(r){
    const info=E.routeInfo(r),n=regions.length;
    const rows=regions.map((reg,i)=>{
      const start=info.circuitStart+i*4+(i>=info.regionsPerCircuit?info.regionsPerCircuit*4:0),here=i===r.region;
      return '<button data-region="'+i+'" class="atlas-stop'+(here?' here':'')+'" style="--region-color:'+reg.color+'" '+(here?'aria-current="step"':'')+'><span class="atlas-number">'+String(i+1).padStart(2,'0')+'</span><span><b>'+esc(reg.name)+'</b><small>Waves '+start+'–'+(start+3)+(here?' · You are here':'')+'</small></span></button>';
    }).join('');
    const odds=E.lootOdds(r),boss=E.lootOdds({...r,round:info.regionEnd},true),pct=f=>Number((f*100).toFixed(2))+'%';
    return '<h2 class="dialog-title">Route atlas</h2><p class="body-copy">'+n+' regions · '+(n*4)+' waves per full circuit. Select a stop to see the landscape, its creatures and its relics.'+(info.regionsPerCircuit<n?' Your saved '+info.regionsPerCircuit+'-region circuit will finish before the expanded road begins next circuit.':'')+'</p><div class="atlas-grid">'+rows+'</div>'+
      '<div class="subhead">Finds & rarity · '+esc(regions[r.region].name)+'</div><p class="body-copy">A normal wave has a '+pct(E.dropChances.normal)+' chance of at least one relic; a guardian wave has a '+pct(E.dropChances.guardian)+' chance, regardless of group size. These are the rarity odds for each relic that drops:</p><table class="rarity-odds"><thead><tr><th>Tier</th><th>Normal</th><th>Guardian</th><th>Power</th></tr></thead><tbody>'+Object.entries(window.Catalog.rarities).map(([key,tier])=>'<tr><th style="color:'+tier.color+'">'+esc(tier.name)+'</th><td>'+pct(odds[key])+'</td><td>'+pct(boss[key])+'</td><td>×'+tier.powerScale+'</td></tr>').join('')+'</tbody></table><p class="mini-note">Unavailable tiers have zero odds until their relics unlock. Fusion recipes still let you craft stronger gear from the right ingredients.</p>';
  }
  function regionDossier(r,i){
    const reg=regions[i],start=E.routeInfo(r).circuitStart+4*i,roster=enemies.filter(e=>e.region===i);
    const local=relics.filter(d=>d.dropRegion===i);
    const jump=(index,word)=>'<button class="outline compact" data-region="'+index+'">'+word+'</button>';
    return '<h2 class="dialog-title">'+esc(reg.name)+'</h2><p class="body-copy">'+esc(reg.subtitle)+'</p><canvas id="region-art" width="368" height="136" aria-label="'+esc(reg.name)+' landscape"></canvas>'+
      '<div class="stat-row"><span class="stat-pill">Waves '+start+'–'+(start+3)+'</span><span class="stat-pill">'+roster.length+' creatures</span></div>'+
      '<div class="subhead">Creatures on this road</div><div class="chip-row atlas-creatures">'+[...roster].sort((a,b)=>Number(b.boss)-Number(a.boss)).map(e=>'<button class="outline compact" data-species="'+e.id+'">'+(e.boss?'Guardian · ':'')+esc(e.name)+'</button>').join('')+'</div>'+
      (local.length?'<div class="subhead">Regional relics</div><div class="atlas-relics">'+local.map(d=>'<button class="outline compact" data-catalog="'+d.id+'" data-rarity="'+d.rarity+'"><span class="atlas-icon">'+ItemArt.svg(d.id,23)+'</span><span>'+esc(d.name)+'</span>'+tierBadge(d)+'</button>').join('')+'</div>':'')+
      '<div class="dialog-actions atlas-nav">'+(i?jump(i-1,'‹ Previous'):'')+'<button class="outline compact" data-dialog="route">All regions</button>'+(i<regions.length-1?jump(i+1,'Next ›'):'')+'</div>';
  }
  function relicCard(item, recipe, seen) {
    const element = elements[item.element];
    const names = recipe ? item.recipe.map(id => items[id].name).join(' + ') : '';
    return `<button type="button" class="frame guide-card" data-catalog="${esc(item.id)}" data-rarity="${item.rarity}" style="--el:${element.color}">
      <span class="icon-tile">${ItemArt.svg(item.id,40)}</span>
      <span><b>${esc(item.name)}</b><small>${recipe ? esc(names) : element.name+' · '+styleLabel(item.style)}</small>${item.special?'<small class="relic-signature">✦ '+esc(cap(item.special))+'</small>':''}${item.counter?'<small class="relic-counter">Counters · '+esc(cap(item.counter))+'</small>':''}<small>${tierBadge(item)} ${seen ? '✓ Discovered' : 'Undiscovered'}</small></span>
    </button>`;
  }
  function enemyCard(enemy, seen) {
    return `<button type="button" class="frame guide-card" data-species="${esc(enemy.id)}">
      <canvas class="icon-tile" data-portrait-id="${esc(enemy.id)}" width="48" height="48" style="--el:${enemy.color}" aria-hidden="true"></canvas>
      <span><b>${esc(enemy.name)}</b><small>${esc(regions[enemy.region].name)} · ${enemy.boss ? 'Guardian' : cap(enemy.trait)}</small>${enemy.affliction?'<small>'+esc(cap(enemy.affliction))+'</small>':''}<small>${seen ? '✓ Encountered' : 'Unseen'}</small></span>
    </button>`;
  }
  function guide(profile, tab='items', query='', tier='all') {
    if(!Object.hasOwn(tabs,tab)) tab='items';
    const isEnemy = tab==='enemies', isRecipe = tab==='recipes', isUpgrade=tab==='upgrades';
    const entries = isUpgrade?Object.values(window.Progression.upgrades):isEnemy ? enemies : isRecipe ? recipes : relics;
    const seen = new Set(isUpgrade?Object.keys(profile.run?.skills||{}):isEnemy ? profile.seenEnemies : profile.seenItems);
    const count = entries.filter(entry => seen.has(entry.id)).length;
    const terms = String(query).toLocaleLowerCase().trim().split(/\s+/).filter(Boolean);
    const results = entries.filter(entry=>{
      if(!isUpgrade&&!isEnemy&&tier!=='all'&&entry.rarity!==tier)return false;
      const fields = isUpgrade?[entry.name,entry.element,entry.kind,entry.category,entry.description]:isEnemy ? [entry.name, entry.trait, entry.affliction||'',afflictionText[entry.affliction]||'',entry.species, regions[entry.region].name, entry.boss?'guardian boss':'', 'weak '+entry.weak, 'resists '+entry.resist] :
        [relicSearchText(entry),...(isRecipe ? entry.recipe.map(id=>items[id].name) : [])];
      const key = fields.join(' ').toLocaleLowerCase(); return terms.every(term=>key.includes(term));
    });
    const hint = isUpgrade?'Search upgrades and abilities':isRecipe?'Search by fusion or ingredient':isEnemy?'Search by name, trait or region':'Search by relic, element or effect';
    return lead('PIP’S FIELD NOTES','Field guide','A record of relics, fusion recipes, enemies and expedition upgrades.')+elementHandbook()+
      `<div class="tools-row"><div class="tabs" aria-label="Field guide sections">${Object.entries(tabs).map(([key,title])=>`<button type="button" data-guide-tab="${key}" aria-pressed="${key===tab}" class="${key===tab?'selected':''}">${title}</button>`).join('')}</div>
        <input id="guide-search" class="search" type="search" value="${esc(query)}" aria-label="${hint}" placeholder="${hint}"></div>
        ${!isUpgrade&&!isEnemy?'<div class="guide-filters"><div class="guide-roles" aria-label="Rarity filter"><span>Rarity</span>'+['all',...Object.keys(window.Catalog.rarities)].map(t=>'<button type="button" data-guide-tier="'+t+'" data-rarity="'+t+'" aria-pressed="'+(tier===t)+'">'+(t==='all'?'All':window.Catalog.rarities[t].name)+'</button>').join('')+'</div>'+(tab==='items'?'<div class="guide-roles" aria-label="Find recovery gear"><span>Recovery gear</span>'+['Healing','Lifesteal'].map(role=>'<button type="button" data-gear-role="'+role+'" aria-pressed="'+(query.toLowerCase()===role.toLowerCase())+'">'+role+'</button>').join('')+'</div>':'')+'</div>':''}
        <div class="guide-count" aria-live="polite">${isUpgrade?'LEARNED THIS RUN':isEnemy?'ENCOUNTERED':'DISCOVERED'} ${count} / ${entries.length}${(terms.length||(!isEnemy&&!isUpgrade&&tier!=='all'))?' &nbsp; · &nbsp; '+results.length+' '+(results.length===1?'RESULT':'RESULTS'):''}</div>
        ${isRecipe ? '<aside class="frame guide-intro"><div><strong>At the forge</strong><p>Drag one ingredient onto its partner to fuse them. Select one relic to make valid partners glow. Tap a glowing partner to preview the fusion. A fusion inherits the average level of its ingredients. Two identical relics at the same level gain one level.</p></div></aside>':''}
        <div class="guide-grid">${results.length ? results.map(entry=>isUpgrade?
          `<button class="frame guide-card" data-skill="${entry.id}" style="--el:${elements[entry.element].color}"><span class="icon-tile">${ItemArt.svg(entry.icon,40)}</span><span><b>${esc(entry.name)}</b><small>${esc(entry.category)} · ${entry.kind==='ability'?'Auto-cast ability':'Passive'}</small><small>${seen.has(entry.id)?'Rank '+profile.run.skills[entry.id]:'Unlearned'}</small></span></button>`
          :isEnemy?enemyCard(entry,seen.has(entry.id)):relicCard(entry,isRecipe,seen.has(entry.id))).join('') :
          '<p class="frame empty-note">No entries match this search.</p>'}</div>`;
  }
  function paintPortraits(root) {
    if(typeof window.WorldArt?.portrait!=='function') return;
    root.querySelectorAll('canvas[data-portrait-id]').forEach(canvas=>{
      const d=enemies.find(e=>e.id===canvas.dataset.portraitId);
      if(d) window.WorldArt.portrait(canvas,d.species,d.color);
    });
  }
  const seeds = n => nfmt(n)+' '+(n===1?'seed':'seeds');
  const perkIcons = {heart:'heart',claw:'swords',pace:'road'};
  function perkShop(p,compact=false) {
    const locked=p.run&&p.run.phase!=='dead';
    return '<div class="camp-upgrades'+(compact?' camp-upgrades-compact':'')+'">'+Object.entries(E.perks).map(([key,perk])=>{
      const rank=p.perks[key],cost=E.perkCost(p,key),maxed=cost===null;
      const affordable=!maxed&&p.points>=cost;
      const {current,next}=E.perkPreview(p,key);
      const buttonText=maxed?'Max rank':seeds(cost);
      const note=maxed?'All ranks purchased':locked?'Opens at camp':affordable?'Buy rank '+(rank+1):seeds(cost-p.points)+' short';
      return `<article class="camp-upgrade">
        <div class="camp-perk-title"><span class="section-icon">${glyph(perkIcons[key]||'sprout')}</span>
          <div><h3>${esc(perk.name)}</h3><span class="camp-caption">PERMANENT · RANK ${nfmt(rank)}</span></div></div>
        <p>${esc(perk.description)}</p><div class="camp-preview"><span>${esc(current)}</span>${maxed?'':' <span aria-label="to">→</span> <strong>'+esc(next)+'</strong>'}</div>
        <div class="camp-buy-row"><span>${note}</span><button type="button" class="${affordable&&!locked?'primary':'outline'} compact" data-buy="${esc(key)}" ${maxed||locked||!affordable?'disabled':''} aria-label="${esc(maxed?perk.name+' fully upgraded':'Buy '+perk.name+' rank '+(rank+1)+' for '+seeds(cost))}">${maxed?'':glyph('seed')}${buttonText}</button></div>
      </article>`;
    }).join('')+'</div>';
  }
  function recordNote(p,r){
    const best=p.best, prior=r.bestAtStart;
    if(Number.isFinite(prior)&&r.kills>prior)return 'New personal best · '+nfmt(r.kills)+' cleared (previously '+nfmt(prior)+')';
    if(Number.isFinite(prior)&&r.kills>0&&r.kills===prior)return 'Matched your personal best · '+nfmt(prior)+' cleared';
    return 'Personal best · '+nfmt(best)+' '+(best===1?'wave':'waves')+' cleared';
  }
  function closingBlow(r){
    const c=r.deathCause,enemy=c?.enemyId?enemies.find(e=>e.id===c.enemyId):null;
    const source=enemy?esc(enemy.name):'A trail foe';
    const loss=Number.isFinite(c?.damage)?' ('+nfmt(Number(c.damage.toFixed(1)))+' HP)':'';
    if(c?.kind==='strike')return source+' landed the final strike'+loss+'.';
    if(c?.kind==='venom')return (enemy?source+'’s venom':'Lingering venom')+' took Pip’s last health'+loss+'.';
    if(c?.kind==='scorch')return 'Lingering scorch took Pip’s last health'+loss+'. Try Blastguard or a cleansing relic.';
    if(c?.kind==='burst')return source+' fell, then its final burst brought Pip down'+loss+'.';
    const remaining=r.enemies?.filter(e=>e.hp>0)||[];
    if(remaining.length){const e=remaining[0];return 'When Pip fell, '+esc(e.name)+' still had '+nfmt(Math.ceil(e.hp))+' of '+nfmt(Math.ceil(e.maxHp))+' HP.';}
    return 'Pip fell during the encounter on wave '+nfmt(r.round)+'.';
  }
  function expeditionRanks(r){
    const ranks=Object.entries(r.skills||{}).filter(([id,n])=>n>0&&window.Progression.upgrades[id]);
    const sum=ranks.reduce((n,[,rank])=>n+rank,0);
    const rows=ranks.sort(([a,na],[b,nb])=>nb-na||window.Progression.upgrades[a].name.localeCompare(window.Progression.upgrades[b].name)).map(([id,rank])=>{
      const d=window.Progression.upgrades[id];
      return `<li>${ItemArt.svg(d.icon,20)}<span>${esc(d.name)}</span><b>R${nfmt(rank)}</b></li>`;
    }).join('');
    return sum?`<details class="camp-ranks"><summary>Expedition build · ${nfmt(sum)} temporary ${sum===1?'rank':'ranks'}</summary><p>These ranks lasted for this expedition.</p><ul>${rows}</ul></details>`:'';
  }
  function debrief(p){
    const r=p.run;
    if(!r||r.phase!=='dead')return '';
    const breakdown=E.awardBreakdown(r).filter(x=>x.amount>0);
    return `<div class="camp-debrief">
      <div class="camp-opening"><span class="camp-region">${esc(regions[r.region].name)} · Wave ${nfmt(r.round)}</span><h2 class="dialog-title">Back at the trailhead</h2><p>${closingBlow(r)}</p></div>
      <div class="camp-metrics"><div><strong>${nfmt(r.kills)}</strong><span>Waves cleared</span></div><div><strong>${nfmt(r.foes)}</strong><span>Foes defeated</span></div><div><strong>${nfmt(r.level)}</strong><span>Expedition level</span></div></div>
      <p class="camp-record">${glyph('flag')}${recordNote(p,r)}</p>
      <section class="camp-reward"><div class="camp-reward-head"><h3>Seeds earned</h3><strong>+${nfmt(r.lastAward)}</strong></div>
        <dl>${breakdown.map(line=>'<div><dt>'+esc(line.label)+'</dt><dd>+'+nfmt(line.amount)+'</dd></div>').join('')}</dl>
      </section>
      ${expeditionRanks(r)}
      <div class="camp-shop-head"><div><span class="camp-caption">KEEP BETWEEN RUNS</span><h3>Legacy upgrades</h3></div><strong>${glyph('seed')}${seeds(p.points)}</strong></div>
      ${perkShop(p,true)}
      <div class="camp-actions"><button type="button" class="primary" data-do="start">${glyph('play')}Start next expedition</button><button type="button" class="flat" data-nav="legacy">View legacy</button></div>
    </div>`;
  }
  function legacy(p) {
    const r=p.run,traveling=r&&r.phase!=='dead';
    const account=`<div class="camp-wallet frame"><span class="section-icon">${glyph('seed')}</span><div><span class="camp-caption">TRAIL SEED BALANCE</span><strong>${seeds(p.points)}</strong></div><p>${traveling?'This run has earned '+seeds(E.awardBreakdown(r).reduce((sum,line)=>sum+line.amount,0))+'. It will be added to your balance when Pip falls.':'Buy a permanent rank here before Pip sets out.'}</p></div>`;
    const recap=r?.phase==='dead'?`<section class="frame camp-last"><div><span class="camp-caption">LAST EXPEDITION</span><h3>${esc(regions[r.region].name)} · Wave ${nfmt(r.round)}</h3><p>${nfmt(r.foes)} foes defeated · +${seeds(r.lastAward)}</p></div><button type="button" class="outline compact" data-dialog="debrief">Run results</button></section>`:'';
    return lead('THE TRAILHEAD','Legacy','Invest trail seeds in the health, relic power and replay pace Pip brings to every expedition.')+
      account+perkShop(p)+
      '<p class="camp-explainer">'+glyph('road')+'<span>Trail memory speeds up cleared waves. Pip returns to 1× on new ground. Gear and expedition talents are collected anew each run.</span></p>'+
      recap+'<div class="camp-actions">'+(traveling?'<button type="button" class="outline" data-nav="expedition">'+glyph('road')+'Return to expedition</button>':'<button type="button" class="primary" data-do="start">'+glyph('play')+(p.deaths?'Start next expedition':'Begin expedition')+'</button>')+'</div>';
  }
  window.Journal={guide,legacy,debrief,paintPortraits,traitText,afflictionText,elementText,relicEffects,styleLabel,relicSearchText,tierBadge,relicSource,routeAtlas,regionDossier};
})();
