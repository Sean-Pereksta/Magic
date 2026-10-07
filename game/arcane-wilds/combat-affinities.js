'use strict';

/* One affinity resolver for direct hits, projectiles, fields, summons and DoTs.
 * The existing damage pipeline still owns shields, equipment, reactions and deaths.
 * Source metadata preserves elemental identity when an old projectile uses a
 * generic sprite or an upgrade tag such as iceExecute/chain instead of an element.
 */
(() => {
  if(window.AWCombatAffinity)return;
  const colors={fire:'#ff995b',frost:'#bcefff',lightning:'#96f4ff',earth:'#c9ac86',wind:'#c5f4dc',water:'#8bcdff',physical:'#e1bc91',arcane:'#c6a7ff',solar:'#fff0a4',shadow:'#ce97ff',poison:'#b8ef86',nature:'#9bd585'};
  const schools={Fire:'fire',Frost:'frost',Storm:'lightning',Lightning:'lightning',Arcane:'arcane',Celestial:'solar',Holy:'solar',Nature:'nature','Shadow/Void':'shadow',Shadow:'shadow',Void:'shadow',Earth:'earth',Wind:'wind',Water:'water'};
  const baseSpells={firebolt:'fire',frostnova:'frost',thorns:'nature',arcaneMissiles:'arcane',gust:'wind',ward:'arcane',chain:'lightning',poison:'poison',chakram:'physical',spirits:'arcane',quake:'earth',meteor:'fire',voidrift:'shadow',icelance:'frost',soulflame:'shadow',tempest:'lightning',timestop:'arcane',phoenix:'fire',starfall:'solar',singularity:'shadow',solarLance:'solar',stormSpear:'lightning'};
  function element(tag=''){
    if(/water|tidal|soak/i.test(tag))return 'water';
    if(/wind|gust|gale/i.test(tag))return 'wind';
    if(/earth|quake|stonewake/i.test(tag))return 'earth';
    if(/poison|venom|toxic|spore|rot(?:Seed)?/i.test(tag))return 'poison';
    if(/solar|celestial|holy|sunLance|star|constellation/i.test(tag))return 'solar';
    if(/frost|ice|glacial|snow|rime/i.test(tag))return 'frost';
    if(/fire|flame|ember|burn|meteor|phoenix|cinder/i.test(tag))return 'fire';
    if(/lightning|storm|thunder|tempest|fulgurite|overload/i.test(tag))return 'lightning';
    if(/shadow|void|soul|singularity|gravity|eclipse/i.test(tag))return 'shadow';
    if(/earth|impact|hammer|quake|physical|blade|arrow|chakram|weapon|wind|saw|bleed/i.test(tag))return 'physical';
    if(/nature|thorn|briar|root|bloom/i.test(tag))return 'nature';
    if(/arcane|prism|mirror|rune|time|spirit|wisp|chain|shard/i.test(tag))return 'arcane';
    return '';
  }
  function spellElement(id){
    const s=SPELLS[id];return baseSpells[id]||(/poison|venom|toxic|rotseed|rotbloom/i.test(id)?'poison':'')||schools[s?.category]||element(id)||'arcane';
  }
  function catalogTags(id,t){
    const name=id+' '+t.name,biomes=t.biomes||[],tags=new Set(t.combatTags||[]);
    if(/frost|ice|rime|glacier|glacial|shardburrow|prismscarab|shardram|glassoracle|shardcaster|prismMimic|regionalCrystal/i.test(name)||biomes.length===1&&['frost','crystal'].includes(biomes[0]))tags.add('ICE');
    if(/cinder|ember|magma|ash drake|phoenix|fire|flame/i.test(name)||biomes.length===1&&biomes[0]==='volcanic')tags.add('FIRE');
    if(/storm|thunder|galeweaver|skyraider|lightning/i.test(name)||biomes.length===1&&biomes[0]==='stormlands')tags.add('LIGHTNING');
    if(/thorn|briar|root|mossback|mosscolossus|behemoth|bloom|spore|shaman|plant/i.test(name))tags.add('PLANT');
    if(/wolf|hound|beast|boar|beetle|scarab|ram|charger|wasp|harpy|roc|drake|serpent|spider|moth|prowler|alpha/i.test(name))tags.add('BEAST');
    if(/golem|sentinel|construct|obelisk|crystal|prism|shardram|shardburrow|mimic/i.test(name))tags.add('CONSTRUCT');
    if(/skeleton|necro|revenant|vampire|grave|bone|leech|ashchoir|crypt blade/i.test(name))tags.add('UNDEAD');
    if(/void|gloam|dusk|shadow|eclipse|abyss|riftstalk|sunless|veil|sunless/i.test(name)||t.shadowOnly||biomes.length===1&&biomes[0]==='gloam')tags.add('SHADOW');
    if(/mage|witch|oracle|shaman|cultist|wisp|seer|caster|priest|weaver|starcaller|sunwarden|puppeteer|shepherd|necro|voidbinder|mirror/i.test(name))tags.add('ARCANE');
    if(/knight|guard|warden|golem|beetle|sentinel|scarab|juggernaut|chainwarden|executioner|captain/i.test(name)||t.ai==='shield')tags.add('ARMORED');
    if(/harpy|wasp|moth|phoenix|roc|drake|skyrazor|skyraider|seraph/i.test(name))tags.add('FLYING');
    if(/tide|siren|leviathan|trench|water/i.test(name))tags.add('WET');
    if(/bandit|archer|assassin|witch|mage|shaman|cultist|priest|executioner|captain|knight/i.test(name)&&!tags.has('UNDEAD')&&!tags.has('CONSTRUCT'))tags.add('HUMANOID');
    return [...tags];
  }
  for(const [id,t] of Object.entries(ENEMY_TYPES))t.combatTags=catalogTags(id,t);
  function tags(e){
    if(!e._combatTags)e._combatTags=new Set([...(ENEMY_TYPES[e.type]?.combatTags||[]),...(e.combatTags||[])]);
    // Shadow traits and wards can be assigned after spawnEnemy returns.
    if(e.shadow)e._combatTags.add('SHADOW');
    if(e.shadowTraits?.includes('Frozen'))e._combatTags.add('ICE');
    if(e.shadowTraits?.includes('Burning'))e._combatTags.add('FIRE');
    if(e.shadowTraits?.includes('Armored'))e._combatTags.add('ARMORED');
    return e._combatTags;
  }
  function resolve(kind,e){
    const t=tags(e),has=k=>t.has(k),wet=has('WET')||e.wet>0||e.regionalWet>0||e.inWater===true;
    if(kind==='fire'&&(has('ICE')||has('PLANT')))return {multiplier:1.35,label:has('ICE')?'MELTED!':'IGNITED!',color:colors.fire};
    if(kind==='frost'&&has('FIRE'))return {multiplier:1.3,label:'EXTINGUISHED!',color:colors.frost};
    if(kind==='fire'&&has('UNDEAD'))return {multiplier:1.25,label:'SEARED!',color:colors.fire};
    if(kind==='fire'&&wet)return {multiplier:.75,label:'DAMPENED',color:colors.water};
    if(kind==='water'&&has('FIRE'))return {multiplier:1.35,label:'QUENCHED!',color:colors.water};
    if(kind==='earth'&&(has('ARMORED')||has('CONSTRUCT')||e.shield>0))return {multiplier:1.45,label:'ARMOR BREAK!',color:colors.earth};
    if(kind==='wind'&&e.combatAbility)return {multiplier:1.15,label:'INTERRUPT!',color:colors.wind};
    if(kind==='lightning'&&(wet||has('ARMORED')))return {multiplier:wet?1.4:1.3,label:'OVERLOAD!',color:colors.lightning};
    if(kind==='physical'&&has('CONSTRUCT'))return {multiplier:1.3,label:'SHATTER!',color:colors.physical};
    if(kind==='arcane'&&(e.shield>0||e.magicShield>0)&&has('ARCANE'))return {multiplier:1.35,label:'WARD BREAK!',color:colors.arcane};
    if(kind==='solar'&&(has('UNDEAD')||has('SHADOW')))return {multiplier:1.4,label:'PURIFIED!',color:colors.solar};
    if(kind==='shadow'&&has('ARCANE'))return {multiplier:1.3,label:'DISRUPTED!',color:colors.shadow};
    if(kind==='poison'){
      if(has('CONSTRUCT')||has('UNDEAD'))return {multiplier:.8,label:'RESIST',color:colors.poison};
      if(has('BEAST')||has('HUMANOID'))return {multiplier:1.25,label:'VENOM!',color:colors.poison};
    }
    if(kind==='fire'&&has('FIRE')||kind==='frost'&&has('ICE')||kind==='lightning'&&has('LIGHTNING')||kind==='shadow'&&has('SHADOW'))return {multiplier:.85,label:'RESIST',color:colors[kind]};
    return {multiplier:1,label:'',color:colors[kind]||'#e0d5ff'};
  }
  let castSource=null,hitSource=null,secondary=false;
  const oldRadial=radialDamage;
  radialDamage=function(x,y,r,amount,tag='',knock=0,source=null){
    const previous=hitSource;hitSource=source||{x,y,combatElement:castSource?.combatElement||element(tag)};
    try{return oldRadial(x,y,r,amount,tag,knock);}finally{hitSource=previous;}
  };
  // Wrap the final cast table, including regional spells and their item wrappers.
  for(const key of Object.keys(SPELL_CASTS)){
    const fn=SPELL_CASTS[key];SPELL_CASTS[key]=function(id,...args){
      const previous=castSource;castSource={spellId:id,combatElement:spellElement(id)};
      const priorSummons=new Set(game.summons),priorActors=new Set(window.AWContinentalSpells?.state().actors||[]);
      try{return fn(id,...args);}finally{
        const spawned=[...game.summons.filter(s=>!priorSummons.has(s)),...(window.AWContinentalSpells?.state().actors||[]).filter(s=>!priorActors.has(s))];
        for(const s of spawned){
          s.combatTags=['treant','seed'].includes(s.kind)||castSource.combatElement==='nature'?['PLANT']:castSource.combatElement==='frost'?['ICE']:castSource.combatElement==='fire'?['FIRE']:['ARCANE'];
          if(['sentinel','mirror','crystal'].includes(s.kind))s.combatTags.push('CONSTRUCT','ARMORED');
          s.r=s.r||.25;if(s.hp===undefined)s.hp=s.maxHp=30;
        }
        castSource=previous;
      }
    };
  }
  const oldProjectile=magicProjectile;
  magicProjectile=function(opts={}){
    const q=oldProjectile(opts);if(!q)return q;
    q.combatElement=opts.combatElement||castSource?.combatElement||element(opts.tag)||(opts.trail==='weapon'?'physical':element(opts.kind))||'arcane';
    q.damageOrigin={x:opts.x??game.player.x,y:opts.y??game.player.y};
    q.spellId=castSource?.spellId||null;return q;
  };
  function enemyElement(e){
    const t=tags(e);
    return e.combatElement||(t.has('FIRE')?'fire':t.has('ICE')?'frost':t.has('LIGHTNING')?'lightning':t.has('SHADOW')?'shadow':t.has('PLANT')?'nature':t.has('ARCANE')?'arcane':'physical');
  }
  const oldEnemyProjectile=enemyProjectile;
  enemyProjectile=function(e,...args){
    const count=game.projectiles.length,result=oldEnemyProjectile(e,...args);
    for(let i=count;i<game.projectiles.length;i++)game.projectiles[i].combatElement=enemyElement(e);
    return result;
  };
  const oldGround=groundEffect;
  groundEffect=function(...args){const h=oldGround(...args);if(h)h.combatElement=castSource?.combatElement||element(h.kind);return h;};
  function feedback(e,kind,result,dot){
    if(dot||result.multiplier===1||(e.affinityFeedbackAt??-1)>elapsed)return;
    e.affinityFeedbackAt=elapsed+.65;e.affinityTint=result.color;e.affinityFlash=.22;
    floatText(e.x,e.y-.3,result.label,result.color);
    if(result.multiplier>1){
      window.AWPresentation?.fx.bursts.add({x:e.x,y:e.y,color:result.color,kind:'impact',life:.32,size:1.25});
      window.AWPresentation?.audio.play('perfect',result.color);
      burst(e.x,e.y,result.color,10,.7,12);
      if(kind==='fire'&&tags(e).has('ICE')){burst(e.x,e.y,'#e1faff',8,.75,18);fx('frostNova',e.x,e.y,.25,'#c3f3ff',{r:.55});}
      if(kind==='lightning')fx('lightning',e.x-.35,e.y,.22,result.color,{toX:e.x+.35,toY:e.y-.3,width:2});
      if(kind==='physical')window.AWPresentation?.fx.decals.add({x:e.x,y:e.y,color:result.color,kind:'crack',life:.5,size:23});
      if(kind==='solar'){burst(e.x,e.y,'#fff7df',6,.8,16);burst(e.x,e.y,'#786190',5,.5,12);}
    }
  }
  const oldHit=damageEnemy;
  damageEnemy=function(e,amount,tag='',dot=false,source=null){
    if(!e||e.dead||!Number.isFinite(amount)||amount<=0)return;
    source=source||hitSource||castSource;
    const kind=source?.combatElement||element(tag),result=resolve(kind,e);
    let damage=amount*result.multiplier;
    const origin=source?.damageOrigin||(Number.isFinite(source?.x)?source:game.player);
    if(e.combatCounter&&origin&&Number.isFinite(origin.x)&&!dot){
      const d=norm(origin.x-e.x,origin.y-e.y),f=e.combatCounter.dir;
      if(d.x*f.x+d.y*f.y>.3&&!['lightning','arcane','shadow'].includes(kind)){
        if(!['regional_moss','aw30_mirrorknight'].includes(e.ai))damage*=.5;
        fx('shieldHit',e.x,e.y,.2,'#cadfff',{r:e.r*1.5});
        e.combatCounter.retaliate=true;
      }
    }
    if(kind==='fire'&&tags(e).has('ICE')){e.slow=Math.max(0,(e.slow||0)-1);if(e.shield>0)e.shield=Math.max(0,e.shield-amount*.25);}
    const before=e.hp+(e.shield||0);
    // A solar beam should not accidentally trigger fire-only item bonuses.
    const actualTag=kind==='solar'&&tag==='fire'?'solar':tag;
    const priorNumber=$('floatLayer').lastElementChild;
    oldHit(e,damage,actualTag,dot);
    const number=$('floatLayer').lastElementChild;
    if(!dot&&result.multiplier>1&&number&&number!==priorNumber&&/^\d+$/.test(number.textContent)){
      number.style.fontSize='19px';number.style.color=result.color;
    }
    if(e.hp+(e.shield||0)>=before)return;
    feedback(e,kind,result,dot);
    if(kind==='frost'&&tags(e).has('FIRE')){
      e.combatSuppressed=Math.max(e.combatSuppressed||0,1.4);
      window.AWEnemyCombat?.extinguish(e,1.6);
    }
    if(kind==='frost'&&e.speed>=2.3)e.slow=Math.max(e.slow||0,1.2);
    if(kind==='water'){e.wet=3;if(tags(e).has('FIRE'))window.AWEnemyCombat?.extinguish(e,1.8);}
    if(kind==='earth'&&result.multiplier>1){e.shield=Math.max(0,(e.shield||0)-amount*.5);e.dangerArmorBroken=3;e.stun=Math.max(e.stun||0,e.boss?.12:.45);}
    if(kind==='wind'&&!dot&&e.combatAbility){e.stun=Math.max(e.stun||0,e.boss?.12:.6);if(!e.boss)window.AWEnemyCombat?.cancel(e);}
    if(kind==='arcane'&&result.multiplier>1&&e.shield<=0)e.stun=Math.max(e.stun||0,e.boss?.12:.35);
    if(kind==='shadow'&&result.multiplier>1)e.combatSuppressed=Math.max(e.combatSuppressed||0,.75);
    if(!dot&&!secondary&&result.multiplier>1&&(e.affinityProcAt??-1)<=elapsed){
      e.affinityProcAt=elapsed+1.1;
      if(kind==='lightning')e.stun=Math.max(e.stun||0,e.boss?.08:.18);
      const propagation=kind==='fire'&&tags(e).has('PLANT')||kind==='lightning'&&tags(e).has('ARMORED');
      if(propagation){
        const target=nearestEnemy(e,kind==='fire'?1.9:3,o=>o!==e&&!o.dead&&tags(o).has(kind==='fire'?'PLANT':'ARMORED'));
        if(target){secondary=true;try{damageEnemy(target,amount*.18,kind,true);fx(kind==='fire'?'soulLink':'lightning',e.x,e.y,.2,result.color,{toX:target.x,toY:target.y,width:2});}finally{secondary=false;}}
      }
    }
  };
  const oldDraw=drawEnemy;
  drawEnemy=function(e){oldDraw(e);if(!(e.affinityFlash>0))return;const s=worldToScreen(e.x,e.y,14);ctx.save();ctx.strokeStyle=e.affinityTint;ctx.globalAlpha=clamp(e.affinityFlash*4,0,1);ctx.lineWidth=2;ctx.beginPath();ctx.ellipse(s.x,s.y,e.r*45,e.r*28,0,0,TAU);ctx.stroke();ctx.restore();};
  window.AWCombatAffinity={element,spellElement,enemyElement,tags,resolve,colors};
})();

