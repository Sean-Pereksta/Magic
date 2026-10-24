/** Extensions to Faction Forge's existing tile simulation. No clocks, random
 * numbers, DOM operations, or account writes: the browser and authoritative
 * replay invoke the same hooks with their simulation time and grid helpers. */
import {afterComboAttack,tickCombos,noteComboDamage,noteComboAmbush} from './combos.js';
import {applyEvolution,afterEvolutionAttack,tickEvolutions} from './evolutions.js';
import {COMBAT_LIMITS,calculateCounterDamage} from './combat-rules.js';
export * from './combat-rules.js';
export {COMBOS,COMBO_BY_ID,COMBO_LIMITS,comboStatus} from './combos.js';
export {EVOLUTIONS,RANK_ENHANCEMENTS,evolutionFor,evolutionDescription,applySummonEvolution} from './evolutions.js';
export const NEW_UNIT_KEYS = Object.freeze([
  'railbreaker', 'signalhawk', 'chronoweaver', 'miragewarden', 'siphonleech', 'hiveherald'
]);
export const UNIT_LIMITS = Object.freeze({
  temporalFieldsPerSide: 4, decoysPerSide: 1, summonsPerSide: 2,
  summonsPerParent: 1, aliveUnitsPerSide: COMBAT_LIMITS.activePerSide, markBonus: 1,
  minimumCooldownFactor: .65, maximumCooldownFactor: 1.4
});
export const NEW_UNITS = Object.freeze({
  railbreaker: {faction:'Vanir',name:'Railbreaker',emoji:'⚡',role:'back',armorType:'armored',
    base:{hp:14,dmg:7,range:7,cd:1900,spd:1,armor:1},strongVs:['armored'], rail:true},
  signalhawk: {faction:'Vanir',name:'Signal Hawk',emoji:'🛰️',role:'back',
    base:{hp:7,dmg:1,range:5,cd:1300,spd:2,armor:0},signal:true},
  chronoweaver: {faction:'Aesir',name:'Chronoweaver',emoji:'⌛',role:'back',
    base:{hp:9,dmg:2,range:4,cd:1300,spd:1,armor:0},temporal:true},
  miragewarden: {faction:'Aesir',name:'Mirage Warden',emoji:'🪞',role:'back',
    base:{hp:12,dmg:3,range:3,cd:1200,spd:1,armor:1},mirage:true},
  siphonleech: {faction:'Helions',name:'Siphon Leech',emoji:'🩸',role:'front',
    base:{hp:18,dmg:5,range:1,cd:1000,spd:2,armor:1},siphon:true},
  hiveherald: {faction:'Helions',name:'Hive Herald',emoji:'🦗',role:'back',
    base:{hp:14,dmg:3,range:3,cd:1100,spd:1,armor:1},herald:true}
});
export const NEW_LEGENDARIES = Object.freeze({
  railbreaker:{name:'Overload Shot',desc:'Every third charged rail shot pierces two additional enemies with stronger penetration.',notes:'3-shot cycle; no splash.'},
  signalhawk:{name:'Coordinated Strike',desc:'Marks two targets per scan. Nearby allies gain +1 damage; marks never stack.',notes:'Two marks; 2-tile ally radius.'},
  chronoweaver:{name:'Time Fracture',desc:'Every third field briefly slows enemy cooldowns more strongly.',notes:'Bounded temporal field.'},
  miragewarden:{name:'Mirror Legion',desc:'Creates a decoy, or a temporary mirror shield while a decoy or other summoned allies are already active.',notes:'One decoy; summoned allies share a bounded ability budget.'},
  siphonleech:{name:'Blood Communion',desc:'One point of excess siphoned healing can restore a nearby damaged Helions ally.',notes:'Wound reduces self/ally healing.'},
  hiveherald:{name:'Hive Frenzy',desc:'A nearby swarm death grants surviving swarms brief haste, without additive stacking.',notes:'Frenzy lasts 2 seconds.'}
});
const counters = {
  railbreaker:['Precision Anti-Armor','Armor Piercing / Slow Burst',['Armored tanks','Large units','Aligned backlines'],['Fast swarms','Assassins','Close combat'],['Vanir Guard','Signal Hawk','Suppressor'],'Protect its charge and line up enemies for penetration.','A swarm can overwhelm its slow reload.'],
  signalhawk:['Recon Support','Target Marking / Reveal',['Ambushers','Burrowers','Single-target lines'],['Marksman','Burst damage','Assassins'],['Riflemen','Railbreaker','Vanir Guard'],'Keep ranged allies close enough to benefit from marked targets.','It has little personal damage and a fragile body.'],
  chronoweaver:['Temporal Support','Tempo / Control',['Slow attackers','Sustained brawls','Heavy frontline'],['EMP','Assassins','Burst damage'],['Blademasters','Phasers','Guardian'],'Center its field on your sustained damage units.','Fields expire and never additively stack.'],
  miragewarden:['Defensive Illusionist','Decoy / Target Disruption',['Single-target burst','Assassins','Snipers'],['Splash','Chain lightning','Sustained AoE'],['Phasers','Storm Seer','Wardmaker'],'Place decoys between fragile allies and single-target threats.','Decoys have no damage and dissolve quickly.'],
  siphonleech:['Sustain Skirmisher','Melee Life Drain',['Light melee','Isolated opponents','Attrition fights'],['Armored tanks','Ranged burst','Wound'],['Defiler','Carrion Priest','Hive Herald'],'Use it in sustained close fights, supported against heavy armor.','Healing depends on actual damage and respects wound.'],
  hiveherald:['Swarm Commander','Swarm / Tempo Support',['Single-target lines','Slow reload units','Attrition'],['Marksman','Splash','Assassins'],['Hell Spawn','Brood Matron','Defiler'],'Keep swarms within three tiles for focus fire and haste.','Its aura is ineffective when isolated.']
};
export const NEW_ARCHETYPES = Object.freeze({railbreaker:['control'],signalhawk:['control'],chronoweaver:['control','sustain'],miragewarden:['control','sustain'],siphonleech:['sustain'],hiveherald:['swarm','control']});

export function installUnits({TROOPS,FACTIONS,LEGENDARY_ABILITIES,TROOP_COUNTERS,TROOP_ARCHETYPES,TROOP_VISUALS}) {
  for (const key of NEW_UNIT_KEYS) {
    const t = NEW_UNITS[key];
    TROOPS[key] = {...t,base:{...t.base},unlockable:true};
    if (FACTIONS[t.faction] && !FACTIONS[t.faction].includes(key)) FACTIONS[t.faction].push(key);
    if (LEGENDARY_ABILITIES) LEGENDARY_ABILITIES[key] = {...NEW_LEGENDARIES[key]};
    if (TROOP_ARCHETYPES) TROOP_ARCHETYPES[key] = [...NEW_ARCHETYPES[key]];
    if (TROOP_COUNTERS) {
      const [roleLabel,archetype,strongAgainst,weakAgainst,bestWith,counterTip,dangerTip] = counters[key];
      TROOP_COUNTERS[key] = {roleLabel,archetype,strongAgainst,weakAgainst,bestWith,counterTip,dangerTip,
        counterArchetype:NEW_ARCHETYPES[key][0],secondaryArchetype:NEW_ARCHETYPES[key][1]||null};
    }
    if (TROOP_VISUALS) TROOP_VISUALS[key] = {factionClass:'faction'+t.faction,
      roleClass:t.role==='front'?'roleBruiser':'roleSupport',
      idleClass:t.faction==='Helions'?'idleDarkFloat':t.faction==='Aesir'?'idleCasterFloat':'idleMechanicalPulse',
      actionClass:t.role==='front'?'venomStrike':t.faction==='Vanir'?'precisionShot':'phaseCast'};
  }
  return TROOPS;
}

const clampTier = value => Math.max(1,Math.min(3,Number(value)||1));
const alive = u => u && u.hp>0;
const distance = (a,b,hooks={}) => hooks.distance ? hooks.distance(a,b) : Math.max(Math.abs(a.x-b.x),Math.abs(a.y-b.y));
const disabled = (u,now) => (u.statusShockUntil||0)>now || (u.bindUntil||0)>now;
const isSwarm = u => u.type==='hellspawn' || (u.summoned && u.type==='blood');
const emit = (hooks,kind,u,target,now) => hooks.event?.({kind,sourceId:u.id,targetId:target?.id,x:u.x,y:u.y,targetX:target?.x,targetY:target?.y,at:now});

/** battleRank mirrors legacy lvl. It never derives from permanent forgeTier. */
export function initializeForgeUnit(u,{forgeTier=1,ranked=false,role='back',applyTier=true}={}) {
  u.battleRank = clampTier(u.lvl||u.battleRank);
  u.forgeTier = clampTier(forgeTier);
  u.effectiveForgeTier = ranked ? 1 : u.forgeTier;
  u.legendary = u.battleRank>=3;
  // BloodBurst's signature explosion is a real splash even at starting rank.
  if(u.type==='blood')u.splash1=true;
  if (u.illusion || u._forgeInitialized) return u;
  u._forgeInitialized = true;
  const boost = applyTier ? u.effectiveForgeTier-1 : 0;
  const support = ['medic','wardmaker','architect','matron','chronoweaver','signalhawk','miragewarden','hiveherald'].includes(u.type);
  if (boost) {
    const hpFactor = role==='front' ? .08 : .04;
    u.maxhp = Math.max(1,Math.round(u.maxhp*(1+hpFactor*boost))); u.hp = u.maxhp;
    if (support) {
      for (const key of ['healCd','wardCd','wardHealCd','broodCd']) if (u[key]>0) u[key] = Math.round(u[key]*(1-.04*boost));
    } else if (role==='back') u.cd = Math.max(150,Math.round(u.cd*(1-.04*boost)));
    for (const key of ['woundDur','bindDur','suppressDur','shockDur']) if (u[key]>0) u[key]=Math.round(u[key]*(1+.04*boost));
  }
  // New abilities' role improvements are small duration/rate bonuses.
  u.forgeAbilityScale = 1+.06*boost;
  return u;
}

/** Apply the already-drafted formation, relics and tactics exactly once to a
 * freshly rebuilt army. Placement plans reserve whole 2×2 footprints. */
export function applyArmyOptions(st,now,hooks={}) {
  const field=st.field,units=field.units||[],token=units[0]?.id||'empty';
  if (field.armyOptionsAppliedFor===token) return;
  field.armyOptionsAppliedFor=token;field.medkitUsed={p0:false,p1:false};
  const width=hooks.width||8,height=hooks.height||16;
  for (let p=0;p<2;p++) {
    const side=p===0?'p0':'p1',relics=new Set(st.relics?.[side]||[]),tactics=new Set(st.tactics?.[side]||[]);
    const allies=units.filter(v=>alive(v)&&v.p===p&&!v.illusion),dir=p===0?-1:1;
    const sideMin=p===0?Math.floor(height/2):0,sideMax=p===0?height:Math.floor(height/2);
    let formation=st.formations?.[side]||'balanced';
    if (formation==='balanced'&&tactics.has('scatter_formation')) formation='wide';
    if (hooks.rectangleFree) {
      const reserved=units.filter(v=>alive(v)&&v.p!==p),plans=[];let valid=true;
      const ordered=allies.slice().sort((a,b)=>{
        if((a.size||1)!==(b.size||1))return (b.size||1)-(a.size||1);
        if(formation==='spearhead')return (hooks.roleFor?.(a.type)==='front'?0:1)-(hooks.roleFor?.(b.type)==='front'?0:1)||b.maxhp-a.maxhp;
        return 0;
      });
      const counters={front:0,back:0};
      for (const u of ordered) {
        const size=u.size===2?2:1,role=hooks.roleFor?.(u.type)||((u.range||1)>1?'back':'front'),index=counters[role]++;
        const frontRow=p===0?Math.floor(height/2)+2:Math.floor(height/2)-3;
        const backRow=p===0?Math.floor(height/2)+5:Math.floor(height/2)-6;
        let x=[3,4,2,5,1][index%5],y=role==='front'?frontRow:backRow;
        if(formation==='wide')x=index%2?width-size-Math.floor(index/2)*2:Math.floor(index/2)*2;
        else if(formation==='protected_backline'){x=[3,4,2,5,1][index%5];if(role==='back')y=frontRow-dir*2;}
        else if(formation==='spearhead'&&role==='front'&&index===0){x=Math.floor((width-size)/2);y+=dir;}
        else if(formation==='ambush'&&['miner','blade','viper','burrower','hellspawn','blood'].includes(u.type)){x=index%2?width-size:0;y+=dir;}
        x=Math.max(0,Math.min(width-size,x));y=Math.max(sideMin,Math.min(sideMax-size,y));
        const candidates=[[x,y],[x,u.y],[u.x,u.y]];
        for(let row=p===0?height-size:0;p===0?row>=Math.floor(height/2):row<Math.floor(height/2);row+=dir){
          // A fixed side-local scan is the fallback, never enemy-side placement.
          for(let col=0;col<=width-size;col++)candidates.push([col,row]);
        }
        const tile=candidates.find(([cx,cy])=>cy>=sideMin&&cy+size<=sideMax&&hooks.rectangleFree(reserved,cx,cy,size));
        if(tile){const placed={...u,x:tile[0],y:tile[1]};reserved.push(placed);plans.push({u,x:tile[0],y:tile[1]});}
        else{valid=false;break;}
      }
      if(valid)for(const plan of plans){plan.u.x=plan.x;plan.u.y=plan.y;}
    }
    for (const u of allies) {
      const path=st.evolutions?.[side]?.[u.type]||st.roster?.[side]?.find(r=>r.type===u.type&&r.evolution)?.evolution||'a';
      applyEvolution(u,path);
      const role=hooks.roleFor?.(u.type)||((u.range||1)>1?'back':'front');
      if(relics.has('banner_frontline')&&role==='front'){u.maxhp+=2;u.hp+=2;}
      if(relics.has('sharpshooter_doctrine')&&role==='back'){u.range+=1;u.maxhp=Math.max(1,u.maxhp-1);u.hp=Math.min(u.hp,u.maxhp);}
      if(relics.has('anti_armor_rounds')&&(u.range||1)>1)u.antiArmorRounds=true;
      if(relics.has('poisoned_blades')&&(u.range||1)<=1)u.poisonedBlades=true;
      if(relics.has('arcane_battery'))u.arcaneBatteryReady=true;
      if(tactics.has('forced_march')&&role==='front'&&u.y+dir>=sideMin&&u.y+dir+(u.size||1)<=sideMax&&hooks.canMoveTo?.(u,units,u.x,u.y+dir))u.y+=dir;
    }
    if(tactics.has('bodyguard')){
      const body=allies.filter(u=>(hooks.roleFor?.(u.type)||((u.range||1)>1?'back':'front'))==='back').sort((a,b)=>b.dmg-a.dmg||b.maxhp-a.maxhp)[0];
      if(body){body.startShield=3;body.startShieldUntil=now+6000;}
    }
    if(tactics.has('cleanse'))for(const key of ['poison','toxic','acid'])field[key]=(field[key]||[]).filter(t=>p===0?t.y<height/2:t.y>=height/2);
  }
  for(let p=0;p<2;p++){
    const side=p===0?'p0':'p1',tactics=new Set(st.tactics?.[side]||[]),enemies=units.filter(v=>alive(v)&&v.p!==p&&!v.illusion);
    if(tactics.has('sabotage')&&enemies.length){const target=enemies.slice().sort((a,b)=>b.maxhp-a.maxhp)[0];target.hp=Math.min(target.hp,Math.max(1,Math.round(target.maxhp*.85)));}
    if(tactics.has('opening_volley')&&enemies.length){const row=p===0?Math.max(...enemies.map(v=>v.y+(v.size||1)-1)):Math.min(...enemies.map(v=>v.y));for(const target of enemies)if(target.y<=row&&target.y+(target.size||1)-1>=row)target.hp-=2;}
  }
}

export function canSpawnSummon(st,parent,illusion=false) {
  const units = st.field.units||[];
  return alive(parent)&&!parent.illusion&&
    units.filter(v=>alive(v)&&v.p===parent.p&&v.summoned).length<UNIT_LIMITS.summonsPerSide &&
    units.filter(v=>alive(v)&&v.summonedBy===parent.id).length<UNIT_LIMITS.summonsPerParent &&
    (!illusion || units.filter(v=>alive(v)&&v.p===parent.p&&v.illusion).length<UNIT_LIMITS.decoysPerSide);
}

/** tick once per simulation logic tick, before targets/attacks are processed. */
export function tickForgeAbilities(st,now,hooks={}) {
  const field=st.field, units=field.units||[];
  tickCombos(st,now,hooks);
  tickEvolutions(st,now,hooks);
  for(let p=0;p<2;p++){
    const side=p===0?'p0':'p1',relics=new Set(st.relics?.[side]||[]);
    if(relics.has('emergency_medkit')&&!field.medkitUsed?.[side]){
      const wounded=units.filter(v=>alive(v)&&v.p===p&&!v.illusion&&v.hp/v.maxhp<.35).sort((a,b)=>a.hp/a.maxhp-b.hp/b.maxhp)[0];
      if(wounded){const scale=hooks.healScale?.(wounded,now)??((wounded.woundUntil||0)>now?.5:1);wounded.hp=Math.min(wounded.maxhp,wounded.hp+Math.max(1,Math.round(4*scale)));(field.medkitUsed||(field.medkitUsed={}))[side]=true;}
    }
    if(relics.has('swarm_instinct'))for(const v of units)if(alive(v)&&v.p===p&&v.summoned&&!v.illusion&&!v.swarmInstinctApplied){v.swarmInstinctApplied=true;v.swarmBaseSpd=v.spd||1;v.spd+=1;v.swarmMoveUntil=now+3000;}
  }
  field.temporalFields = (field.temporalFields||[]).filter(f=>f.until>now && units.some(u=>alive(u)&&u.id===f.sourceId));
  for (const u of units) {
    if (!alive(u)) continue;
    if (u.illusion) {
      if ((u.expiresAt||0)<=now || !units.some(v=>alive(v)&&!v.illusion&&v.p===u.p)) u.hp=0;
      continue;
    }
    if ((u.heraldEmpoweredUntil||0)<=now && u.heraldEmpoweredBonus) {u.dmg-=u.heraldEmpoweredBonus;u.heraldEmpoweredBonus=0;}
    if((u.swarmMoveUntil||0)<=now&&u.swarmBaseSpd){u.spd=u.swarmBaseSpd;u.swarmBaseSpd=0;}
    if (disabled(u,now)) continue;
    const enemies=units.filter(v=>alive(v)&&v.p!==u.p&&!v.illusion);
    if (!enemies.length) continue;
    const ordered=enemies.slice().sort((a,b)=>distance(u,a,hooks)-distance(u,b,hooks) || (b.armor||0)-(a.armor||0));
    if (u.type==='signalhawk') {
      for (const v of enemies) if (distance(u,v,hooks)<=3) v.revealedUntil=Math.max(v.revealedUntil||0,now+700);
      if (now>=(u.nextSignal||0)) {
        const priority=v=>['wardmaker','architect','medic','matron','storm','railbreaker','vrocket'].includes(v.type)?0:1;
        const targets=ordered.filter(v=>distance(u,v,hooks)<=8).sort((a,b)=>priority(a)-priority(b)||distance(u,a,hooks)-distance(u,b,hooks)).slice(0,Math.min(2,u.signalTargets||(u.legendary?2:1)));
        for (const v of targets) {v.markedBySide=u.p;v.markedUntil=Math.max(v.markedUntil||0,now+Math.round((u.signalDuration||4000)*(u.forgeAbilityScale||1))); emit(hooks,'mark',u,v,now);}
        if (targets.length) {u.nextSignal=now+(u.signalCooldown||5000);u.animType='cast';u.animUntil=now+500;}
      }
      const close=ordered[0];
      if (close && distance(u,close,hooks)<=2 && now>=(u.nextEvade||0)) {
        const candidates=[[u.x,u.y+(u.p===0?1:-1)],[u.x-1,u.y],[u.x+1,u.y]];
        const safe=candidates.filter(([x,y])=>hooks.canMoveTo?.(u,units,x,y)).sort((a,b)=>distance({x:b[0],y:b[1]},close,hooks)-distance({x:a[0],y:a[1]},close,hooks));
        if (safe.length && distance({x:safe[0][0],y:safe[0][1]},close,hooks)>distance(u,close,hooks)) {u.x=safe[0][0];u.y=safe[0][1];u.nextEvade=now+1400;}
      }
    } else if (u.type==='chronoweaver' && now>=(u.nextTemporal||0)) {
      if (field.temporalFields.filter(f=>f.p===u.p).length<UNIT_LIMITS.temporalFieldsPerSide) {
        u.temporalCasts=(u.temporalCasts||0)+1;
        const fracture=u.legendary && u.temporalCasts%3===0;
        field.temporalFields.push({sourceId:u.id,p:u.p,x:u.x,y:u.y,radius:u.temporalRadius||2,
          until:now+Math.round((u.temporalDuration||(fracture?2000:2800))*(u.forgeAbilityScale||1))+(u.arcaneBatteryReady?500:0),haste:.88,slow:fracture?1.38:(u.temporalSlow||1.2),fracture});
        u.arcaneBatteryReady=false;
        u.nextTemporal=now+(u.temporalCooldown||6000);u.animType='cast';u.animUntil=now+500;emit(hooks,fracture?'time-fracture':'temporal-field',u,null,now);
      } else u.nextTemporal=now+800;
    } else if (u.type==='miragewarden' && now>=(u.nextMirage||0)) {
      const cap=1;
      const own=units.filter(v=>alive(v)&&v.illusion&&v.summonedBy===u.id).length;
      if (own<cap && canSpawnSummon(st,u,true) && hooks.spawnIllusion?.(st,u,now)) {
        u.nextMirage=now+(u.mirageCooldown||(u.legendary?2600:5000));u.animType='cast';u.animUntil=now+500;emit(hooks,'mirage',u,null,now);
      } else if(!canSpawnSummon(st,u,true)) {
        const ally=units.filter(v=>alive(v)&&v.p===u.p&&!v.illusion&&distance(u,v,hooks)<=3).sort((a,b)=>a.hp/a.maxhp-b.hp/b.maxhp)[0];
        if(ally){ally.comboShield=Math.max(ally.comboShield||0,u.legendary?3:2);ally.comboShieldUntil=now+2200;emit(hooks,'mirage',u,ally,now);}
        u.nextMirage=now+(u.mirageCooldown||5000);
      } else u.nextMirage=now+600;
    } else if (u.type==='hiveherald') {
      const focus=ordered[0];u.heraldFocusId=focus?.id;
      for (const v of units) if (alive(v)&&v.p===u.p&&v.summoned&&!v.illusion&&isSwarm(v)&&distance(u,v,hooks)<=(u.heraldRadius||3)&&!v.heraldWasEmpowered) {
        v.heraldWasEmpowered=true;v.heraldEmpoweredBonus=1;v.dmg+=1;v.heraldEmpoweredUntil=now+Math.round(3000*(u.forgeAbilityScale||1));
      }
    }
  }
}

export function attackCooldown(u,now,st,hooks={}) {
  let haste=1, slow=1;
  for (const f of st.field.temporalFields||[]) {
    const source=st.field.units.find(v=>v.id===f.sourceId);
    if (f.until<=now || !alive(source) || disabled(source,now) || distance(u,f,hooks)>f.radius) continue;
    if (u.p===f.p) haste=Math.min(haste,f.haste); else slow=Math.max(slow,f.slow);
  }
  if (isSwarm(u)) for (const herald of st.field.units) if (alive(herald)&&herald.p===u.p&&herald.type==='hiveherald'&&!disabled(herald,now)&&distance(herald,u,hooks)<=3) haste=Math.min(haste,.9);
  if ((u.hiveFrenzyUntil||0)>now) haste=Math.min(haste,.82);
  if ((u.broodSurgeUntil||0)>now) haste=Math.min(haste,.8);
  if((u.slowUntil||0)>now)slow=Math.max(slow,1.2);
  const factor=Math.max(UNIT_LIMITS.minimumCooldownFactor,Math.min(UNIT_LIMITS.maximumCooldownFactor,haste*slow));
  return Math.max(150,Math.round((u.cd||600)*factor));
}

export function markedDamageBonus(u,victim,now,st,hooks={}) {
  if (victim.markedBySide!==u.p || (victim.markedUntil||0)<=now) return 0;
  return st.field.units.some(h=>alive(h)&&h.p===u.p&&h.type==='signalhawk'&&!disabled(h,now)&&distance(u,h,hooks)<=2) ? UNIT_LIMITS.markBonus : 0;
}

export function chooseForgeTarget(u,units,now,hooks={}) {
  const enemies=units.filter(v=>alive(v)&&v.p!==u.p);
  if (!enemies.length) return null;
  if(['vrocket','marksman','railbreaker'].includes(u.type)) {
    const marked=enemies.filter(v=>!v.illusion&&v.markedBySide===u.p&&(v.markedUntil||0)>now&&distance(u,v,hooks)<=(u.range||1));
    if(marked.length)return marked.sort((a,b)=>a.hp-b.hp||distance(u,a,hooks)-distance(u,b,hooks))[0];
  }
  if (!u.splash1 && !u.chain3 && !u.kamikaze) {
    const nearest=Math.min(...enemies.filter(v=>!v.illusion).map(v=>distance(u,v,hooks)));
    const decoys=enemies.filter(v=>v.illusion&&distance(u,v,hooks)<=Math.min(u.range||1,nearest+1));
    if (decoys.length) return decoys.sort((a,b)=>distance(u,a,hooks)-distance(u,b,hooks))[0];
  }
  if (u.type==='railbreaker') {
    const armored=enemies.filter(v=>!v.illusion&&(v.armor>=2 || v.size===2)&&distance(u,v,hooks)<=(u.range||7));
    if (armored.length) return armored.sort((a,b)=>(b.armor||0)-(a.armor||0) || distance(u,a,hooks)-distance(u,b,hooks))[0];
  }
  if (isSwarm(u)) {
    const heralds=units.filter(h=>alive(h)&&h.p===u.p&&h.type==='hiveherald'&&!disabled(h,now)&&distance(h,u,hooks)<=3);
    for (const h of heralds) {const focus=enemies.find(v=>v.id===h.heraldFocusId);if (focus&&distance(u,focus,hooks)<=(u.range||1)+2)return focus;}
  }
  return null;
}

/** Returns null for the legacy attack, true when Railbreaker handles the attack. */
export function attackForgeUnit(u,t,now,st,hooks={}) {
  if (u.type!=='railbreaker' || u.illusion) return null;
  if (now<(u.nextAtk||0)) return true;
  if (!u.railChargeUntil) {
    u.railChargeUntil=now+Math.round((u.railChargeMs||520)/(u.forgeAbilityScale||1));u.railTargetId=t.id;
    u.animType='cast';u.animUntil=u.railChargeUntil;emit(hooks,'rail-charge',u,t,now);return true;
  }
  if (now<u.railChargeUntil) return true;
  u.railChargeUntil=0;u.railShots=(u.railShots||0)+1;
  const overload=u.legendary&&u.railShots%3===0;
  const doHit=(v,scale=1)=> {
    let raw=Math.max(1,Math.round(u.dmg*scale)) + markedDamageBonus(u,v,now,st,hooks);
    raw=hooks.wardDamage ? hooks.wardDamage(u,v,raw,st.field) : raw;
    const aura=hooks.auraReduce ? hooks.auraReduce(v,st.field) : 0;
    const result=calculateCounterDamage({...u,armorPenetration:overload?1:.75},v,raw,{now,auraReduce:aura});
    hooks.applyDamage?.(v,result.damage,st);emit(hooks,'rail-impact',u,v,now);
  };
  doHit(t);
  const ux=u.x+((u.size||1)-1)/2,uy=u.y+((u.size||1)-1)/2;
  const tx=t.x+((t.size||1)-1)/2,ty=t.y+((t.size||1)-1)/2;
  const dx=tx-ux,dy=ty-uy,len=Math.hypot(dx,dy)||1;
  const behind=(st.field.units||[]).filter(v=>alive(v)&&v.p!==u.p&&v.id!==t.id).map(v=>{
    const vx=v.x+((v.size||1)-1)/2-ux,vy=v.y+((v.size||1)-1)/2-uy;
    return {v,projection:(vx*dx+vy*dy)/len,perpendicular:Math.abs(vx*dy-vy*dx)/len};
  }).filter(o=>o.projection>len+.25 && o.projection<=len+4 && o.perpendicular<=.55).sort((a,b)=>a.projection-b.projection);
  for (const hit of behind.slice(0,Math.min(3,(overload?2:1)+(u.railPierce||0)))) doHit(hit.v,overload?.8:.6);
  if (t.hp>0&&(t.size||1)===1&&(t.armor||0)<2) hooks.knock?.(u.x,u.y,t,st.field.units,false);
  u.animType='attack';u.animUntil=now+420;u.nextAtk=now+attackCooldown(u,now,st,hooks);
  emit(hooks,overload?'rail-overload':'rail-shot',u,t,now);
  afterForgeAttack(u,t,now,st,hooks);return true;
}

/** Each hook is invoked exactly once by the shared browser/server attack path. */
export function afterForgeAttack(u,t,now,st,hooks={}) {
  const priorBudget=st.field._secondaryBudget;
  st.field._secondaryBudget={sourceId:u.id,remaining:rankDamageBudget(u)};
  try {
    afterEvolutionAttack(u,t,now,st,hooks);
    afterComboAttack(u,t,now,st,hooks);
  } finally {
    if(priorBudget)st.field._secondaryBudget=priorBudget;else delete st.field._secondaryBudget;
  }
}
function rankDamageBudget(u) {return (u.lvl||u.battleRank||1)>=3?12:8;}
export function noteForgeDamage(victim,actualDamage,st,now,hooks={}) {
  victim.evolutionDamageTaken=Math.min(24,(victim.evolutionDamageTaken||0)+Math.max(0,actualDamage));
  noteComboDamage(victim,actualDamage,st,now,hooks);
}
export function noteForgeAmbush(u,t,now) {noteComboAmbush(u,t,now);}
export function absorbForgeShield(victim,damage,now) {
  if((victim.comboShieldUntil||0)>now&&(victim.comboShield||0)>0){const absorbed=Math.min(damage,victim.comboShield);victim.comboShield-=absorbed;return damage-absorbed;}
  return damage;
}

export function siphonAfterDamage(u,victim,actualDamage,now,st,hooks={}) {
  if (u.type!=='siphonleech' || u.illusion || actualDamage<=0 || !alive(u)) return;
  const scale=hooks.healScale ? hooks.healScale(u,now) : ((u.woundUntil||0)>now?.5:1);
  const raw=Math.min(2,Math.max(1,Math.round(actualDamage*.35*(u.forgeAbilityScale||1))));
  const amount=Math.max(0,Math.floor(raw*scale));
  const missing=Math.max(0,u.maxhp-u.hp),restored=Math.min(missing,amount);
  u.hp=Math.min(u.maxhp,u.hp+restored);
  if (restored) emit(hooks,'siphon',u,victim,now);
  if (u.legendary && amount>restored) {
    const ally=st.field.units.filter(v=>alive(v)&&v.p===u.p&&v.id!==u.id&&!v.illusion&&v.hp<v.maxhp&&distance(u,v,hooks)<=2&&
      (hooks.factionFor ? hooks.factionFor(v.type)==='Helions' : ['hellspawn','roach','defiler','blood','spitter','matron','viper','carrion','burrower','siphonleech','hiveherald'].includes(v.type))).sort((a,b)=>a.hp/a.maxhp-b.hp/b.maxhp)[0];
    if (ally) {const allyScale=hooks.healScale ? hooks.healScale(ally,now) : ((ally.woundUntil||0)>now?.5:1);ally.hp=Math.min(ally.maxhp,ally.hp+Math.floor(Math.min(1,amount-restored)*allyScale));}
  }
}

export function noteForgeDeath(victim,st,now,hooks={}) {
  if (!isSwarm(victim) || victim.illusion || victim._hiveDeathProcessed) return;
  victim._hiveDeathProcessed=true;
  for (const h of st.field.units) if (alive(h)&&h.p===victim.p&&h.type==='hiveherald'&&h.legendary&&!disabled(h,now)&&distance(h,victim,hooks)<=3) {
    for (const ally of st.field.units) if (alive(ally)&&ally.p===h.p&&isSwarm(ally)&&distance(h,ally,hooks)<=3) ally.hiveFrenzyUntil=Math.max(ally.hiveFrenzyUntil||0,now+2000);
    emit(hooks,'hive-frenzy',h,victim,now);
  }
}
