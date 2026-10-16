import {combatEvent,comboHit,COMBO_LIMITS} from './combos.js';

const path = (id,name,description,tradeoff,mods,signature) => Object.freeze({id,name,description,tradeoff,mods:Object.freeze(mods),signature});
const pair = (a,b) => Object.freeze({a:path('a',...a),b:path('b',...b)});
/** Two deliberately short, mutually exclusive specializations for all 32 units.
 * Rank II uses the first path until a choice is drafted. Forge tiers never pick
 * paths or ranks and ranked normalization therefore remains independent. */
export const EVOLUTIONS = Object.freeze({
  rifleman:pair(['Sustained Fire','Faster bursts build a third-shot follow-up.','Lighter individual rounds.',{cd:.86,dmg:.9},'burst'],['Penetrator Rounds','Shots bypass two armor; reload takes longer.','Slower sustained fire.',{cd:1.15,armorPierce:2},'pierce']),
  vrocket:pair(['Heavy Missiles','Heavy rockets add an outer blast ring.','Longer reload.',{cd:1.2,dmg:1.12,blastRadius:2},'outer-blast'],['Coordinated Salvo','Every third launch fires a smaller real missile.','Smaller primary blast.',{cd:.84,dmg:.88},'salvo']),
  vguard:pair(['Siegebreaker','Shield slams sweep additional adjacent enemies.','Slower shield swings.',{cd:1.12},'sweep'],['Bulwark','Periodic shield pulses protect nearby allies.','Lighter strikes.',{dmg:.85},'shield-pulse']),
  miner:pair(['Tunnel Duelist','Every third strike lands a quick second pick hit.','Less protection.',{armorAdd:-1,cd:.9},'burst'],['Saboteur','Strikes briefly bind a backline target on a cooldown.','Slower attacks.',{cd:1.15},'snare']),
  medic:pair(['Field Hospital','Healing reaches farther and pulses to nearby wounded troops.','More fragile medic.',{healRangeAdd:1,maxhp:.85},'heal-pulse'],['Trauma Specialist','Larger single heals with a slower recovery cycle.','Less frequent healing.',{healAmtAdd:2,healCd:1.25},'triage']),
  suppressor:pair(['Crossfire','Suppression spreads to one nearby enemy.','Weaker bullets.',{dmg:.85},'suppression-spread'],['Lockdown','Suppression also briefly reduces enemy range.','Longer reload.',{cd:1.18,suppressDur:1.25},'range-lock']),
  shieldcap:pair(['Shield Network','Defensive pulses reach a wider shield network.','Lighter melee hits.',{auraRangeAdd:1,dmg:.85},'shield-pulse'],['Counterguard','After taking repeated hits, retaliate with a shield strike.','Narrower protection radius.',{auraRangeAdd:-1},'counter']),
  marksman:pair(['Hunter Scope','Opening shots gain range and a piercing follow-up.','Longer reload.',{rangeAdd:1,cd:1.15},'pierce'],['Finishers','Hits on wounded enemies gain a bounded precision follow-up.','Shorter firing range.',{rangeAdd:-1,cd:.9},'execute']),
  emp:pair(['EMP Field','Disruption spreads through a small pulsing electrical field.','Longer grenade reload.',{cd:1.2},'emp-field'],['Rapid Disruptor','Grenades cycle faster with shorter disruption.','Less control per grenade.',{cd:.82,shockDur:.75},'emp-echo']),
  railbreaker:pair(['Overpenetration','Charged shots pierce an additional aligned enemy.','Longer charge and reload.',{cd:1.18,railChargeMs:650,railPierceAdd:1},'rail-pierce'],['Quick Capacitor','Shorter rail charge for rapid precision fire.','Reduced rail damage.',{cd:.84,dmg:.9,railChargeMs:300},'quick-rail']),
  signalhawk:pair(['Wideband Scanner','Scans mark a second priority enemy.','Longer scan cycle.',{signalTargets:2,signalCooldown:6200},'wide-scan'],['Rapid Targeting','Shorter scans maintain frequent priority marks.','Shorter mark duration.',{signalCooldown:3600,signalDuration:2800},'rapid-scan']),
  phaser:pair(['Phase Echo','Every third attack leaves an electrical echo.','Lighter primary bolts.',{dmg:.9,cd:.92},'burst'],['Afterimage','Blinking and periodic casts create a fragile decoy.','Longer attack recovery.',{cd:1.15},'illusion']),
  blade:pair(['Whirlwind Chain','Every third strike adds a rapid follow-up.','Reduced strike strength.',{dmg:.9,cd:.86},'burst'],['Crescent Sweep','Blade arcs sweep two additional adjacent foes.','Slower swings.',{cd:1.2},'sweep']),
  storm:pair(['Tempest Crown','Storms chain to more enemies.','Longer channel.',{cd:1.2,chainAdd:2},'storm-chains'],['Storm Rhythm','Frequent smaller storms leave brief charged ground.','Weaker lightning bolts.',{cd:.82,dmg:.85},'storm-field']),
  guardian:pair(['Earthshatter','Slams add a damaging outer shockwave.','Longer recovery.',{cd:1.2,blastRadius:2},'outer-blast'],['Aegis Wave','Slams pulse protective shields to nearby allies.','Lighter impact damage.',{dmg:.85},'shield-on-hit']),
  wardmaker:pair(['Resonant Wards','Wards last longer and emit bounded healing pulses.','Slower ward placement.',{wardDur:1.3,wardCd:1.2},'heal-pulse'],['Swift Wards','Places smaller wards more often.','Less ward health.',{wardCd:.78,wardHpAdd:-1},'ward-pulse']),
  architect:pair(['Arcane Turrets','Ward shots gain reach and damage.','Slower ward fire.',{wardDmgAdd:1,wardRangeAdd:1,wardCdShoot:1.18},'turret'],['Barrier Grid','Linked wards periodically shield nearby allies.','Lighter ward shots.',{wardDmgAdd:-1,wardCd:.9},'shield-pulse']),
  golem:pair(['Titan Sweep','Slow heavy blows sweep the frontline.','Slower attacks.',{cd:1.2,dmg:1.1},'sweep'],['Living Fortress','Repeated hits build a capped defensive shield.','Less attack damage.',{dmg:.85,rangedReduceAdd:1},'counter-shield']),
  runebinder:pair(['Binding Constellation','Binding chains reach one additional nearby enemy.','Longer cast cycle.',{cd:1.18},'bind-spread'],['Snap Runes','Rapid shorter binds create a damaging rune echo.','Shorter immobilization.',{cd:.82,bindDur:.7},'rune-echo']),
  chronoweaver:pair(['Temporal Horizon','Temporal fields cover a larger area.','Longer field recovery.',{temporalRadius:3,temporalCooldown:7200},'wide-time'],['Time Shatter','Short fields disrupt enemy tempo more strongly.','Shorter field duration.',{temporalDuration:1800,temporalSlow:1.35,temporalCooldown:5200},'strong-time']),
  miragewarden:pair(['Mirror Legion','Maintains two fragile illusions.','Shorter illusion lifetime.',{mirageCap:2,mirageDuration:2600},'many-mirrors'],['Prismatic Guardian','A single durable decoy casts a protective pulse.','Slower decoy creation.',{mirageHp:7,mirageCooldown:6200},'shield-pulse']),
  hellspawn:pair(['Frenzied Swarm','Every third bite adds a quick follow-up.','Fragile swarm bodies.',{maxhp:.9,cd:.86},'burst'],['Brood Armor','Living swarms build small protective shields.','Slower bites.',{cd:1.15,armorAdd:1},'counter-shield']),
  roach:pair(['Corrosive Saliva','Attacks leave a small short toxic field.','Slower firing.',{cd:1.15},'toxic-field'],['Skirmish Brood','Third attacks echo a rapid acid shot.','Lighter saliva hits.',{dmg:.9,cd:.86},'burst']),
  defiler:pair(['Plague Engine','Toxic strikes leave a longer plague field.','Slower strikes.',{cd:1.2},'toxic-field'],['Venom Carapace','Toxic attacks spread wound to nearby foes.','Lighter direct damage.',{dmg:.88},'wound-spread']),
  blood:pair(['Cataclysm','Detonation adds a damaging outer blast ring.','More fragile explosive bodies.',{dmg:1.2,maxhp:.85,blastRadius:2},'outer-blast'],['Blood Aftermath','Detonation leaves a short damaging blood field.','Smaller initial explosion.',{dmg:.85},'blood-field']),
  spitter:pair(['Acid Pools','Acid splashes leave a damaging bubbling pool.','Slower spit cycle.',{cd:1.18,acidDur:1.25},'acid-field'],['Caustic Volley','Every third spit adds a small acid projectile.','Shorter acid lifetime.',{cd:.86,acidDur:.8},'salvo']),
  matron:pair(['Endless Brood','Produces swarms more often.','Fragile newborn swarms.',{broodCd:.75,summonHpScale:.8},'fast-brood'],['Royal Brood','Newborn swarms gain health and stronger bites.','Slower brood production.',{broodCd:1.25,summonHpScale:1.3,summonDamageAdd:1},'royal-brood']),
  viper:pair(['Ambush Fang','Opening leap strikes gain a controlled follow-up.','Longer recovery.',{cd:1.18,leapDistAdd:1},'opening'],['Predator','Strikes against wounded targets gain execution damage.','Weaker opening strikes.',{dmg:.9,cd:.9},'execute']),
  carrion:pair(['Blood Curse','Wounds spread to one adjacent enemy.','Slower curse cycle.',{cd:1.15,woundDur:1.25},'wound-spread'],['Blood Offering','Curses pulse small healing to nearby wounded allies.','Shorter wound duration.',{woundDur:.75},'heal-pulse']),
  burrower:pair(['Deep Ambush','Emergence strikes briefly snare the enemy backline.','Longer burrow recovery.',{burrowCd:1.25,burrowDistAdd:1},'snare'],['Rupture','Ambush hits leave a small damaging ground field.','Weaker direct strikes.',{dmg:.88,burrowCd:.85},'rupture']),
  siphonleech:pair(['Sanguine Bond','Life drain periodically pulses healing to a nearby ally.','Lighter bites.',{dmg:.9},'heal-pulse'],['Ravenous Leech','Wounded-target bites gain a bounded follow-up.','Longer bite cycle.',{cd:1.18},'execute']),
  hiveherald:pair(['Swarm Conductor','Command pulses shield nearby summoned swarms.','Lighter attacks.',{dmg:.85,heraldRadius:4},'shield-pulse'],['War Chorus','Every third command attack adds a damaging echo.','Narrower command reach.',{heraldRadius:2,cd:.88},'burst'])
});
export const RANK_ENHANCEMENTS = Object.freeze(Object.fromEntries(Object.entries(EVOLUTIONS).map(([type,paths])=>[type,Object.freeze({2:paths.a.description,3:`Legendary ${paths.a.name}: stronger signature with bounded additional targets or duration.`})])));
export function evolutionFor(type,pathId='a') {return EVOLUTIONS[type]?.[pathId]||EVOLUTIONS[type]?.a||null;}
export function evolutionDescription(type,pathId='a',rank=2) {const p=evolutionFor(type,pathId);return p?`${rank>=3?'Legendary: ':''}${p.description} ${p.tradeoff}`:'';}
const alive=u=>u&&u.hp>0&&!u.illusion;
const dist=(a,b,hooks={})=>hooks.distance?hooks.distance(a,b):Math.max(Math.abs(a.x-b.x),Math.abs(a.y-b.y));
const rank=u=>Math.max(1,Math.min(3,u.lvl||u.battleRank||1));
const emit=(hooks,kind,u,t,now,extra={})=>hooks.event?.(combatEvent(kind,u,t,now,extra));
export function applyEvolution(u,pathId='a') {
  if(u._evolutionApplied||u.illusion||rank(u)<2)return u;
  const p=evolutionFor(u.type,pathId);if(!p)return u;
  u._evolutionApplied=true;u.evolution=p.id;u.evolutionSignature=p.signature;
  const scale=rank(u)>=3?1.25:1;
  for(const [key,value] of Object.entries(p.mods)){
    if(key.endsWith('Add')){const property=key.slice(0,-3);u[property]=Math.max(0,(u[property]||0)+value);}
    else if(['cd','dmg','maxhp','healCd','wardCd','wardDur','wardCdShoot','broodCd','burrowCd','shockDur','bindDur','woundDur','suppressDur','acidDur'].includes(key)){
      const before=u[key];if(before!=null)u[key]=Math.max(key==='dmg'||key==='maxhp'?1:0,(value<1&&['dmg','maxhp'].includes(key)?Math.floor:Math.round)(before*value));
    }else u[key]=value;
  }
  u.hp=Math.min(u.hp,u.maxhp);u.chainHops=Math.min(7,(rank(u)>=3?4:3)+(p.mods.chainAdd||0));
  u.evolutionPower=scale;return u;
}
export function applySummonEvolution(parent,summon) {
  if(!parent._evolutionApplied||summon.illusion)return summon;
  if(parent.summonHpScale){summon.maxhp=Math.max(1,Math.round(summon.maxhp*parent.summonHpScale));summon.hp=summon.maxhp;}
  if(parent.summonDamage||parent.summonDamageAdd)summon.dmg+=parent.summonDamage||parent.summonDamageAdd;
  summon.parentEvolution=parent.evolution;return summon;
}
function secondary(st,u,t,now,hooks,kind,{radius=0,max=1,raw=1,outer=false,ignoreArmor=false,followUp=false}={}) {
  const targets=st.field.units.filter(v=>alive(v)&&v.p!==u.p&&dist(t,v,hooks)<=radius&&(!outer||dist(t,v,hooks)>1)).sort((a,b)=>dist(t,a,hooks)-dist(t,b,hooks)).slice(0,max);
  const hits=targets.map(v=>({id:v.id,x:v.x,y:v.y,amount:comboHit(st,u,v,raw,now,hooks,ignoreArmor)}));
  if(hits.length)emit(hooks,kind,u,t,now,{targets:hits,radius,duration:900,evolution:u.evolution,followUp,sourceSize:u.size||1});return hits;
}
function field(st,u,t,now,kind,hooks) {
  const fields=st.field.comboFields||(st.field.comboFields=[]);if(fields.filter(f=>f.p===u.p&&f.until>now).length>=COMBO_LIMITS.fieldsPerSide)return;
  const serial=st.field.comboFieldSerial=(st.field.comboFieldSerial||0)+1;
  fields.push({id:`evo-${u.p}-${serial}`,sourceId:u.id,unitType:u.type,p:u.p,kind,x:t.x,y:t.y,radius:1,damage:1,nextTick:now+800,tickMs:800,ticks:0,until:now+(rank(u)>=3?2400:1800)});
  emit(hooks,kind,u,t,now,{radius:1,duration:1800,evolution:u.evolution});
}
function shield(st,u,now,hooks,radius=2) {
  const targets=st.field.units.filter(v=>alive(v)&&v.p===u.p&&dist(u,v,hooks)<=radius).slice(0,rank(u)>=3?4:3);
  for(const v of targets){v.comboShield=Math.max(v.comboShield||0,rank(u)>=3?3:2);v.comboShieldUntil=Math.max(v.comboShieldUntil||0,now+2400);}
  emit(hooks,'shield',u,u,now,{targets:targets.map(v=>({id:v.id,x:v.x,y:v.y,amount:v.comboShield})),radius,duration:900});
}
export function afterEvolutionAttack(u,t,now,st,hooks={}) {
  if(!u._evolutionApplied||!t)return;
  u.evolutionAttacks=(u.evolutionAttacks||0)+1;const s=u.evolutionSignature,power=rank(u)>=3?2:1;
  if(['burst','salvo','emp-echo','rune-echo'].includes(s)&&u.evolutionAttacks%3===0)secondary(st,u,t,now,hooks,s==='salvo'?'rocket':s==='emp-echo'?'lightning':'evolution-echo',{radius:s==='salvo'?1:0,max:s==='salvo'?2:1,raw:power+1,followUp:true});
  if(s==='pierce')secondary(st,u,t,now,hooks,'precision-hit',{raw:power,ignoreArmor:true});
  if(s==='sweep')secondary(st,u,t,now,hooks,'slash',{radius:1,max:rank(u)>=3?3:2,raw:power+1,outer:false});
  if(s==='outer-blast')secondary(st,u,t,now,hooks,u.type==='vrocket'?'rocket-explosion':'shockwave',{radius:2,max:4,raw:power+1,outer:true});
  if(s==='execute'&&t.hp<=t.maxhp*.5)secondary(st,u,t,now,hooks,'precision-hit',{raw:power+1,ignoreArmor:true});
  if(s==='opening'&&u.evolutionAttacks===1)secondary(st,u,t,now,hooks,'ambush',{raw:power+2,ignoreArmor:true});
  if(s==='shield-on-hit'&&now>=(u.nextEvolutionProc||0)){shield(st,u,now,hooks);u.nextEvolutionProc=now+4200;}
  if(s==='range-lock'&&alive(t))t.rangeDebuffUntil=Math.max(t.rangeDebuffUntil||0,now+Math.min(1200,u.suppressDur||900));
  if(['bind-spread','suppression-spread','wound-spread'].includes(s)&&now>=(u.nextEvolutionProc||0)){
    const v=st.field.units.find(v=>alive(v)&&v.p!==u.p&&v.id!==t.id&&dist(t,v,hooks)<=1);
    if(v&&(s==='wound-spread'||now>=(v.comboControlReadyAt||0))){const key=s==='bind-spread'?'bindUntil':s==='suppression-spread'?'slowUntil':'woundUntil';v[key]=Math.max(v[key]||0,now+(s==='wound-spread'?1800:450));if(s!=='wound-spread')v.comboControlReadyAt=now+1800;emit(hooks,s==='bind-spread'?'binding':s==='wound-spread'?'wound':'suppression',u,v,now,{duration:900});u.nextEvolutionProc=now+3200;}
  }
  if(s==='snare'&&alive(t)&&now>=(u.nextEvolutionProc||0)&&now>=(t.comboControlReadyAt||0)){t.bindUntil=Math.max(t.bindUntil||0,now+450);t.comboControlReadyAt=now+1800;u.nextEvolutionProc=now+4200;emit(hooks,'binding',u,t,now,{duration:450});}
  if(['storm-field','emp-field','toxic-field','blood-field','acid-field','rupture'].includes(s)&&now>=(u.nextEvolutionProc||0)){field(st,u,t,now,s==='storm-field'||s==='emp-field'?'lightning-field':s==='blood-field'?'blood-field':s==='rupture'?'rupture':'toxic-cloud',hooks);u.nextEvolutionProc=now+4200;}
}
export function tickEvolutions(st,now,hooks={}) {
  for(const u of st.field.units){if(!alive(u)||!u._evolutionApplied||(u.statusShockUntil||0)>now||(u.bindUntil||0)>now)continue;
    const s=u.evolutionSignature;if(now<(u.nextEvolutionPulse||0))continue;
    if(s==='shield-pulse'||s==='counter-shield'){
      if(s==='counter-shield'&&(u.evolutionDamageTaken||0)<6)continue;
      shield(st,u,now,hooks,u.heraldRadius||2);u.evolutionDamageTaken=0;u.nextEvolutionPulse=now+5000;
    }else if(s==='heal-pulse'||s==='triage'){
      const targets=st.field.units.filter(v=>alive(v)&&v.p===u.p&&v.hp<v.maxhp&&dist(u,v,hooks)<=3).sort((a,b)=>a.hp/a.maxhp-b.hp/b.maxhp).slice(0,s==='triage'?1:2);
      if(!targets.length)continue;
      const hits=[];for(const v of targets){const amount=Math.min(v.maxhp-v.hp,Math.floor((s==='triage'?3:rank(u)>=3?2:1)*(hooks.healScale?.(v,now)??((v.woundUntil||0)>now?.5:1))));v.hp+=amount;if(amount)hits.push({id:v.id,x:v.x,y:v.y,amount});}
      emit(hooks,'healing',u,targets[0],now,{targets:hits,radius:3,duration:900});u.nextEvolutionPulse=now+5000;
    }else if(s==='illusion'){
      if(hooks.spawnIllusion?.(st,u,now)){emit(hooks,'mirage',u,u,now,{duration:1000});u.nextEvolutionPulse=now+7000;}else u.nextEvolutionPulse=now+800;
    }else if(s==='counter'&&(u.evolutionDamageTaken||0)>=6){
      secondary(st,u,u,now,hooks,'shockwave',{radius:1,max:3,raw:rank(u)>=3?3:2});u.evolutionDamageTaken=0;u.nextEvolutionPulse=now+5000;
    }
  }
}
