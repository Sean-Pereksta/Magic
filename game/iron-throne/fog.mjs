import { marriageSupport } from './marriage.mjs';
import { economyProjection, populationProjection } from './core.mjs';
import { operationProgress } from './operations.mjs';
import { BUILDINGS, RESOURCES, UNITS } from './data.mjs';
import { distance } from './world-hex.mjs';

// Observation records are authoritative snapshots. Rendering never consults the
// current enemy board to fill holes in a remembered record.
const clone=x=>structuredClone(x);
const level=(t,id)=>t.levels?.[id]||(t.building===id||t[id]?1:0);
const size=a=>Object.values(a.units).reduce((n,v)=>n+v,0);
export const unitVision=id=>['scout','lightCavalry'].includes(id)?5:UNITS[id]?.family==='mounted'?4:UNITS[id]?.family==='ranged'?3:UNITS[id]?.family==='siege'?1:2;
export const armyVision=a=>Math.max(0,...Object.entries(a.units).filter(([,n])=>n>0).map(([id])=>unitVision(id)));
export function structureVision(t) {
  return Math.max(t.capital&&t.building==='city'?4:0,t.building==='city'?3:t.building==='town'?2:0,
    level(t,'fort')?2+level(t,'fort'):0,level(t,'watchtower')?5+level(t,'watchtower'):0);
}
export function initializeFog(s) { s.fog??={version:1,houses:{}};return s.fog; }
export function setupVisible(s,viewer) { return s.phase==='founding'&&!s.founding?.houses[viewer]?.founded; }
export function visionTiles(s,viewer,{allied=true}={}) {
  const seen=new Set(),sources=[];
  const area=(id,radius)=>{const center=s.tiles[id];if(!center)return;for(let r=center.r-radius;r<=center.r+radius;r++)for(let q=center.q-radius;q<=center.q+radius;q++){const t=s.tiles[`${q},${r}`];if(t&&distance(center,t)<=radius)seen.add(t.id);}};
  for(const t of Object.values(s.tiles))if(t.owner===viewer){sources.push([t.id,structureVision(t)]);}
  for(const a of s.armies)if(a.owner===viewer)sources.push([a.tile,armyVision(a)]);
  // Strong alliances share their own settlements and major forces, never the
  // ally's explored map, spy reports, or the vision of its other allies.
  const allies=(allied?s.treaties||[]:[]).filter(t=>t.type==='alliance'&&t.expires>s.turn&&t.parties.includes(viewer)).map(t=>t.parties.find(id=>id!==viewer)).filter(id=>(s.kingdoms.find(k=>k.id===id)?.relations[viewer]?.trust||0)+marriageSupport(s,id,viewer)>=50);
  for(const t of Object.values(s.tiles))if(allies.includes(t.owner)&&['city','town','fort'].includes(t.building))sources.push([t.id,1]);
  for(const a of s.armies)if(allies.includes(a.owner)&&size(a)>=20)sources.push([a.tile,1]);
  for(const [id,radius] of sources)area(id,radius);
  return seen;
}
const geographyKeys=['id','q','r','terrain','resource','quality','owner','building','road','river','capital','name','region','biome','mountainPass','levels'];
function rememberedTile(t) { return Object.fromEntries(geographyKeys.filter(k=>t[k]!==undefined).map(k=>[k,clone(t[k])])); }
function record(s,viewer) { return initializeFog(s).houses[viewer]??={tiles:{},armies:{},battles:[],actions:[]}; }
function observe(s,k,seen,source='sight',spy=null) {
  for(const id of seen) k.tiles[id]={turn:s.turn,source,spyId:spy?.id||null,host:spy?.assignedHouse||null,mission:spy?.mission||null,tile:rememberedTile(s.tiles[id])};
  for(const [id,a] of Object.entries(k.armies))if(seen.has(a.tile)&&!s.armies.some(x=>x.id===id&&x.tile===a.tile))delete k.armies[id];
  for(const a of s.armies.filter(a=>seen.has(a.tile))){const n=size(a),spread=source==='spy'?Math.max(5,Math.ceil(n*.1)):Math.max(2,Math.ceil(n*.05));k.armies[a.id]={id:a.id,owner:a.owner,tile:a.tile,turn:s.turn,minimum:Math.max(0,n-spread),maximum:n+spread,source,...(spy?{spyId:spy.id,host:spy.assignedHouse}:{}),...(spy&&spy.network>=85&&a.target?{objective:a.target}:{})};}
}
export function refreshKnowledge(s,viewers=s.kingdoms.map(k=>k.id)) {
  if(s.knowledgeView||s.projectionOnly)return;
  initializeFog(s);
  for(const viewer of viewers){
    if(setupVisible(s,viewer))continue; // The founding preview creates no exploration memory.
    const k=record(s,viewer),seen=visionTiles(s,viewer),direct=visionTiles(s,viewer,{allied:false});
    observe(s,k,new Set([...seen].filter(id=>!direct.has(id))),'ally');observe(s,k,direct);
    // A lost holding is known to its own administration, without revealing
    // subsequent construction or the conqueror's garrison.
    for(const [id,old]of Object.entries(k.tiles))if(old.tile.owner===viewer&&s.tiles[id].owner!==viewer)old.tile.owner=s.tiles[id].owner;
    for(const e of s.militaryEvents||[])if(e.turn===s.turn&&(seen.has(e.tile)||[e.attacker,e.defender].includes(viewer))&&!k.battles.includes(e.id))k.battles.push(e.id);
    k.battles=k.battles.filter(id=>(s.militaryEvents||[]).some(e=>e.id===id)).slice(-100);
    // Actions become dated reports at observation, not when browsing history later.
    for(const round of s.strategy?.history||[])if(round.turn===s.turn)for(const h of round.houses)for(let i=0;i<h.actions.length;i++){
      const a=h.actions[i],key=`${round.turn}:${h.owner}:${i}`;
      if(['build','complete'].includes(a.kind)&&seen.has(a.tile)&&!k.actions.includes(key))k.actions.push(key);
    }
    k.actions=k.actions.filter(id=>Number(id.split(':')[0])>=s.turn-12).slice(-384);
    // Bounded history; a lost army marker never follows the current army.
    for(const [id,a]of Object.entries(k.armies))if(a.owner===viewer||s.turn-a.turn>20)delete k.armies[id];
  }
}
export function recordSpyMapIntelligence(s,spy) {
  if(spy.status!=='Embedded'||!['military','economy'].includes(spy.mission)||spy.network<(spy.mission==='military'?60:20))return;
  const sites=Object.values(s.tiles).filter(t=>t.owner===spy.assignedHouse&&['city','town','fort'].includes(t.building)).sort((a,b)=>Number(!!b.capital)-Number(!!a.capital)||a.id.localeCompare(b.id));
  const deep=spy.network>=80,radius=deep?3:spy.network>=50?2:1,centers=sites.slice(0,deep?3:1);
  if(spy.mission==='military')centers.push(...s.armies.filter(a=>a.owner===spy.assignedHouse).sort((a,b)=>size(b)-size(a)).slice(0,deep?4:1).map(a=>s.tiles[a.tile]));
  const seen=new Set(Object.values(s.tiles).filter(t=>centers.some(c=>distance(c,t)<=radius)).map(t=>t.id));
  observe(s,record(s,spy.owner),seen,'spy',spy);
}
function blankTile(t) {return {id:t.id,q:t.q,r:t.r,terrain:t.terrain,resource:null,quality:'normal',owner:null,building:null,road:false,river:false,walls:0,project:null,capital:null,levels:{}};}
function spyCurrent(s,x,viewer) {return x.source==='spy'&&s.turn-x.turn<=2&&s.intelligence?.agents.some(a=>a.id===x.spyId&&a.owner===viewer&&a.status==='Embedded'&&a.assignedHouse===x.host&&(!x.mission||a.mission===x.mission));}
export function knowledgeView(s,viewer='ashen',{refresh=false}={}) {
  if(s.knowledgeView)return s;
  if(refresh)refreshKnowledge(s,[viewer]);
  const v=clone({...s,fog:undefined,tiles:{},armies:[]}),k=s.fog?.houses[viewer]||{tiles:{},armies:{},battles:[],actions:[]},setup=setupVisible(s,viewer),seen=setup?new Set(Object.keys(s.tiles)):visionTiles(s,viewer);
  v.knowledgeView=viewer;v.viewHouseId=viewer;delete v.fog;
  if(s.kingdoms.some(k=>k.id===viewer))v.ownAccounting={economy:economyProjection(s,viewer),population:populationProjection(s,viewer)};
  v.tiles=Object.fromEntries(Object.values(s.tiles).map(t=>{
    if(seen.has(t.id))return [t.id,{...clone(t),fog:'visible',observedTurn:s.turn}];
    const old=k.tiles[t.id];
    const known=old?{...blankTile(t),...clone(old.tile),fog:'explored',observedTurn:old.turn,intelligenceFresh:spyCurrent(s,old,viewer),intelligenceSource:old.source}: {...blankTile(t),fog:'unknown'};
    // Only the original site and name are public. Ownership and garrison are unknown.
    const capital=Object.entries(s.founding?.houses||{}).find(([,f])=>f.capital===t.id)?.[0]||(!s.founding&&t.capital);
    if(capital&&!old){known.capital=capital;known.knownCapital=capital;known.building='city';known.name=capitalName(capital);known.levels={city:1};}
    return [t.id,known];
  }));
  v.armies=s.armies.filter(a=>a.owner===viewer||seen.has(a.tile)).map(a=>a.owner===viewer?clone(a):{id:a.id,owner:a.owner,tile:a.tile,units:clone(a.units),formation:a.formation,morale:a.morale,retreats:0,path:[],target:null,order:'observed'});
  v.lastSeenArmies=Object.values(k.armies).filter(a=>!v.armies.some(x=>x.id===a.id)).map(a=>({...clone(a),intelligenceFresh:spyCurrent(s,a,viewer)}));
  v.militaryEvents=(s.militaryEvents||[]).filter(e=>[e.attacker,e.defender].includes(viewer)||k.battles.includes(e.id));
  v.events=(s.events||[]).filter(e=>e.public||e.audience?.includes(viewer)||setup||e.turn===0&&e.message.includes('founds '));
  v.ambassadors=(s.ambassadors||[]).filter(a=>a.owner===viewer||a.detainedBy===viewer||seen.has(a.tile)).map(a=>a.owner===viewer?clone(a):{...clone(a),path:[],target:null});
  v.intelligence={...v.intelligence,agents:(v.intelligence?.agents||[]).filter(a=>a.owner===viewer||a.captor===viewer),reports:(v.intelligence?.reports||[]).filter(a=>a.owner===viewer),incidents:(v.intelligence?.incidents||[]).filter(a=>[a.owner,a.actor].includes(viewer))};
  for(const a of v.intelligence.agents)if(a.owner===viewer){a.risk=0;delete a.investigation;}
  v.intrigue={plans:(v.intrigue?.plans||[]).filter(p=>p.actor===viewer),audit:[]};
  v.cooperation={...v.cooperation,operations:(v.cooperation?.operations||[]).filter(o=>o.owner===viewer||o.participants.some(p=>p.house===viewer&&['invited','accepted','counter'].includes(p.status))),proposals:(v.cooperation?.proposals||[]).filter(p=>[p.from,p.to].includes(viewer)),balance:[]};
  for(const o of v.cooperation.operations)for(const member of o.participants)member.reportedProgress=operationProgress(s,o,member);
  v.pledges=(v.pledges||[]).filter(p=>[p.debtor,p.creditor].includes(viewer)||p.operationId&&v.cooperation.operations.some(o=>o.id===p.operationId));
  v.commerce={...v.commerce,offers:(v.commerce?.offers||[]).filter(o=>(o.to||'ashen')===viewer),aiTrades:{},cooldowns:{}};
  v.strategy={...v.strategy,history:(v.strategy?.history||[]).map(round=>({...round,houses:round.houses.map(h=>({...h,goal:'UNKNOWN',reason:'',orders:0,omitted:0,actions:h.actions.filter((a,i)=>['war','peace'].includes(a.kind)||k.actions.includes(`${round.turn}:${h.owner}:${i}`))}))}))};
  if(v.royalBonds){
    v.royalBonds.negotiations=Object.fromEntries(Object.entries(v.royalBonds.negotiations).filter(([,n])=>[n.proposer,n.host].includes(viewer)));
    v.royalBonds.marriages=v.royalBonds.marriages.map(m=>m.parties.includes(viewer)?m:{id:m.id,parties:m.parties,members:m.members,status:m.status,turn:m.turn});
  }
  v.diplomacy={...v.diplomacy,tradeHistory:(v.diplomacy?.tradeHistory||[]).filter(x=>[x.from,x.to].includes(viewer))};
  const c=s.controllers?s.courts?.[viewer]:{conversations:s.conversations,offers:s.diplomacy.offers,messages:s.diplomacy.messages};
  v.courts=c?{[viewer]:clone(c)}:{};v.conversations=clone(c?.conversations||{});v.diplomacy.offers=clone(c?.offers||{});v.diplomacy.messages=clone(c?.messages||{turn:s.turn,regular:0,hosts:{}});
  v.treaties=v.treaties.map(t=>t.parties.includes(viewer)?t:{type:t.type,parties:t.parties,expires:t.expires});
  v.crownProgress={[viewer]:s.crownProgress?.[viewer]||0};
  v.humanProposals=(v.humanProposals||[]).filter(p=>[p.from,p.to].includes(viewer));
  for(const h of v.kingdoms){
    h.knownAlive=Object.values(s.tiles).some(t=>t.owner===h.id&&['city','town'].includes(t.building));
    for(const r of Object.values(h.id===viewer?{}:h.relations)){r.observations={};r.movements={};r.contacts={};r.history=[];}
    if(h.id===viewer)continue;
    for(const [id,r]of Object.entries(h.relations))if(id!==viewer)delete r.personal;
    h.confidantConcerns=h.relations[viewer]?.personal?.bonds.includes('Trusted Confidant')?(h.priorities||[]).slice(0,2).map(text=>text.replace(/\s*\(\d+\)/g,'')):[];
    h.resources=Object.fromEntries(RESOURCES.map(r=>[r,0]));h.population=0;h.happiness=0;h.commands=0;h.goal='UNKNOWN';delete h.economicPlan;
    h.memories=h.memories.filter(m=>m.subject===viewer&&['interpretation','speech','agreement','cooperation','war','espionage','threat','insult','relief','trade-interrupted','promise-fulfilled','promise-broken','promise-released','marriage','marriage-strained','marriage-broken'].includes(m.kind));h.memorySummary='';h.priorities=[];h.relationshipSummaries={};h.conversationSummaries={[viewer]:h.conversationSummaries?.[viewer]||''};if(s.controllers)h.conversationSummary='';
    h.resourcesUnknown=true;
  }
  return v;
}
const capitalName=id=>({ashen:'Emberkeep',wintermere:'Frostwatch',thornwall:'Briarhold',sunspire:'Solstice',vesper:'Moonveil',redharbor:'Redhaven',stormholt:'Stormwatch',goldmere:'Gildenspire',ravenfell:'Ravenhold',oakwarden:'Oakheart',dawnreach:'Dawnkeep',saltwynd:'Saltwatch'}[id]||'Known capital');
export function validateFog(s) {
  initializeFog(s);const fail=()=>{throw new Error('Damaged exploration data.');},f=s.fog;
  const obj=x=>x&&typeof x==='object'&&!Array.isArray(x),turn=n=>Number.isInteger(n)&&n>=0&&n<=s.turn;
  if(f.version!==1||!obj(f.houses)||Object.keys(f.houses).some(id=>!s.kingdoms.some(k=>k.id===id)))fail();
  for(const k of Object.values(f.houses)){
    if(!obj(k)||!obj(k.tiles)||Object.keys(k.tiles).length>s.width*s.height||!obj(k.armies)||Object.keys(k.armies).length>500||!Array.isArray(k.battles)||k.battles.length>100||!Array.isArray(k.actions)||k.actions.length>384)fail();
    for(const [id,x]of Object.entries(k.tiles)){
      const t=x?.tile;if(!s.tiles[id]||!turn(x.turn)||!['sight','spy','ally'].includes(x.source)||!obj(t)||t.id!==id||t.q!==s.tiles[id].q||t.r!==s.tiles[id].r||t.terrain!==s.tiles[id].terrain||Object.keys(t).some(key=>!geographyKeys.includes(key))||t.owner&&!s.kingdoms.some(h=>h.id===t.owner)||t.building&&!Object.hasOwn(BUILDINGS,t.building)||t.name!==undefined&&(typeof t.name!=='string'||t.name.length>100)||!obj(t.levels)||Object.entries(t.levels).some(([id,n])=>!BUILDINGS[id]||!Number.isInteger(n)||n<1||n>BUILDINGS[id].maxLevel))fail();
    }
    for(const [id,a]of Object.entries(k.armies))if(!a||id!==a.id||typeof id!=='string'||id.length>80||!s.tiles[a.tile]||!turn(a.turn)||!s.kingdoms.some(h=>h.id===a.owner)||!Number.isInteger(a.minimum)||!Number.isInteger(a.maximum)||a.minimum<0||a.maximum<a.minimum||a.maximum>2000000||a.objective&&!s.tiles[a.objective])fail();
    if(k.battles.some(id=>!Number.isInteger(id))||k.actions.some(id=>typeof id!=='string'||id.length>80))fail();
  }
}
