import { houseIds } from './multiplayer-rounds.mjs';
import { court } from './house-control.mjs';

export const MAX_DOCUMENT_BYTES=750000;
const clone=value=>structuredClone(value);
export function guardDocument(value) {
  const size=new TextEncoder().encode(JSON.stringify(value)).byteLength;
  if(size>MAX_DOCUMENT_BYTES)throw new Error(`Campaign data exceeds the safe document size (${size} bytes). The previous saved version is intact.`);
  return value;
}
export async function encodePayload(value) {
  const text=JSON.stringify(value);
  if(typeof CompressionStream==='undefined')return guardDocument({format:'json',data:text});
  const bytes=new Uint8Array(await new Response(new Blob([text]).stream().pipeThrough(new CompressionStream('gzip'))).arrayBuffer());
  let binary='';for(let i=0;i<bytes.length;i+=32768)binary+=String.fromCharCode(...bytes.subarray(i,i+32768));
  return guardDocument({format:'gzip',data:btoa(binary)});
}
export async function decodePayload(payload) {
  if(!payload||!['json','gzip'].includes(payload.format)||typeof payload.data!=='string')throw new Error('Unreadable campaign snapshot.');
  guardDocument(payload);
  if(payload.format==='json')return JSON.parse(payload.data);
  const bytes=Uint8Array.from(atob(payload.data),c=>c.charCodeAt(0));
  const reader=new Blob([bytes]).stream().pipeThrough(new DecompressionStream('gzip')).getReader();
  const chunks=[];let total=0;
  while(true){const {done,value}=await reader.read();if(done)break;total+=value.length;if(total>8000000){await reader.cancel();throw new Error('Expanded campaign is too large.');}chunks.push(value);}
  const output=new Uint8Array(total);let offset=0;for(const c of chunks){output.set(c,offset);offset+=c.length;}
  return JSON.parse(new TextDecoder().decode(output));
}
// Full simulation is controller-readable; courts and report histories are separate
// documents. A lease successor reassembles these before executing any command.
export function splitCampaign(state) {
  const canonical=clone(state),privateByHouse={};
  for(const id of houseIds){
    privateByHouse[id]={rulerKnowledge:Object.fromEntries(state.kingdoms.filter(k=>k.id!==id).map(k=>[k.id,{memories:clone(k.memories.filter(m=>!m.subject||m.subject===id)),summary:k.relationshipSummaries?.[id]||'',interpretation:k.conversationSummaries?.[id]||'',priorities:k.priorities}])),court:clone(court(state,id)),reports:clone((state.intelligence?.reports||[]).filter(r=>r.owner===id)),
      proposals:clone((state.humanProposals||[]).filter(p=>p.from===id||p.to===id)),
      agents:clone((state.intelligence?.agents||[]).filter(a=>a.owner===id||a.captor===id)),
      incidents:clone((state.intelligence?.incidents||[]).filter(i=>i.owner===id||i.actor===id))};
  }
  delete canonical.viewHouseId;delete canonical.presentation;delete canonical.courts;
  canonical.conversations={};canonical.diplomacy.offers={};canonical.humanProposals=[];
  canonical.intelligence.reports=[];
  const world=clone(canonical);
  world.intelligence={agents:[],reports:[],incidents:[],lastTurn:state.intelligence.lastTurn};
  world.intrigue={plans:[],audit:[]};world.events=world.events.filter(e=>e.kind!=='intelligence');
  world.commerce.offers=[]; // Incoming offers are put in the recipient's private document.
  for(const id of houseIds)privateByHouse[id].tradeOffers=clone(state.commerce.offers.filter(o=>(o.to||'ashen')===id));
  for(const k of world.kingdoms){k.memories=[];k.memorySummary='';k.conversationSummary='';k.conversationSummaries={};k.relationshipSummaries={};k.priorities=[];delete k.economicPlan;}
  world.strategy.history=world.strategy.history.map(r=>({...r,houses:r.houses.map(h=>({...h,reason:'',actions:h.actions.filter(a=>['build','complete','war','peace'].includes(a.kind))}))}));
  return {canonical,world,privateByHouse};
}
export function joinCampaign(canonical, privateByHouse) {
  const s=clone(canonical);s.courts={};s.intelligence.reports=[];
  const proposals=new Map();
  for(const id of houseIds){
    const p=privateByHouse[id];if(!p)throw new Error(`Missing private snapshot for ${id}.`);
    s.courts[id]=clone(p.court);s.intelligence.reports.push(...clone(p.reports));
    for(const offer of p.proposals)proposals.set(offer.id,clone(offer));
  }
  s.humanProposals=[...proposals.values()];return s;
}
export function playerView(world, privateData, houseId) {
  if(!houseIds.includes(houseId)||!privateData)throw new Error('This ruler has no reserved House.');
  const s=clone(world),p=clone(privateData);s.viewHouseId=houseId;
  s.courts={[houseId]:p.court};s.conversations=p.court.conversations;
  s.diplomacy.offers=p.court.offers;s.diplomacy.messages=p.court.messages;
  s.intelligence.agents=p.agents;s.intelligence.reports=p.reports;s.intelligence.incidents=p.incidents;
  s.humanProposals=p.proposals;s.commerce.offers=p.tradeOffers;
  for(const k of s.kingdoms){const known=p.rulerKnowledge?.[k.id];if(known){k.memories=known.memories;k.memorySummary=known.summary;k.conversationSummaries={[houseId]:known.interpretation||''};k.relationshipSummaries={[houseId]:known.summary};k.priorities=known.priorities;}}
  if(s.outcome)s.outcome.won=(s.outcome.coalition||[s.outcome.winnerHouseId]).includes(houseId);
  return s;
}
export async function packCampaign(state, meta) {
  const {canonical,world,privateByHouse}=splitCampaign(state);
  const envelope=async value=>({stateVersion:meta.stateVersion,turn:state.turn,epoch:meta.epoch,payload:await encodePayload(value)});
  const values=await Promise.all([envelope(canonical),envelope(world),...houseIds.map(id=>envelope(privateByHouse[id]))]);
  return {canonical:values[0],world:values[1],privateByHouse:Object.fromEntries(houseIds.map((id,i)=>[id,values[i+2]]))};
}
