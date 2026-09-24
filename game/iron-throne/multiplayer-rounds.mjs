import { HOUSES } from './data.mjs';
import { alive, createGame } from './core.mjs';
import { endTurn } from './diplomacy.mjs';
import { court } from './house-control.mjs';

export const LEASE_MS = 30000, HEARTBEAT_MS = 10000, ONLINE_MS = 35000, GRACE_MS = 90000;
export const houseIds = HOUSES.map(h => h.id);
export const millis = value => typeof value?.toMillis === 'function' ? value.toMillis() : Number(value || 0);
export const present = (p, now) => !!p && now - millis(p.at) < ONLINE_MS;
export const seatFor = (meta, uid) => houseIds.find(id => meta?.seats[id]?.uid === uid && meta.seats[id].kind === 'human');
export const ownsLease = (meta, uid, token, now) => meta?.lease?.uid === uid && meta.lease.token === token && millis(meta.lease.expiresAt) > now;
export function setupMeta(lobby, now) {
  return { schema:1, phase:'setup', turn:0, stateVersion:0, epoch:0, hostUid:lobby.hostUid,
    seats:Object.fromEntries(houseIds.map(id=>[id,{kind:'open',uid:null,name:''}])), ready:{}, sequences:{},
    options:{seed:8147,preset:'crossroads',timerSeconds:0,absent:'hold'}, planningAt:now, deadline:0, lease:null };
}
export function claimSeat(meta, uid, name, houseId) {
  if(meta.phase!=='setup'||!houseIds.includes(houseId))throw new Error('House selection is locked.');
  const seat=meta.seats[houseId];
  if(seat.kind==='ai'||seat.uid&&seat.uid!==uid)throw new Error('That House is already claimed.');
  const before=seatFor(meta,uid);
  if(before)meta.seats[before]={kind:'open',uid:null,name:''};
  meta.seats[houseId]={kind:'human',uid,name:String(name||'Ruler').slice(0,40),substitute:false};
  meta.ready={};return meta;
}
export function configureSeat(meta, uid, houseId, kind) {
  if(meta.phase!=='setup'||meta.hostUid!==uid||!houseIds.includes(houseId)||!['ai','open'].includes(kind)||meta.seats[houseId].uid)throw new Error('Only the host can change an unclaimed seat.');
  meta.seats[houseId]={kind,uid:null,name:''};return meta;
}
export function configureCampaign(meta, uid, options) {
  if(meta.phase!=='setup'||meta.hostUid!==uid)throw new Error('Only the host can configure this campaign.');
  const {seed,preset,timerSeconds,absent}=options;
  if(!Number.isInteger(seed)||seed<1||seed>4294967295||!['crossroads','highlands'].includes(preset)||![0,120,300,600].includes(timerSeconds)||!['hold','ai'].includes(absent))throw new Error('Choose a valid seed, map and round timer.');
  meta.options={seed,preset,timerSeconds,absent};return meta;
}
export function startCampaign(meta, uid, now) {
  if(meta.phase!=='setup'||meta.hostUid!==uid||!seatFor(meta,uid))throw new Error('The host must choose a House before starting.');
  for(const id of houseIds)if(meta.seats[id].kind==='open')meta.seats[id]={kind:'ai',uid:null,name:''};
  const state=createGame(meta.options.seed,meta.options.preset);
  state.controllers=structuredClone(meta.seats);state.courts={};state.humanProposals=[];
  for(const id of houseIds)court(state,id);
  meta.phase='planning';meta.turn=state.turn;meta.stateVersion=1;meta.ready={};
  meta.startedAt=now;meta.planningAt=now;meta.deadline=meta.options.timerSeconds?now+meta.options.timerSeconds*1000:0;
  return state;
}
export function requiredRulers(meta, presence, now, state) {
  return houseIds.filter(id=>{
    const seat=meta.seats[id];
    if(seat.kind!=='human'||seat.substitute||state&&!alive(state,id))return false;
    // The seat is reserved even after the grace expires. Only readiness is waived.
    return now-Math.max(millis(presence[seat.uid]?.at),meta.startedAt??meta.planningAt)<GRACE_MS;
  });
}
export function resolutionDue(meta, presence, now, state) {
  if(meta.phase!=='planning'||state.outcome)return false;
  const humans=houseIds.filter(id=>meta.seats[id].kind==='human'&&alive(state,id));
  if(!humans.some(id=>present(presence[meta.seats[id].uid],now)))return false; // Pause when everyone leaves.
  if(meta.deadline&&now>=meta.deadline)return true;
  const required=requiredRulers(meta,presence,now,state);
  return required.length ? required.every(id=>meta.ready[id]) : humans.some(id=>meta.seats[id].substitute);
}
export function resolveRound(state, meta, presence, now) {
  if(meta.phase!=='resolving'||meta.turn!==state.turn)throw new Error('This round is not locked for resolution.');
  for(const id of houseIds){
    const seat=meta.seats[id];
    seat.substitute=seat.kind==='human'&&(!!seat.forcedSubstitute||!present(presence[seat.uid],now)&&meta.options.absent==='ai');
    delete seat.forcedSubstitute;
  }
  state.controllers=structuredClone(meta.seats);
  endTurn(state);
  // Return control only at the new planning boundary, never halfway through an AI turn.
  for(const id of houseIds){
    const seat=meta.seats[id];
    if(seat.kind==='human'&&present(presence[seat.uid],now))seat.substitute=false;
  }
  state.controllers=structuredClone(meta.seats);
  state.humanProposals=(state.humanProposals||[]).filter(p=>p.status==='pending'&&p.expires>=state.turn||p.resolvedTurn>=state.turn-3).slice(-60);
  meta.turn=state.turn;meta.phase=state.outcome?'ended':'planning';meta.ready={};meta.planningAt=now;
  meta.deadline=meta.options.timerSeconds?now+meta.options.timerSeconds*1000:0;
  return state;
}
export function requestTakeover(meta, uid, houseId, presence, now, permanent=false) {
  const seat=meta.seats[houseId];
  if(uid!==meta.hostUid||meta.phase!=='planning'||seat?.kind!=='human'||now-Math.max(millis(presence[seat.uid]?.at),meta.startedAt??meta.planningAt)<GRACE_MS)throw new Error('AI takeover requires an absent ruler and a 90-second grace period.');
  seat.takeover=permanent?'permanent':'temporary';
  meta.ready[houseId]=true;
}
export function applyBoundaryTakeovers(meta) {
  for(const id of houseIds){
    const s=meta.seats[id];
    if(s.takeover==='permanent')meta.seats[id]={kind:'ai',uid:null,name:''};
    else if(s.takeover==='temporary'){s.substitute=true;s.forcedSubstitute=true;delete s.takeover;}
  }
}
