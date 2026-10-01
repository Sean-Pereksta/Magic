import { knowledgeView } from './fog.mjs';
import { ongoingOperation, operationMember } from './cooperation-state.mjs';

export const OBJECTIVE_TYPES = {attack:'Attack',capture:'Capture position',siege:'Siege',defend:'Defend position',hold:'Hold position',rally:'Rally',flank:'Flank',reinforce:'Reinforce',move:'Move'};
export const offensiveObjective = type => ['attack','capture','siege'].includes(type || 'attack');
export function normalizeLocation(s, raw) {
  if (!raw || !Object.hasOwn(OBJECTIVE_TYPES,raw.objectiveType) || typeof raw.targetTile !== 'string' || !Object.hasOwn(s.tiles,raw.targetTile)) return null;
  return {objectiveType:raw.objectiveType,targetTile:raw.targetTile};
}
// Return a display-only allowlist, never a tile or a reference into world state.
export function strategicLocationInfo(s, viewer, id) {
  const view=knowledgeView(s,viewer),t=view.tiles[id];
  if (!t) return null;
  const known=t.fog!=='unknown';
  return {tileId:t.id,q:t.q,r:t.r,displayName:known?(t.name||`Hex ${t.q},${t.r}`):`Unexplored Location — ${t.q},${t.r}`,knownOwner:known?t.owner||null:null,terrain:known?t.terrain:null,fogState:t.fog||'visible',lastObservedTurn:known?t.observedTurn??null:null};
}
export function strategicPickerView(s, viewer) {
  const view=structuredClone(knowledgeView(s,viewer));
  // The main map historically includes static terrain under dark fog. The picker
  // deliberately supplies no unexplored geography to its renderer or hit tests.
  for (const [id,t] of Object.entries(view.tiles)) if(t.fog==='unknown') view.tiles[id]={id:t.id,q:t.q,r:t.r,terrain:'plains',fog:'unknown',owner:null,building:null,resource:null,levels:{}};
  return view;
}
export function strategicMarkers(view, viewer) {
  const markers=[];
  for(const o of view.cooperation?.operations||[]) {
    if(!ongoingOperation(o)||!(o.owner===viewer||['accepted','invited','counter'].includes(operationMember(o,viewer)?.status)))continue;
    const type=o.objectiveType||'attack';
    markers.push({tileId:o.targetTile,operationId:o.id,type,label:o.name});
    for(const p of o.participants.filter(p=>p.status==='accepted'))markers.push({tileId:p.rally,operationId:o.id,type:'rally',label:`${o.name} · Rally`});
  }
  // Council attachments are proposals, not automatic orders. Their immutable
  // council audience is the only audience for these recent discussion markers.
  for(const c of view.allianceCouncils||[])if(c.participants.includes(viewer))for(const m of c.messages.filter(m=>m.location&&m.turn>=view.turn-2))markers.push({tileId:m.location.targetTile,councilId:c.id,type:m.location.objectiveType,label:`Council · ${OBJECTIVE_TYPES[m.location.objectiveType]}`});
  for(const p of view.cooperation?.formalProposals||[])if(p.approved&&p.targetTile&&p.audience.includes(viewer)&&p.expires>=view.turn){
    const type={PLEDGE_ATTACK:'attack',DEFEND:'defend',PLEDGE_DEFEND:'defend',POSITION:'rally',BUILD_DEFENSES:'defend'}[p.intent?.type]||'move';
    markers.push({tileId:p.targetTile,proposalId:p.id,councilId:p.councilId,ruler:p.proposer===viewer?p.requestedHouses[0]:p.proposer,type,label:`Formal proposal · ${OBJECTIVE_TYPES[type]}`});
  }
  return markers;
}
