export const ACTIVE_CONVERSATION_TURNS=2;
export function dispatchTitle(kind='') {
  if(kind==='border')return 'Border Concern';
  if(kind==='withdrawal')return 'Border Withdrawal';
  if(kind==='alliance-renewal')return 'Alliance Renewal';
  if(kind==='alliance')return 'Alliance Opportunity';
  if(kind==='shared-enemy')return 'Shared Enemy';
  if(kind.startsWith('war-relief'))return 'Request for Military Aid';
  if(kind==='war-desperation')return 'Desperate War Situation';
  if(/broken|breach/.test(kind))return 'Broken Promise';
  if(/peace/.test(kind))return 'Peace Request';
  if(/ambassador|execution/.test(kind))return 'Ambassador Dispatch';
  if(/trade|supply/.test(kind))return 'Trade Dispatch';
  return 'Diplomatic Dispatch';
}
export function conversationWindow(messages,turn) {
  const dialogue=messages.filter(m=>['player','ruler'].includes(m.role)&&m.kind!=='relationship');
  const recent=m=>Number.isInteger(m.turn)&&m.turn>=turn-ACTIVE_CONVERSATION_TURNS&&m.turn<=turn;
  return {
    history:dialogue.filter(recent).slice(-12).map(m=>({role:m.role,turn:m.turn,text:m.text.slice(0,600),...(m.dispatch?{initiated:true}:{})})),
    historical:dialogue.filter(m=>!recent(m)).slice(-3).map(m=>`Historical dialogue · ${Number.isInteger(m.turn)?`turn ${m.turn}`:'undated'} · ${m.role}: ${m.text.slice(0,220)}`)
  };
}
export function dispatchContext(event,turn,ruler,actor) {
  if(!event)return null;
  const value=typeof event==='string'?{text:event}:event;
  return {mode:'ai-initiated',turn,reason:String(value.reason||value.kind||'diplomatic-update').slice(0,60),
    title:dispatchTitle(value.reason||value.kind),speakerHouseId:ruler,recipientHouseId:actor,
    text:String(value.text||'').slice(0,600),
    ...(['border','withdrawal'].includes(value.reason||value.kind)?{armyOwner:actor,territoryOwner:ruler}:{})};
}
