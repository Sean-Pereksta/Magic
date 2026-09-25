export const OPERATION_ROLES = {assault:'Main assault', flank:'Flanking force', siege:'Siege support', supply:'Supply food', defend:'Defend ally', hold:'Hold position'};
export const OPERATION_STATUSES = ['Preparing','Executing','Completed','Abandoned'];
export const ongoingOperation = o => ['Preparing','Executing'].includes(o.status);
export const acceptedMembers = o => o.participants.filter(p => p.status === 'accepted');
export const operationFor = (s, id) => s.cooperation?.operations.find(o => o.id === id);
export const operationMember = (o, house) => o?.participants.find(p => p.house === house);
export const memberOperation = (s, house) => s.cooperation?.operations.find(o => ongoingOperation(o) && operationMember(o, house)?.status === 'accepted');
export function initializeCooperation(s) {
  s.cooperation ??= {operations:[],proposals:[],balance:[],lastDiplomacyTurn:0};
}
export function pruneCooperation(s) {
  initializeCooperation(s);
  const live = s.cooperation.operations.filter(ongoingOperation);
  const keep = new Set([...live, ...s.cooperation.operations.filter(o => !ongoingOperation(o)).slice(-Math.max(0, 24-live.length))].map(o => o.id));
  s.cooperation.operations = s.cooperation.operations.filter(o => keep.has(o.id));
  const pending=s.cooperation.proposals.filter(p=>['pending','counter'].includes(p.status));
  const resolved=s.cooperation.proposals.filter(p=>!pending.includes(p)&&s.turn-p.created<=12).slice(-(60-pending.length));
  s.cooperation.proposals=s.cooperation.proposals.filter(p=>pending.includes(p)||resolved.includes(p));
  s.pledges = s.pledges.filter(p => !p.operationId || keep.has(p.operationId));
  if (s.intelligence) s.intelligence.reports = s.intelligence.reports.filter(r => !r.operationId || keep.has(r.operationId));
}
