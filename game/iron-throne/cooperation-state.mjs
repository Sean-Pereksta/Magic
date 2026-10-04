export const OPERATION_ROLES = {assault:'Main assault', flank:'Flanking force', siege:'Siege support', supply:'Supply food', defend:'Defend ally', hold:'Hold position'};
export const OPERATION_STATUSES = ['Preparing','Executing','Completed','Abandoned'];
export const ongoingOperation = o => ['Preparing','Executing'].includes(o.status);
export const acceptedMembers = o => o.participants.filter(p => p.status === 'accepted');
export const operationFor = (s, id) => s.cooperation?.operations.find(o => o.id === id);
export const operationMember = (o, house) => o?.participants.find(p => p.house === house);
export const memberOperation = (s, house) => s.cooperation?.operations.find(o => ongoingOperation(o) && operationMember(o, house)?.status === 'accepted');
export function initializeCooperation(s) {
  s.cooperation ??= {operations:[],proposals:[],balance:[],lastDiplomacyTurn:0};
  s.cooperation.vassalOrders??=[];
}
export function pruneCooperation(s) {
  initializeCooperation(s);
  const live = s.cooperation.operations.filter(ongoingOperation);
  const keep = new Set([...live, ...s.cooperation.operations.filter(o => !ongoingOperation(o)).slice(-Math.max(0, 24-live.length))].map(o => o.id));
  s.cooperation.operations = s.cooperation.operations.filter(o => keep.has(o.id));
  for(const p of s.cooperation.formalProposals||[]){
    // Accepted commitments survive a turn change; unfinished AI speakers do
    // not start a new conversation automatically on the following turn.
    if(p.approved&&p.sentTurn<s.turn){
      for(const response of Object.values(p.responses))if(['waiting','considering'].includes(response.status))Object.assign(response,{status:'invalid',reasonCodes:['circumstances_changed'],message:'The turn changed before this ruler answered.',resolvedTurn:s.turn});
      if(p.status==='processing'&&!Object.values(p.responses).some(r=>r.status==='awaiting-human'))p.status='resolved';
    }
    if(s.turn<=p.expires&&(!p.councilId||s.allianceCouncils?.some(c=>c.id===p.councilId))&&(!p.operationId||keep.has(p.operationId)))continue;
    if(p.status==='draft')p.status='dismissed';
    if(p.status==='processing'){for(const response of Object.values(p.responses))if(['waiting','considering','awaiting-human'].includes(response.status))Object.assign(response,{status:'invalid',reasonCodes:['circumstances_changed'],message:'This proposal expired or its conversation changed.',resolvedTurn:s.turn});p.status='resolved';}
  }
  const pending=s.cooperation.proposals.filter(p=>['pending','counter'].includes(p.status));
  const resolved=s.cooperation.proposals.filter(p=>!pending.includes(p)&&s.turn-p.created<=12).slice(-(60-pending.length));
  s.cooperation.proposals=s.cooperation.proposals.filter(p=>pending.includes(p)||resolved.includes(p));
  s.pledges = s.pledges.filter(p => !p.operationId || keep.has(p.operationId));
  if (s.intelligence) s.intelligence.reports = s.intelligence.reports.filter(r => !r.operationId || keep.has(r.operationId));
}
