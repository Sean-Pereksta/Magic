const {test}=require('node:test');
const assert=require('node:assert/strict');
const {simulation}=require('./helpers/expansion-harness.cjs');

test('issuing move, attack, stop and hold immediately presents a pending marker without moving a peer unit',()=>{
  for(const type of ['move','attack','stop','hold']){
    const run=simulation(),result=run.plain(`(()=>{
      const unit=addEntity('unit','rifleman',0,3,4),enemy=addEntity('unit','zealot',1,10,10),before=JSON.stringify(unit);
      const command={type:'${type}',entityId:unit.id,targetId:enemy.id,x:12,y:8,slot:0,id:'one'};
      orderFeedbackIssued(command);const entry=renderOrderFeedback();drawCommandMarkers();
      return {unchanged:before===JSON.stringify(unit),kind:entry.kind,status:entry.status,size:orderFeedbackEntries.size,text:document.getElementById('tcOrderStatus').textContent,hidden:document.getElementById('tcOrderStatus').hidden};
    })()`);
    assert.equal(result.unchanged,true);assert.equal(result.kind,type);assert.equal(result.status,'pending');assert.equal(result.size,1);assert.equal(result.hidden,false);assert.match(result.text,/Pending controller/);
  }
});

test('new orders replace overlapping troop markers while another group keeps its destination',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    const units=[3,4,5].map((x,i)=>addEntity('unit','rifleman',0,x,4));
    orderFeedbackIssued({type:'groupMove',entityIds:[1,2],x:15,y:8,slot:0,id:'old'});
    orderFeedbackIssued({type:'move',entityId:3,x:20,y:12,slot:0,id:'independent'});
    orderFeedbackIssued({type:'stop',entityId:1,slot:0,id:'stop'});
    const retained=[...orderFeedbackEntries.values()].map(e=>({id:e.id,ids:e.ids,total:e.total}));
    orderFeedbackIssued({type:'hold',entityId:2,slot:0,id:'hold'});
    orderFeedbackResolved({id:'old'},{count:2,total:2});
    return {retained,current:[...orderFeedbackEntries.keys()],latest:orderFeedbackLatest};
  })()`);
  assert.deepEqual(result.retained,[{id:'old',ids:[2],total:1},{id:'independent',ids:[3],total:1},{id:'stop',ids:[1],total:1}]);
  assert.deepEqual(result.current,['independent','stop','hold']);assert.equal(result.latest,'hold');
});

test('pending markers remain until acknowledgement and connection failures do not claim acceptance',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    const unit=addEntity('unit','rifleman',0,3,4),command={type:'move',entityId:unit.id,x:12,y:8,slot:0,id:'one'};
    orderFeedbackIssued(command);testClock+=20000;orderFeedbackRejected(command,'offline');renderOrderFeedback();
    const pending={text:document.getElementById('tcOrderStatus').textContent,status:orderFeedbackEntries.get('one').status};
    orderFeedbackResolved(command,{count:0,total:1,status:'rejected',reason:'Destination unavailable'});renderOrderFeedback();
    const rejected={text:document.getElementById('tcOrderStatus').textContent,status:orderFeedbackEntries.get('one').status};
    testClock+=3601;renderOrderFeedback();return {pending,rejected,expired:orderFeedbackEntries.size,hidden:document.getElementById('tcOrderStatus').hidden};
  })()`);
  assert.equal(result.pending.status,'pending');assert.match(result.pending.text,/Waiting for connection/);
  assert.equal(result.rejected.status,'rejected');assert.match(result.rejected.text,/Destination unavailable/);assert.equal(result.expired,0);assert.equal(result.hidden,true);
});

test('authoritative result counts accepted movement and attack eligibility without changing rejected units',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    const own=addEntity('unit','rifleman',0,3,4),tank=addEntity('unit','siegeTank',0,3,5),foreign=addEntity('unit','rifleman',1,3,6),enemy=addEntity('unit','phoenix',1,10,10),engineer=addEntity('unit','engineer',0,3,7);tank.siegeMode=true;
    const move=applyCommand({type:'groupMove',entityIds:[own.id,own.id,tank.id,foreign.id],x:12,y:8,slot:0});
    const attack=applyCommand({type:'groupAttack',entityIds:[own.id,engineer.id],targetId:enemy.id,slot:0});
    const hold=applyCommand({type:'groupHold',entityIds:[own.id,engineer.id],slot:0});
    return {move,attack,hold,tankDestination:tank.moveDestination,foreignDestination:foreign.moveDestination};
  })()`);
  assert.equal(result.move.count,1);assert.equal(result.move.total,3);assert.match(result.move.reason,/Some troops/);
  assert.equal(result.attack.count,1);assert.equal(result.attack.total,2);assert.equal(result.hold.count,2);assert.equal(result.hold.total,2);
  assert.equal(result.tankDestination,null);assert.equal(result.foreignDestination,null);
});

test('selection feedback includes troops filtered out by existing command eligibility',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    const own=addEntity('unit','rifleman',0,3,4),tank=addEntity('unit','siegeTank',0,3,5);tank.siegeMode=true;selectEntities([own,tank]);
    sendCommand=command=>orderFeedbackIssued({...command,slot:PLAYER,id:'selection'});
    sendSelectionMovement({x:12,y:8});const issued=orderFeedbackEntries.get('selection');
    orderFeedbackResolved({id:'selection'},{count:1,total:1});
    return {ids:issued.ids,total:issued.total,status:issued.status,text:document.getElementById('tcOrderStatus').textContent};
  })()`);
  assert.deepEqual(result.ids,[1]);assert.equal(result.total,2);assert.equal(result.status,'partial');assert.match(result.text,/1\/2 troops/);assert.match(result.text,/1 unavailable/);
});

test('reset clears order presentation and acknowledged status changes never alter simulation state',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    const own=addEntity('unit','rifleman',0,3,4),command={type:'move',entityId:own.id,x:12,y:8,slot:0,id:'one'};
    orderFeedbackIssued(command);const before=JSON.stringify(serializeEntity(own));orderFeedbackResolved(command,{count:1,total:1});
    const unchanged=before===JSON.stringify(serializeEntity(own));resetMatchPresentation();return {unchanged,size:orderFeedbackEntries.size,hidden:document.getElementById('tcOrderStatus').hidden};
  })()`);
  assert.deepEqual(result,{unchanged:true,size:0,hidden:true});
});

test('an entirely immobile selection gets a useful local rejection without sending a command',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    const tank=addEntity('unit','siegeTank',0,3,4);tank.siegeMode=true;selectEntity(tank);let sent=0;sendCommand=()=>sent++;
    const accepted=sendSelectionMovement({x:12,y:8});return {accepted,sent,status:renderOrderFeedback()?.status,text:document.getElementById('tcOrderStatus').textContent,destination:tank.moveDestination};
  })()`);
  assert.equal(result.accepted,false);assert.equal(result.sent,0);assert.equal(result.status,'rejected');assert.match(result.text,/transported or deployed/);assert.equal(result.destination,null);
});

test('reload restores only the latest pending order for each troop without replacing existing newer markers',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    [3,4,5].forEach(x=>addEntity('unit','rifleman',0,x,4));
    const commands=[{type:'groupMove',entityIds:[1,2],x:15,y:8,slot:0,id:'old',clientSeq:1,clientStream:'tab'},{type:'hold',entityId:3,slot:0,id:'other',clientSeq:2,clientStream:'tab'},{type:'stop',entityId:1,slot:0,id:'latest',clientSeq:3,clientStream:'tab'}];
    orderFeedbackRestore(commands);const before=[...orderFeedbackEntries.values()].map(e=>({id:e.id,ids:e.ids,status:e.status,total:e.total}));
    orderFeedbackRestore(commands);orderFeedbackResolved(commands[0],{count:2,total:2});
    return {before,size:orderFeedbackEntries.size,latest:orderFeedbackLatest,oldCount:orderFeedbackEntries.get('old').count,oldTotal:orderFeedbackEntries.get('old').total};
  })()`);
  assert.deepEqual(result.before,[{id:'old',ids:[2],status:'pending',total:1},{id:'other',ids:[3],status:'pending',total:1},{id:'latest',ids:[1],status:'pending',total:1}]);assert.equal(result.size,3);assert.equal(result.latest,'latest');assert.equal(result.oldCount,1);assert.equal(result.oldTotal,1);
});

test('an old cursor acknowledgement does not claim acceptance when its detailed result has expired',()=>{
  const run=simulation(),result=run.plain(`(()=>{
    addEntity('unit','rifleman',0,3,4);const command={type:'move',entityId:1,x:12,y:8,slot:0,id:'old'};orderFeedbackIssued(command);orderFeedbackResolved(command,{status:'acknowledged'});
    return {status:renderOrderFeedback().status,text:document.getElementById('tcOrderStatus').textContent};
  })()`);
  assert.equal(result.status,'acknowledged');assert.match(result.text,/Processed by controller/);assert.doesNotMatch(result.text,/Accepted/);
});
