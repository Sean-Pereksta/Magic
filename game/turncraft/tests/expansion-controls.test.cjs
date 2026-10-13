const {test}=require('node:test');
const assert=require('node:assert/strict');
const {simulation}=require('./helpers/expansion-harness.cjs');

test('a group tunnel command moves five distinct owned troops once and closes the spent route',()=>{
  const run=simulation('brood');
  const result=run.plain(`(()=>{const maw=addEntity('unit','tunnelMaw',0,5,5),exit=addEntity('structure','tunnelExit',0,13,5);maw.tunnelExitId=exit.id;maw.tunnelExpiresAt=testClock+15000;maw.tunnelRemainingUses=5;exit.tunnelSourceId=maw.id;exit.temporaryLogistics=true;exit.expiresAt=maw.tunnelExpiresAt;const troops=DIR8.slice(0,6).map(([dx,dy])=>addEntity('unit','zergling',0,5+dx,5+dy));const moved=applyCommand({type:'groupTunnelTravel',slot:0,mawId:maw.id,entityIds:[troops[0].id,...troops.map(u=>u.id)]});return {moved,nearExit:troops.filter(u=>dist(u,exit)<=2.9).length,unique:new Set(troops.map(u=>u.x+','+u.y)).size,closed:!maw.tunnelExitId,exitAlive:exit.alive!==false,remainingAtEntrance:dist(troops[5],maw)<=1.6}})()`);
  assert.equal(result.moved,true);assert.equal(result.nearExit,5);assert.equal(result.unique,6);assert.equal(result.closed,true);assert.equal(result.exitAlive,false);assert.equal(result.remainingAtEntrance,true);
});

test('group tunnel commands reject foreign ownership and cannot relocate enemy or summoned units',()=>{
  const run=simulation('brood');
  const result=run.plain(`(()=>{const maw=addEntity('unit','tunnelMaw',0,5,5),exit=addEntity('structure','tunnelExit',0,13,5);maw.tunnelExitId=exit.id;maw.tunnelExpiresAt=testClock+15000;maw.tunnelRemainingUses=5;exit.tunnelSourceId=maw.id;exit.temporaryLogistics=true;exit.expiresAt=maw.tunnelExpiresAt;const own=addEntity('unit','zergling',0,4,5),enemy=addEntity('unit','zealot',1,6,5),summon=addEntity('unit','broodling',0,5,4);summon.temporarySummon=true;const forged=applyCommand({type:'groupTunnelTravel',slot:1,mawId:maw.id,entityIds:[own.id]}),accepted=applyCommand({type:'groupTunnelTravel',slot:0,mawId:maw.id,entityIds:[enemy.id,summon.id,own.id]});return {forged,accepted,uses:maw.tunnelRemainingUses,enemyX:enemy.x,summonY:summon.y,ownMoved:own.x>10}})()`);
  assert.equal(result.forged,false);assert.equal(result.accepted,true);assert.equal(result.uses,4);assert.equal(result.enemyX,6);assert.equal(result.summonY,4);assert.equal(result.ownMoved,true);
});
