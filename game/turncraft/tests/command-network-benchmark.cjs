// Controlled JSON payload accounting; does not contact Firestore.
// Optional argument selects an unmodified turncraft.html baseline.
const {simulation}=require('./helpers/expansion-harness.cjs');
const path=require('node:path');
const options=process.argv[2]?{sourceFile:path.resolve(process.argv[2])}:{};
const bytes=value=>Buffer.byteLength(JSON.stringify(value));
(async()=>{
  const run=simulation('frontier',options);
  run(`Math.random=()=>.123456789;lobbyId='benchmark';username='Player 0';auth.currentUser={uid:'host'};isController=false;controllerLease={epoch:1,ownerUid:'host'};globalThis.unit=addEntity('unit','rifleman',0,5,5)`);
  await run(`sendCommand({type:'move',entityId:unit.id,x:30,y:5})`);
  const sample=run.network.writes.at(-1).data;
  const empty=simulation('frontier',options),army=simulation('frontier',options);
  army(`for(let i=0;i<1000;i++)addEntity('unit','rifleman',0,i%BOARD_W,Math.floor(i/BOARD_W));`);
  const armyBefore=bytes(army('serializeState()'));
  army(`lobbyId='benchmark';stateRef='state';username='Player 0';auth.currentUser={uid:'host'};controllerLease={epoch:1,ownerUid:'host'};isController=true;applyCommand=()=>({count:1,total:1});manageCommandListener()`);
  const commands=Array.from({length:64},(_,i)=>({id:`peer_session_${i+1}`,clientCommandId:`peer_session_${i+1}`,clientStream:'peer_session',clientSeq:i+1,by:'Player 1',slot:1,createdAtMs:100000,type:i%2?'stop':'move',entityId:1,x:30,y:5}));
  army.network.listener({docChanges:()=>commands.map(c=>({type:'added',doc:{id:c.id,data:()=>c}}))});
  await army('writeState(true)');
  console.log(JSON.stringify({source:process.argv[2]?'baseline':'updated',representativeMoveCommandBytes:bytes(sample),emptySnapshotBytes:bytes(empty('serializeState()')),staticArmy1000SnapshotBytes:armyBefore,army1000After64CommandsBytes:bytes(army.network.writes.at(-1).data),commandDocumentsDeleted:army.network.deletes.length,snapshotWrites:army.network.writes.length,stateWriteIntervalMs:army('STATE_WRITE_MS')},null,2));
})().catch(error=>{console.error(error);process.exitCode=1});
