const {simulation}=require('./helpers/expansion-harness.cjs');
const args=process.argv.slice(2),sourceFile=args.find(arg=>arg!=='--all');
const types=args.includes('--all')?simulation('frontier',{sourceFile}).plain(`Object.keys(UNITS).filter(type=>UNITS[type].move>0)`):['rifleman','medic','hellion','dragoon','phoenix','zergling','ultralisk'];
const result={travel:[],response:[]};
for(const type of types){
  const run=simulation('frontier',{sourceFile});
  result.travel.push(run.plain(`(()=>{setBoardDimensions('rift-dominion');terrain=Array.from({length:BOARD_W*BOARD_H},(_,i)=>({x:i%BOARD_W,y:Math.floor(i/BOARD_W),type:'ground',blocked:false}));const e=addEntity('unit',${JSON.stringify(type)},0,5,5),start=testClock,cd=effectiveMoveCd(e)*1000,range=UNITS[e.type].move;issueMoveOrder(e,105,5);let steps=0;while(e.moveDestination&&testClock-start<300000){testClock+=10;const x=e.x,y=e.y;updatePersistentOrders();if(e.x!==x||e.y!==y)steps++}return {type:e.type,range,cd,arrivalMs:testClock-start,cycleCompleteMs:e.moveReadyAt-start,steps:steps+1}})()`));
  const response=simulation('frontier',{sourceFile});
  result.response.push(response.plain(`(()=>{const e=addEntity('unit',${JSON.stringify(type)},0,15,15);issueMoveOrder(e,35,15);testClock++;const start=testClock,x=e.x;issueMoveOrder(e,5,15);while(e.x===x&&testClock-start<10000){testClock++;updatePersistentOrders()}return {type:e.type,reversalMs:testClock-start}})()`));
}
console.log(JSON.stringify(result,null,2));
