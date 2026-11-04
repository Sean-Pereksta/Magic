'use strict';
// Run separately from other heavy jobs. Simulation timings are not browser FPS.
const {performance}=require('node:perf_hooks');
global.window=global;
for(const file of ['data','tactics','impacts','siege','ramparts','engine'])require('../src/'+file+'.js');
const samples=Math.max(5,Math.min(600,Number(process.argv[2])||90));
const e=new BattleEngine({map:'custom',seed:430247,customMap:{name:'Colossal performance field',width:16000,height:11000,regions:[]},
  armies:[0,1].map(()=>({doctrine:'balanced',roster:{swordsman:10000,pikeman:5000,archer:3000,cavalry:2000}}))});
e.start();
for(let i=0;i<20;i++)e.step(1/30);
const milliseconds=[];let peakCertified=0;
for(let i=0;i<samples;i++){
  const start=performance.now();e.step(1/30);milliseconds.push(performance.now()-start);
  peakCertified=Math.max(peakCertified,e.fastMarchUnits||0);
}
if(e.units.some(u=>![u.x,u.y,u.hp].every(Number.isFinite)||u.x<0||u.y<0||u.x>e.width||u.y>e.height))throw new Error('Invalid large-battle state');
milliseconds.sort((a,b)=>a-b);const round=n=>Math.round(n*100)/100;
console.log(JSON.stringify({soldiers:e.units.length,formations:e.regiments.length,simulatedSeconds:round(e.time),samples,
  medianStepMs:round(milliseconds[Math.floor(samples*.5)]),p90StepMs:round(milliseconds[Math.floor(samples*.9)]),
  meanStepMs:round(milliseconds.reduce((a,b)=>a+b,0)/samples),peakCertifiedMarchers:peakCertified,
  note:'Simulation-only measurement; scene, hardware, camera drawing and battle contact affect actual frame rate.'},null,2));
