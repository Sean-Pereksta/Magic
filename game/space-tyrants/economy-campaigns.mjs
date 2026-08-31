// Reproducible, full-runtime balance campaigns. No browser or HTML preview.
// node game/space-tyrants/economy-campaigns.mjs [duration] [output.json]
import {createSimulation} from './simulation-harness.mjs';
import {writeFileSync} from 'node:fs';
const seconds=Number(process.argv[2])||1800;
const results=[];
for(const seed of [937145,1776,20260831]){
  const h=createSimulation({seed});
  h.run(`state.nextCommand=1e9;state.running=true;state.speed=1;state.lastAutosave=1e9;state.campaign={observed:{},completed:[],cancelled:0,peakFreight:0,peakEntities:0,frames:0,invalid:[]};`);
  const snapshots=[],start=performance.now();
  for(let frame=0;frame<seconds*2;frame++){
    h.run(`
      if(state.simTime%20===0){
        // The player's simple controller only chooses an existing project;
        // it receives no stock, money, population, technology or speed grants.
        const current=stxPSPriorityProject(0);if(!current){
          const options=stxPSAllProjectDescriptors(0).sort((a,b)=>(b.kind==='expansion'?30:0)-(a.kind==='expansion'?30:0)||(a.q.startedAt||0)-(b.q.startedAt||0));
          if(options[0]){empire(0).stxPriorityProjectId=options[0].id;empire(0).stxPriorityPlanetId=options[0].p.id;empire(0).stxPrioritySetAt=state.simTime;}
        }
      }
      const before=state.planets.flatMap(p=>stxSDDescriptors(p)).map(d=>({id:d.id,kind:d.kind,p:d.p.id,owner:d.p.owner,q:d.q,startedAt:d.q.startedAt??state.simTime}));
      simulate(.5);
      const ids=new Set(state.planets.flatMap(p=>stxSDDescriptors(p)).map(d=>d.id));
      for(const d of before)if(!ids.has(d.id)){
        if(d.q.progress>=.999||d.kind==='expansion'&&state.ships.some(s=>s.type==='colony'&&s.from===d.p))state.campaign.completed.push({kind:d.kind,owner:d.owner,seconds:state.simTime-d.startedAt});
        else state.campaign.cancelled++;
      }
      state.campaign.peakFreight=Math.max(state.campaign.peakFreight,state.ships.filter(s=>s.stxIFPhysicalCargo).length);
      state.campaign.peakEntities=Math.max(state.campaign.peakEntities,state.ships.length);state.campaign.frames++;
    `);
    if((frame+1)%240===0||frame===seconds*2-1){
      const snapshot=h.run(`({time:state.simTime,ships:state.ships.length,empires:state.empires.map(e=>{
        const worlds=owned(e.id),projects=worlds.flatMap(p=>stxSDDescriptors(p)),bases=stxDSBases(e.id,true),r={};
        for(const key of STX_IF_RESOURCES)r[key]=worlds.reduce((n,p)=>n+p.stock[key],0);
        const priority=projects.find(d=>d.id===e.stxPriorityProjectId);return{id:e.id,priority:priority?{kind:priority.kind,title:priority.title,age:state.simTime-(e.stxPrioritySetAt??state.simTime),bottleneck:stxSDBottleneck(priority)}:null,worlds:worlds.length,population:totalPop(e.id),fleetPower:stxGBFleetPower(e.id),industry:empireIndustry(e.id),credits:e.credits,stock:r,production:worlds.reduce((n,p)=>n+stxIFN(p.stxAllocationOutput?.componentRate)+stxIFN(p.stxAllocationOutput?.equipmentRate),0),activeProjects:projects.length,oldestProject:projects.length?Math.max(...projects.map(d=>state.simTime-(d.q.startedAt||0))):0,shortages:projects.map(d=>stxSDBottleneck(d)).filter(b=>b&&b.remaining>stxIFEpsilon(b.resource)).reduce((a,b)=>(a[b.resource]=(a[b.resource]||0)+1,a),{}),freightUsed:stxIFFreightUsed(e.id),freightCapacity:stxIFFreightLimit(e.id),stations:bases.length,stationTiers:[1,2,3].map(t=>bases.filter(b=>b.tier===t).length),colonies:worlds.filter(p=>p.stxColony&&p.stxColony.stage!=='developed').length};
      }),invalid:state.planets.filter(p=>p.owner!==null).flatMap(p=>Object.entries(p.stock).filter(([r,n])=>!Number.isFinite(n)||n<-.000001).map(([r,n])=>({planet:p.id,resource:r,value:n})))})`);
      snapshots.push(JSON.parse(JSON.stringify(snapshot)));if(snapshot.invalid.length)throw new Error(`Invalid stock: ${JSON.stringify(snapshot.invalid)}`);
    }
  }
  const metrics=JSON.parse(JSON.stringify(h.run('state.campaign'))),final=snapshots.at(-1);
  const durations=metrics.completed.map(c=>c.seconds);
  const item={seed,seconds,wallSeconds:(performance.now()-start)/1000,peakFreight:metrics.peakFreight,peakEntities:metrics.peakEntities,completedProjects:metrics.completed.length,completedByKind:metrics.completed.reduce((a,c)=>(a[c.kind]=(a[c.kind]||0)+1,a),{}),meanCompletionSeconds:durations.reduce((n,v)=>n+v,0)/Math.max(1,durations.length),cancelledProjects:metrics.cancelled,snapshots};
  results.push(item);
  console.log(JSON.stringify({seed,seconds,wallSeconds:item.wallSeconds,worlds:final.empires.map(e=>e.worlds),fleetPower:final.empires.map(e=>Math.round(e.fleetPower)),stations:final.empires.map(e=>e.stationTiers),completed:item.completedProjects,peakEntities:item.peakEntities}));
}
if(process.argv[3])writeFileSync(process.argv[3],JSON.stringify({simulationStep:.5,controller:'existing-project priority only; no resource grants',results},null,2)+'\n');
