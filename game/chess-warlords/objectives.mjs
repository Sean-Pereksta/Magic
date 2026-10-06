// Match-time objectives. This module has no wall clock, rendering, or Firebase dependencies.
export const WAR_MODES = Object.freeze({
  grand: {name:'Grand War', dominion:true, capitals:true, description:'120 Dominion points, enemy capitals, or the last surviving king wins.'},
  dominion: {name:'Dominion', dominion:true, capitals:true, description:'First to 120 points wins. Capitals weaken production; regicide still wins.'},
  capital: {name:'Capital Conquest', dominion:false, capitals:true, description:'Hold enemy capitals simultaneously, or defeat every rival king.'},
  regicide: {name:'Total War / Regicide', dominion:false, capitals:false, description:'Classic war: eliminate all rival kings.'}
});
export const WAR_RULES = Object.freeze({target:120, interval:5, kingBonus:15, bannerSeconds:6, capitalSeconds:12, radius:2});
const distance = (a,b) => Math.hypot(a.x-b.x,a.y-b.y);
const near = (a,b,r) => (a.x-b.x)**2+(a.y-b.y)**2 <= r*r;
const alive = (factions,id) => factions.some(f=>f.idx===id && f.alive);
export const normalWarMode = id => WAR_MODES[id] ? id : 'grand';
export const enemyCapitalTarget = count => Math.max(1,Math.min(4,count-1));
export const fortificationType = (p,defs) => p.type==='general' || p.type==='marshal' ? 'general' :
  (['tower','ballista','citadel'].includes(defs[p.type]?.pattern) || p.type==='gatehouse') ? 'tower' : null;

// Repair disconnected starting regions with the fewest obstacle crossings. Existing land is preferred.
// Runs only during objective-match creation/reconstruction, never in the frame loop.
export function connectWarRealms(board,width,height,starts){
  if(!starts.length)return;
  const connected=()=>{
    const seen=new Set(),queue=[starts[0].y*width+starts[0].x];seen.add(queue[0]);
    for(let head=0;head<queue.length;head++){
      const i=queue[head],x=i%width,y=Math.floor(i/width);
      for(const [dx,dy] of [[1,0],[-1,0],[0,1],[0,-1]]){
        const nx=x+dx,ny=y+dy,ni=ny*width+nx,t=board[ni];
        if(nx>=0&&ny>=0&&nx<width&&ny<height&&!seen.has(ni)&&t&&t.type!=='water'&&t.type!=='mountain'){seen.add(ni);queue.push(ni);}
      }
    }
    return seen;
  };
  for(const start of starts.slice(1)){
    const seen=connected(),source=start.y*width+start.x;if(seen.has(source))continue;
    const cost=new Float64Array(board.length).fill(Infinity),parent=new Int32Array(board.length).fill(-1),heap=[];
    const push=(index,score)=>{let k=heap.length;heap.push({index,score});while(k>0){const p=(k-1)>>1;if(heap[p].score<=score)break;heap[k]=heap[p];k=p;}heap[k]={index,score};};
    const pop=()=>{const first=heap[0],last=heap.pop();if(heap.length){let k=0;while(k*2+1<heap.length){let child=k*2+1;if(child+1<heap.length&&heap[child+1].score<heap[child].score)child++;if(heap[child].score>=last.score)break;heap[k]=heap[child];k=child;}heap[k]=last;}return first;};
    cost[source]=0;push(source,0);let goal=-1;
    while(heap.length){
      const {index:i,score}=pop();if(score!==cost[i])continue;if(seen.has(i)){goal=i;break;}
      const x=i%width,y=Math.floor(i/width);
      for(const [dx,dy] of [[1,0],[-1,0],[0,1],[0,-1]]){
        const nx=x+dx,ny=y+dy,ni=ny*width+nx;if(nx<0||ny<0||nx>=width||ny>=height)continue;
        const next=score+(['water','mountain'].includes(board[ni]?.type)?25:1);
        if(next<cost[ni]){cost[ni]=next;parent[ni]=i;push(ni,next);}
      }
    }
    for(let i=goal;i!==-1;i=parent[i])if(['water','mountain'].includes(board[i]?.type))board[i].type='road';
  }
}

// Flood-fill once at match creation. Banners must be reachable by every starting realm.
export function placeWarBanners(board,width,height,starts) {
  const pass = t => t && t.type!=='water' && t.type!=='mountain';
  const component = new Int32Array(width*height).fill(-1);
  let label=0;
  for(let i=0;i<board.length;i++){
    if(component[i]!==-1 || !pass(board[i])) continue;
    const queue=[i]; component[i]=label;
    for(let head=0;head<queue.length;head++){
      const index=queue[head],x=index%width,y=Math.floor(index/width);
      for(const [dx,dy] of [[1,0],[-1,0],[0,1],[0,-1]]){
        const nx=x+dx,ny=y+dy,ni=ny*width+nx;
        if(nx>=0 && ny>=0 && nx<width && ny<height && component[ni]===-1 && pass(board[ni])){component[ni]=label;queue.push(ni);}
      }
    }
    label++;
  }
  const realms=starts.map(s=>component[s.y*width+s.x]);
  const center={x:(width-1)/2,y:(height-1)/2};
  const spacing=Math.max(5,Math.min(width,height)*.15);
  const homeBuffer=Math.max(5,Math.min(width,height)*.18);
  const candidates=board.filter(t=>pass(t) && realms.every(c=>c===component[t.y*width+t.x]) &&
    t.x>=2 && t.y>=2 && t.x<width-2 && t.y<height-2 && starts.every(s=>distance(t,s)>=homeBuffer));
  const placed=[];
  for(let i=0;i<7;i++){
    const angle=(i-1)*Math.PI/3-Math.PI/2;
    const desired=i===0?center:{x:center.x+Math.cos(angle)*width*.24,y:center.y+Math.sin(angle)*height*.24};
    let best=null,bestScore=-Infinity;
    for(const t of candidates){
      if(placed.some(p=>distance(p,t)<spacing)) continue;
      const terrainBonus=t.type==='road'?1.5:t.type==='ruins'?1:0;
      const score=-distance(t,desired)+terrainBonus;
      if(score>bestScore){best=t;bestScore=score;}
    }
    if(best) placed.push({id:`banner-${i}`,kind:'banner',name:i===0?'Heart of the Vastboard':`War Banner ${i}`,
      x:best.x,y:best.y,central:i===0,value:i===0?2:1,radius:WAR_RULES.radius,seconds:WAR_RULES.bannerSeconds,
      owner:null,claimant:null,progress:0,pointClock:0,status:'neutral',general:false,tower:false});
  }
  return placed;
}

export function createWar({board,width,height,starts,factions,mode='grand'}) {
  mode=normalWarMode(mode);
  const rules=WAR_MODES[mode];
  if(rules.capitals)connectWarRealms(board,width,height,starts);
  const objectives=rules.dominion?placeWarBanners(board,width,height,starts):[];
  if(rules.capitals) factions.forEach((f,i)=>objectives.push({id:`capital-${f.idx}`,kind:'capital',name:`${f.short} Capital`,
    ...starts[i],home:f.idx,owner:f.idx,claimant:null,progress:0,pointClock:0,status:'controlled',
    radius:WAR_RULES.radius,seconds:WAR_RULES.capitalSeconds,general:false,tower:false}));
  return {version:1,mode,elapsed:0,target:WAR_RULES.target,capitalTarget:enemyCapitalTarget(factions.length),objectives,
    scores:Object.fromEntries(factions.map(f=>[f.idx,0])),
    stats:Object.fromEntries(factions.map(f=>[f.idx,{banners:0,capitals:0,kings:0}])),defeatedRealms:[],victory:null};
}
export const capitalLost = (war,faction) => !!war?.objectives.some(o=>o.kind==='capital' && o.home===faction && o.owner!==faction);
export const capitalProduction = (war,faction) => capitalLost(war,faction)?.5:1;
export const capitalKingExposed = (war,piece) => piece?.type==='king' && capitalLost(war,piece.faction);
export const enemyCapitalsHeld = (war,faction) => war?.objectives.filter(o=>o.kind==='capital' && o.home!==faction && o.owner===faction).length || 0;
export const bannersHeld = (war,faction) => war?.objectives.filter(o=>o.kind==='banner' && o.owner===faction).length || 0;

export function objectiveVictory(war,factions) {
  if(war?.victory) return war.victory;
  const survivors=factions.filter(f=>f.alive);
  if(survivors.length===1) return {slot:survivors[0].idx,reason:'conquest'};
  if(!war) return null;
  const mode=WAR_MODES[war.mode];
  if(mode.dominion){
    const winners=survivors.filter(f=>(war.scores[f.idx]||0)>=war.target).sort((a,b)=>(war.scores[b.idx]||0)-(war.scores[a.idx]||0) || a.idx-b.idx);
    if(winners.length) return {slot:winners[0].idx,reason:'dominion'};
  }
  if(war.mode==='grand' || war.mode==='capital'){
    const winner=survivors.find(f=>enemyCapitalsHeld(war,f.idx)>=war.capitalTarget);
    if(winner) return {slot:winner.idx,reason:'imperial'};
  }
  return null;
}

export function advanceWar(war,dt,pieces,factions,defs) {
  if(!war || war.victory || !Number.isFinite(dt) || dt<=0) return [];
  const events=[];
  // Each objective reads the nearby army once per update, independent of frame rate and army size elsewhere.
  const live=pieces.filter(p=>p.alive && alive(factions,p.faction));
  const local=war.objectives.map(o=>({occupants:live.filter(p=>near(o,p,o.radius)),forts:live.filter(p=>near(o,p,o.radius+1))}));
  let remaining=dt;
  while(remaining>1e-8 && !war.victory){
    const step=Math.min(.25,remaining);remaining-=step;war.elapsed+=step;
    war.objectives.forEach((o,index)=>{
      const nearby=local[index].occupants,occupants=new Set(nearby.map(p=>p.faction));
      const previousStatus=o.status;
      const ownerUnits=local[index].forts.filter(p=>p.faction===o.owner);
      o.general=ownerUnits.some(p=>fortificationType(p,defs)==='general');
      o.tower=ownerUnits.some(p=>fortificationType(p,defs)==='tower');
      const hostile=[...occupants].some(id=>id!==o.owner);
      if(occupants.size>1){o.status='contested';}
      else {
        const faction=[...occupants][0];
        const capturing=nearby.some(p=>p.faction===faction && !defs[p.type]?.building);
        if(faction!=null && faction!==o.owner && capturing){
          if(o.claimant!==faction){o.claimant=faction;o.progress=0;}
          o.status='capturing';
          o.progress+=step/(o.seconds*(o.tower?1.5:1)*(o.general?4/3:1));
          if(o.progress>=1-1e-8){
            const previous=o.owner;o.owner=faction;o.claimant=null;o.progress=0;o.pointClock=0;o.status='controlled';
            war.stats[faction][o.kind==='capital'?'capitals':'banners']++;
            events.push({type:'captured',id:o.id,kind:o.kind,owner:faction,previous,home:o.home});
          }
        }else{
          o.status=o.owner==null?'neutral':'controlled';
          o.progress=Math.max(0,o.progress-step/o.seconds*(o.general?2:1));
          if(o.progress===0)o.claimant=null;
        }
      }
      if(o.status!==previousStatus && (o.status==='contested'||o.status==='capturing'))events.push({type:o.status,id:o.id,kind:o.kind,owner:o.owner,claimant:o.claimant,home:o.home});
      // A raid denies scoring immediately. A completed capture begins its own full five-second interval.
      if(o.kind==='banner' && o.owner!=null && alive(factions,o.owner) && !hostile && o.status==='controlled'){
        o.pointClock+=step;
        while(o.pointClock>=WAR_RULES.interval-1e-8){o.pointClock-=WAR_RULES.interval;war.scores[o.owner]+=o.value;}
      }
    });
    war.victory=objectiveVictory(war,factions);
  }
  return events;
}

export function absorbWarRealm(war,winner,loser) {
  if(!war || war.victory || war.defeatedRealms?.includes(loser)) return;
  (war.defeatedRealms||=[]).push(loser);
  war.stats[winner].kings++;
  if(WAR_MODES[war.mode].dominion)war.scores[winner]+=WAR_RULES.kingBonus;
  for(const o of war.objectives){
    if(o.owner===loser){o.owner=winner;o.pointClock=0;o.status='controlled';o.claimant=null;o.progress=0;}
    if(o.claimant===loser){o.claimant=null;o.progress=0;}
  }
}

// Personalities change weights in one planner, rather than separate scripts per faction.
export const STRATEGIC_STYLES=Object.freeze({
  balanced:{attack:1,defense:1,raid:1,spread:1,group:4,risk:1.25},
  aggressive:{attack:1.4,defense:.85,raid:1.1,spread:.9,group:5,risk:1.5},
  defensive:{attack:.8,defense:1.5,raid:.65,spread:.8,group:5,risk:1.1},
  expansion:{attack:1,defense:.85,raid:.8,spread:1.5,group:3,risk:1.2},
  assassin:{attack:.9,defense:.95,raid:1.6,spread:1.1,group:3,risk:1.15},
  fluid:{attack:1.1,defense:.9,raid:1.3,spread:1.1,group:4,risk:1.25},
  honor:{attack:1,defense:1.2,raid:1,spread:1,group:4,risk:1.2}
});
const strength=(p,defs)=>Math.min(12,defs[p.type]?.value||1)*(.6+.4*Math.max(0,p.hp||1)/Math.max(1,p.maxHp||1));
const pointKey=p=>`${p.x},${p.y}`;

export class ObjectivePlanner {
  constructor(){this.memory=new Map();this.routes=new Map();this.board=null;}
  reset(){this.memory.clear();this.routes.clear();this.board=null;}
  route(board,width,height,target,amphibious=false){
    if(this.board!==board){this.board=board;this.routes.clear();}
    const key=`${width}:${height}:${pointKey(target)}:${amphibious}`;
    if(this.routes.has(key))return this.routes.get(key);
    const field=new Int16Array(width*height).fill(30000),queue=[];
    const pass=(x,y)=>x>=0&&y>=0&&x<width&&y<height&&board[y*width+x]?.type!=='mountain' &&
      (amphibious || board[y*width+x]?.type!=='water');
    if(pass(target.x,target.y)){field[target.y*width+target.x]=0;queue.push(target.y*width+target.x);}
    for(let head=0;head<queue.length;head++){
      const i=queue[head],x=i%width,y=Math.floor(i/width);
      for(const [dx,dy] of [[1,0],[-1,0],[0,1],[0,-1],[1,1],[-1,1],[1,-1],[-1,-1]]){
        const nx=x+dx,ny=y+dy,ni=ny*width+nx;
        if(pass(nx,ny) && field[ni]>field[i]+1){field[ni]=field[i]+1;queue.push(ni);}
      }
    }
    // Bound memory even when moving kings or staging positions change repeatedly.
    if(this.routes.size>=64)this.routes.delete(this.routes.keys().next().value);
    this.routes.set(key,field);return field;
  }
  plan({war,faction,factions,pieces,defs,board,width,height,time,visible=()=>true,force=false}) {
    if(!war?.objectives.length || war.victory)return null;
    const old=this.memory.get(faction.idx);
    const own=pieces.filter(p=>p.alive&&p.faction===faction.idx);
    const signature=war.objectives.map(o=>`${o.owner}:${o.claimant}:${o.status}`).join('|');
    const leader=factions.filter(f=>f.alive&&f.idx!==faction.idx).sort((a,b)=>(war.scores[b.idx]||0)-(war.scores[a.idx]||0))[0];
    const leaderScore=war.scores[leader?.idx]||0;
    const pressure=Math.max(0,(leaderScore/war.target-.7)/.3);
    if(!force&&old&&time-old.time<2.5&&old.signature===signature&&old.count===own.length && old.pressureBand===Math.floor(pressure*10))return old;
    const style=STRATEGIC_STYLES[faction.aiStyle]||STRATEGIC_STYLES.balanced;
    const enemies=pieces.filter(p=>p.alive&&p.faction!==faction.idx&&alive(factions,p.faction)&&visible(p));
    const enemyDanger=new Set();
    for(const p of enemies)for(let dy=-2;dy<=2;dy++)for(let dx=-2;dx<=2;dx++)if(dx*dx+dy*dy<=4)enemyDanger.add(`${p.x+dx},${p.y+dy}`);
    const king=own.find(p=>p.type==='king');
    const home=war.objectives.find(o=>o.kind==='capital'&&o.home===faction.idx)||king;
    const army=own.filter(p=>p.type!=='king'&&!defs[p.type]?.building);
    const deployablePower=army.reduce((s,p)=>s+strength(p,defs),0)*.4;
    const evaluation=war.objectives.map(o=>{
      const friendly=own.filter(p=>near(p,o,7)).reduce((s,p)=>s+strength(p,defs),0);
      const hostile=enemies.filter(p=>near(p,o,7)).reduce((s,p)=>s+strength(p,defs),0);
      const isHome=o.kind==='capital'&&o.home===faction.idx;
      const owned=o.owner===faction.idx;
      const threat=owned && (hostile>0 || o.claimant!=null || o.status==='contested');
      const losses=old?.attrition?.[o.id]||0;
      const leaderPressure=o.owner===leader?.idx?pressure:0;
      const from=king||home||o;
      const route=this.route(board,width,height,o,faction.aiStyle==='fluid');
      const travel=route[from.y*width+from.x];
      let score=(o.kind==='banner'?42*o.value:(war.mode==='capital'?150:95)*style.raid)*style.attack;
      if(o.owner==null)score*=style.spread;
      if(o.kind==='capital' && WAR_MODES[war.mode].capitals)score+=enemyCapitalsHeld(war,faction.idx)*12;
      score+=leaderPressure*100+(isHome&&!owned?140:0)+(threat?75*style.defense:0);
      score-=travel*1.8+Math.max(0,hostile-Math.max(friendly,deployablePower)*style.risk)*2.5+losses*12;
      if(owned&&!threat)score=-10;
      return {objective:o,friendly,hostile,score,threat,isHome,travel,losses,leaderPressure};
    });
    const homeEval=evaluation.find(e=>e.isHome);
    const homeThreat=!!homeEval && (homeEval.threat||homeEval.objective.owner!==faction.idx);
    const catastrophic=homeThreat && homeEval.hostile>homeEval.friendly*1.6+6;
    const emergency=leaderScore>=war.target-3 && pressure>0;
    const assignments=new Map(),remaining=new Set(army.map(p=>p.id));
    const assign=(role,target,quota,extra={})=>{
      const candidates=army.filter(p=>remaining.has(p.id));
      candidates.sort((a,b)=>{
        const affinity=p=>(fortificationType(p,defs)?(role.includes('defend')?-4:5):0)+(old?.assignments.get(p.id)?.objectiveId===target?.id?-2:0);
        return distance(a,target)+affinity(a)-distance(b,target)-affinity(b)||a.id-b.id;
      });
      const chosen=candidates.slice(0,Math.max(0,quota));
      for(const p of chosen){remaining.delete(p.id);assignments.set(p.id,{role,target,objectiveId:target.id,...extra});}
      return chosen;
    };
    const n=army.length;
    if(king)assignments.set(king.id,{role:'king_guard',target:king});
    // Home and mobile reserves remain funded before offensive squads are allocated.
    if(home)assign('capital_defender',home,Math.min(n,Math.max(1,Math.round(n*(catastrophic?.55:homeThreat?.35:.18)))));
    const threatenedBanner=evaluation.filter(e=>e.threat&&!e.isHome).sort((a,b)=>b.score-a.score)[0]?.objective;
    const heldBanners=evaluation.filter(e=>e.objective.kind==='banner'&&e.objective.owner===faction.idx);
    let reserveTarget=homeThreat?home:threatenedBanner||home||king;
    if(!homeThreat&&!threatenedBanner&&heldBanners.length>=2){
      const middle={x:Math.round((heldBanners[0].objective.x+heldBanners[1].objective.x)/2),y:Math.round((heldBanners[0].objective.y+heldBanners[1].objective.y)/2)};
      const t=board[middle.y*width+middle.x];if(t && !['water','mountain'].includes(t.type))reserveTarget=middle;
    }
    if(reserveTarget && remaining.size)assign('reserve',reserveTarget,Math.max(1,Math.round(n*.1)));
    const threats=evaluation.filter(e=>e.threat&&!e.isHome).sort((a,b)=>b.score-a.score);
    let defenseBudget=Math.round(n*.2);
    for(const e of threats){
      const quota=Math.min(defenseBudget,Math.max(1,Math.ceil(e.hostile/4)));
      assign('objective_defender',e.objective,quota);defenseBudget-=quota;
    }
    // Light patrol/fortification, never a permanent ten-piece garrison on a quiet flag.
    const secure=evaluation.filter(e=>e.objective.owner===faction.idx&&!e.isHome&&!e.threat).sort((a,b)=>b.objective.value-a.objective.value);
    for(const e of secure.slice(0,2))if(defenseBudget>0){assign('objective_defender',e.objective,1);defenseBudget--;}
    const candidates=evaluation.filter(e=>e.objective.owner!==faction.idx && e.travel<30000 && e.score>0).sort((a,b)=>b.score-a.score||a.objective.id.localeCompare(b.objective.id));
    const attackBudget=Math.min(remaining.size,Math.max(n>=4?2:1,Math.floor(n*(emergency?.65:catastrophic?.1:.4))));
    const perTargetCap=Math.max(1,Math.ceil(n*(emergency?.65:.3)));
    let spending=attackBudget;
    const squads=[];
    for(const e of candidates){
      if(spending<=0 || !remaining.size)break;
      const o=e.objective;
      const quota=Math.min(spending,perTargetCap,Math.max(e.hostile>0?style.group:1,Math.ceil(e.hostile/4)));
      const from=home||king||o;
      // Stage on our side of the objective, preferring a foothold of friendly land.
      let staging=null,stageScore=Infinity;
      const route=this.route(board,width,height,o);
      for(let y=Math.max(0,o.y-7);y<=Math.min(height-1,o.y+7);y++)for(let x=Math.max(0,o.x-7);x<=Math.min(width-1,o.x+7);x++){
        const t=board[y*width+x],d=distance(t,o);
        if(d<3 || d>7 || route[y*width+x]>=30000)continue;
        const score=distance(t,from)+(t.owner===faction.idx?-8:0)+(enemyDanger.has(`${x},${y}`)?12:0);
        if(score<stageScore){staging={x,y};stageScore=score;}
      }
      staging=staging||from;
      const units=assign(o.kind==='capital'?'capital_raider':'objective_attacker',o,quota,{staging,phase:'attack'});
      spending-=units.length;
      const power=units.reduce((s,p)=>s+strength(p,defs),0);
      const staged=units.filter(p=>near(p,staging,3));
      const stagedPower=staged.reduce((s,p)=>s+strength(p,defs),0);
      const prev=old?.squads.find(s=>s.objectiveId===o.id);
      const since=prev?.since??time;
      let phase='attack';
      if(e.hostile>power*style.risk && !emergency){phase='retreat';}
      else if(e.hostile>0 && stagedPower<e.hostile/style.risk && !(prev?.phase==='attack' && power>=e.hostile/style.risk))phase='stage';
      // Re-evaluate an unsuccessful assembly instead of trickling into an unwinnable position forever.
      if(phase==='stage' && time-since>25)phase='retreat';
      for(const p of units){const a=assignments.get(p.id);a.phase=phase;a.hostile=e.hostile;a.priority=e.score;}
      squads.push({objectiveId:o.id,ids:units.map(p=>p.id),phase,since,staging,power});
    }
    // Free units preserve territory pressure and hunting rather than joining the closest banner.
    for(const p of army.filter(p=>remaining.has(p.id))){
      const previous=old?.assignments.get(p.id),danger=evaluation.find(e=>e.objective.id===previous?.objectiveId);
      if(previous && ['objective_attacker','capital_raider'].includes(previous.role) && danger && danger.hostile>danger.friendly*style.risk+3){
        assignments.set(p.id,{role:'reserve',target:danger.objective,staging:home||king,phase:'retreat',objectiveId:danger.objective.id});
      }else assignments.set(p.id,{role:style===STRATEGIC_STYLES.assassin?'enemy_hunter':'frontier',target:home||king||p});
    }
    const attrition={...(old?.attrition||{})};
    if(old){
      const ids=new Set(own.map(p=>p.id));
      for(const squad of old.squads){
        const lost=squad.ids.filter(id=>!ids.has(id)).length;
        attrition[squad.objectiveId]=Math.max(0,(attrition[squad.objectiveId]||0)-Math.max(0,time-old.time)/18)+lost;
      }
    }
    const plan={time,signature,count:own.length,pressureBand:Math.floor(pressure*10),assignments,evaluation,squads,attrition,home,king,homeThreat,emergency,style,width,height,board,defs};
    this.memory.set(faction.idx,plan);return plan;
  }
  priority(plan,piece){
    const a=plan?.assignments.get(piece.id);if(!a)return 0;
    const threat=plan.evaluation.find(e=>e.objective.id===a.objectiveId)?.threat;
    if(!plan.homeThreat&&!threat && ['capital_defender','objective_defender','reserve'].includes(a.role) && a.phase!=='retreat' && near(piece,a.target,2))return -35;
    if(a.phase==='stage'&&near(piece,a.staging,2))return -20;
    if(a.role==='king_guard')return -75;
    if(a.role==='capital_defender')return plan.homeThreat?26:4;
    if(a.role==='objective_defender')return plan.evaluation.find(e=>e.objective.id===a.objectiveId)?.threat?22:4;
    if(a.role==='reserve')return 2;
    if(a.role==='frontier'||a.role==='enemy_hunter')return 8;
    return 18+(a.priority||0)*.04+(a.phase==='retreat'?8:0);
  }
  moveScore(plan,piece,move,{owner=null,capture=false}={}){
    const a=plan?.assignments.get(piece.id);if(!a)return 0;
    if(a.role==='frontier'||a.role==='enemy_hunter')return 0;
    if(a.role==='king_guard')return -distance(move,plan.home||piece)*2;
    const target=a.phase==='stage'||a.phase==='retreat'?a.staging:a.target;
    const field=this.route(plan.board,plan.width,plan.height,target,!!plan.defs[piece.type]?.amphibious);
    const from=field[piece.y*plan.width+piece.x],to=field[move.y*plan.width+move.x];
    if(to>=30000)return -60;
    let score=(from-to)*16-to*.7;
    if(a.role==='reserve'&&a.phase!=='retreat')score-=Math.max(0,distance(move,a.target)-6)*8;
    if(a.role==='capital_defender')score-=Math.max(0,distance(move,a.target)-5)*10;
    if(a.role==='objective_defender')score-=Math.max(0,distance(move,a.target)-4)*10;
    if(a.phase==='stage')score-=Math.max(0,4-distance(move,a.target))*24;
    if(a.phase==='retreat')score-=Math.max(0,7-distance(move,a.target))*22;
    if(owner===piece.faction)score+=4;
    if(a.target.kind && near(move,a.target,a.target.radius) && a.phase==='attack')score+=24;
    if(capture && a.phase==='retreat')score-=60;
    return score;
  }
  shouldHold(plan,piece,moves,options){
    const a=plan?.assignments.get(piece.id);
    if(!a || ['frontier','enemy_hunter'].includes(a.role))return false;
    const target=a.phase==='stage'||a.phase==='retreat'?a.staging:a.target;
    const lostHome=a.role==='capital_defender'&&a.target.owner!==piece.faction;
    const radius=a.role==='reserve'?4:a.role==='king_guard'?0:a.role==='capital_defender'&&!lostHome?3:a.target.radius||2;
    if(!near(piece,target,radius))return false;
    // Let tactical captures/check responses through; otherwise stay available instead of oscillating.
    if(options?.danger || moves.some(m=>m.capture || options?.enemyAt?.(m.x,m.y)))return false;
    return true;
  }
  spawnScore(plan,type,tile,defs){
    if(!plan)return 0;
    const fort=fortificationType({type},defs);
    let best=-Infinity;
    for(const e of plan.evaluation){
      if(e.objective.owner===plan.king?.faction && (fort || e.threat)){
        best=Math.max(best,(e.threat?50:fort?32:5)-distance(tile,e.objective)*5);
      }
    }
    if(plan.homeThreat)best=Math.max(best,85-distance(tile,plan.home)*7);
    for(const s of plan.squads)best=Math.max(best,28-distance(tile,s.staging)*3);
    return Number.isFinite(best)?best:0;
  }
}
