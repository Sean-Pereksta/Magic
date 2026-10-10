import {storagePrefix} from './league.mjs';
const finite=value=>value!==null&&value!==''&&value!==undefined&&Number.isFinite(Number(value));
const clamp=value=>Math.max(0,Math.min(100,value));
const id=value=>String(value??'');
const side=(competitors,teamId)=>competitors.find(c=>id(c.team?.id||c.id)===id(teamId))?.homeAway;
// Fixed schematic orientation: home attacks right, away attacks left.
// Use possession-relative ESPN distances; never infer geometry from play yardage.
export function fieldPosition(point,competitors){
 if(!point)return null;
 const direction=side(competitors,point.team?.id);
 if(finite(point.yardsToEndzone)&&Number(point.yardsToEndzone)>=0&&Number(point.yardsToEndzone)<=100&&direction){
  return direction==='home'?100-Number(point.yardsToEndzone):Number(point.yardsToEndzone);
 }
 const spot=String(point.possessionText||point.downDistanceText||'').match(/(?:at\s+)?([A-Z][A-Z0-9&]*)\s+(\d{1,2})\s*$/i);
 if(spot){
  const owner=competitors.find(c=>String(c.team?.abbreviation).toUpperCase()===spot[1].toUpperCase());
  const yards=Number(spot[2]);
  if(owner&&yards<=50)return owner.homeAway==='home'?yards:100-yards;
 }
 if(/^50$/.test(String(point.possessionText||'')))return 50;
 return null;
}
export function fieldModel(event,entry){
 const c=event?.competitions?.[0],competitors=c?.competitors||[],s=c?.situation;
 const status=c?.status||event?.status||{},state=status.type?.state||'pre';
 const raw=entry?.raw||s?.lastPlay;
 const liveSpot=s?fieldPosition({...s,team:{id:s.possession}},competitors):null;
 const end=fieldPosition(raw?.end,competitors),start=fieldPosition(raw?.start,competitors);
 const possession=id(s?.possession||raw?.end?.team?.id||raw?.start?.team?.id);
 const direction=side(competitors,possession)==='home'?1:side(competitors,possession)==='away'?-1:0;
 const ball=state==='pre'?null:liveSpot??end;
 const type=String(raw?.type?.text||''),text=String(raw?.text||'');
 const invalid=/no play|nullified|overturned|incomplete|incompletion|intercept|fumble|penalty/i.test(type+' '+text)||raw?.isPenalty||raw?.isTurnover;
 const pass=!invalid&&/pass|reception/i.test(type+' '+text);
 const run=!invalid&&(/rush|run/i.test(type)||entry?.kind==='run');
 const route=start!==null&&end!==null&&(pass||run)?{start,end,kind:pass?'pass':'run'}:null;
 const down=s?.shortDownDistanceText||s?.downDistanceText||raw?.end?.shortDownDistanceText||'';
 const distance=s?.distance??raw?.end?.distance;
 const target=ball!==null&&finite(distance)&&Number(distance)>=0&&direction?clamp(ball+direction*Number(distance)):null;
 return {competitors,state,ball,route,possession,direction,target,down,spot:s?.possessionText||raw?.end?.possessionText||'',status:status.type?.shortDetail||status.type?.detail||'Scheduled',last:entry?.text||text||'Waiting for the first play.',lastPosition:liveSpot===null&&end!==null};
}
const node=(tag,text,cls)=>{const n=document.createElement(tag);if(text!=null)n.textContent=text;if(cls)n.className=cls;return n;};
const svgNode=(tag,attrs,text)=>{const n=document.createElementNS('http://www.w3.org/2000/svg',tag);for(const [k,v] of Object.entries(attrs||{}))n.setAttribute(k,String(v));if(text!=null)n.textContent=text;return n;};
function drawField(model){
 const svg=svgNode('svg',{viewBox:'0 0 120 58',role:'img','aria-label':model.ball===null?'Field position unavailable':`Ball at ${model.spot||Math.round(model.ball)+' on schematic field'}. ${model.down}.`});
 svg.append(svgNode('rect',{x:0,y:0,width:120,height:58,rx:3,fill:'#14543b'}));
 svg.append(svgNode('rect',{x:0,y:0,width:10,height:58,fill:'#203b50'}),svgNode('rect',{x:110,y:0,width:10,height:58,fill:'#203b50'}));
 for(let yard=0;yard<=100;yard+=10){
  const x=yard+10;svg.append(svgNode('line',{x1:x,x2:x,y1:0,y2:58,stroke:'#d7f5e4','stroke-width':.25,opacity:.65}));
  if(yard>0&&yard<100)svg.append(svgNode('text',{x,y:8,'text-anchor':'middle',fill:'#cee7d6','font-size':3.5},Math.min(yard,100-yard)));
 }
 for(let yard=5;yard<100;yard+=5)for(const y of [20,38])svg.append(svgNode('line',{x1:yard+10,x2:yard+10,y1:y-1,y2:y+1,stroke:'#c3ddca','stroke-width':.35}));
 for(const [role,x] of [['home',5],['away',115]]){
  const team=model.competitors.find(c=>c.homeAway===role)?.team;
  svg.append(svgNode('text',{x,y:29,transform:`rotate(-90 ${x} 29)`,'text-anchor':'middle',fill:'white','font-size':4},team?.abbreviation||role.toUpperCase()));
 }
 if(model.target!==null&&model.state==='in')svg.append(svgNode('line',{x1:model.target+10,x2:model.target+10,y1:10,y2:52,stroke:'#ffdd57','stroke-width':.8,'stroke-dasharray':'2 1'}));
 if(model.route){
  const a=model.route.start+10,b=model.route.end+10;
  const d=model.route.kind==='pass'?`M ${a} 29 Q ${(a+b)/2} 10 ${b} 29`:`M ${a} 29 L ${b} 29`;
  svg.append(svgNode('circle',{cx:a,cy:29,r:1.4,fill:'none',stroke:'#fff','stroke-width':.7}));
  svg.append(svgNode('path',{d,fill:'none',stroke:model.route.kind==='pass'?'#88d5ff':'#ffdb8b','stroke-width':1.3,'stroke-linecap':'round',class:'lastPlayRoute'}));
  svg.append(svgNode('circle',{cx:b,cy:29,r:1.5,fill:'#fff'}));
 }
 if(model.ball!==null){
  const x=model.ball+10;
  svg.append(svgNode('circle',{cx:x,cy:29,r:3.1,fill:'#c7f568',stroke:'#102515','stroke-width':.9}));
  if(model.direction)svg.append(svgNode('path',{d:`M ${x+model.direction*4} 27 L ${x+model.direction*6} 29 L ${x+model.direction*4} 31`,fill:'none',stroke:'#c7f568','stroke-width':.9}));
 }else svg.append(svgNode('text',{x:60,y:31,'text-anchor':'middle',fill:'white','font-size':4},model.state==='pre'?'Kickoff upcoming':'Position unavailable'));
 return svg;
}
export function installFieldView(){
 const grid=document.getElementById('fieldGrid');if(!grid)return null;
 let latest=[],ids=[],getEntry=()=>null,stale=false,view='play',paintKey='';
 try{view=localStorage.getItem(`${storagePrefix}:view`)==='visual'?'visual':'play';}catch{}
 const paint=()=>{
  if(view!=='visual')return;
  const models=ids.map(gameId=>{const event=latest.find(e=>id(e.id)===gameId);return {gameId,event,model:fieldModel(event,getEntry(gameId))};});
  const key=JSON.stringify([models,stale]);if(key===paintKey)return;paintKey=key;
  grid.replaceChildren();
  for(const {gameId,event,model:m} of models){
   const card=node('article',null,'fieldCard');card.dataset.gameId=gameId;
   const score=m.competitors.slice().sort((a,b)=>(a.homeAway==='away'?-1:1)-(b.homeAway==='away'?-1:1)).map(c=>`${c.team?.abbreviation||'Team'} ${m.state==='pre'?'—':c.score??'—'}`).join(' · ');
   card.append(node('h3',event?score:'Game '+gameId),node('p',stale?'Data delayed · last known position':event?m.status:'Waiting for game data','fieldStatus'),drawField(m));
   const possessor=m.competitors.find(c=>id(c.team?.id||c.id)===m.possession)?.team?.abbreviation;
   card.append(node('p',[possessor?`${possessor} ${m.direction>0?'→':'←'}`:'',m.down,m.spot,m.lastPosition?'Last reported spot':''].filter(Boolean).join(' · '),'fieldSituation'),node('p',m.last,'fieldLast'));
   grid.append(card);
  }
  if(!ids.length)grid.append(node('p','Add games to your rotation below to see their fields.','emptyState'));
 };
 const switchView=value=>{
  view=value;
  document.getElementById('visualPanel').hidden=view!=='visual';
  for(const id of ['activeGame','playPanel'])document.getElementById(id).hidden=view==='visual';
  for(const button of document.querySelectorAll('[data-view]'))button.setAttribute('aria-selected',String(button.dataset.view===view));
  try{localStorage.setItem(`${storagePrefix}:view`,view);}catch{}
  paint();
 };
 for(const button of document.querySelectorAll('[data-view]')){
  button.onclick=()=>switchView(button.dataset.view);
  button.onkeydown=e=>{if(['ArrowLeft','ArrowRight','Home','End'].includes(e.key)){e.preventDefault();const buttons=[...document.querySelectorAll('[data-view]')];const target=e.key==='Home'?buttons[0]:e.key==='End'?buttons.at(-1):buttons.find(b=>b!==button);target.focus();switchView(target.dataset.view);}};
 }
 switchView(view);
 return {render(events,selected,getLatest,options={}){latest=events;ids=selected;getEntry=getLatest;stale=!!options.stale;paint();}};
}
