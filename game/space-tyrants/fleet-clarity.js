/* Space Tyrants — fleet clarity + peacetime directive tuning.
   Loaded after the Tactical Expanse patches so it can replace the large
   tactical rail with a compact locator and refine fleet map presentation. */

const STX_FLEET_FRIENDLY = "#5cf2a2";
const STX_baseFleetPosition = stxFleetPosition;

function stxFleetPhase(id){
  const text=String(id||"");let hash=0;
  for(let i=0;i<text.length;i++)hash=(hash*31+text.charCodeAt(i))>>>0;
  return (hash%6283)/1000;
}
function stxStationedFleetIndex(f,planet){
  const fleets=state.fleets.filter(x=>x.owner===f.owner&&!x.destroyed&&x.location===planet.id&&!state.ships.some(s=>s.fleetId===x.id)).sort((a,b)=>String(a.id).localeCompare(String(b.id)));
  return Math.max(0,fleets.findIndex(x=>x.id===f.id));
}

/* Stationed fleets occupy a visible orbit instead of sitting directly on top
   of the planet marker. Travelling fleets retain their actual ship position. */
stxFleetPosition=function(f){
  const base=STX_baseFleetPosition(f);
  if(!base||base.ship||!base.planet)return base;
  const p=base.planet,index=stxStationedFleetIndex(f,p),now=performance.now()/1000;
  const ring=50+(index%3)*18+Math.floor(index/3)*8;
  const direction=index%2?-1:1;
  const angle=stxFleetPhase(f.id)+now*(.13+(index%3)*.022)*direction;
  return {...base,x:p.x+Math.cos(angle)*ring,y:p.y+Math.sin(angle)*ring*.46,orbitRadius:ring,orbitAngle:angle,stationed:true};
};

function stxDrawFleetShipGlyph(x,y,angle,color,scale=1){
  ctx.save();ctx.translate(x,y);ctx.rotate(angle);ctx.fillStyle="rgba(3,10,18,.92)";ctx.strokeStyle=color;ctx.lineWidth=1.2;
  ctx.beginPath();ctx.moveTo(6*scale,0);ctx.lineTo(-4*scale,-3.2*scale);ctx.lineTo(-2.3*scale,0);ctx.lineTo(-4*scale,3.2*scale);ctx.closePath();ctx.fill();ctx.stroke();ctx.restore();
}

/* Friendly fleets always get the requested dotted green location marker.
   When stationed, the orbit itself is also visible and the fleet glyphs move
   around the host world. */
stxDrawFleetHighlights=function(){
  const now=performance.now()/1000,pulse=.5+.5*Math.sin(now*4.5);ctx.save();
  state.fleets.filter(stxFleetVisible).forEach(f=>{
    const pos=stxFleetPosition(f);if(!pos||!visible(pos.x,pos.y,170))return;
    const s=worldToScreen(pos.x,pos.y),mine=f.owner===0,color=mine?STX_FLEET_FRIENDLY:empire(f.owner).color;
    if(mine&&pos.stationed&&pos.planet){
      const ps=worldToScreen(pos.planet.x,pos.planet.y),rx=Math.max(24,(pos.orbitRadius||50)*state.camera.zoom),ry=Math.max(11,rx*.46);
      ctx.globalAlpha=.48;ctx.strokeStyle=STX_FLEET_FRIENDLY;ctx.lineWidth=1.25;ctx.setLineDash([2,5]);ctx.beginPath();ctx.ellipse(ps.x,ps.y,rx,ry,0,0,6.283);ctx.stroke();ctx.setLineDash([]);ctx.globalAlpha=1;
      const glyphs=Math.min(4,Math.max(2,Math.ceil((f.strength||1)/24)));
      for(let i=0;i<glyphs;i++){
        const spread=(i-(glyphs-1)/2)*7,heading=(pos.orbitAngle||0)+Math.PI/2;
        stxDrawFleetShipGlyph(s.x+Math.cos(heading+Math.PI/2)*spread,s.y+Math.sin(heading+Math.PI/2)*spread,heading,STX_FLEET_FRIENDLY,.82);
      }
    }
    const r=mine?13+pulse*2.5:10+pulse*2;
    ctx.strokeStyle=color;ctx.lineWidth=mine?2.1:1.35;ctx.globalAlpha=mine?.96:.68;ctx.setLineDash(mine?[2,5]:[3,4]);ctx.beginPath();ctx.arc(s.x,s.y,r+5,0,6.283);ctx.stroke();ctx.setLineDash([]);ctx.globalAlpha=1;
    if(!mine||!pos.stationed)stxDrawFleetShipGlyph(s.x,s.y,(pos.ship?Math.atan2((state.planets.find(p=>p.id===pos.ship.to)?.y||pos.y)-pos.y,(state.planets.find(p=>p.id===pos.ship.to)?.x||pos.x)-pos.x):now*.2),color,mine?.92:.78);
    const focused=f.id===state.stxFocusedFleet;
    if(mine&&(state.camera.zoom>.48||focused)){
      const label=`◆ ${Math.round(f.strength)} · ${f.name}`.slice(0,34),w=Math.min(178,Math.max(86,label.length*5.15));
      ctx.fillStyle="rgba(3,8,17,.82)";ctx.fillRect(s.x+13,s.y-17,w,20);ctx.fillStyle=STX_FLEET_FRIENDLY;ctx.font="700 8.5px system-ui";ctx.textAlign="left";ctx.fillText(label,s.x+17,s.y-5);
    }
  });ctx.restore();
};

function stxFleetLocatorLabel(f){
  const pos=stxFleetPosition(f);
  if(!pos)return `${f.name} — position unknown`;
  if(pos.planet)return `${f.name} — stationed at ${pos.planet.name}`;
  const target=pos.ship&&state.planets.find(p=>p.id===pos.ship.to);
  return `${f.name} — en route${target?` to ${target.name}`:""}`;
}
function stxInstallFleetLocator(){
  $("stxTacticalRail")?.remove();
  let locator=$("stxFleetLocator");
  if(!locator){
    locator=document.createElement("div");locator.id="stxFleetLocator";locator.className="stx-fleet-locator glass";locator.hidden=true;
    locator.innerHTML=`<span class="stx-fleet-locator-dot"></span><select id="stxFleetSelect" aria-label="Find one of your fleets"></select>`;
    document.body.appendChild(locator);
    $("stxFleetSelect").addEventListener("change",e=>{if(e.target.value)stxFocusFleet(e.target.value);e.target.value=""});
  }
  if(!$("stxFleetLocatorStyles")){
    const style=document.createElement("style");style.id="stxFleetLocatorStyles";style.textContent=`
.stx-fleet-locator{position:fixed;z-index:21;right:312px;top:90px;width:238px;height:40px;border-radius:12px;padding:5px 7px;display:flex;align-items:center;gap:6px;box-shadow:0 10px 28px rgba(0,0,0,.34)}
.stx-fleet-locator-dot{width:9px;height:9px;border:2px dotted #5cf2a2;border-radius:50%;box-shadow:0 0 9px rgba(92,242,162,.6);flex:0 0 auto}
.stx-fleet-locator select{min-width:0;width:100%;height:28px;border:0;outline:0;background:rgba(10,20,38,.74);color:#dfffee;border-radius:8px;padding:0 7px;font:700 .62rem system-ui;cursor:pointer}
.stx-fleet-locator select option{background:#091326;color:#eaf7ff}
@media(max-width:1050px){.stx-fleet-locator{right:14px;width:224px}}
@media(max-width:760px){.stx-fleet-locator{right:8px;top:82px;width:205px;height:36px}.stx-fleet-locator select{height:26px;font-size:.58rem}}
`;document.head.appendChild(style);
  }
}
function stxRefreshFleetLocator(){
  const locator=$("stxFleetLocator");if(!locator)return;
  locator.hidden=!state.running;if(!state.running)return;
  const select=$("stxFleetSelect"),fleets=state.fleets.filter(f=>f.owner===0&&!f.destroyed).sort((a,b)=>{
    const ap=stxFleetPosition(a),bp=stxFleetPosition(b);return Number(!!bp?.planet)-Number(!!ap?.planet)||String(ap?.planet?.name||"").localeCompare(String(bp?.planet?.name||""))||b.strength-a.strength;
  });
  const options=[`<option value="">FLEET LOCATOR · ${fleets.length} ACTIVE</option>`];
  fleets.forEach(f=>options.push(`<option value="${f.id}">${stxFleetLocatorLabel(f)}</option>`));
  select.innerHTML=options.join("");
}

/* Existing HUD hooks call this symbol dynamically, so replacing it removes the
   large command log while preserving all refresh/focus behavior. */
stxRefreshTacticalRail=function(){stxRefreshFleetLocator()};
stxInstallFleetLocator();stxRefreshFleetLocator();

function stxPlayerAtWar(){return state.wars.some(w=>w.active&&(w.a===0||w.b===0))}
function stxWarOnlyDirective(c){
  const key=`${c?.id||""} ${c?.title||""}`.toLowerCase();
  return /invasion|invade|planetary assault|total war|\braid\b|conquest/.test(key);
}

/* The generic invasion card used to remain eligible in peace, and could win
   the normal weighted command draw. It is now unavailable in peace and very
   competitive once a player war is active. */
const STX_genericInvasionCommand=COMMANDS.find(c=>c.id==="invasion");
if(STX_genericInvasionCommand){
  const oldScore=STX_genericInvasionCommand.score;
  STX_genericInvasionCommand.score=()=>stxPlayerAtWar()?Math.max(92,Number(oldScore?.()||0)):0;
}

const STX_warAwareOpenCommandPhase=openCommandPhase;
openCommandPhase=function(){
  STX_warAwareOpenCommandPhase();
  if($("commandModal").hidden||stxPlayerAtWar())return;
  let choices=state.commandChoices.filter(c=>!stxWarOnlyDirective(c));
  const ids=new Set(choices.map(c=>c.id)),cats=new Set(choices.map(c=>c.cat));
  const peaceful=COMMANDS.filter(c=>!stxWarOnlyDirective(c)&&!ids.has(c.id)).map(c=>({c,score:Number(c.score?.()||0)+rand(-8,8)})).filter(x=>x.score>5).sort((a,b)=>b.score-a.score);
  for(const entry of peaceful){
    if(choices.length>=4)break;
    if(cats.has(entry.c.cat))continue;
    choices.push({...entry.c,targetObj:entry.c.target?.()||null});ids.add(entry.c.id);cats.add(entry.c.cat);
  }
  state.commandChoices=choices.slice(0,4);state.commandSelected.clear();renderCommands();
};
