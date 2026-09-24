import { ART, AssetCache } from './asset-manifest.mjs';
import { buildingLevel } from './economy.mjs';
import { familyCount } from './warfare.mjs';
import { HOUSES, BUILDINGS, UNITS } from './data.mjs';
import { PLAYER, settlements, sizeOf, tileId } from './core.mjs';

import { BattleEffects } from './battle-effects.mjs';
import { MapArt, tileVariant } from './art.mjs';
import { HEX_DIRECTIONS, GeographyCache } from './geography.mjs';
import { GeographyArt, landClipPath } from './geography-art.mjs';

const DIRECTIONS = HEX_DIRECTIONS;
const RADIUS = 25, SQRT3 = Math.sqrt(3);
export const hexPixel = t => ({ x: RADIUS * SQRT3 * (t.q + t.r / 2), y: RADIUS * 1.5 * t.r });
export function pixelHex(x, y) {
  const q = (SQRT3 / 3 * x - y / 3) / RADIUS, r = 2 / 3 * y / RADIUS;
  let rq = Math.round(q), rr = Math.round(r), rs = Math.round(-q - r);
  const dq = Math.abs(rq - q), dr = Math.abs(rr - r), ds = Math.abs(rs + q + r);
  if (dq > dr && dq > ds) rq = -rr - rs; else if (dr > ds) rr = -rq - rs;
  return tileId(rq, rr);
}
export class WorldMap {
  constructor(canvas, { getState, onSelect }) {
    this.canvas = canvas; this.ctx = canvas.getContext('2d'); this.getState = getState; this.onSelect = onSelect;
    this.art = new MapArt(); this.assets=new AssetCache(()=>this.draw()); this.effects = new BattleEffects(); this.reducedEffects = false; this.motion = matchMedia('(prefers-reduced-motion: reduce)');
    this.zoom = 1; this.x = 0; this.y = 0; this.selected = '5,6'; this.armyId = null; this.pointers = new Map(); this.drag = null; this.moved = false; this.frame = null;
    this.observer = new ResizeObserver(() => this.resize()); this.observer.observe(canvas);
    canvas.addEventListener('pointerdown', e => {
      canvas.setPointerCapture(e.pointerId); this.pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
      this.drag = { x: e.clientX, y: e.clientY, startX: e.clientX, startY: e.clientY };
      this.moved = this.pointers.size > 1; this.pinch = this.pinchDistance();
    });
    canvas.addEventListener('pointermove', e => {
      if (!this.pointers.has(e.pointerId)) return;
      const old = this.pointers.get(e.pointerId); this.pointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
      if (this.pointers.size === 2) {
        const d = this.pinchDistance(); if (this.pinch > 0) this.setZoom(this.zoom * d / this.pinch); this.pinch = d; this.moved = true;
      } else if (this.drag) {
        this.x -= (e.clientX - old.x) / this.zoom; this.y -= (e.clientY - old.y) / this.zoom;
        if (Math.hypot(e.clientX - this.drag.startX, e.clientY - this.drag.startY) > 6) this.moved = true;
        this.draw();
      }
    });
    const release = e => {
      if (!this.pointers.has(e.pointerId)) return;
      const click = e.type === 'pointerup' && !this.moved && this.pointers.size === 1;
      this.pointers.delete(e.pointerId);
      if (click) {
        const rect = canvas.getBoundingClientRect();
        const x = (e.clientX - rect.left - this.width / 2) / this.zoom + this.x;
        const y = (e.clientY - rect.top - this.height / 2) / this.zoom + this.y;
        const badge = this.hits?.findLast(h => x >= h.left && x <= h.right && y >= h.top && y <= h.bottom);
        const id = badge?.tile || pixelHex(x, y);
        if (this.getState().tiles[id]) this.onSelect(id);
      }
      if (!this.pointers.size) this.drag = null;
      else { const p = [...this.pointers.values()][0]; this.drag = { ...p, startX: p.x, startY: p.y }; this.moved = true; }
    };
    canvas.addEventListener('pointerup', release); canvas.addEventListener('pointercancel', release); canvas.addEventListener('lostpointercapture', release);
    canvas.addEventListener('wheel', e => { e.preventDefault(); this.setZoom(this.zoom * Math.exp(-e.deltaY * .001)); }, { passive: false });
    canvas.addEventListener('keydown', e => {
      const delta = { ArrowRight: [1, 0], ArrowLeft: [-1, 0], ArrowUp: [0, -1], ArrowDown: [0, 1] }[e.key];
      if (delta) { e.preventDefault(); const s = this.getState(), t = s.tiles[this.selected], next = s.tiles[tileId(t.q + delta[0], t.r + delta[1])]; if (next) { this.selected = next.id; this.center(next.id); this.onSelect(next.id); } }
      if (e.key === 'Enter') { e.preventDefault(); this.onSelect(this.selected); }
      if (e.key === '+' || e.key === '=') this.setZoom(this.zoom * 1.2);
      if (e.key === '-') this.setZoom(this.zoom / 1.2);
      if (e.key === 'Home') { e.preventDefault(); this.home(); }
    });
    this.home();
  }
  pinchDistance() { const p = [...this.pointers.values()]; return p.length === 2 ? Math.hypot(p[0].x - p[1].x, p[0].y - p[1].y) : 0; }
  resize() {
    const rect = this.canvas.getBoundingClientRect(); this.width = rect.width; this.height = rect.height;
    this.dpr = Math.min(2, globalThis.devicePixelRatio || 1);
    this.canvas.width = Math.round(this.width * this.dpr); this.canvas.height = Math.round(this.height * this.dpr); this.draw();
  }
  setZoom(zoom) { this.zoom = Math.min(2.8, Math.max(.22, zoom)); this.draw(); }
  center(id) { const t = this.getState().tiles[id]; if (!t) return; const p = hexPixel(t); this.x = p.x; this.y = p.y; this.draw(); }
  home() { this.zoom = this.width < 600 ? .85 : 1.25; this.center(settlements(this.getState(), PLAYER)[0]?.id || '5,6'); }
  fit() { const bottom = hexPixel({ q: 39, r: 29 }); this.x = bottom.x / 2; this.y = bottom.y / 2; this.setZoom(Math.min(this.width / (bottom.x + 100), this.height / (bottom.y + 100))); }
  draw() { if (!this.frame) this.frame = requestAnimationFrame(() => { this.frame = null; this.render(); }); }
  hex(x, y, radius = RADIUS) {
    const c = this.ctx; c.beginPath();
    for (let i = 0; i < 6; i++) { const angle = (60 * i - 30) * Math.PI / 180; const px = x + Math.cos(angle) * radius, py = y + Math.sin(angle) * radius; if (i) c.lineTo(px, py); else c.moveTo(px, py); } c.closePath();
  }
  edge(p, i) {
    const c = this.ctx; c.beginPath();
    for (let j=0;j<2;j++) {const a=(60*(i+j)-30)*Math.PI/180; const x=p.x+Math.cos(a)*RADIUS,y=p.y+Math.sin(a)*RADIUS; if(j)c.lineTo(x,y);else c.moveTo(x,y);}
  }
  groundArt(c,t,x,y,coastMask=0) {
    // The photographic hexes have transparent margins. Fill the exact tile first,
    // then clip the inland texture so there are no ocean-colored gaps on dry land.
    c.save();this.hex(x,y);c.clip();
    if(coastMask) {
      this.art.ground(c,{...t,terrain:'water'},x,y);
      this.assets.draw(c,ART.terrain.water[0],x-25,y-25,50,50);
      c.translate(x,y);c.clip(landClipPath(coastMask),'evenodd');c.translate(-x,-y);
    }
    this.art.ground(c,t,x,y);
    const loaded=this.assets.draw(c,ART.terrain[t.terrain]?.[t.terrain==='water'?0:tileVariant(t)%6],x-25,y-25,50,50);
    if(!loaded&&this.zoom>.38&&!t.building)this.art.terrain(c,t,x,y);
    c.restore();
  }
  structureArt(c,url,x,y,w,h,color) {
    return this.assets.drawOutlined(c,url,x,y,w,h,color,this.zoom,this.dpr);
  }
  constructionArt(c,progress,color) {
    if(this.structureArt(c,ART.construction[Math.max(0,Math.min(2,Math.floor(progress*3)))],-26,-35,52,52,color))return;
    // Scaffolding is only a fallback when neither PNG nor bundled SVG is ready.
    for(const x of [-13,13]){c.beginPath();c.moveTo(x,-13);c.lineTo(x,8);c.stroke();}
    c.beginPath();c.moveTo(-13,-12);c.lineTo(13,-12);c.lineTo(-13,7);c.lineTo(13,7);c.stroke();
  }
  armyArt(c,units,x,y,color,sigil,phase) {
    const dominant=Object.keys(units).filter(u=>units[u]>0).sort((a,b)=>(units[b]*(UNITS[b].family==='siege'?8:1))-(units[a]*(UNITS[a].family==='siege'?8:1)))[0];
    if(this.structureArt(c,ART.units[dominant],x-22,y-36+phase,44,44,color))return;
    this.art.formation(c,{levy:familyCount({units},'infantry'),archer:familyCount({units},'ranged'),cavalry:familyCount({units},'mounted'),siege:familyCount({units},'siege')},x,y+4,color,sigil,this.zoom>.65,phase);
  }
  render() {
    if (!this.width || !this.height) return;
    const c=this.ctx,s=this.getState(), now=performance.now();
    this.assets ||= new AssetCache(()=>this.draw());
    this.geography ||= new GeographyCache();this.geographyArt ||= new GeographyArt();
    const geography=this.geography.get(s.tiles);
    this.effects ||= new BattleEffects(); this.effects.ingest(s, now);
    const reduced = this.motion.matches || this.reducedEffects;
    const focus=`${s.seed}:${s.turn}:${this.selected}`;
    if(this.focus!==focus){this.focus=focus;this.pulseUntil=now+650;}
    c.setTransform(this.dpr,0,0,this.dpr,0,0);
    const sea=c.createLinearGradient(0,0,this.width,this.height);sea.addColorStop(0,'#193a4a');sea.addColorStop(1,'#0e222e');c.fillStyle=sea;c.fillRect(0,0,this.width,this.height);
    c.translate(this.width/2,this.height/2);c.scale(this.zoom,this.zoom);c.translate(-this.x,-this.y);
    const colors=Object.fromEntries(HOUSES.map(h=>[h.id,h.color]));
    const inView=p=>Math.abs(p.x-this.x)<this.width/this.zoom/2+90&&Math.abs(p.y-this.y)<this.height/this.zoom/2+90;
    const visible=Object.values(s.tiles).filter(t=>inView(hexPixel(t)));
    // Separate ground and object passes keep roads, borders and taller sprites coherent.
    for(const t of visible){const p=hexPixel(t),g=geography.get(t.id);this.groundArt(c,{...t,terrain:g.ground},p.x,p.y,g.coastMask);}
    for(const t of visible){
      const p=hexPixel(t);
      c.save();
      const coastMask=geography.get(t.id).coastMask;
      if(coastMask){c.translate(p.x,p.y);c.clip(landClipPath(coastMask),'evenodd');c.translate(-p.x,-p.y);}
      this.hex(p.x,p.y);c.strokeStyle='#122f3326';c.lineWidth=.55;c.stroke();
      if(t.owner){c.fillStyle=`${colors[t.owner]}12`;c.fill();}
      DIRECTIONS.forEach(([dq,dr],i)=>{
        const n=s.tiles[tileId(t.q+dq,t.r+dr)];
        if(t.owner&&n?.owner!==t.owner){this.edge(p,i);c.strokeStyle='#122630a0';c.lineWidth=3.5;c.stroke();c.strokeStyle=colors[t.owner];c.lineWidth=1.6;c.stroke();}
      });
      c.restore();
      this.geographyArt.draw(c,geography.get(t.id),p.x,p.y,this.assets,ART.geography);
      if(t.road){
        const roadRendered=this.structureArt(c,ART.structures.road[buildingLevel(t,'road')],p.x-22,p.y-16,44,32,colors[t.owner]);
        if(!roadRendered)for(const [dq,dr] of DIRECTIONS.slice(0,3)){
          const n=s.tiles[tileId(t.q+dq,t.r+dr)];if(!n?.road)continue;const v=hexPixel(n);
          c.beginPath();c.moveTo(p.x,p.y);c.lineTo(v.x,v.y);c.strokeStyle='#40504c';c.lineWidth=4;c.stroke();c.strokeStyle='#cfbf8d';c.lineWidth=Math.min(buildingLevel(t,'road'),buildingLevel(n,'road'))+1;c.stroke();
          c.setLineDash([1,3]);c.strokeStyle='#f3dfac';c.lineWidth=.6;c.stroke();c.setLineDash([]);
        }
        if(geography.get(t.id).hasRiver&&!this.structureArt(c,ART.overlays[buildingLevel(t,'road')>1?'bridge_stone':'bridge_wood'],p.x-22,p.y-16,44,32,colors[t.owner])){c.save();c.translate(p.x,p.y);c.rotate(-.5);c.fillStyle='#ad9973';c.fillRect(-8,-3,16,6);c.strokeStyle='#efdab1';c.lineWidth=.8;c.strokeRect(-8,-3,16,6);c.restore();}
      }
    }
    for(const t of visible){
      const p=hexPixel(t);
      if(t.building){
        const rendered=this.structureArt(c,ART.structures[t.building]?.[buildingLevel(t,t.building)],p.x-31,p.y-45,62,62,colors[t.owner]);
        if(!rendered)this.art.building(c,t,p.x,p.y,colors[t.owner]||'#d7d3b5');
        if(rendered&&['city','town','fort'].includes(t.building)&&this.zoom>.65){const improvements=Object.keys(BUILDINGS).filter(id=>BUILDINGS[id].settlement&&buildingLevel(t,id)).sort((a,b)=>buildingLevel(t,b)-buildingLevel(t,a)).slice(0,3);improvements.forEach((id,i)=>this.structureArt(c,ART.structures[id][buildingLevel(t,id)],p.x-33+i*23,p.y-8,25,25,colors[t.owner]));}
        if(t.siege){c.fillStyle='#d58e63';c.font='bold 9px system-ui';c.fillText(t.walls+(t.fortIntegrity||0)>0?'SIEGE':'BREACH',p.x,p.y+28);}
      }
      if(t.resource&&!t.building&&this.zoom>.8&&t.resource!=='wood'&&!this.assets.draw(c,ART.resources[t.resource],p.x-7,p.y+9,14,14)){
        c.fillStyle='#e7d9a5';c.strokeStyle='#243a3a';c.lineWidth=2;c.font='10px Georgia';c.textAlign='center';const label={iron:'⚒',stone:'◆',food:'ˇˇˇ'}[t.resource]||'';c.strokeText(label,p.x,p.y+17);c.fillText(label,p.x,p.y+17);
      }
      if(t.project){
        c.save();c.translate(p.x,p.y);c.strokeStyle='#ebca84';c.lineWidth=1;c.setLineDash([3,3]);this.hex(0,0,21);c.stroke();c.setLineDash([]);
        const progress=1-t.project.remaining/t.project.total;
        this.constructionArt(c,progress,colors[t.owner]);
        c.fillStyle='#152d32';c.fillRect(-15,19,30,4);c.fillStyle='#f2cd7e';c.fillRect(-15,19,30*Math.max(.08,progress),4);c.restore();
      }
    }
    const a=s.armies.find(a=>a.id===this.armyId);
    if(a?.path.length){
      c.beginPath();const from=hexPixel(s.tiles[a.tile]);c.moveTo(from.x,from.y);
      for(const id of a.path){const p=hexPixel(s.tiles[id]);c.lineTo(p.x,p.y);}
      c.strokeStyle='#142c32';c.lineWidth=5;c.stroke();c.setLineDash([5,4]);c.lineWidth=2.2;c.strokeStyle='#ffe6a3';c.stroke();c.setLineDash([]);
      const end=hexPixel(s.tiles[a.path.at(-1)]);this.hex(end.x,end.y,9);c.fillStyle='#f4d58a38';c.fill();c.strokeStyle='#ffdf90';c.lineWidth=1.5;c.stroke();
    }
    this.hits=[];
    const grouped=new Map(),offsets=new Map();
    for(const army of s.armies){const key=`${army.tile}:${army.owner}`;if(!grouped.has(key))grouped.set(key,[]);grouped.get(key).push(army);}
    for(const group of grouped.values()){
      const army=group[0],p=hexPixel(s.tiles[army.tile]);if(!inView(p))continue;
      const row=offsets.get(army.tile)||0;offsets.set(army.tile,row+1);
      const selected=group.some(a=>a.id===this.armyId),total=group.reduce((n,a)=>n+sizeOf(a),0),color=colors[army.owner];
      const badgeScale=Math.max(1,.55/this.zoom), badgeY=p.y+17+row*23;
      this.hits.push({tile:army.tile,left:p.x-19*badgeScale,right:p.x+19*badgeScale,top:badgeY-8*badgeScale,bottom:badgeY+12*badgeScale});
      const units=Object.fromEntries(Object.keys(UNITS).map(u=>[u,group.reduce((n,a)=>n+(a.units[u]||0),0)]));
      const marching=group.some(a=>a.path.length)&&!reduced&&now<this.pulseUntil;
      this.armyArt(c,units,p.x,p.y+row*23,color,HOUSES.find(h=>h.id===army.owner).sigil,marching?Math.sin(now/75)*.7:0);
      c.save();c.translate(p.x,badgeY);c.scale(badgeScale,badgeScale);
      c.fillStyle='#081c2999';c.beginPath();c.ellipse(2,7,22,7,0,0,Math.PI*2);c.fill();
      c.beginPath();c.roundRect(-19,-8,38,20,4);const plate=c.createLinearGradient(0,-8,0,12);plate.addColorStop(0,'#344b55');plate.addColorStop(1,'#102932');c.fillStyle=plate;c.fill();c.strokeStyle=selected?'#fff0b5':color;c.lineWidth=selected?2:1.3;c.stroke();
      c.fillStyle=color;c.beginPath();c.moveTo(-16,-5);c.lineTo(-8,-5);c.lineTo(-8,3);c.lineTo(-12,7);c.lineTo(-16,3);c.closePath();c.fill();
      c.fillStyle='#132936';c.font='bold 8px Georgia';c.textAlign='center';c.fillText(group.some(a=>familyCount(a,'siege'))?'♜':group.some(a=>familyCount(a,'mounted'))?'♞':'⚔',-12,2);
      c.font='bold 11px system-ui';c.fillStyle='#fff1d0';c.fillText(total,5,6);c.restore();
    }
    for(const envoy of (s.ambassadors||[]).filter(a=>a.status!=='dead')) {
      const tile=s.tiles[envoy.tile],p=tile&&hexPixel(tile);if(!p||!inView(p))continue;
      c.save();c.translate(p.x-23,p.y+3);c.scale(Math.max(1,.6/this.zoom),Math.max(1,.6/this.zoom));
      c.fillStyle='#122c39';c.strokeStyle=colors[envoy.owner];c.lineWidth=1.3;c.beginPath();c.roundRect(-9,-11,18,23,5);c.fill();c.stroke();
      c.fillStyle=colors[envoy.owner];c.font='bold 14px Georgia';c.textAlign='center';c.fillText(envoy.status==='detained'?'⊠':'⚜',0,5);c.restore();
      this.hits.push({tile:envoy.tile,left:p.x-34,right:p.x-12,top:p.y-12,bottom:p.y+17});
    }
    this.effects.drawWorld(c,s,hexPixel,inView,now,reduced,this.zoom);
    const selected=s.tiles[this.selected];
    if(selected){const p=hexPixel(selected);const pulse=reduced?0:Math.max(0,(this.pulseUntil-now)/650);this.hex(p.x,p.y,24);c.lineWidth=4;c.strokeStyle='#172d38';c.stroke();c.lineWidth=2;c.strokeStyle='#fff0b4';c.stroke();
      if(pulse){this.hex(p.x,p.y,24+(1-pulse)*12);c.globalAlpha=pulse*.55;c.stroke();c.globalAlpha=1;}
    }
    for(const t of visible.filter(t=>['city','town'].includes(t.building))){
      if(this.zoom<.65&&!t.capital)continue;const p=hexPixel(t),label=t.name||'Town';
      c.save();c.translate(p.x,p.y-(t.building==='city'?49:34));c.scale(Math.max(1,.6/this.zoom),Math.max(1,.6/this.zoom));
      c.font=t.capital?'bold 10px Georgia':'9px Georgia';c.textAlign='center';const w=c.measureText(label).width;
      c.fillStyle='#112733eb';c.beginPath();c.roundRect(-w/2-7,-10,w+14,16,3);c.fill();c.fillStyle=colors[t.owner]||'#eddfb9';c.fillRect(-w/2-3,5,w+6,.8);c.fillText(label,0,1);c.restore();
    }
    c.setTransform(this.dpr,0,0,this.dpr,0,0);
    const vignette=c.createRadialGradient(this.width/2,this.height/2,this.height*.3,this.width/2,this.height/2,Math.max(this.width,this.height)*.7);vignette.addColorStop(0,'#061b2400');vignette.addColorStop(1,'#06111a66');c.fillStyle=vignette;c.fillRect(0,0,this.width,this.height);
    // Restrained cartographic compass, entirely non-interactive.
    if(this.width>500&&this.height>240){const x=this.width-42,y=this.height-68;c.strokeStyle='#dcc89590';c.lineWidth=1;c.beginPath();c.arc(x,y,18,0,Math.PI*2);c.stroke();c.fillStyle='#e6d6a6';c.beginPath();c.moveTo(x,y-16);c.lineTo(x-5,y+7);c.lineTo(x,y+3);c.lineTo(x+5,y+7);c.closePath();c.fill();c.font='10px Georgia';c.textAlign='center';c.fillText('N',x,y-24);}
    this.effects.drawResults(c,s,this.width,this.height,now);
    if(!document.hidden&&((!reduced&&now<this.pulseUntil)||this.effects.animating(now,reduced)))this.draw();
    // A single expiry timer clears a result without running idle animation.
    if(this.effects.results.length&&!this.resultTimer)this.resultTimer=setTimeout(()=>{this.resultTimer=null;this.draw();},5600);
  }
}
