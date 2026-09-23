// Native canvas artwork: bounded, reusable sprites; no downloads or per-frame randomness.
const PALETTE = {
  plains: ['#8c9b65', '#657f50', '#4d6647'], forest: ['#547b58', '#365e48', '#294939'],
  hills: ['#a5a17b', '#7d8464', '#626d53'], mountain: ['#889395', '#677777', '#4a5d60'],
  water: ['#427a8d', '#2c5b74', '#1c3c57'], coast: ['#c5bc89', '#a29e72', '#7f8c69']
};
export function tileVariant(t) { return ((t.q * 73856093) ^ (t.r * 19349663)) >>> 0; }
function rng(seed) { return () => { seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0; return seed / 4294967296; }; }
function poly(c, points, fill, stroke) {
  c.beginPath(); points.forEach(([x,y], i) => i ? c.lineTo(x,y) : c.moveTo(x,y)); c.closePath();
  if (fill) { c.fillStyle = fill; c.fill(); } if (stroke) { c.strokeStyle = stroke; c.lineWidth = .65; c.stroke(); }
}
function line(c, points, color, width = 1) { c.beginPath(); points.forEach(([x,y], i) => i ? c.lineTo(x,y) : c.moveTo(x,y)); c.strokeStyle = color; c.lineWidth = width; c.stroke(); }
function ellipse(c, x,y,rx,ry,color) { c.beginPath(); c.ellipse(x,y,rx,ry,0,0,Math.PI*2); c.fillStyle=color; c.fill(); }
function pine(c,x,y,h) {
  ellipse(c,x+3,y+2,h*.32,2,'#152d3040'); c.fillStyle='#6d6045'; c.fillRect(x-.7,y-3,1.4,5);
  for(let j=0;j<3;j++) { const top=y-h+j*h*.24, w=h*(.23+j*.045); poly(c,[[x,top],[x-w,top+h*.53],[x+w,top+h*.53]],'#244d3b'); poly(c,[[x,top],[x-w,top+h*.53],[x-.4,top+h*.4]],j===0?'#739169':'#507954'); }
}
function rock(c,x,y,w,h,snow=false) {
  ellipse(c,x+3,y+2,w*.7,3,'#243b3d30');
  poly(c,[[x-w,y],[x-w*.2,y-h],[x+w,y]],snow?'#889795':'#9b9e7a','#566b6250');
  poly(c,[[x-w*.2,y-h],[x+w,y],[x+w*.05,y-2]],snow?'#4f646a':'#657858');
  line(c,[[x-w*.75,y-2],[x-w*.2,y-h+1],[x-w*.1,y-h*.43]],'#c5cdb080',.7);
  if(snow) poly(c,[[x-w*.2,y-h],[x-w*.48,y-h*.58],[x-w*.13,y-h*.7],[x+w*.04,y-h*.52],[x+w*.24,y-h*.6]],'#e2e6d5');
}
function cottage(c,x,y,scale=1,roof='#885441') {
  c.save(); c.translate(x,y); c.scale(scale,scale);
  ellipse(c,2,4,8,3,'#102b304f');
  poly(c,[[-6,-3],[1,-5],[7,-2],[7,5],[0,8],[-6,4]],'#c8ba92');
  poly(c,[[0,-2],[7,-2],[7,5],[0,8]],'#8b947d');
  poly(c,[[-8,-3],[-2,-10],[7,-6],[9,-1],[0,2]],roof,'#283c3b');
  poly(c,[[-8,-3],[-2,-10],[0,2]],'#bc8b61');
  c.fillStyle='#3a4844'; c.fillRect(-4,0,2,4); c.fillStyle='#efce84'; c.fillRect(3,1,1.5,2);
  c.restore();
}
function tower(c,x,y,h=19,roof=false) {
  ellipse(c,x+3,y+2,6,2,'#122b3245');
  c.fillStyle='#b2b6a0'; c.fillRect(x-4,y-h,8,h); c.fillStyle='#d9d4b8'; c.fillRect(x-4,y-h,3,h); c.fillStyle='#748b88'; c.fillRect(x+2,y-h,3,h);
  for(let row=3;row<h;row+=4) line(c,[[x-4,y-h+row],[x+4,y-h+row]],'#637c7955',.4);
  c.fillStyle='#2d464b'; c.fillRect(x-.8,y-h+5,1.6,4);
  c.fillStyle='#e1d9b8'; for(let i=-4;i<=4;i+=3) c.fillRect(x+i,y-h-2,2,4);
  if(roof) poly(c,[[x-6,y-h],[x,y-h-10],[x+6,y-h]],'#496675','#adc4bd');
}
export class MapArt {
  constructor() { this.cache = new Map(); }
  sprite(key, paint) {
    if (!this.cache.has(key)) {
      const canvas=document.createElement('canvas'); canvas.width=192; canvas.height=192;
      const c=canvas.getContext('2d'); c.scale(2,2); c.translate(48,48); paint(c);
      // Even a long campaign remains bounded (terrain variants + building combinations).
      if(this.cache.size>=192) this.cache.delete(this.cache.keys().next().value);
      this.cache.set(key,canvas);
    }
    return this.cache.get(key);
  }
  ground(c,t,x,y) {
    const variant=tileVariant(t)%6;
    c.drawImage(this.sprite(`ground:${t.terrain}:${variant}`,g=>{
      const random=rng(variant+17), p=PALETTE[t.terrain];
      const points=Array.from({length:6},(_,i)=>{const a=(60*i-30)*Math.PI/180;return [Math.cos(a)*25.3,Math.sin(a)*25.3];});
      poly(g,points); g.save(); g.clip();
      const grad=g.createLinearGradient(-20,-25,18,25); p.forEach((v,i)=>grad.addColorStop(i/2,v)); g.fillStyle=grad; g.fillRect(-26,-26,52,52);
      for(let i=0;i<65;i++) { const px=random()*52-26,py=random()*52-26; g.fillStyle=i%2?'#dae2ac12':'#0b323518'; g.fillRect(px,py,random()*5+1,.5+random()); }
      if(t.terrain==='water') for(let i=0;i<6;i++) { const px=random()*27-18,py=-19+i*7; line(g,[[px,py],[px+4,py+1],[px+10,py]],'#8ac2ca50',.6); }
      if(t.terrain==='plains') for(let i=0;i<9;i++) {const px=random()*30-15,py=random()*32-16; line(g,[[px-2,py],[px-1,py-2],[px,py],[px+2,py-3]],'#ccce8b55',.65);}
      if(t.terrain==='coast') for(let i=0;i<8;i++) ellipse(g,random()*30-15,random()*30-15,1.4,.7,'#e6d9a43d');
      g.restore();
    }),x-48,y-48,96,96);
  }
  terrain(c,t,x,y) {
    const variant=tileVariant(t)%6;
    c.drawImage(this.sprite(`terrain:${t.terrain}:${variant}`,g=>{
      const random=rng(variant+41);
      if(t.terrain==='forest') { const trees=Array.from({length:8},()=>({x:random()*29-14,y:random()*23-6,h:10+random()*13})).sort((a,b)=>a.y-b.y); for(const p of trees) pine(g,p.x,p.y,p.h); }
      if(t.terrain==='mountain') { rock(g,-10,-1,10,18,true); rock(g,9,5,11,25,true); rock(g,-4,10,15,34,true); }
      if(t.terrain==='hills') { rock(g,-6,1,13,10); rock(g,7,10,14,14); for(let i=0;i<3;i++) ellipse(g,random()*23-12,random()*14,1.7,1,'#c3be9280'); }
    }),x-48,y-48,96,96);
  }
  formation(c, units, x, y, color, sigil, detail = true, phase = 0) {
    const total = Object.values(units).reduce((n, v) => n + v, 0);
    const ranks = total > 70 ? 3 : total > 24 ? 2 : 1;
    const mounted = units.cavalry > Math.max(0, units.levy + units.archer) / 2;
    const archers = units.archer > units.levy, siege = units.siege > 0;
    const key = `formation:${color}:${ranks}:${mounted}:${archers}:${siege}:${detail}`;
    const sprite = this.sprite(key, g => {
      ellipse(g, 1, 5, 18, 7, '#091b2870');
      const count = detail ? ranks * 4 : 3;
      for (let i = 0; i < count; i++) {
        const px = (i % 4) * 7 - 11, py = Math.floor(i / 4) * 6 - 5;
        if (mounted && detail) { ellipse(g, px, py + 2, 4, 2, '#7c644d'); line(g, [[px-2,py+3],[px-3,py+6]], '#302e2d'); line(g, [[px+2,py+3],[px+3,py+6]], '#302e2d'); }
        ellipse(g, px, py - 5, 1.8, 1.8, '#e3d4b1');
        g.fillStyle = color; g.fillRect(px - 2, py - 3, 4, 5);
        line(g, [[px-1,py+2],[px-2,py+5]], '#1c2b33'); line(g, [[px+1,py+2],[px+2,py+5]], '#1c2b33');
        if (!detail) continue;
        if (archers) { g.beginPath(); g.ellipse(px+3,py-2,2,4,0,-Math.PI/2,Math.PI/2); g.strokeStyle='#dab680'; g.lineWidth=.7; g.stroke(); }
        else { line(g, [[px+3,py+2],[px+3,py-11]], '#e8e2c6', .7); ellipse(g,px-2,py-1,2,2.6,'#c3c7ba'); }
      }
      if (siege && detail) { line(g,[[-8,8],[8,8]],'#896b47',3); line(g,[[0,8],[0,-4],[7,-8]],'#d1ba82',2); ellipse(g,-5,10,2,2,'#24303a');ellipse(g,5,10,2,2,'#24303a'); }
    });
    c.drawImage(sprite, x - 48, y - 48 + phase, 96, 96);
    line(c, [[x+2,y+1],[x+2,y-25]], '#e5d8b6', 1);
    poly(c, [[x+2,y-25],[x+16,y-23+phase],[x+14,y-12+phase],[x+2,y-15]], color, '#12232e');
    c.fillStyle='#182a34'; c.font='bold 9px Georgia'; c.textAlign='center';c.fillText(sigil,x+9,y-17+phase);
  }
  building(c,t,x,y,color) {
    const extras=[...(t.envoyOffice?['envoy']:[]), ...(t.chancery?['chancery']:[]),...(t.market?['market']:[]), ...(t.workshop?['workshop']:[])];
    const key=`building:${t.building}:${!!t.walls}:${!!t.capital}:${extras.includes('market')}:${extras.includes('workshop')}:${extras.includes('envoy')}:${extras.includes('chancery')}`;
    c.drawImage(this.sprite(key,g=>{
      ellipse(g,3,8,21,8,'#122d3c60');
      if(['city','fort'].includes(t.building)) {
        if(t.building==='city') { cottage(g,-12,-1,.65); cottage(g,12,-3,.6); }
        poly(g,[[-13,-5],[10,-10],[16,6],[-11,10]],'#6f8178');
        g.fillStyle='#b3b6a1'; g.fillRect(-10,-13,20,19); g.fillStyle='#81938a';g.fillRect(3,-13,8,19);
        for(let i=0;i<3;i++) tower(g,-10+i*10,4,i===1?28:19,t.building==='city');
        g.fillStyle='#263f43';g.beginPath();g.arc(0,1,3,Math.PI,0);g.lineTo(3,7);g.lineTo(-3,7);g.fill();
        g.fillStyle='#d7aa64';g.fillRect(-2,1,.8,5);
      } else if(t.building==='town') { cottage(g,-8,-5,.85);cottage(g,7,-7,.7);cottage(g,1,5,1.05); }
      else if(t.building==='farm') {
        poly(g,[[-19,-6],[9,-11],[20,10],[-9,15]],'#938052','#d3bd83');
        for(let i=0;i<7;i++) line(g,[[-16+i*4,-6-i*.15],[-7+i*3.5,12-i*.35]],i%2?'#d2bb65':'#5f7945',1.8);
        cottage(g,-8,-8,.6);g.fillStyle='#decba4';g.fillRect(10,-12,4,11);poly(g,[[8,-12],[12,-17],[16,-12]],'#83624a');
        line(g,[[7,-17],[17,-7]],'#efddad',1.4);line(g,[[17,-17],[7,-7]],'#efddad',1.4);
      } else if(t.building==='lumber') {
        pine(g,-11,-3,19);cottage(g,5,-4,.8);
        for(let i=0;i<4;i++) {line(g,[[-9+i*3,6],[1+i*3,10]],'#79583b',3);ellipse(g,1+i*3,10,1.4,1.4,'#d7ba7c');}
      } else if(t.building==='quarry') {
        for(let i=0;i<3;i++) poly(g,[[-16+i*3,-6+i*4],[8,-12+i*5],[17-i*3,6+i*2],[-6,13-i]],['#b2b7a4','#8a9b91','#637e79'][i]);
        for(let i=0;i<4;i++) {g.fillStyle='#d0cdb0';g.fillRect(-12+i*6,9-(i%2)*3,4,3);}
      } else if(t.building==='mine') {
        rock(g,0,8,19,23);poly(g,[[-8,8],[-8,-4],[0,-10],[8,-4],[8,8]],'#253c3d','#c6b386');
        line(g,[[-11,15],[-4,1]],'#c6c6a7',1.1);line(g,[[2,16],[3,1]],'#c6c6a7',1.1);
        for(let i=0;i<4;i++)line(g,[[-9+i,13-i*3],[2,13-i*3]],'#836f52',1.5);
        g.fillStyle='#efb763';g.fillRect(-5,-3,2,3);
      }
      if(t.walls>0) {line(g,[[-19,-5],[-19,10],[0,16],[20,9],[20,-6]],'#546e6c',5);line(g,[[-19,-5],[-19,8],[0,14],[20,7],[20,-6]],'#d4ccb1',2);tower(g,-18,8,12);tower(g,18,7,12);}
      if(extras.includes('market')) {poly(g,[[8,8],[17,7],[20,12],[10,14]],'#bb6651');line(g,[[10,14],[10,18]],'#e0c794');line(g,[[20,12],[20,16]],'#e0c794');}
      if(extras.includes('envoy')) {cottage(g,16,2,.55,'#6f789a');line(g,[[17,-3],[17,-15]],'#efd69b');poly(g,[[17,-15],[24,-13],[17,-9]],'#e6cf9b');}
      if(extras.includes('chancery')) {tower(g,-22,-2,16,true);}
      if(extras.includes('workshop')) {cottage(g,-13,8,.6,'#647880');g.fillStyle='#d1c09a';g.fillRect(-13,-3,3,7);ellipse(g,-12,-7,2,3,'#d0d9cd45');}
    }),x-48,y-48,96,96);
    if(['city','town','fort'].includes(t.building)) {
      const top=t.building==='city'?-39:t.building==='fort'?-30:-23;
      line(c,[[x+3,y+top+12],[x+3,y+top-3]],'#eee0b5',.85);
      poly(c,[[x+3,y+top-2],[x+14,y+top],[x+11,y+top+4],[x+3,y+top+3]],color,'#f7eadc70');
      if(t.capital) {c.fillStyle='#f8df99';c.font='9px Georgia';c.textAlign='center';c.fillText('♛',x-7,y+top+3);}
    }
  }
}
