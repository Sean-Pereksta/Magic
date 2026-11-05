/* Trash Tactics — illustrated, resolution-independent battlefield renderer. */
const INK='#354549', TEAL='#269b98', CORAL='#ed795d';
const TEAM_COLORS=[TEAL,CORAL,'#9e7bc5','#d4aa4e'];
const teamColor=t=>TEAM_COLORS[t]||'#91a588';
const SPRITES={}; if(typeof Image!=='undefined')for(const type of ['rock','paper','scissors','twig']){const img=new Image();img.src=new URL('./assets/'+type+'-icon.png',import.meta.url).href;SPRITES[type]=img;}
function rr(c,x,y,w,h,r=8){c.beginPath();c.roundRect(x,y,w,h,r);}
function shape(c,fill,stroke=INK,width=2){if(fill){c.fillStyle=fill;c.fill();}if(stroke){c.strokeStyle=stroke;c.lineWidth=width;c.stroke();}}
function line(c,points,color=INK,width=3){c.beginPath();points.forEach((p,i)=>i?c.lineTo(...p):c.moveTo(...p));c.strokeStyle=color;c.lineWidth=width;c.lineCap='round';c.lineJoin='round';c.stroke();}
function ellipse(c,x,y,rx,ry,fill,stroke=null,width=2){c.beginPath();c.ellipse(x,y,rx,ry,0,0,Math.PI*2);shape(c,fill,stroke,width);}
function eye(c,x,y){ellipse(c,x,y,1.7,2.5,INK);}
function limbs(c,type,walk=0,t=0){const sw=Math.sin(walk)*3;line(c,[[-11,6],[-19,12+sw],[-20,7+sw]],INK,2.8);line(c,[[12,6],[19,12-sw],[22,8-sw]],INK,2.8);line(c,[[-6,18],[-7-sw,25],[-12-sw,25]],INK,3);line(c,[[6,18],[7+sw,25],[12+sw,25]],INK,3);}
/** Paint a cheerful unit or building icon. x/y is its center; size is full nominal width. */
export function drawIcon(c,type,x,y,size=50,team=0,options={}){
 c.save();c.translate(x,y);c.scale(size/50,size/50);c.lineJoin='round';c.lineCap='round';const tc=teamColor(team);const walk=options.walk||0;
 const sprite=SPRITES[type];
 if(options.sprite&&sprite?.complete&&sprite.naturalWidth){
  const attack=Math.max(0,Math.min(1,(options.attackAnim||0)/.25));const angle=options.facing||0;
  c.translate(Math.cos(angle)*attack*8,Math.sin(angle)*attack*5);c.rotate(walk?Math.sin(walk)*.045:0);c.scale(1+attack*.08,1-attack*.06);
  if(walk){
   // Paper-cut puppet animation: body sway and independently stepping feet.
   const iw=sprite.naturalWidth,ih=sprite.naturalHeight,cut=Math.round(ih*.76),split=Math.round(iw/2),step=Math.sin(walk)*2.4;
   c.drawImage(sprite,0,0,iw,cut,-29,-30,58,58*cut/ih);
   c.drawImage(sprite,0,cut,split,ih-cut,-29,-30+58*cut/ih-step,29,58*(ih-cut)/ih);
   c.drawImage(sprite,split,cut,iw-split,ih-cut,0,-30+58*cut/ih+step,29,58*(ih-cut)/ih);
  }else c.drawImage(sprite,-29,-30,58,58);
  c.restore();return;
 }
 if(['rock','paper','scissors','twig'].includes(type)){
  limbs(c,type,walk);
  if(type==='rock'){
   c.beginPath();c.moveTo(-19,9);c.lineTo(-20,-4);c.lineTo(-10,-17);c.lineTo(8,-19);c.lineTo(20,-6);c.lineTo(18,14);c.lineTo(4,20);c.lineTo(-12,17);c.closePath();shape(c,'#a8b8bf',INK,2.4);
   c.beginPath();c.moveTo(-18,-4);c.lineTo(-9,-15);c.lineTo(7,-17);c.lineTo(2,-9);c.lineTo(-7,-8);c.lineTo(-12,1);c.closePath();shape(c,'#d2dce0',null);
   c.beginPath();c.moveTo(18,-4);c.lineTo(16,12);c.lineTo(5,18);c.lineTo(10,7);c.closePath();shape(c,'#879ba3',null);
   eye(c,-6,2);eye(c,7,2);line(c,[[-9,-3],[-3,-2]],INK,2);line(c,[[4,-2],[10,-4]],INK,2);c.beginPath();c.arc(1,7,4,0,Math.PI);shape(c,null,INK,1.8);
   rr(c,-12,11,24,5,2);shape(c,tc,null);ellipse(c,-13,5,3,1.7,'#e7b0a2');ellipse(c,13,5,3,1.7,'#e7b0a2');
  }else if(type==='paper'){
   c.save();c.rotate(-.07);c.beginPath();c.moveTo(-17,-22);c.lineTo(7,-22);c.lineTo(19,-10);c.lineTo(19,18);c.quadraticCurveTo(1,24,-18,18);c.closePath();shape(c,'#fffef0',INK,2.3);
   c.beginPath();c.moveTo(7,-22);c.lineTo(7,-11);c.lineTo(19,-10);shape(c,'#dbe9e3',INK,1.6);
   line(c,[[-11,-14],[1,-14]],'#b9cdd4',1.4);line(c,[[-11,-8],[3,-8]],'#b9cdd4',1.4);eye(c,-6,1);eye(c,7,1);c.beginPath();c.arc(.5,6,4.2,0,Math.PI);shape(c,null,INK,1.7);ellipse(c,-11,4,3,1.7,'#f5b4a3');ellipse(c,12,4,3,1.7,'#f5b4a3');line(c,[[-11,14],[12,14]],tc,3.5);c.restore();
  }else if(type==='scissors'){
   c.beginPath();c.moveTo(-4,1);c.lineTo(-16,-24);c.quadraticCurveTo(-17,-28,-12,-25);c.lineTo(7,-5);c.closePath();shape(c,'#e2eced',INK,2.2);
   c.beginPath();c.moveTo(4,1);c.lineTo(17,-26);c.quadraticCurveTo(20,-29,20,-23);c.lineTo(9,4);c.closePath();shape(c,'#b4ccd2',INK,2.2);
   ellipse(c,-8,12,10,11,tc,INK,2.3);ellipse(c,11,12,10,11,tc,INK,2.3);ellipse(c,-8,13,4,5,'#fff8e8',INK,1.4);ellipse(c,11,13,4,5,'#fff8e8',INK,1.4);
   ellipse(c,2,0,10,8,'#efba80',INK,2);eye(c,-1,-1);eye(c,6,-1);c.beginPath();c.arc(2,2,3,0,Math.PI);shape(c,null,INK,1.3);ellipse(c,2,-7,2,2,'#f6ebc9',INK,1);
  }else if(type==='twig'){
   line(c,[[0,17],[-2,-16],[-10,-25]],INK,9);line(c,[[0,17],[-2,-16],[-10,-25]],'#ba8451',5.5);line(c,[[-1,-9],[10,-23]],INK,7);line(c,[[-1,-9],[10,-23]],'#c7935e',4);
   c.save();c.translate(11,-21);c.rotate(-.55);ellipse(c,1,-3,7,4,'#88ba78',INK,1.5);line(c,[[-4,-2],[5,-4]],'#648d58',1);c.restore();
   ellipse(c,0,4,10,13,'#c99763',INK,2.1);eye(c,-4,1);eye(c,4,1);c.beginPath();c.arc(0,5,3.2,0,Math.PI);shape(c,null,INK,1.6);rr(c,-9,10,18,5,2);shape(c,tc,null);
  }
 }else if(type==='desk')drawDesk(c,tc);else if(type==='printer')drawPrinter(c,tc);else if(type==='cabinet')drawCabinet(c,tc);else if(type==='cdreader')drawCDReader(c,tc,options);else if(type==='cd')drawCD(c,tc,options);else if(type==='mouse')drawMouse(c,tc,options);else if(type==='trash')drawTrash(c,options);
 c.restore();
}
function drawDesk(c,tc){
 line(c,[[-21,7],[-21,26]],INK,5);line(c,[[22,7],[22,26]],INK,5);line(c,[[-21,7],[-21,24]],'#b58555',2.5);line(c,[[22,7],[22,24]],'#b58555',2.5);
 rr(c,-29,-10,58,26,5);shape(c,'#c18e5b',INK,2);rr(c,-31,-15,62,25,5);shape(c,'#e6b97b',INK,2.5);line(c,[[-24,4],[25,4]],'#cc965f',1.5);
 rr(c,-12,-28,26,18,3);shape(c,'#536c73',INK,2);rr(c,-9,-25,20,11,1);shape(c,'#b5e8df',null);line(c,[[-4,-11],[-4,-8],[6,-8]],INK,2.5);rr(c,-11,-4,24,6,2);shape(c,'#fff7dd',INK,1.2);
 rr(c,-24,-9,8,9,2);shape(c,tc,INK,1.2);line(c,[[-20,-14],[-20,-7]],'#f2c263',1.8);rr(c,18,-9,8,8,1);shape(c,'#fcf9e7',INK,1);line(c,[[19,-5],[24,-5]],tc,1.5);
}
function drawPrinter(c,tc){
 rr(c,-18,-29,35,27,2);shape(c,'#fffcf1',INK,2);line(c,[[-12,-22],[10,-22]],'#c3d3d5',1.5);line(c,[[-12,-17],[10,-17]],'#c3d3d5',1.5);
 rr(c,-27,-9,54,30,6);shape(c,'#c9d5d5',INK,2.5);rr(c,-27,-14,54,20,5);shape(c,'#e5eeee',INK,2.5);rr(c,-21,-7,14,5,2);shape(c,tc,INK,1);ellipse(c,19,-4,2.1,2.1,'#a5cb70',INK,1);
 rr(c,-19,8,39,8,2);shape(c,'#47616a',INK,1.7);c.beginPath();c.moveTo(-13,12);c.lineTo(12,12);c.lineTo(18,26);c.lineTo(-16,26);c.closePath();shape(c,'#fffbee',INK,1.5);line(c,[[-8,19],[9,19]],tc,2);
}
function drawCabinet(c,tc){
 rr(c,-22,-29,44,57,5);shape(c,'#e9c57d',INK,2.5);rr(c,-18,-24,36,22,2);shape(c,'#f5d99b',INK,1.5);rr(c,-18,2,36,21,2);shape(c,'#f5d99b',INK,1.5);rr(c,-7,-18,14,6,2);shape(c,tc,INK,1.4);rr(c,-7,8,14,6,2);shape(c,tc,INK,1.4);line(c,[[-17,29],[-17,32]],INK,3);line(c,[[17,29],[17,32]],INK,3);line(c,[[-13,-6],[12,-6]],'#d4ab66',1);
}
// Optical drive hangar and airborne disc share the hand-drawn office-toy palette.
function drawCD(c,tc,options={}){
 const spin=options.spin||0;
 ellipse(c,0,3,25,15,'#8d9fb5',INK,2.3);
 const shine=c.createLinearGradient(-22,-12,22,12);shine.addColorStop(0,'#f5f3e9');shine.addColorStop(.22,'#b8e4df');shine.addColorStop(.44,'#c7c3ec');shine.addColorStop(.63,'#f6cbb4');shine.addColorStop(.82,'#d8edb9');shine.addColorStop(1,'#d6e7ee');
 ellipse(c,0,0,25,15,shine,INK,2.2);
 // Rotating reflection sectors and concentric grooves read as a real compact disc.
 c.save();c.scale(1,.6);c.rotate(spin);
 for(let i=0;i<3;i++){const a=i*Math.PI*2/3;c.beginPath();c.moveTo(Math.cos(a)*7,Math.sin(a)*7);c.arc(0,0,22,a,a+.22);c.lineTo(Math.cos(a+.22)*7,Math.sin(a+.22)*7);c.closePath();shape(c,'#fffefa99',null);}
 for(const r of [15,19,22])ellipse(c,0,0,r,r,null,'#fffef65e',.65);
 c.restore();ellipse(c,0,0,9,5.4,'#edf5e8','#9baeb0',1.1);ellipse(c,0,0,4,2.4,'#465f65',INK,1.1);
 // Team stripe and a tiny forward beacon remain fixed as the surface spins.
 c.beginPath();c.ellipse(0,0,25,15,0,.12,Math.PI-.12);shape(c,null,tc,2.5);ellipse(c,0,13,2.2,1.7,'#f8df8e',INK,.8);
}
function drawCDReader(c,tc,options={}){
 const active=options.active,phase=options.phase||0,slide=active?2+Math.sin(phase*3)*1.5:0;
 rr(c,-28,-18,56,39,5);shape(c,'#92a9b1',INK,2.4);
 rr(c,-28,-22,56,28,5);shape(c,'#e5eddf',INK,2.4);
 // Lid has a recognizable optical-disc emblem rather than a generic machine panel.
 ellipse(c,0,-13,12,6,'#cadbdc','#7e999d',1.1);ellipse(c,0,-13,3,1.7,'#edf5e8','#7e999d',1);
 line(c,[[-21,-18],[-17,-18]],'#fffdf0',1.5);line(c,[[17,-18],[21,-18]],'#fffdf0',1.5);
 rr(c,-23,4,46,12,3);shape(c,'#3b535c',INK,1.5);
 rr(c,-19,8+slide,37,12,2);shape(c,'#bccbd0',INK,1.5);
 ellipse(c,-1,12+slide,12,5,'#d4e4dd','#8ca3a8',1);
 ellipse(c,-1,12+slide,3,1.4,'#455e67');
 rr(c,-23,-2,31,4,1);shape(c,tc,null);
 ellipse(c,20,-1,2.1,2.1,active&&Math.sin(phase*9)>0?'#f6df79':'#9ed49f',INK,.8);
 line(c,[[15,11],[21,11]],'#e7ecde',1.2);line(c,[[-19,22],[-19,25]],INK,3);line(c,[[19,22],[19,25]],INK,3);
 if(active){c.save();c.translate(0,-32-Math.sin(phase*4)*2);c.scale(.6,.6);drawCD(c,tc,{spin:phase*3});c.restore();}
}
function drawMouse(c,tc,options={}){
 const pressing=(options.attackAnim||0)>0;
 // Cable, separated mouse buttons, and wheel make this read as a computer mouse.
 c.beginPath();c.moveTo(0,-20);c.bezierCurveTo(0,-34,-22,-24,-16,-36);shape(c,null,INK,2.6);
 c.beginPath();c.moveTo(-20,-2);c.bezierCurveTo(-20,-25,20,-25,20,-2);c.lineTo(20,6);c.bezierCurveTo(20,31,-20,31,-20,6);c.closePath();shape(c,'#91a7ad',INK,2.5);
 c.beginPath();c.moveTo(-19,-4);c.bezierCurveTo(-18,-28,18,-28,19,-4);c.lineTo(19,3);c.bezierCurveTo(18,26,-18,26,-19,3);c.closePath();shape(c,'#e8eee2',INK,2.1);
 c.beginPath();c.moveTo(-1,-21);c.bezierCurveTo(-11,-21,-19,-17,-19,-5);c.lineTo(-1,-3);c.closePath();shape(c,pressing?'#a9d3c5':'#fbf9eb',INK,1.2);
 c.beginPath();c.moveTo(1,-21);c.bezierCurveTo(11,-21,19,-17,19,-5);c.lineTo(1,-3);c.closePath();shape(c,'#d6e1d7',INK,1.2);
 rr(c,-3,-16,6,11,2.5);shape(c,'#6d8587',INK,1);for(let i=0;i<3;i++)line(c,[[-1,-13+i*2],[1,-13+i*2]],'#c9d7cc',.7);
 c.beginPath();c.ellipse(0,1,18,22,0,.15,Math.PI-.15);shape(c,null,tc,2.5);
 // Printed pointer insignia links the defensive building to its shots.
 c.beginPath();c.moveTo(-4,2);c.lineTo(-4,14);c.lineTo(-1,11);c.lineTo(2,16);c.lineTo(5,14);c.lineTo(2,9);c.lineTo(7,9);c.closePath();shape(c,tc,INK,.8);
 if(pressing){ellipse(c,-9,-12,5,5,'#fff3bc88');for(const a of [-2.5,-1.7,-.9])line(c,[[-9+Math.cos(a)*8,-12+Math.sin(a)*8],[-9+Math.cos(a)*12,-12+Math.sin(a)*12]],'#e9b34e',1.7);}
}
function drawPointer(c,x,y,angle,tc,size=1){
 c.save();c.translate(x,y);c.rotate(angle);c.scale(size,size);
 // Full arrow, notch and tail, outlined so the projectile stays visible over scenery.
 c.beginPath();c.moveTo(12,0);c.lineTo(-12,-10);c.lineTo(-8,14);c.lineTo(-3,6);c.lineTo(5,14);c.lineTo(9,10);c.lineTo(1,2);c.closePath();shape(c,'#fffef4',INK,2.1);
 line(c,[[-6,-4],[4,0],[-3,0]],tc,1.5);c.restore();
}
function drawTrash(c,options={}){
 if(options.amount===0)c.globalAlpha*=.6;
 c.beginPath();c.moveTo(-19,-16);c.lineTo(18,-16);c.lineTo(14,24);c.quadraticCurveTo(0,30,-15,24);c.closePath();shape(c,'#91b5a4',INK,2.3);
 c.beginPath();c.moveTo(0,-11);c.lineTo(17,-13);c.lineTo(13,23);c.quadraticCurveTo(4,26,0,25);c.closePath();shape(c,'#76a18f',null);
 line(c,[[-10,-5],[-8,19]],'#d3e3cf',2);line(c,[[0,-5],[0,20]],'#c8dcc6',2);line(c,[[9,-5],[7,19]],'#bdd3bd',2);
 c.save();c.rotate(-.13);rr(c,-23,-23,46,10,3);shape(c,'#aac7b1',INK,2.2);rr(c,-6,-28,13,6,2);shape(c,'#bad2b7',INK,1.7);c.restore();
 if(options.amount!==0){c.save();c.translate(1,-20);c.rotate(.22);rr(c,-10,-10,11,12,2);shape(c,'#fcf6df',INK,1);c.restore();c.save();c.translate(13,-20);c.rotate(.3);rr(c,-6,-7,8,10,2);shape(c,'#e6a28e',INK,1);c.restore();}
}

export class Renderer {
 constructor(canvas,game){this.canvas=canvas;this.ctx=canvas.getContext('2d');this.game=game;this.camera={x:0,y:0,zoom:.9};this.width=800;this.height=600;this.marker=null;this.lastDraw=performance.now();this.motion=new Map();this.resize();this.centerOn(600,800);}
 resize(){const rect=this.canvas.getBoundingClientRect();this.width=Math.max(1,rect.width);this.height=Math.max(1,rect.height);const dpr=Math.min(window.devicePixelRatio||1,2);this.dpr=dpr;this.canvas.width=Math.round(this.width*dpr);this.canvas.height=Math.round(this.height*dpr);this.clampCamera();}
 clampCamera(){const g=this.game;const w=g.width||2400,h=g.height||1600;const vw=this.width/this.camera.zoom,vh=this.height/this.camera.zoom;this.camera.x=Math.max(-80,Math.min(w-vw+80,this.camera.x));this.camera.y=Math.max(-80,Math.min(h-vh+80,this.camera.y));if(vw>w+160)this.camera.x=(w-vw)/2;if(vh>h+160)this.camera.y=(h-vh)/2;}
 centerOn(x,y){this.camera.x=x-this.width/this.camera.zoom/2;this.camera.y=y-this.height/this.camera.zoom/2;this.clampCamera();}
 pan(dx,dy){this.camera.x-=dx/this.camera.zoom;this.camera.y-=dy/this.camera.zoom;this.clampCamera();}
 zoomAt(factor,clientX,clientY){const rect=this.canvas.getBoundingClientRect();const x=(clientX??rect.left+this.width/2)-rect.left,y=(clientY??rect.top+this.height/2)-rect.top;const wx=this.camera.x+x/this.camera.zoom,wy=this.camera.y+y/this.camera.zoom;this.camera.zoom=Math.max(.4,Math.min(1.65,this.camera.zoom*factor));this.camera.x=wx-x/this.camera.zoom;this.camera.y=wy-y/this.camera.zoom;this.clampCamera();}
 screenToWorld(clientX,clientY){const r=this.canvas.getBoundingClientRect();return{x:this.camera.x+(clientX-r.left)/this.camera.zoom,y:this.camera.y+(clientY-r.top)/this.camera.zoom};}
 worldToScreen(x,y){return{x:(x-this.camera.x)*this.camera.zoom,y:(y-this.camera.y)*this.camera.zoom};}
 draw(state={}){
 const c=this.ctx,g=this.game,cam=this.camera;const now=performance.now(),dt=Math.min(.1,(now-this.lastDraw)/1000);this.lastDraw=now;this.time=state.time??g.time??now/1000;
 c.setTransform(this.dpr,0,0,this.dpr,0,0);c.clearRect(0,0,this.width,this.height);c.fillStyle='#e2d8c3';c.fillRect(0,0,this.width,this.height);c.save();c.scale(cam.zoom,cam.zoom);c.translate(-cam.x,-cam.y);this.drawGround(c);
 const entities=(g.entities||[]).filter(e=>e.hp>0&&!e.transportedBy);const selected=state.selected||new Set();
 // Defense coverage is painted on the ground before troops and structures.
 for(const e of entities)if(e.type==='mouse'&&e.team===0&&selected.has(e.id)){c.save();c.setLineDash([7,9]);ellipse(c,e.x,e.y,310,310,teamColor(e.team)+'05',teamColor(e.team)+'60',1.4/cam.zoom);c.restore();}
 for(const e of entities)if(e.type==='trash')this.drawEntity(c,e,selected.has(e.id));
 for(const e of entities.filter(e=>e.type!=='trash'&&e.type!=='cd').sort((a,b)=>a.y-b.y))if(this.visible(e.x,e.y,110))this.drawEntity(c,e,selected.has(e.id));
 // Air transports stay above every ground troop, structure, and obstacle.
 for(const e of entities)if(e.type==='cd'&&this.visible(e.x,e.y,110))this.drawEntity(c,e,selected.has(e.id));
 for(const projectile of g.projectiles||[])this.drawProjectile(c,projectile);
 for(const fx of g.effects||[])this.drawEffect(c,fx);
 if(this.marker){this.marker.life-=dt;if(this.marker.life>0){const m=this.marker;const color=m.type==='attack'?'#e56d54':TEAL;const p=1-m.life/.8;c.strokeStyle=color;c.lineWidth=2.5/cam.zoom;c.globalAlpha=1-p;ellipse(c,m.x,m.y,12+p*25,6+p*12,null,color,2.5/cam.zoom);line(c,[[m.x-6,m.y],[m.x+6,m.y]],color,2/cam.zoom);line(c,[[m.x,m.y-6],[m.x,m.y+6]],color,2/cam.zoom);c.globalAlpha=1;}else this.marker=null;}
 this.drawPlacement(c,state);
 c.restore();
 if(state.drag){const d=state.drag;const x=Math.min(d.x1,d.x2),y=Math.min(d.y1,d.y2),w=Math.abs(d.x2-d.x1),h=Math.abs(d.y2-d.y1);rr(c,x,y,w,h,3);shape(c,'#3baaa024',TEAL,1.7);}
 c.setTransform(1,0,0,1,0,0);
 }
 visible(x,y,r=0){return x+r>this.camera.x&&y+r>this.camera.y&&x-r<this.camera.x+this.width/this.camera.zoom&&y-r<this.camera.y+this.height/this.camera.zoom;}
 drawGround(c){
 const g=this.game,w=g.width||2400,h=g.height||1600;
 if(this._groundMap!==g.map||!this._groundCanvas){
  this._groundMap=g.map;this._groundCanvas=document.createElement('canvas');this._groundCanvas.width=w;this._groundCanvas.height=h;this.paintGround(this._groundCanvas.getContext('2d'));
 }
 c.drawImage(this._groundCanvas,0,0);
 for(const e of g.entities||[])if(e.type==='desk'&&e.hp>0){ellipse(c,e.x,e.y+12,135,90,teamColor(e.team)+'0a');}
 }
 paintGround(c){
 const g=this.game,w=g.width||2400,h=g.height||1600,breakroom=g.map?.id==='breakroom';
 // All textured scenery is painted once; units, buildings and effects stay crisp above it.
 const wood=c.createLinearGradient(0,0,0,h);wood.addColorStop(0,'#bfa477');wood.addColorStop(.5,'#ddc69b');wood.addColorStop(1,'#b99c70');rr(c,0,0,w,h,25);shape(c,wood,'#9a815b',3);
 for(let y=18;y<h;y+=23){c.beginPath();c.moveTo(0,y);c.bezierCurveTo(w*.25,y+Math.sin(y)*5,w*.7,y+Math.sin(y*.2)*7,w,y+2);shape(c,null,y%2?'#b0915e20':'#f9e3b333',1);}
 rr(c,43,48,w-79,h-83,20);shape(c,'#5c5e4521',null);rr(c,34,36,w-78,h-82,17);shape(c,breakroom?'#e9e9d8':'#eee7cc','#d1c2a1',2);
 c.save();c.beginPath();c.roundRect(35,37,w-80,h-84,16);c.clip();
 if(breakroom){
  const tile=120;for(let x=34;x<w-44;x+=tile)for(let y=36;y<h-46;y+=tile){const odd=(Math.round((x-34)/tile)+Math.round((y-36)/tile))%2;c.fillStyle=odd?'#e3e7d4':'#eeebd9';c.fillRect(x+1,y+1,tile-2,tile-2);line(c,[[x+3,y+tile-2],[x+tile-2,y+tile-2]],'#bfcbb53a',1);}
 }else{
  const paper=c.createLinearGradient(0,0,w,h);paper.addColorStop(0,'#f4edd6');paper.addColorStop(.5,'#eae4c9');paper.addColorStop(1,'#f4edd8');c.fillStyle=paper;c.fillRect(35,37,w-80,h-84);
  for(let x=96;x<w-50;x+=60)line(c,[[x,38],[x,h-48]],'#9eaf9b21',1);for(let y=80;y<h-50;y+=60)line(c,[[36,y],[w-46,y]],'#9eaf9b21',1);
  line(c,[[143,38],[143,h-48]],'#d795842c',2);line(c,[[151,38],[151,h-48]],'#d7958418',1);
  for(let y=105;y<h-80;y+=115){ellipse(c,74,y,8,10,'#c7b99b33');ellipse(c,72,y-1,6,8,'#af9c7635');line(c,[[39,y-4],[67,y-4]],'#7e8b7a44',3);line(c,[[39,y-1],[67,y-1]],'#ebe6d2',1);}
 }
 // Slightly uneven paper grain makes the tabletop feel drawn, without obscuring units.
 for(let i=0;i<2200;i++){const x=42+(i*593.113)%(w-95),y=43+(i*367.831)%(h-100);ellipse(c,x,y,.5+(i%3)*.22,.35,'#8e8b6020');}
 const lanes=g.map?.lanes||[
  [[330,800],[590,800],[840,800],[1200,800],[1560,800],[1810,800],[2070,800]],
  [[460,710],[670,410],[1030,390],[1370,390],[1730,410],[1940,710]],
  [[460,895],[680,1220],[1030,1240],[1370,1240],[1720,1220],[1940,895]]
 ];
 lanes.forEach((path,i)=>{const points=Array.isArray(path)?path:(path.points||[]);if(points.length<2)return;c.save();c.lineJoin='round';c.lineCap='round';line(c,points,breakroom?'#c5ceb52c':'#d4c9a837',i===0?173:135);line(c,points,breakroom?'#f3f0dc77':'#f7f0db77',i===0?154:116);line(c,points,'#fff8e52a',i===0?120:87);c.restore();});
 // Flattened sticky notes and napkins mark the sheltered starting yards.
 for(const desk of (g.entities||[]).filter(e=>e.type==='desk')){
  const x=desk.x; c.save();c.translate(x,desk.y+15);c.rotate(x<w/2?-.025:.025);rr(c,-182,-186,365,382,4);shape(c,teamColor(desk.team)+'08','#c6cbaa35',1);line(c,[[-175,-180],[174,-180]],'#fff7e54d',2);rr(c,-42,-194,85,21,2);shape(c,'#e5d4a13d',null);c.restore();
 }
 // Resource pads are drawn into the surface, with ample clean space for gathering.
 for(const e of g.entities||[])if(e.type==='trash'){
  ellipse(c,e.x,e.y+15,43,29,breakroom?'#bdc8a31b':'#b6bf8d16');c.save();c.setLineDash([3,7]);ellipse(c,e.x,e.y+14,47,31,null,'#99ad8130',1);c.restore();
 }
 for(const e of g.map?.expansions||[]){const x=Array.isArray(e)?e[0]:e.x,y=Array.isArray(e)?e[1]:e.y;ellipse(c,x,y,117,79,'#eff1d42a');for(const sx of [-1,1])for(const sy of [-1,1])line(c,[[x+sx*92,y+sy*48],[x+sx*92,y+sy*61],[x+sx*78,y+sy*61]],'#91ab7c35',2);}
 // Peripheral cable, coffee marks and little paper scraps add scale, staying flat under play.
 this.paintDecor(c,breakroom);
 c.restore();
 for(const o of g.map?.obstacles||[])this.drawObstacle(c,o,true);
 // The paper curls at two corners, leaving the warm wooden table visible beneath.
 c.beginPath();c.moveTo(w-44,h-118);c.lineTo(w-44,h-46);c.lineTo(w-115,h-46);c.quadraticCurveTo(w-74,h-72,w-44,h-118);shape(c,'#ded3b5','#c5b697',1.2);c.beginPath();c.moveTo(w-44,h-118);c.quadraticCurveTo(w-74,h-72,w-115,h-46);shape(c,null,'#f8f1d9',2);
 }
 paintDecor(c,breakroom){
 const g=this.game,w=g.width||2400,h=g.height||1600;
 const safe=(x,y,r=30)=>!(g.map?.obstacles||[]).some(o=>x>o.x-r&&x<o.x+o.w+r&&y>o.y-r&&y<o.y+o.h+r)&&!(g.entities||[]).some(e=>e.type==='trash'?Math.hypot(x-e.x,y-e.y)<r+65:['desk','printer','cabinet','cdreader','mouse'].includes(e.type)&&Math.hypot(x-e.x,y-e.y)<r+100);
 c.save();c.globalAlpha=.52;c.beginPath();c.moveTo(82,460);c.bezierCurveTo(180,265,325,300,455,180);c.bezierCurveTo(550,100,630,170,730,90);shape(c,null,'#a5a990',5);c.beginPath();c.moveTo(w-84,h-420);c.bezierCurveTo(w-180,h-265,w-325,h-300,w-455,h-180);c.bezierCurveTo(w-550,h-100,w-630,h-170,w-730,h-90);shape(c,null,'#a5a990',5);c.restore();
 const rings=breakroom?[[630,210,79],[1780,1340,69],[1800,240,43],[575,1360,48]]:[[610,220,65],[1780,1380,48]];
 for(const [x,y,r]of rings){if(!safe(x,y,r))continue;c.save();c.translate(x,y);c.rotate(.27);c.beginPath();c.ellipse(0,0,r,r*.9,0,.1,Math.PI*1.96);shape(c,null,'#b8996d20',6);c.beginPath();c.ellipse(-2,1,r-5,r*.9-4,0,.15,Math.PI*1.92);shape(c,null,'#bea47c18',2);ellipse(c,r*.8,r*.78,7,4,'#b99a6e13');c.restore();}
 for(let i=0;i<88;i++){
  const x=180+(i*137.47)%(w-340),y=105+(i*219.83)%(h-205);if(!safe(x,y,23))continue;c.save();c.translate(x,y);c.rotate(i*1.173);
  if(i%5===0){rr(c,-10,-5,21,10,2);shape(c,'#c8d2c333','#9fac9a55',1.1);rr(c,-6,-2,12,4,1);shape(c,null,'#9fac9a55',.9);}
  else if(i%5===1){c.beginPath();c.moveTo(-8,-6);c.lineTo(5,-8);c.lineTo(10,4);c.lineTo(-4,7);c.closePath();shape(c,'#f6f0dc77','#c5bca13b',.8);line(c,[[-3,-2],[4,1]],'#c7b78e55',1);}
  else if(i%5===2){c.beginPath();c.arc(0,0,9,.2,4.7);shape(c,null,'#cbad714a',2.7);line(c,[[3,-9],[8,-5]],'#b99b6640',1);}
  else if(i%5===3){line(c,[[-7,4],[0,-4],[6,4]],'#8d9c7055',1.2);line(c,[[0,6],[0,-8]],'#8d9c7055',1.2);ellipse(c,-4,-4,3,1.5,'#a2b68133');}
  else{for(let q=0;q<3;q++)ellipse(c,q*5-5,(q%2)*3,1.5,1.5,'#aab18b4d');}
  c.restore();
 }
 // Worn pieces of tape fix the largest paper corners to the table.
 for(const [x,y,rot]of [[210,56,-.12],[w-230,60,.1],[170,h-62,.14]]){c.save();c.translate(x,y);c.rotate(rot);rr(c,-48,-13,96,26,2);shape(c,'#c6b7882c',null);for(let j=-35;j<40;j+=12)line(c,[[j,-10],[j+8,10]],'#b2a06a0d',1);c.restore();}
 } drawObstacle(c,o,all=false){
 if(!all&&!this.visible(o.x+o.w/2,o.y+o.h/2,Math.max(o.w,o.h)))return;
 const{x,y,w,h}=o,kind=o.type||o.kind||'eraser';c.save();c.translate(x,y);rr(c,5,9,w,h,12);shape(c,'#625f4824',null);
 if(kind==='pencil'||kind==='ruler'){
  // Turn the illustration with the footprint, so its visible edges match pathfinding.
  const vertical=h>w; if(vertical){c.translate(w,0);c.rotate(Math.PI/2);}const pw=vertical?h:w,ph=vertical?w:h;
  if(kind==='pencil'){
   rr(c,0,0,pw,ph,8);shape(c,'#dfb15c','#8c794c',2.5);c.save();c.beginPath();c.roundRect(0,0,pw,ph,8);c.clip();c.fillStyle='#efc96d';c.fillRect(0,ph*.18,pw*.78,ph*.27);c.fillStyle='#cda14f';c.fillRect(0,ph*.72,pw*.8,ph*.28);c.fillStyle='#df9f96';c.fillRect(pw*.82,0,pw*.18,ph);line(c,[[pw*.78,0],[pw*.78,ph]],'#a7b0a9',ph*.1);line(c,[[pw*.81,0],[pw*.81,ph]],'#d5d8c5',ph*.025);c.restore();
   c.beginPath();c.moveTo(0,0);c.lineTo(pw*.13,ph/2);c.lineTo(0,ph);shape(c,'#e9cea0','#9e875c',1.2);c.beginPath();c.moveTo(0,ph*.3);c.lineTo(pw*.07,ph/2);c.lineTo(0,ph*.7);shape(c,'#627270',null);
  }else{
   rr(c,0,0,pw,ph,7);shape(c,'#e4cb84','#a18b54',2.2);rr(c,4,4,pw-8,ph-8,4);shape(c,'#f2dfa362',null);line(c,[[8,ph*.4],[pw-8,ph*.4]],'#c7ac6d55',1);const n=Math.floor(pw/12);for(let i=1;i<n;i++){const tick=i%5===0?ph*.36:i%5===2?ph*.25:ph*.18;line(c,[[i*12,ph-3],[i*12,ph-tick]],'#8c845e',i%5===0?1.5:1);if(i%5===0)ellipse(c,i*12,ph*.42,2,2,'#a29464');}ellipse(c,pw-20,ph*.24,5,5,'#d2bb7b','#a08c5e',1);
  }
 }else if(kind==='tape'){
  rr(c,0,0,w,h,14);shape(c,'#a2bbac','#6d8c7d',2.5);rr(c,6,5,w-12,h-14,11);shape(c,'#b4cabb',null);const radius=Math.min(w*.37,h*.3);ellipse(c,w*.5,h*.35,radius,radius,'#e4d6ad','#7a8f7a',2.2);ellipse(c,w*.5,h*.35,radius*.69,radius*.69,'#f4ead0','#c6b988',2);ellipse(c,w*.5,h*.35,radius*.43,radius*.43,'#a6b6a0','#788f7d',1.7);c.beginPath();c.moveTo(w*.34,h*.55);c.lineTo(w*.71,h*.55);c.lineTo(w*.79,h*.88);c.lineTo(w*.3,h*.88);c.closePath();shape(c,'#e0d9b8','#879d89',1.5);line(c,[[w*.25,h*.9],[w*.83,h*.9]],'#587c6f',6);for(let i=0;i<8;i++)line(c,[[w*.27+i*w*.07,h*.86],[w*.3+i*w*.07,h*.92]],'#e3e6ca',1.2);
 }else if(kind==='book'||kind==='notebook'||kind==='binder'){
  const cover=o.color||((x+y)%3<1?'#a9bcb3':'#a7b5bd');rr(c,0,0,w,h,9);shape(c,'#849e98','#607e79',2.5);rr(c,7,10,w-12,h-14,5);shape(c,'#f1e7cc','#9aa08a',1.7);for(let i=0;i<6;i++)line(c,[[11,h-12-i*3],[w-10,h-12-i*3]],'#c8c4a7',1);rr(c,0,-3,w-4,h-24,7);shape(c,cover,'#68877f',2.3);rr(c,5,0,23,h-29,3);shape(c,'#779b8b44',null);line(c,[[30,2],[30,h-28]],'#e2e7ce44',1.5);for(let yy=20;yy<h-40;yy+=30)line(c,[[12,yy],[20,yy]],'#567b7055',2);
  rr(c,w*.28,h*.25,w*.45,h*.24,4);shape(c,'#e6e0c0','#849e8e',1.3);line(c,[[w*.36,h*.32],[w*.65,h*.32]],'#91a18a',2);line(c,[[w*.4,h*.4],[w*.6,h*.4]],'#a7af93',1.5);line(c,[[w*.84,0],[w*.84,h-25]],'#658e7b',6);line(c,[[w*.84-2,0],[w*.84-2,h-25]],'#b4c5a3',1.1);
  c.beginPath();c.moveTo(w*.62,h-26);c.lineTo(w*.7,h-26);c.lineTo(w*.7,h+4);c.lineTo(w*.66,h-3);c.lineTo(w*.62,h+4);c.closePath();shape(c,'#d8a06f','#a98963',1);
 }else if(kind==='sponge'||kind==='spill'){
  rr(c,0,0,w,h,Math.min(w,h)*.14);shape(c,'#c9cc89','#919c66',2.3);rr(c,1,0,w-2,h*.3,Math.min(w,h)*.13);shape(c,'#8fa58d','#6d8c76',2);c.save();c.beginPath();c.roundRect(2,2,w-4,h*.25,18);c.clip();for(let i=0;i<80;i++){const px=10+(i*41)%(w-20),py=5+(i*31)%(h*.24);line(c,[[px,py],[px+8,py+4]],'#bfd0ac55',1.6);}c.restore();for(let i=0;i<44;i++){const px=15+(i*71)%(w-30),py=h*.4+(i*47)%(h*.46);ellipse(c,px,py,2.5+(i%4),1.7+(i%3),'#aeb775');}line(c,[[14,h*.34],[w-14,h*.34]],'#e7e6b2',2);
 }else if(kind==='cup'||kind==='mug'||kind==='coffee'){
  // A tall travel mug on a square coaster: the entire footprint is a solid prop.
  rr(c,0,0,w,h,19);shape(c,'#baab89','#8e8e70',2);rr(c,7,6,w-14,h-13,14);shape(c,'#d4c7a5','#b3ab88',1);for(let i=0;i<7;i++)line(c,[[13,20+i*(h-40)/6],[w-13,20+i*(h-40)/6]],'#b9ac8833',1);
  const cx=w*.46,cy=h*.46,rx=w*.36,ry=h*.37;ellipse(c,cx+rx*.9,cy+15,w*.14,h*.16,'#d5ddd0','#7f9687',3);ellipse(c,cx+rx*.9,cy+15,w*.07,h*.09,'#c4b995','#9ba58d',1.6);ellipse(c,cx,cy+14,rx,ry,'#8aa797','#648577',2.5);ellipse(c,cx,cy,rx,ry,'#e3e6d5','#718e7c',2.5);ellipse(c,cx,cy,rx*.82,ry*.84,'#bfcbb6','#9ead97',2);ellipse(c,cx,cy,rx*.72,ry*.74,'#8e7151','#6f7358',1.8);ellipse(c,cx-rx*.15,cy-ry*.2,rx*.39,ry*.22,'#c4a67a55');c.beginPath();c.ellipse(cx,cy,rx*.59,ry*.59,-.3,.2,Math.PI*1.5);shape(c,null,'#d7bd8760',2);ellipse(c,cx+rx*.23,cy+ry*.35,4,3,'#e4c58c55');
 }else{
  rr(c,0,0,w,h,13);shape(c,'#b6cfbf','#819c87',2.3);c.save();c.beginPath();c.roundRect(0,0,w,h,13);c.clip();c.fillStyle='#dfab9b';c.fillRect(w*.75,0,w*.25,h);c.fillStyle='#efe4c7';c.fillRect(w*.24,0,w*.51,h);line(c,[[w*.24+8,h*.18],[w*.75-8,h*.18]],'#d4c39a',2);line(c,[[w*.24+8,h*.8],[w*.75-8,h*.8]],'#d4c39a',1.5);for(let j=0;j<3;j++){c.save();c.translate(w*(.4+j*.1),h*.47);c.rotate(j*.3);line(c,[[-4,0],[4,0]],'#b4af87',1.5);line(c,[[0,-4],[0,4]],'#b4af87',1.5);c.restore();}c.restore();line(c,[[12,h-8],[w-12,h-8]],'#8fa69255',2);line(c,[[10,8],[w-10,8]],'#f0ecd07a',1.7);
 }
 c.restore();
 } drawEntity(c,e,selected){
 if(e.transportedBy)return;
 const isUnit=['rock','paper','scissors','twig'].includes(e.type),isCD=e.type==='cd',isTrash=e.type==='trash',tc=teamColor(e.team);let size=isUnit?49:e.type==='desk'?112:e.type==='printer'?94:e.type==='cdreader'?98:e.type==='mouse'?80:e.type==='cabinet'?74:68;
 const old=this.motion.get(e.id);const moving=old&&Math.hypot(e.x-old.x,e.y-old.y)>.08;this.motion.set(e.id,{x:e.x,y:e.y});const bob=isUnit&&moving?Math.sin(this.time*13+Number(e.id)*.8)*1.8:Math.sin(this.time*2.5+(Number(e.id)||0))*0.7;
 const leap=isCD?18+Math.sin(this.time*3+e.id)*2:e.leap?Math.sin(Math.PI*(1-e.leap.remaining/e.leap.total))*75:0;
 c.save();
 // Ground footprint and team ring.
 ellipse(c,e.x,e.y+11,isUnit?19:size*.4,isUnit?7:size*.14,'#565b4021');
 if(selected){ellipse(c,e.x,e.y+10,isUnit?25:size*.54,isUnit?12:size*.23,'#2ba79d10',TEAL,2.2/this.camera.zoom);if(!isUnit){const rradius=size*.56;line(c,[[e.x-rradius,e.y-rradius*.6],[e.x-rradius,e.y-rradius*.8],[e.x-rradius+12,e.y-rradius*.8]],TEAL,2);line(c,[[e.x+rradius,e.y-rradius*.6],[e.x+rradius,e.y-rradius*.8],[e.x+rradius-12,e.y-rradius*.8]],TEAL,2);}}
 else if(isUnit||isCD)ellipse(c,e.x,e.y+10,isCD?31:19,isCD?11:8,tc+'16',tc+'cc',1.8);
 if(e.constructing>0){const progress=1-e.constructing/(e.buildTotal||10);c.globalAlpha=.3+progress*.5;c.setLineDash([5,5]);rr(c,e.x-size*.5,e.y-size*.4,size,size*.8,8);shape(c,tc+'15',tc,2);c.setLineDash([]);}
 if(e.paperInvulnerable>0){c.save();c.translate(e.x,e.y-6-leap);ellipse(c,0,0,25+Math.sin(this.time*10)*2,25+Math.sin(this.time*10)*2,tc+'30',tc,1.8);c.rotate(this.time*(moving?12:3));ellipse(c,0,0,16,16,'#fffdec',INK,2);for(let i=0;i<5;i++){c.rotate(Math.PI*2/5);line(c,[[12,-4],[3,0],[8,10]],'#a1b6ad',1.3);}c.restore();}else drawIcon(c,e.type,e.x,e.y-9+bob-leap,size,e.team,{walk:moving?this.time*14:0,amount:e.amount,sprite:isUnit,attackAnim:e.attackAnim,facing:e.facing,spin:this.time*(moving?6:2)+e.id,active:e.type==='cdreader'&&!!e.queue?.length,phase:this.time});
 c.globalAlpha=1;
 if(e.clickDamage?.length){
  const yy=e.y-9-leap,flash=.45+Math.sin(this.time*7+e.id)*.12;
  c.save();c.globalAlpha=flash;ellipse(c,e.x,yy,isCD?39:29,isCD?23:27,null,'#d98145',1.4);c.restore();
  drawPointer(c,e.x+size*.28,yy-size*.24,-Math.PI*.72,'#d98145',.38);
 }
 if(isCD){
  const count=e.cargo?.length||0,by=e.y+25;
  rr(c,e.x-31,by-7,62,16,6);shape(c,'#fbf7e9ed',tc,1.2);
  for(let i=0;i<6;i++)ellipse(c,e.x-23+i*6,by,2.1,2.1,i<count?tc:'#d5dccc');
  c.font='bold 9px \"Trebuchet MS\",sans-serif';c.textAlign='right';c.textBaseline='middle';c.fillStyle=INK;c.fillText(count+'/6',e.x+26,by+.4);
  if(selected&&e.order?.type==='drop'){line(c,[[e.x,e.y+12],[e.x,e.y+18]],tc,1.5);}
 }
 if(isUnit&&(e.carrying>0)){rr(c,e.x+10,e.y-18,12,12,2);shape(c,'#deb76f',INK,1.3);line(c,[[e.x+13,e.y-14],[e.x+19,e.y-14]],'#fff4c9',1);}
 if(e.type==='twig'&&e.workState==='collecting'){
  const target=this.game.getEntity?.(e.resourceId||e.order.targetId);if(target&&Math.hypot(target.x-e.x,target.y-e.y)<85){
   const a=Math.atan2(target.y-e.y,target.x-e.x),swing=Math.sin(this.time*13),handX=e.x+Math.cos(a)*21,handY=e.y+Math.sin(a)*16-8;
   line(c,[[e.x+Math.cos(a)*7,e.y-8],[handX,handY+swing*6]],'#b67e46',4);
   for(let i=0;i<3;i++){const phase=(this.time*1.9+i/3)%1;const xx=target.x+(e.x-target.x)*phase;const yy=target.y+(e.y-target.y)*phase-Math.sin(phase*Math.PI)*18;rr(c,xx-2,yy-2,4,4,1);shape(c,['#d3a75f','#f6ead0','#df9a7b'][i],null);}
  }
 }
 if(isUnit&&e.attackAnim>0&&e.attackTarget){
  const p=e.attackAnim/.25,a=Math.atan2(e.attackTarget.y-e.y,e.attackTarget.x-e.x);c.save();c.translate(e.x,e.y-4);c.rotate(a);
  c.beginPath();c.arc(19,0,18,-.8+p*.9,.8+p*.9);shape(c,null,e.type==='scissors'?'#d1e5e8':e.type==='paper'?'#fffceb':'#cfac79',3.5);c.restore();
 }
 const damaged=e.hp<e.maxHp-.1;const queue=e.queue?.[0];
 if((selected||damaged)&&!isTrash){const barW=isUnit?36:size*.75,y=e.y-size*.57-leap;rr(c,e.x-barW/2,y,barW,5,2);shape(c,'#64716844',null);rr(c,e.x-barW/2,y,barW*Math.max(0,e.hp/e.maxHp),5,2);shape(c,e.hp/e.maxHp<.3?'#e67659':tc,null);}
 if(e.constructing>0||queue){const total=e.constructing>0?(e.buildTotal||10):queue.total;const remaining=e.constructing>0?e.constructing:queue.remaining;const y=e.y+size*.48;rr(c,e.x-size*.42,y,size*.84,5,2);shape(c,'#c5c6ae',null);rr(c,e.x-size*.42,y,size*.84*Math.max(0,Math.min(1,1-remaining/total)),5,2);shape(c,'#e6b34d',null);}
 if(isTrash){
  const amount=Math.max(0,e.amount??1500),ratio=Math.min(1,amount/(e.maxAmount||1500)),barW=36;
  rr(c,e.x-barW/2,e.y+34,barW,3,1.5);shape(c,'#71836624',null);
  if(ratio>0){rr(c,e.x-barW/2,e.y+34,barW*ratio,3,1.5);shape(c,ratio<.18?'#bfa16b':'#7f9f73',null);}
  if(e.collectorId)ellipse(c,e.x+23,e.y-23,3.2,3.2,'#e0b55b','#6b8065',.8);
 } c.restore();
 }
 drawProjectile(c,p){
 if(p.type!=='pointer'||p.life<=0||!this.visible(p.x,p.y,70))return;
 const target=this.game.getEntity?.(p.targetId),angle=Number.isFinite(p.angle)?p.angle:Math.atan2((target?.y??p.y)-(p.fromY??p.y),(target?.x??p.x)-(p.fromX??p.x)),tc=teamColor(p.team);
 c.save();const x=p.x,y=p.y-14;
 line(c,[[x-Math.cos(angle)*34,y-Math.sin(angle)*34],[x-Math.cos(angle)*10,y-Math.sin(angle)*10]],tc+'66',4);
 line(c,[[x-Math.cos(angle)*25,y-Math.sin(angle)*25],[x-Math.cos(angle)*10,y-Math.sin(angle)*10]],'#fffce4cc',1.4);
 drawPointer(c,x,y,angle,tc,.8);c.restore();
 }
 drawEffect(c,fx){
 if(!this.visible(fx.x,fx.y,160))return;const t=Math.max(0,Math.min(1,1-fx.life/(fx.total||.6)));c.save();c.globalAlpha=1-t;const tc=teamColor(fx.team);
 if(fx.type==='sweep'){c.translate(fx.x,fx.y);c.rotate(fx.angle||0);c.beginPath();c.moveTo(0,0);c.arc(0,0,(fx.radius||125)*(.6+t*.4),-Math.PI*.34,Math.PI*.34);c.closePath();shape(c,'#f8d27744','#e8ac45',3);for(let i=0;i<3;i++){c.beginPath();c.arc(0,0,(fx.radius||125)*(.45+i*.2),-Math.PI*.32,Math.PI*.32);shape(c,null,'#fff4c7',2);}
 const closing=(1-t)*Math.PI*.34+.025;for(const side of [-1,1]){c.save();c.rotate(closing*side);c.beginPath();c.moveTo(-4,-4);c.lineTo((fx.radius||125)*.85,0);c.lineTo(3,8);c.closePath();shape(c,side<0?'#dfefed':'#b7d2d5',INK,1.4);c.restore();}ellipse(c,0,0,4,4,'#eab364',INK,1.2);}
 else if(fx.type==='death'){for(let i=0;i<7;i++){const a=i*Math.PI*2/7;ellipse(c,fx.x+Math.cos(a)*t*35,fx.y+Math.sin(a)*t*24,3*(1-t)+1,3*(1-t)+1,i%2?'#edbc58':tc);}}
 else if(fx.type==='hit'){line(c,[[fx.x-7-t*5,fx.y-7-t*5],[fx.x+7+t*5,fx.y+7+t*5]],'#fff8db',3);line(c,[[fx.x-7-t*5,fx.y+7+t*5],[fx.x+7+t*5,fx.y-7-t*5]],'#f3b65c',3);}
 else if(fx.type==='deposit'){c.textAlign='center';c.font='800 17px "Trebuchet MS",sans-serif';c.fillStyle=tc;c.strokeStyle='#f7f1df';c.lineWidth=3;c.strokeText('+5',fx.x,fx.y-35-t*28);c.fillText('+5',fx.x,fx.y-35-t*28);}
 else if(fx.type==='leap'){ellipse(c,fx.x,fx.y,14+t*48,7+t*24,null,'#b4a477',3);}
 else if(fx.type==='build'){for(let i=0;i<4;i++)ellipse(c,fx.x+(i-1.5)*14,fx.y-10-t*28,2.5,2.5,'#e4b34d');}
 else if(['transportLoad','transportDrop','board','drop'].includes(fx.type)){
  const loading=fx.type==='transportLoad'||fx.type==='board',radius=loading?30*(1-t)+7:12+t*36;
  ellipse(c,fx.x,fx.y+7,radius,radius*.45,tc+'12',tc,2);
  for(let i=0;i<3;i++){const yy=fx.y+12-(loading?t:1-t)*35-i*5;line(c,[[fx.x-5,yy+3],[fx.x,yy],[fx.x+5,yy+3]],tc,2);}
 }
 else if(fx.type==='click'||fx.type==='clickTick'){
  const tick=fx.type==='clickTick',radius=(tick?10:15)+t*(tick?10:23);
  ellipse(c,fx.x,fx.y-14,radius,radius*.65,null,tick?'#d98145':tc,tick?1.5:2.2);
  if(!tick)for(let i=0;i<5;i++){const a=i*Math.PI*2/5;line(c,[[fx.x+Math.cos(a)*radius*.9,fx.y-14+Math.sin(a)*radius*.65],[fx.x+Math.cos(a)*(radius+7),fx.y-14+Math.sin(a)*(radius+7)*.65]],'#edc26e',1.8);}
 }
 else if(fx.type==='command'){ellipse(c,fx.x,fx.y,10+t*25,5+t*12,null,tc,2);}
 c.restore();
 }
 drawPlacement(c,state){
 const p=state.pointer;if(!p||!state.mode)return;const mode=state.mode;
 if(mode==='mouse'){c.save();c.setLineDash([7,9]);ellipse(c,p.x,p.y,310,310,TEAL+'05',TEAL+'60',1.4/this.camera.zoom);c.restore();}
 if(['desk','cabinet','printer','cdreader','mouse'].includes(mode)){const raw=this.game.canBuild?.(mode,p.x,p.y);const valid=typeof raw==='object'?raw.ok:raw!==false;const r=mode==='desk'?64:['printer','cdreader'].includes(mode)?50:mode==='mouse'?42:39;c.save();c.globalAlpha=.55;drawIcon(c,mode,p.x,p.y,mode==='desk'?112:mode==='cdreader'?98:mode==='mouse'?80:mode==='printer'?94:74,0);ellipse(c,p.x,p.y+12,r,r*.5,valid?'#32a18c11':'#df715c22',valid?TEAL:CORAL,2);c.restore();}
 else if(mode==='ability'){const selection=(this.game.entities||[]).filter(e=>state.selected?.has(e.id));if(selection.some(e=>e.type==='scissors')){for(const e of selection.filter(e=>e.type==='scissors')){c.save();c.translate(e.x,e.y);c.rotate(Math.atan2(p.y-e.y,p.x-e.x));c.beginPath();c.moveTo(0,0);c.arc(0,0,125,-Math.PI*.34,Math.PI*.34);c.closePath();shape(c,'#eac45a20','#d1a23e99',1.5);c.restore();}}ellipse(c,p.x,p.y,24,12,'#eac45a15','#c49635',1.5);}
 else if(mode==='drop'){
  const cds=(this.game.entities||[]).filter(e=>e.type==='cd'&&state.selected?.has(e.id)&&e.hp>0);
  c.save();c.setLineDash([5,5]);ellipse(c,p.x,p.y,42+Math.sin(this.time*4)*3,24,'#269b9810',TEAL,2);
  for(const e of cds)line(c,[[e.x,e.y],[p.x,p.y]],'#269b9877',1.2);
  c.setLineDash([]);line(c,[[p.x,p.y-22],[p.x,p.y+4]],TEAL,2.5);line(c,[[p.x-7,p.y-3],[p.x,p.y+4],[p.x+7,p.y-3]],TEAL,2.5);
  for(let i=0;i<6;i++){const a=i*Math.PI/3;ellipse(c,p.x+Math.cos(a)*33,p.y+Math.sin(a)*18,3,2,TEAL+'99');}
  c.restore();
 }
 else if(mode==='attack'){line(c,[[p.x-9,p.y],[p.x+9,p.y]],CORAL,2);line(c,[[p.x,p.y-9],[p.x,p.y+9]],CORAL,2);ellipse(c,p.x,p.y,15,15,null,CORAL,1);}
 }
 drawMinimap(canvas){
 const c=canvas.getContext('2d');const rect=canvas.getBoundingClientRect();const dpr=Math.min(window.devicePixelRatio||1,2);if(canvas.width!==Math.round(rect.width*dpr)||canvas.height!==Math.round(rect.height*dpr)){canvas.width=Math.round(rect.width*dpr);canvas.height=Math.round(rect.height*dpr);}const w=rect.width,h=rect.height,g=this.game,sx=w/(g.width||2400),sy=h/(g.height||1600);c.setTransform(dpr,0,0,dpr,0,0);c.fillStyle='#ece7d5';c.fillRect(0,0,w,h);c.strokeStyle='#d7d1bb';c.lineWidth=.5;for(let x=0;x<w;x+=20){c.beginPath();c.moveTo(x,0);c.lineTo(x,h);c.stroke();}for(let y=0;y<h;y+=20){c.beginPath();c.moveTo(0,y);c.lineTo(w,y);c.stroke();}
 for(const o of g.map?.obstacles||[]){c.fillStyle='#c6c5ae';c.fillRect(o.x*sx,o.y*sy,o.w*sx,o.h*sy);}
 for(const e of g.entities||[]){if(e.hp<=0||e.transportedBy)continue;const building=['desk','printer','cabinet','cdreader','mouse'].includes(e.type);const r=building?3:e.type==='trash'?2.3:1.5;c.fillStyle=e.type==='trash'?'#94ac82':teamColor(e.team);if(building)c.fillRect(e.x*sx-r,e.y*sy-r,r*2,r*2);else if(e.type==='cd'){c.beginPath();c.moveTo(e.x*sx,e.y*sy-3);c.lineTo(e.x*sx+3,e.y*sy);c.lineTo(e.x*sx,e.y*sy+3);c.lineTo(e.x*sx-3,e.y*sy);c.closePath();c.fill();}else{c.beginPath();c.arc(e.x*sx,e.y*sy,r,0,Math.PI*2);c.fill();}}
 c.fillStyle='#ffffff15';c.strokeStyle='#667a6f';c.lineWidth=1.5;const cam=this.camera;c.fillRect(cam.x*sx,cam.y*sy,this.width/cam.zoom*sx,this.height/cam.zoom*sy);c.strokeRect(cam.x*sx,cam.y*sy,this.width/cam.zoom*sx,this.height/cam.zoom*sy);c.setTransform(1,0,0,1,0,0);
 }
}








