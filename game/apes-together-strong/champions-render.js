/* Champion overlays stay outside the shared species atlases. At most 24 living
   champions use richer geometry; ordinary crowds keep their cached sprites. */
(() => {
'use strict';
const P=ATSRenderer.prototype,ape=P.drawApe,sprite=P.drawApeSprite,map=P.drawMap;
const line=(c,x,y,u,v,color,width=2)=>{c.beginPath();c.moveTo(x,y);c.lineTo(u,v);c.strokeStyle=color;c.lineWidth=width;c.lineCap='round';c.stroke()},ellipse=(c,x,y,w,h,color)=>{c.beginPath();c.ellipse(x,y,w,h,0,0,Math.PI*2);c.fillStyle=color;c.fill()};
// An authored cloth illustration, shared by team color rather than by actor.
// The standard is worn on the back: front-facing bodies occlude its mounting,
// rear-facing bodies show the pole and harness over their full armor.
const standards=new Map();
function standardImage(color){
 if(standards.has(color))return standards.get(color);
 const canvas=typeof OffscreenCanvas==='function'?new OffscreenCanvas(140,210):typeof document!=='undefined'?document.createElement('canvas'):null;if(!canvas)return null;
 canvas.width=140;canvas.height=210;canvas.atsChampionStandard=true;const c=canvas.getContext('2d');c.scale(2,2);
 line(c,7,100,7,6,'#23352f',4);line(c,6.4,99,6.4,6,'#a98a52',2.5);line(c,5.8,96,5.8,8,'#e1c88b',.7);
 ellipse(c,7,5,2.5,3.8,'#e7cd8e');
 c.beginPath();c.moveTo(9,9);c.bezierCurveTo(25,5,40,15,61,11);c.lineTo(53,27);c.lineTo(59,47);c.bezierCurveTo(39,51,24,39,9,44);c.closePath();c.fillStyle='#293c36';c.fill();c.strokeStyle='#d3b36f';c.lineWidth=2;c.stroke();
 c.beginPath();c.moveTo(11,12);c.bezierCurveTo(28,9,41,18,57,14);c.lineTo(49,28);c.lineTo(55,43);c.bezierCurveTo(37,46,24,37,11,40);c.closePath();c.fillStyle=color;c.fill();
 // Painted folds and frayed lower stitches keep the small flag readable as cloth.
 line(c,15,13,15,39,'rgba(255,241,192,.28)',1.5);line(c,46,17,43,40,'rgba(31,42,29,.28)',3);
 for(let i=0;i<7;i++)line(c,14+i*5,40+(i>3?3:0),15+i*5,42+(i>3?3:0),'#e6cf94',.55);
 // Drawn ape fist: four knuckles, folded thumb and a broad armored wrist.
 c.save();c.translate(30,26);c.rotate(.1);c.beginPath();c.moveTo(-6,10);c.lineTo(-7,4);c.quadraticCurveTo(-12,0,-9,-4);c.quadraticCurveTo(-7,-6,-5,-3);c.lineTo(-5,-9);c.quadraticCurveTo(-3,-12,-1,-9);c.quadraticCurveTo(1,-12,3,-8);c.quadraticCurveTo(6,-10,7,-6);c.quadraticCurveTo(11,-7,11,-3);c.lineTo(9,5);c.lineTo(5,10);c.closePath();c.fillStyle='#f1dfae';c.fill();c.strokeStyle='#4a4b34';c.lineWidth=1.3;c.stroke();
 for(const x of[-1,3,7])line(c,x,-7,x,-1,'#6c6443',.8);line(c,-7,0,-1,4,'#6c6443',1);line(c,-5,9,5,9,'#a78248',2.3);c.restore();
 // Leather ties at the back mounting, also visible from the rear.
 for(const y of[83,91]){line(c,3,y,11,y,'#322e23',3);line(c,3,y-.4,11,y-.4,'#b99b66',1)}
 if(standards.size>=8)standards.delete(standards.keys().next().value);standards.set(color,canvas);return canvas;
}
function standardPlacement(a,p={},slot=76){
 if(!a.champion?.prisonChampion||a._atlas||a.hp<=0||p.king||p.human||p.state==='death')return null;
 const scale=Math.max(.72,Math.min(1.08,slot/76)),back=window.ATSHeldItemArt?.sockets(a,p,slot)?.back||{x:-11*scale,y:-33*scale};
 return {layer:p.rear?'front':'rear',x:back.x-7*scale,y:back.y-100*scale,width:70*scale,height:105*scale,rotation:p.state==='climb'?.08:0};
}
P.drawChampionStandard=function(c,a,p,slot,layer){
 const at=standardPlacement(a,p,slot);if(!at||at.layer!==layer)return false;
 const color=window.ATSDivisions?.find(d=>d.id===a.divisionId)?.color||'#d2a85d',image=standardImage(color);if(!image)return false;
 c.save();if(at.rotation){c.translate(at.x+at.width*.1,at.y+at.height*.95);c.rotate(at.rotation);c.translate(-at.x-at.width*.1,-at.y-at.height*.95)}c.drawImage(image,at.x,at.y,at.width,at.height);c.restore();this.championStandardDraws=(this.championStandardDraws||0)+1;return true;
};
window.ATSChampionArt={standardPlacement,standardImage,status:()=>({sharedStandards:standards.size,cacheLimit:8})};
P.drawChampionGear=function(c,a){if(!a.champion||a._atlas)return;const t=a.champion.archetype,flip=Math.cos(a.dir||0)<0?-1:1,near=this.camera.zoom>.65&&this.detailLevel<3;const division=ATSDivisions.find(d=>d.id===a.divisionId),color=division?.color||'#e9c56d';c.save();c.translate(0,-(a.elevation||a.wallClimbHeight||0));c.strokeStyle=color;c.lineWidth=1.7;c.beginPath();c.ellipse(0,5,20,7,0,0,Math.PI*2);c.stroke();if(near){c.scale(flip,1);const sway=this.reducedMotion?0:a.moving?Math.sin(this.time*7+(a.phase||0))*2:0;c.translate(0,sway);line(c,-14,-40,14,-20,'#75664e',5);line(c,-14,-41,14,-21,'#bbaa77',1.4);if(a.species==='gorilla'){ellipse(c,-13,-43,10,5,'#83938c');line(c,-9,-48,-2,-40,'#c5a38a',1.5);if(t==='wallbreaker'){line(c,15,-11,25,-49,'#8c7350',4);line(c,16,-47,34,-42,'#a6afa1',10)}else if(t==='tankBuster'){for(let n=0;n<3;n++)line(c,18,-23+n*3,24,-25+n*3,'#bfc7b9',2)}else if(t==='bulwark'&&!a.shield?.hp){line(c,-20,-12,19,-23,'#675543',10);line(c,-18,-16,16,-26,'#ada67e',3)}}else if(a.species==='orangutan'){ellipse(c,-16,-29,8,10,'#897450');line(c,-21,-30,-12,-26,'#d6bc82',2);line(c,10,-37,17,-16,'#778e82',4);line(c,9,-37,20,-33,'#b4c0a6',4)}else if(a.species==='chimpanzee'){line(c,-13,-21,13,-20,'#9c8860',5);for(let i=0;i<3;i++)line(c,-8+i*7,-25,-8+i*7,-15,'#87958c',3);if(t==='gateRunner')ellipse(c,16,-26,7,9,'#82765c')}else if(a.species==='gibbon'){line(c,-15,-39,16,-13,'#9eaa81',3);line(c,17,-21,21,-35,'#b7bfa8',2);line(c,21,-35,27,-33,'#b7bfa8',3);line(c,-7,-41,-16,-55,color,3)}else if(a.species==='mandrill'){line(c,-15,-45,15,-27,color,6);if(t==='warcaller'){ellipse(c,17,-19,9,6,'#b99859');line(c,10,-19,10,-29,'#ad9f71',3);ellipse(c,17,-29,9,4,'#dfca8b')}else{line(c,19,0,24,-55,'#8b7850',3);ellipse(c,24,-55,4,6,color)}}else{ellipse(c,15,-22,7,8,'#8d7e5b');line(c,-6,-27,15,-15,'#b6aa82',2);if(t==='alarmSpoiler'){line(c,18,-23,23,-37,'#bac6b6',1.5);ellipse(c,15,-21,2,2,'#8cd7c0')}}c.scale(flip,1);if(a.recoveryRemaining){line(c,-7,-48,4,-40,'#d9c8a3',4);line(c,1,-48,-5,-39,'#d9c8a3',3)}}const y=near?-84:-58;c.fillStyle=color;c.beginPath();c.moveTo(0,y-5);c.lineTo(4,y);c.lineTo(0,y+5);c.lineTo(-4,y);c.closePath();c.fill();c.restore()};
P.drawApe=function(c,a,king,exposure){c.save();if(a.champion&&!king){const scale=a.species==='gorilla'?1.1:1.05;c.scale(scale,scale);if(a.recoveryRemaining&&a.moving&&!this.reducedMotion)c.rotate(Math.sin(this.time*5)*.035)}ape.call(this,c,a,king,exposure);if(!king)this.drawChampionGear(c,a);c.restore()};
P.drawApeSprite=function(c,a){if(a.champion){this.drawApe(c,a,false);return false}return sprite.call(this,c,a)};
function viewport(game,canvas){const rect=canvas.getBoundingClientRect(),w=rect.width||640,h=rect.height||420,cell=game.world.revealCellSize||192;let minX=game.king.x/cell-4,maxX=game.king.x/cell+4,minY=game.king.y/cell-4,maxY=game.king.y/cell+4;for(const key of game.world.discovered||[]){const q=key.split(',').map(Number);if(!Number.isFinite(q[0])||!Number.isFinite(q[1]))continue;minX=Math.min(minX,q[0]);maxX=Math.max(maxX,q[0]+1);minY=Math.min(minY,q[1]);maxY=Math.max(maxY,q[1]+1)}const scale=Math.min((w-56)/((maxX-minX)*cell),(h-60)/((maxY-minY)*cell));return{scale,ox:(w-(maxX-minX)*cell*scale)/2-minX*cell*scale,oy:(h-(maxY-minY)*cell*scale)/2-minY*cell*scale}}
P.drawMap=function(game,canvas){map.call(this,game,canvas);const c=canvas.getContext('2d'),v=viewport(game,canvas),p=a=>({x:v.ox+a.x*v.scale,y:v.oy+a.y*v.scale});c.save();for(const d of Object.values(game.champions.divisions)){const commander=game.apesById.get(d.commanderId);if(!commander?.hp)continue;const a=p(commander),b=p(game.champions.anchor(d)),color=ATSDivisions.find(t=>t.id===d.id)?.color||'#e9c56d';c.globalAlpha=d.suspended?.4:.9;c.setLineDash([3,4]);line(c,a.x,a.y,b.x,b.y,color,1.3);c.setLineDash([]);ellipse(c,a.x,a.y,4,4,color);c.strokeStyle=color;c.strokeRect(b.x-5,b.y-5,10,10);c.font='bold 9px system-ui';c.fillStyle=color;c.textAlign='left';c.fillText(d.name,a.x+7,a.y-5)}c.restore()};
window.ATSChampionMapView=viewport;
})();
