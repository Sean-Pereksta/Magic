/* Royal settlement silhouettes use the existing bounded sprite caches. */
(function(){
'use strict';
const P=ATSRenderer.prototype,baseGeometry=P.drawVillageGeometry,baseLodge=P.drawSettlement,baseHut=P.drawHut,baseConstruction=P.drawConstruction;
const line=(c,x,y,a,b,color,width=2)=>{c.beginPath();c.moveTo(x,y);c.lineTo(a,b);c.strokeStyle=color;c.lineWidth=width;c.stroke()},poly=(c,points,color)=>{c.beginPath();points.forEach((p,i)=>i?c.lineTo(...p):c.moveTo(...p));c.closePath();c.fillStyle=color;c.fill()},ellipse=(c,x,y,rx,ry,color)=>{c.beginPath();c.ellipse(x,y,rx,ry,0,0,Math.PI*2);c.fillStyle=color;c.fill()};
function banner(c,x,y,color,warlord=false){line(c,x,y+21,x,y-28,'#c6ad77',3);poly(c,[[x,y-28],[x+22,y-24],[x+19,y-6],[x+10,y-12],[x,y-9]],color);poly(c,[[x+5,y-22],[x+9,y-17],[x+12,y-23],[x+15,y-17],[x+18,y-22],[x+17,y-13],[x+6,y-14]],warlord?'#edcf7b':'#ead8a0')}
P.drawVillageGeometry=function(c,kind,variant,ruin){
 if(ruin||!['spearBattery','spearBallista','nursery','rallyGrove','orchard'].includes(kind))return baseGeometry.call(this,c,kind,variant,ruin);
 ellipse(c,0,5,42,17,'rgba(0,12,10,.3)');
 if(kind==='spearBattery'){
  for(const x of[-26,26]){line(c,x,2,x,-73,'#95784e',7);line(c,x,2,-x,-69,'#5b6548',4)}
  poly(c,[[-39,-66],[0,-85],[39,-66],[0,-46]],'#b09a68');poly(c,[[-39,-66],[0,-46],[0,-37],[-39,-58]],'#6e6244');
  for(const x of[-28,28])line(c,x,-60,x,-91,'#cfb786',4);line(c,-29,-78,29,-78,'#b99c64',4);
  for(const side of[-1,1])for(let i=0;i<4;i++){const x=side*17+i*3;line(c,x,-59,x+9,-97,'#bca26c',2);poly(c,[[x+10,-103],[x+6,-94],[x+13,-94]],'#e2ddae')}
  for(let i=0;i<6;i++)line(c,-5,-i*10,5,-i*10,'#c0aa77',2);banner(c,32,-87,'#5a9275');return;
 }
 if(kind==='spearBallista'){
  for(const x of[-29,29]){line(c,x,3,x,-27,'#826947',8);ellipse(c,x,4,9,13,'#635d43');ellipse(c,x,4,4,7,'#b19a6b')}
  poly(c,[[-37,-19],[0,-37],[37,-19],[0,0]],'#ac9260');line(c,-29,-17,29,-38,'#685c41',10);line(c,-28,-21,28,-42,'#bca878',3);
  line(c,-7,-59,-24,-45,'#b99b65',5);line(c,-24,-45,-39,-45,'#b99b65',5);line(c,-7,-59,18,-58,'#b99b65',5);line(c,18,-58,34,-69,'#b99b65',5);line(c,-39,-45,7,-32,'#d1c491',1.5);line(c,7,-32,34,-69,'#d1c491',1.5);
  line(c,-28,-19,30,-70,'#ddc28a',4);poly(c,[[38,-77],[24,-69],[31,-65]],'#dfdfbd');banner(c,-37,-39,'#9b533f',true);return;
 }
 if(kind==='orchard'){
  poly(c,[[-49,2],[0,-25],[48,2],[0,26]],'#605c37');
  for(const [x,y]of[[-27,2],[0,-12],[27,2],[0,16]]){line(c,x,y,x,y-29,'#a08853',5);ellipse(c,x,y-34,19,14,'#557d42');ellipse(c,x-7,y-39,11,10,'#829b50');for(let i=0;i<3;i++)ellipse(c,x-9+i*8,y-32+(i%2)*5,3.5,4,'#c4a853')}
  line(c,-44,13,1,35,'#b49e67',3);line(c,1,35,45,11,'#b49e67',3);return;
 }
 if(kind==='nursery'){
  for(const x of[-31,31])line(c,x,4,x,-49,'#b19b6a',4);poly(c,[[-42,-48],[0,-70],[42,-48],[0,-24]],'#779a58');poly(c,[[-42,-48],[0,-70],[0,-24]],'#a4b976');
  for(const [x,y]of[[-18,1],[10,9]]){poly(c,[[x-11,y-12],[x+13,y-7],[x+7,y+2],[x-8,y-2]],'#b69763');line(c,x-13,y-13,x-13,y-26,'#c0ac7a',2);line(c,x+14,y-6,x+14,y-21,'#c0ac7a',2);ellipse(c,x,y-8,5,4,'#e3ce9c');ellipse(c,x,y-11,3,3,'#715c3f')}
  banner(c,37,-35,'#80a98b');return;
 }
 ellipse(c,0,3,43,23,'#897e4c');ellipse(c,0,3,31,16,'#c0a76c');
 for(const x of[-35,35]){line(c,x,1,x,-59,'#9b8055',7);poly(c,[[x-10,-60],[x,-78],[x+10,-60]],'#c2b37a')}
 line(c,-36,-56,36,-56,'#a88e5d',6);banner(c,-1,-52,'#a15a3c',true);
 for(const x of[-20,21]){ellipse(c,x,4,12,6,'#624d32');poly(c,[[x-12,4],[x-10,-13],[x+10,-13],[x+12,4]],'#9d7349');ellipse(c,x,-13,10,5,'#dfc58a');line(c,x-8,-10,x+7,3,'#d2b780',1.5)}
};
P.drawConstruction=function(c,p){if(p.kind==='spearBattery'||p.kind==='spearBallista')return baseConstruction.call(this,c,{...p,kind:'spearTower'});if(/Expansion$/.test(p.kind)){c.save();c.scale(1.7,1.3);baseConstruction.call(this,c,p);c.restore();return}return baseConstruction.call(this,c,p)};
P.drawHut=function(c,h){const large=h.kind==='longhouse'||h.kind==='canopyHut';if(!large)return baseHut.call(this,c,h);const war=h.kind==='canopyHut';c.save();c.scale(war?1.65:1.35,war?1.16:1);baseHut.call(this,c,h);c.restore();if(h.hp>0&&(h.stage??4)===4){line(c,-27,-48,27,-28,'#d0b77f',3);if(war){poly(c,[[-17,-57],[8,-70],[35,-51],[10,-36]],'#8c9b57');line(c,8,-70,8,-48,'#d5c78b',2)}banner(c,war?42:33,-30,war?'#9b583d':'#578a68',war)}};
P.drawSettlement=function(c,s){const tier=s.expansionLevel||0;if(!tier)return baseLodge.call(this,c,s);c.save();c.scale(tier===2?1.2:1.08,tier===2?1.15:1.05);baseLodge.call(this,c,s);c.restore();
 for(const side of[-1,1]){const x=side*(tier===2?82:64);poly(c,[[x-29,1],[x,-15],[x+30,1],[x+30,-26],[x,-45],[x-29,-29]],'#89734d');poly(c,[[x-35,-30],[x,-60],[x+35,-30],[x,-11]],tier===2?'#879450':'#809961');line(c,x,-57,x,-19,'#c6ba81',2);banner(c,x+side*25,-43,tier===2?'#9b553c':'#4e8870',tier===2)}
 ellipse(c,0,14,tier===2?75:58,11,'rgba(185,148,73,.14)');poly(c,[[-15,-22],[-15,-34],[-8,-28],[0,-41],[8,-28],[15,-34],[15,-22]],'#e7c875');line(c,-13,-19,13,-19,'#977c43',3);
};
})();
