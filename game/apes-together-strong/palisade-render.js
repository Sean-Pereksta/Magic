/* A climbing pose stays visible when the ordinary crowd uses cached sprites. */
(() => {
'use strict';
const P=ATSRenderer.prototype,ape=P.drawApe,sprite=P.drawApeSprite,geometry=P.drawPrimateGeometry;
P.drawApe=function(c,a,king,exposure){
 const p=window.ATSPalisadePose?.(a);
 if(!p||a.hp<=0)return ape.call(this,c,a,king,exposure);
 c.save();c.beginPath();c.ellipse(0,3,king?20:13,5,0,0,Math.PI*2);c.fillStyle='rgba(0,8,10,.25)';c.fill();
 c.translate(0,-p.height);
 if(!this.reducedMotion){const facing=Math.cos(a.dir||0)<0?-1:1;c.rotate(facing*(p.phase==='jump'?Math.sin(p.progress*Math.PI)*.15:Math.sin(this.time*13+(a.phase||0))*.035));if(p.phase==='crest')c.scale(1,.92)}
 ape.call(this,c,a,king,exposure);c.restore();
};
P.drawApeSprite=function(c,a){if(a.palisadeClimb){this.drawApe(c,a,false);return false}return sprite.call(this,c,a)};
P.drawPrimateGeometry=function(c,a,options={}){
 const p=window.ATSPalisadePose?.(a);
 // Reuse each species' actual arms and fur. The temporary pose exists only
 // inside geometry and cannot interfere with human-fortress siege state.
 if(p&&p.phase!=='jump')return geometry.call(this,c,{...a,moving:false,climbingVehicleId:'friendly-palisade',climbUntil:Infinity},options);
 return geometry.call(this,c,a,options);
};
})();
