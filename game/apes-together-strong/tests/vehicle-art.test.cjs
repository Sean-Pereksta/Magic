'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm');
const root=path.resolve(__dirname,'..'),manifest=JSON.parse(fs.readFileSync(path.join(root,'assets/visual/vehicles/manifest.json')));
function load(){
 const draws=[],fallbacks=[],ctx=new Proxy({globalAlpha:1,drawImage(...args){draws.push(args)},createRadialGradient(){return{addColorStop(){}}}}, {get:(o,k)=>k in o?o[k]:()=>{}});
 class Renderer{constructor(){this.ctx=ctx;this.camera={zoom:1};this.time=4;this.detailLevel=0;this.quality='high';this.vehicleEffectBudget=30;this.vehicleLampBudget=10;this.vehicleTextureBudget=4}draw(){}drawVehicle(){fallbacks.push('vehicle')}drawHeli(){fallbacks.push('heli')}drawObject(){fallbacks.push('object')}drawCorpses(g){fallbacks.push(g.corpses.slice())}health(){}glow(){}visible(){return true}project(x,y,z=0){return{x,y:y-z}}renderPoint(a,z=0){return this.project(a.x,a.y,z)}}
 const images=Object.fromEntries(manifest.atlases.map(a=>[a.id,{width:a.width,height:a.height,id:a.id}]));
 const c=vm.createContext({ATSRenderer:Renderer,document:{createElement(){return {width:0,height:0,getContext:()=>ctx}}}});c.window=c;c.ATSVisualAssets={manifest:{vehicles:manifest},get:id=>images[id]};
 vm.runInContext(fs.readFileSync(path.join(root,'vehicle-art.js'),'utf8'),c);return{api:c.ATSVehicleArt,Renderer,draws,fallbacks,ctx,images};
}
function worldHeading(sector){const sx=Math.cos(sector*Math.PI/4)/.8,sy=Math.sin(sector*Math.PI/4)/.42;return Math.atan2((sy-sx)/2,(sy+sx)/2)}
test('all current vehicle, specialist turret and helicopter classes have eight valid authored facing views',()=>{
 const{api}=load();
 for(const kind of manifest.classes)for(let sector=0;sector<8;sector++){
  const a={kind,dir:worldHeading(sector),hp:100,maxHp:100},p=api.resolve(a,3);assert.equal(p.direction.sector,sector);assert.ok(manifest.frames[p.frame],p.frame);
  if(p.turret)assert.ok(manifest.frames[p.turret]);
 }
 for(const kind of manifest.helicopters)for(let sector=0;sector<8;sector++)assert.ok(manifest.frames[api.resolve({id:'heli-1',kind,dir:worldHeading(sector),hp:100},3).frame]);
 for(const variant of ['repeater','bombard','cyclone'])for(let sector=0;sector<8;sector++){
  const p=api.resolve({kind:'tank',variant,dir:0,turretDir:worldHeading(sector),hp:100},3);assert.ok(manifest.frames[p.turret]);assert.equal(p.turretDirection.sector,sector);
 }
});
test('rotors animate while stationary, on reduced motion and Low; grounded blades stop',()=>{
 const{api}=load(),h={id:'heli-1',kind:'gunship',hp:300,maxHp:360,phase:4,moving:false};
 for(const reducedMotion of [false,true]){const a=api.resolve(h,3,{reducedMotion}),b=api.resolve(h,3.17,{reducedMotion});assert.notEqual(a.rotorFrame,b.rotorFrame);assert.notEqual(a.tailFrame,b.tailFrame)}
 const a=api.resolve({...h,grounded:true},3),b=api.resolve({...h,grounded:true},4);assert.equal(a.rotorFrame,b.rotorFrame);assert.equal(a.tailFrame,b.tailFrame);assert.equal(a.rotorSpeed,0);
 const start=api.resolve({...h,phase:0},0),flying=api.resolve({...h,phase:4},4);assert.ok(start.altitude<flying.altitude);assert.ok(start.rotorSpeed<flying.rotorSpeed);
});
test('damage and independent turret resolution read existing state without altering it',()=>{
 const{api}=load();for(const [a,state]of [[{hp:100},'operational'],[{hp:60},'light'],[{hp:20},'heavy'],[{hp:90,engineDamage:100},'disabled'],[{hp:0},'destroyed']])assert.equal(api.damageState({...a,maxHp:100}),state);
 const actor=Object.freeze({id:'vehicle-1',kind:'tank',variant:'sentinel',dir:0,turretDir:Math.PI,hp:80,maxHp:100,radius:31,moving:true,reversing:true});
 const before=JSON.stringify(actor),p=api.resolve(actor,10);assert.notEqual(p.direction.sector,p.turretDirection.sector);assert.equal(JSON.stringify(actor),before);assert.equal(actor.radius,31);
});
test('every crop is within decoded RGBA atlases; all expected original files are embedded-ready',()=>{
 assert.equal(Object.keys(manifest.frames).length,85);
 for(const atlas of manifest.atlases){const b=fs.readFileSync(path.join(root,'assets/visual',atlas.sourceFile||atlas.file));assert.equal(b.readUInt32BE(16),atlas.width);assert.equal(b.readUInt32BE(20),atlas.height);assert.equal(b[25],6,'original alpha channel retained');assert.ok(fs.existsSync(path.join(root,'assets/visual',atlas.file)))}
 for(const [name,f]of Object.entries(manifest.frames)){const a=manifest.atlases.find(a=>a.id===f.atlas),[x,y,w,h]=f.rect;assert.ok(x>=0&&y>=0&&w>0&&h>0&&x+w<=a.width&&y+h<=a.height,name);assert.ok(f.anchor.every(Number.isFinite))}
});
test('live rendering uses actual atlas pixels for each class and gracefully falls back for missing art',()=>{
 const{Renderer,ctx,draws,fallbacks,images}=load(),r=new Renderer();
 for(const kind of manifest.classes)r.drawVehicle(ctx,{id:'vehicle-'+kind,kind,x:0,y:0,dir:0,hp:100,maxHp:100});
 for(const kind of manifest.helicopters)r.drawHeli(ctx,{id:'heli-'+kind,kind,x:0,y:0,dir:0,hp:100,maxHp:100,phase:4});
 assert.ok(draws.some(d=>d[0].id==='vehicles-light'));assert.ok(draws.some(d=>d[0].id==='vehicles-heavy'));assert.ok(draws.some(d=>d[0].id==='vehicles-turrets'));assert.ok(draws.some(d=>d[0].id==='vehicles-aircraft'));assert.equal(fallbacks.length,0);
 delete images['vehicles-light'];r.drawVehicle(ctx,{kind:'jeep',hp:100});assert.equal(fallbacks.at(-1),'vehicle');
 delete images['vehicles-turrets'];r.drawVehicle(ctx,{kind:'tank',hp:100});assert.equal(fallbacks.at(-1),'vehicle','missing turret falls back to a complete vehicle');
});
test('rotor caches are bounded and surviving world wrecks replace corpse duplicates without mutating simulation',()=>{
 const{Renderer,ctx,fallbacks}=load(),r=new Renderer(),h={id:'heli-1',kind:'scout',x:0,y:0,dir:0,hp:100,phase:4};
 for(let i=0;i<500;i++){r.time=i/60;r.drawHeli(ctx,h)}assert.ok(r.vehicleRotors.size<=32);
 const vehicle={id:'vehicle-1',type:'vehicle',kind:'truck',x:0,y:0,hp:0,age:2,life:20},heli={id:'heli-2',type:'ape',kind:'recon',x:0,y:0,hp:0,age:3,life:20},human={id:'human-1',type:'human'},game={world:{objects:new Map([['vehicle-1:wreck',{}]])},corpses:[vehicle,heli,human]};
 const before=JSON.stringify(game.corpses);r.drawCorpses(game);assert.equal(JSON.stringify(game.corpses),before);assert.deepEqual(Array.from(fallbacks.at(-1)),[human]);
});
