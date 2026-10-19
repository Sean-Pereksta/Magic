const test=require('node:test');
const assert=require('node:assert/strict');
const D=require('../js/player-development.js');

function player(ratings={},abilities={}){
  return {profile:{ratings,abilities,height:1,width:1,shot:1,layup:1,jump:1},hasBall:true,stamina:1,angle:Math.PI,
    group:{position:{x:0,y:0,z:-9.5}},velocity:{x:0,z:-4,length:()=>0},moveMeta:{}};
}
test('every shooting signature alters launch control only in its basketball situation',()=>{
  const scenarios=[
    ['midrangeAssassin',{}, {},{distance:9}],
    ['deadeye',{contest:.45},{},{contest:0}],
    ['catchAndShoot',{now:1},{lastCatchTime:.5,lastPassQuality:.9},{now:4}],
    ['pullUpSpecialist',{type:'pullup'},{},{type:'setshot'}],
    ['stepBackCreator',{type:'stepback'},{stepBackQuality:.9},{type:'setshot'}],
    ['cornerSpecialist',{distance:8,now:1},{lastCatchTime:.5,lastPassQuality:.9,group:{position:{x:7,y:0,z:-8.5}}},{distance:5.5}],
    ['deepRange',{distance:10},{},{distance:5.5}],
    ['clutchShooter',{clutch:true,timingError:.3},{},{clutch:false,timingError:.3}]
  ];
  for(const [id,options,properties,inactive] of scenarios){
    const regular=Object.assign(player(),properties),special=Object.assign(player({}, {[id]:2}),properties);
    const base=D.shotProfile(regular,options),upgraded=D.shotProfile(special,options);
    assert.ok(upgraded.active.includes(id),id+' activates');
    assert.ok(upgraded.spread<base.spread||upgraded.pressure<base.pressure||Math.abs(upgraded.timingBias)<Math.abs(base.timingBias),id+' improves the actual launch');
    assert.ok(!D.shotProfile(special,{...options,...inactive}).active.includes(id),id+' requires its situation');
  }
});
test('quick release reaches its ideal hold sooner and requires a narrower release window',()=>{
  const base={greenStart:.54,greenEnd:.73,meterFillSpeed:1};
  const regular=player(),quick=player({}, {quickRelease:2});
  const normal=D.meterProfile('setshot',regular,{value:0},base),fast=D.meterProfile('setshot',quick,{value:0},base);
  const ideal=m=>(m.greenStart+m.greenEnd)*.5*1.35/m.meterFillSpeed;
  assert.ok(ideal(fast)<ideal(normal)*.85);
  assert.ok(fast.greenEnd-fast.greenStart<normal.greenEnd-normal.greenStart);
  assert.ok(D.shotProfile(quick).releaseDuration<D.shotProfile(regular).releaseDuration);
});
test('height, jump, strength, momentum, fatigue and approach gate real dunk gathers',()=>{
  const hoop={x:0,y:3.28,z:-10.8};
  const rookie=player({dunking:45,jumping:48,strength:42});
  assert.equal(D.canDunk(rookie,hoop,null),false);
  const guard=player({dunking:85,jumping:90,strength:72,aerialControl:88,contactFinishing:80},{highFlyer:2});
  guard.profile.height=.94;
  assert.equal(D.canDunk(guard,hoop,null),true);
  guard.velocity.z=0;assert.equal(D.canDunk(guard,hoop,null),false);
  guard.velocity.z=-4;guard.angle=0;assert.equal(D.canDunk(guard,hoop,null),false);
  guard.angle=Math.PI;guard.stamina=.1;assert.equal(D.canDunk(guard,hoop,null),false);
  const center=player({dunking:78,jumping:66,strength:88,standingDunk:88,contactFinishing:85},{powerFinisher:2});
  center.profile.height=1.11;center.profile.width=1.16;center.velocity.z=0;center.group.position.z=-9.75;
  assert.equal(D.canDunk(center,hoop,null),true,'a strong tall center earns standing dunks');
});
test('signature dunk styles and contact resistance follow earned approach advantages',()=>{
  const ratings={dunking:90,jumping:90,strength:85,aerialControl:90,contactFinishing:88};
  const guard=player(ratings,{highFlyer:2});
  assert.equal(D.dunkProfile(guard,{momentum:4,angle:1}).style,'tomahawk');
  const acrobat=player(ratings,{highFlyer:2,aerialAcrobat:2});
  assert.equal(D.dunkProfile(acrobat,{momentum:4,angle:.4}).style,'reverse');
  assert.equal(D.dunkProfile(acrobat,{momentum:4,angle:.9}).style,'windmill');
  const contact=player(ratings,{contactSpecialist:2});
  assert.equal(D.dunkProfile(contact,{momentum:3,angle:1,defender:player()}).style,'contact');
  const elite=player(ratings,{posterizer:2,powerFinisher:2});
  const poster=D.dunkProfile(elite,{momentum:4,angle:1,defender:player()});
  assert.equal(poster.style,'poster');
  assert.notEqual(D.dunkProfile(elite,{momentum:.8,angle:1,defender:player()}).style,'poster');
  const normal=D.dunkProfile(player(ratings),{momentum:4,angle:1,defender:player()});
  assert.ok(poster.resistance>normal.resistance);
  assert.ok(poster.force>normal.force);
  assert.ok(poster.resistance<1,'abilities never remove defender counterplay');
});

test('qualified dunkers retain open gathers while fatigue and weak approaches still require earned reach',()=>{
  const hoop={x:0,y:3.28,z:-10.8};
  const average=player({dunking:70,jumping:75,strength:65,standingDunk:65,contactFinishing:65,aerialControl:70});
  const elite=player({dunking:92,jumping:94,strength:90,standingDunk:90,contactFinishing:92,aerialControl:92},
    {posterizer:3,powerFinisher:3,contactSpecialist:3,highFlyer:3});
  assert.equal(D.canDunk(average,hoop,null),true);assert.equal(D.canDunk(elite,hoop,null),true);
  const fresh=D.dunkProfile(elite,{momentum:4.2,angle:1});
  average.stamina=elite.stamina=.35;
  assert.equal(D.canDunk(average,hoop,null),false,'an ordinary tired gather cannot invent rim reach');
  assert.equal(D.canDunk(elite,hoop,null),true,'an elite vertical can retain an earned open gather');
  const tired=D.dunkProfile(elite,{momentum:4.2,angle:1});
  assert.ok(tired.finish<fresh.finish);assert.ok(tired.reach<fresh.reach);assert.ok(tired.resistance<1);
  elite.group.position.z=-7;assert.equal(D.canDunk(elite,hoop,null),false,'finishing abilities do not authorize a distant launch');
});
